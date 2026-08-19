# Data analysis libraries (inherited from the project template)
import pandas as pd
import numpy as np
import random

# HTTP download of the sitemap
import requests

# Parsing the sitemap XML (sitemap.org + Google News namespaces)
import xml.etree.ElementTree as ET

# Dates: recency filtering and pipeline timestamps
from datetime import timezone, datetime, timedelta

# Persistence for the extracted news
import sqlite3

# Scraping the article body. Using the ASYNC API (async_api) because
# Jupyter already runs its own asyncio event loop, and Playwright's
# sync API (sync_playwright) is incompatible with that inside a notebook
# (it works fine in a plain terminal script, just not here).
import playwright
from playwright.async_api import async_playwright

# Local LLM for summarization: tokenizer + causal language model, run on CPU.
from transformers import AutoTokenizer, AutoModelForCausalLM
import torch

# PDF report generation with ReportLab's high-level "Platypus" API:
# a list of flowables (Paragraph, Spacer, PageBreak) gets laid out and
# paginated automatically by SimpleDocTemplate, no manual coordinates needed.
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, PageBreak
import pymupdf
from IPython.display import Image
from prefect import flow, task
import asyncio

SEED = 123
np.random.seed(SEED)
random.seed(SEED)

@task
def get_sitemaps():
    x = requests.get('https://www.rtve.es/sitemaps/sitemaps-news.xml', headers={"User-Agent":"AutomatedNewsReportBot/1.0 (https://github.com/AlejandroBeldaFernandez/Automated-News-Report)"})
    print(x.status_code)
    dicc = {"sitemaps": "http://www.sitemaps.org/schemas/sitemap/0.9", "labels": "http://www.google.com/schemas/sitemap-news/0.9"}
    root = ET.fromstring(x.text)
    urls = root.findall('sitemaps:url', dicc)
    return urls, dicc

@task 
def buiding_list(urls, dicc):
    news = []
    for item in urls:
        url = item.find("sitemaps:loc", dicc).text
        title = item.find("labels:news/labels:title", dicc).text
        published_at = item.find("labels:news/labels:publication_date", dicc).text
        publishet_at_correct = datetime.fromisoformat(published_at)
        actual_date = datetime.now(timezone.utc)

        # Recency filter: skip anything older than 48h (see markdown above,
        # the sitemap itself is not pre-filtered by RTVE).
        if (actual_date -  publishet_at_correct) > timedelta(hours=48):
            continue

        # section (e.g. "noticias", "deportes") is the 4th path segment.
        url_splitted = url.split("/")
        section = url_splitted[3]

        # rtve_id is the numeric id at the end of the URL, before ".shtml".
        # Defensive: don't crash on a URL that doesn't follow this pattern,
        # just flag it and store None.
        rtve_id = url_splitted[-1].split(".")[0]
        if rtve_id.isdigit():
            rtve_id = int(rtve_id)
        else:
            rtve_id = None
            print("Not Id in url: ", url)

        element = {"url": url, "title": title, "published_at": published_at, "section": section, "rtve_id": rtve_id, "discovered_at": actual_date.isoformat()}
        news.append(element)
    return news

@task
def create_database(news):
    conn = sqlite3.connect("news.db")
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS News (Url varchar(255) PRIMARY KEY NOT NULL, Rtve_id INTEGER , Title varchar(255) NOT NULL, Section varchar(255), Published_at varchar(255) NOT NULL, Discovered_at varchar(255) NOT NULL, Body varchar(255), Short_Description varchar(255), Scrapped_at varchar(255), Source varchar NOT NULL DEFAULT 'RTVE')")
    tuples = []
    for new in news:
        tuples.append((new['url'], new['rtve_id'], new['title'], new['section'], new['published_at'], new['discovered_at'], None, None, None, 'RTVE'))
    cur.executemany("INSERT OR IGNORE INTO News VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", tuples)
    conn.commit()
    conn.close()
    
@task
async def scrapping_article_body(news):
    conn = sqlite3.connect("news.db")
    cur = conn.cursor()
    async with async_playwright() as p:
        browser = await p.chromium.launch()

        for item in news:
            page = await browser.new_page()
            try:
                await page.goto(item["url"], timeout=30000)
                body = await page.inner_text(".artBody")
                short_description = await page.get_attribute('meta[name="description"]', "content")
            except Exception as e:
                # Defensive: one bad article must not stop the whole batch.
                print("Fail in", item["url"], ":", e)
                body = None
                short_description = None
            await page.close()

            scraped_at = datetime.now(timezone.utc).isoformat()
            cur.execute(
                "UPDATE News SET Body = ?, Short_Description = ?, Scrapped_at = ? WHERE Url = ?",
                (body, short_description, scraped_at, item["url"])
            )
        
        await browser.close()

    conn.commit()
    cur.execute("SELECT COUNT(*), COUNT(Body), COUNT(Short_Description) FROM News")
    total, with_body, with_short_description = cur.fetchone()
    print(f"Total news rows: {total}")
    print(f"Rows with Body scraped: {with_body}")
    print(f"Rows with Short_Description scraped: {with_short_description}")

    cur.execute("SELECT Section, COUNT(*) FROM News GROUP BY Section ORDER BY COUNT(*) DESC")
    print("\nBy section:")
    for section, count in cur.fetchall():
        print(f"  {section}: {count}")
    conn.close()

@task
def relevant_news():
    conn = sqlite3.connect("news.db")
    cur = conn.cursor()
    cur.execute("SELECT Title, Body, Url, Short_Description, Published_at, Source FROM News WHERE Section='noticias' ORDER BY Published_at DESC LIMIT 10")
    news = cur.fetchall()
    conn.close()
    return news

@task 
def text_model(news):
    def build_prompt(title, body, short_description):
        context = "\n".join([title, body, short_description])
        return f"""Eres un experto en resumir noticias. Genera un texto resumen acerca de las noticias que te pasamos sin incluir informacion externa

        Reglas:
        - No uses conocimiento externo ni supongas nada que no esté en las noticias.
        - Responde en un maximo de 100 palabras


        Noticia:
        {context}


        Respuesta:"""
    
    def answer(title, body, short_description):
        # Qwen is a chat/instruct model: apply_chat_template wraps the prompt
        # in the system/user/assistant format it was fine-tuned on.
        messages = [{'role': 'user', 'content': build_prompt(title, body, short_description)}]
        text = llm_tok.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)
        inputs = llm_tok(text, return_tensors='pt')

        # do_sample=False: greedy decoding, deterministic output for the same input.
        with torch.no_grad():
            out = llm.generate(**inputs, max_new_tokens=800, do_sample=False)

        # Slice off the input prompt tokens (inputs['input_ids'].shape[1]) before
        # decoding, otherwise the result includes the whole system/user/assistant
        # prompt text instead of just the model's new answer.
        return llm_tok.decode(out[0][inputs['input_ids'].shape[1]:], skip_special_tokens=True)
    
    LLM = 'Qwen/Qwen2.5-1.5B-Instruct'
    llm_tok = AutoTokenizer.from_pretrained(LLM)
    llm = AutoModelForCausalLM.from_pretrained(LLM, torch_dtype=torch.float32)
    llm.eval()
    reports_entry = []
    for new in news:
        summary = answer(new[0], new[1], new[3])
        report_entry = {"title": new[0], "summary": summary, "source": new[-1], "url": new[2]}
        reports_entry.append(report_entry)
    return reports_entry

@task 
def report(reports_entry):
    doc = SimpleDocTemplate("report.pdf", pagesize=A4)
    styles = getSampleStyleSheet()
    title = Paragraph("Report of RTVE news", styles["Title"])
    story = [title, Spacer(1, 20), PageBreak()]
    for report_entry in reports_entry:
        story.append(Paragraph(report_entry["title"], styles["Heading2"]))
        story.append(Paragraph(report_entry["summary"], styles["Normal"]))
        cite = f'Source: {report_entry["source"]} — <a href="{report_entry["url"]}">{report_entry["url"]}</a>'
        story.append(Paragraph(cite, styles["Normal"]))
        story.append(Spacer(1, 20))
    doc.build(story)
    
    
@flow
async def main():
    urls, dicc = get_sitemaps()
    news   = buiding_list(urls, dicc)
    create_database(news)
    await scrapping_article_body(news)
    news_relevant = relevant_news()
    reports_entry = text_model(news_relevant)
    report(reports_entry)

if __name__ == "__main__":
    asyncio.run(main())
