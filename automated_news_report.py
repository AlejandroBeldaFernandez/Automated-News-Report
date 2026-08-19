"""Automated News Report Generator.

Prefect pipeline (see the notebook of the same name for the exploratory,
step-by-step version this was derived from) that downloads RTVE's live
Google News sitemap, scrapes the selected articles' body/short
description/category with Playwright, summarizes them in Spanish with a
local LLM, and builds a daily PDF report.

Source and credit: all news content comes from RTVE (Radiotelevision
Espanola), the Spanish public broadcaster. This project is strictly
non-commercial and never redistributes the full article text publicly;
the PDF only ever shows title, category, an AI-generated summary, and a
citation (source + link) back to the original RTVE article, built in
plain code from the database, never left for the model to write from
memory. For verified, complete information, always refer to RTVE
directly (rtve.es).

Source design decision: the classic RTVE RSS feed (via feedparser) turned
out to redirect to a stale, robots.txt-blocked host. This pipeline uses
RTVE's Google News sitemap instead (explicitly allowed by robots.txt,
live, links directly to each article).

number_news, hours_limit and category are parameters of main() below,
meant to be wired to user-facing inputs (sliders / a category dropdown)
in an interactive dashboard.
"""

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
from prefect import flow, task
import asyncio
import json

SEED = 123
np.random.seed(SEED)
random.seed(SEED)

@task
def get_sitemaps():
    """Download RTVE's live Google News sitemap with a self-identifying
    User-Agent (RTVE's robots.txt blocks the Python-urllib default) and
    parse it into a list of <url> elements plus the namespace map needed
    to query them (sitemap.org for loc/url, Google News extension for
    news:title/news:publication_date).
    """
    x = requests.get('https://www.rtve.es/sitemaps/sitemaps-news.xml', headers={"User-Agent":"AutomatedNewsReportBot/1.0 (https://github.com/AlejandroBeldaFernandez/Automated-News-Report)"})
    print(x.status_code)
    dicc = {"sitemaps": "http://www.sitemaps.org/schemas/sitemap/0.9", "labels": "http://www.google.com/schemas/sitemap-news/0.9"}
    root = ET.fromstring(x.text)
    urls = root.findall('sitemaps:url', dicc)
    return urls, dicc

@task
def buiding_list(urls, dicc, hours_limit):
    """Parse the sitemap entries and keep only the ones published within
    hours_limit hours (RTVE's sitemap mixes articles from many years, it is
    not pre-filtered by recency, so this filter is required, not optional).
    """
    news = []
    for item in urls:
        url = item.find("sitemaps:loc", dicc).text
        title = item.find("labels:news/labels:title", dicc).text
        published_at = item.find("labels:news/labels:publication_date", dicc).text
        publishet_at_correct = datetime.fromisoformat(published_at)
        actual_date = datetime.now(timezone.utc)

        # Recency filter: skip anything older than hours_limit hours.
        if (actual_date -  publishet_at_correct) > timedelta(hours=hours_limit):
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
    """Create the News table if missing (idempotent) and insert phase-1 data
    (everything the sitemap gives us). Body/Short_Description/Scrapped_at are
    None and Category defaults to 'News' here; all four get their real
    values later, in scrapping_article_body (phase 2). Url is the PRIMARY
    KEY and INSERT OR IGNORE is the deduplication mechanism: re-running this
    on an already-known Url is a safe no-op, not an error.
    """
    conn = sqlite3.connect("news.db")
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS News (Url varchar(255) PRIMARY KEY NOT NULL, Rtve_id INTEGER , Title varchar(255) NOT NULL, Section varchar(255), Published_at varchar(255) NOT NULL, Discovered_at varchar(255) NOT NULL, Body varchar(255), Short_Description varchar(255), Scrapped_at varchar(255), Source varchar(255) NOT NULL DEFAULT 'RTVE', Category varchar(255) NOT NULL DEFAULT 'News')")
    tuples = []
    for new in news:
        tuples.append((new['url'], new['rtve_id'], new['title'], new['section'], new['published_at'], new['discovered_at'], None, None, None, 'RTVE', 'News'))
    # 11 placeholders, one per column (10 would raise "table News has 11
    # columns but 10 values were supplied", verified when Category was added).
    cur.executemany("INSERT OR IGNORE INTO News VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", tuples)
    conn.commit()
    conn.close()

@task
async def scrapping_article_body(news):
    """Visit every article with Playwright and fill in the phase-2 columns:
    Body (.artBody), Short_Description (the meta description), and Category
    (the "name" field inside the data-category JSON attribute on the same
    <article class="mark article"> tag, e.g. {"name": "Economía"} -- this is
    what makes filtering by fine-grained topic possible, unlike the coarse
    Section field which only distinguishes noticias/deportes/play/etc. at
    the URL level).

    Opens its own SQLite connection (not passed in from another task): a
    live sqlite3.Connection object can't be pickled, and Prefect tries to
    hash task arguments for its cache_policy, so passing a connection
    between tasks raised "cannot pickle 'sqlite3.Connection' object" the
    first time this was tried.
    """
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
                category_json = await page.get_attribute("article.mark.article", "data-category")
                category = json.loads(category_json)["name"]
            except Exception as e:
                # Defensive: one bad article must not stop the whole batch.
                # Category falls back to 'News' (the schema default), not
                # None, to satisfy the NOT NULL constraint on that column.
                print("Fail in", item["url"], ":", e)
                body = None
                short_description = None
                category = 'News'
            await page.close()

            scraped_at = datetime.now(timezone.utc).isoformat()
            cur.execute(
                "UPDATE News SET Body = ?, Short_Description = ?, Scrapped_at = ?, Category = ? WHERE Url = ?",
                (body, short_description, scraped_at, category, item["url"])
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
def relevant_news(number_news, category):
    """Select the number_news most recent general-news articles
    (Section='noticias'), optionally narrowed to one specific category
    (e.g. "Economía") if category is not None. Two versions of the query
    instead of one: SQL placeholders (?) can only stand in for values, not
    for conditionally including/excluding a whole "AND Category = ?" clause.
    """
    conn = sqlite3.connect("news.db")
    cur = conn.cursor()
    if category:
        cur.execute("SELECT Title, Body, Url, Short_Description, Published_at, Source, Category FROM News WHERE Section='noticias' AND Category= ?  ORDER BY Published_at DESC LIMIT ?", (category, number_news))
    else:
        cur.execute("SELECT Title, Body, Url, Short_Description, Published_at, Source, Category FROM News WHERE Section='noticias' ORDER BY Published_at DESC LIMIT ?", (number_news,))
    news = cur.fetchall()

    conn.close()
    return news

@task
def text_model(news):
    """Summarize each selected article in Spanish with a local LLM, one call
    per article (not one combined call for all of them), so each entry gets
    its own independent summary/category/citation. Source and URL are never
    requested from the model (see build_prompt): they're taken straight from
    the database row instead, since that's guaranteed correct.
    """
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
    # news rows have 7 columns (see relevant_news): new[0]=Title, new[1]=Body,
    # new[2]=Url, new[3]=Short_Description, new[4]=Published_at,
    # new[-2]=Source, new[-1]=Category.
    for new in news:
        summary = answer(new[0], new[1], new[3])
        report_entry = {"title": new[0], "summary": summary, "source": new[-2], "url": new[2], "category": new[-1]}
        reports_entry.append(report_entry)
    return reports_entry

@task
def report(reports_entry):
    """Build the PDF: a cover page, then one block per news item (title,
    category, AI summary, citation), using ReportLab's Platypus API so
    pagination across the number_news items is handled automatically.
    """
    doc = SimpleDocTemplate("report.pdf", pagesize=A4)
    styles = getSampleStyleSheet()
    title = Paragraph("Report of RTVE news", styles["Title"])
    story = [title, Spacer(1, 20), PageBreak()]
    for report_entry in reports_entry:
        story.append(Paragraph(report_entry["title"], styles["Heading2"]))
        story.append(Paragraph(report_entry["category"], styles["Heading3"]))
        story.append(Paragraph(report_entry["summary"], styles["Normal"]))
        # Citation built here in plain code from the database values, never
        # from the model, so source/url are always correct (see text_model).
        cite = f'Source: {report_entry["source"]} — <a href="{report_entry["url"]}">{report_entry["url"]}</a>'
        story.append(Paragraph(cite, styles["Normal"]))
        story.append(Spacer(1, 20))
    doc.build(story)


@flow
async def main(number_news=10, hours_limit=48, category=None):
    """Entry point. number_news, hours_limit and category are the three
    knobs meant to be exposed as user-facing inputs later (sliders / a
    category dropdown) in an interactive dashboard.
    """
    urls, dicc = get_sitemaps()
    news   = buiding_list(urls, dicc, hours_limit)
    create_database(news)
    await scrapping_article_body(news)
    news_relevant = relevant_news(number_news, category)
    reports_entry = text_model(news_relevant)
    report(reports_entry)

if __name__ == "__main__":
    number_news = 10
    hours_limit = 48
    category = None
    asyncio.run(main(number_news, hours_limit, category))
