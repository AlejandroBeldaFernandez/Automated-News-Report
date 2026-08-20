# Automated News Report Generator — RTVE News Pipeline

Pipeline that scrapes RTVE's live news, summarizes the most relevant articles in Spanish with a local LLM, and builds a daily PDF report. Portfolio project: no commercial use, no monetization planned.

- **Problem:** Keeping up with the day's news requires visiting multiple sections of a news site and reading full articles; there's no lightweight, source-linked summary of just today's relevant headlines
- **Result:** An end-to-end pipeline that turns ~75-190 live RTVE articles per run into a configurable number of AI-generated Spanish summaries (title, category, source citation) in a single PDF, in roughly 15-20 minutes, fully local (no paid API)
- **Value:** Demonstrates web scraping under real compliance constraints (robots.txt, legal notice), workflow orchestration with retries, local LLM inference, containerization, and a working deployment without any paid cloud service

> [README en español](README_ES.md)

---

## Table of Contents

1. [Problem Statement](#problem-statement)
2. [Project Value](#project-value)
3. [Data Source](#data-source)
4. [Source Selection: Sitemap vs RSS vs Scraping](#source-selection-sitemap-vs-rss-vs-scraping)
5. [Legal Compliance](#legal-compliance)
6. [Pipeline Architecture](#pipeline-architecture)
7. [Automation](#automation)
8. [Live Demo and Deployment](#live-demo-and-deployment)
9. [Known Limitations](#known-limitations)
10. [Conclusions](#conclusions)
11. [Possible Improvements](#possible-improvements)
12. [Requirements](#requirements)

---

## Problem Statement

RTVE, like most news sites, publishes far more articles per day than anyone can read. Getting a same-day overview means either scrolling a busy homepage or reading full articles one by one. There's no lightweight report that says, in a page or two, "here's what actually matters today, summarized, with a link to verify it at the source."

This project addresses:

> **Given a live public news source, can an automated pipeline reliably extract, filter, summarize, and package the day's most relevant articles into a citation-backed report, without violating the source's crawling or content-usage terms?**

---

## Project Value

This is a portfolio project, not a product. Its value is in demonstrating a full pipeline, end to end, with real constraints instead of a toy dataset:

- **Real-world scraping constraints.** RTVE blocks the default Python user-agent, redirects its classic RSS feed to a stale, blocked host, and serves the actual page content differently from what a naive scraper would expect. Each of these was discovered by checking `robots.txt` and the live site, not assumed.
- **Orchestration with retries.** The pipeline is idempotent (safe to re-run without duplicating data) and orchestrated with Prefect, the same tool used in production MLOps pipelines.
- **Local inference, no paid API.** Summarization runs on a local LLM (`Qwen/Qwen2.5-1.5B-Instruct`), on CPU, with zero per-request cost.
- **A real, running deployment**, not just code that "should work": a public, interactive dashboard that executes the actual pipeline on demand.

---

## Data Source

- **Source:** [RTVE (Radiotelevisión Española)](https://www.rtve.es), the Spanish public broadcaster
- **Feed:** RTVE's Google News sitemap, `rtve.es/sitemaps/sitemaps-news.xml`
- **Typical volume:** 75-190 articles per fetch, spanning multiple sections (`noticias`, `deportes`, `catalunya`, `play`, `rtve`)

All rights over the article content belong to RTVE. For verified, complete information, always refer to RTVE directly; this project produces short automated summaries, not a substitute for the original reporting.

---

## Source Selection: Sitemap vs RSS vs Scraping

| Option | Outcome |
|---|---|
| Classic RSS (`feedparser`) | Redirects to `api2.rtve.es`, a host that is stale (dated 2022) **and** blocked by its own `robots.txt` |
| Plain scraping of section pages | Works, but duplicates what a sitemap already provides in structured form, with no clear crawling permission |
| **Google News sitemap (chosen)** | Explicitly allowed in `robots.txt` (`Allow: /sitemaps/*.xml$`), live, links directly to each article |

The sitemap does **not** self-limit to the last 48 hours the way Google News sitemaps typically do — a live check found entries mixed in from 2008 through the current date. The recency filter is therefore enforced in the pipeline itself, not assumed from the source.

A custom, self-identifying User-Agent is used for the sitemap download, since RTVE's `robots.txt` explicitly blocks the Python default (`Python-urllib`). Playwright, used to scrape each article page, deliberately keeps its normal browser User-Agent instead: RTVE's `robots.txt` is served through an `/akamai/` path, indicating a bot-management layer that is more likely to flag an atypical user-agent on pages meant for human visitors.

---

## Legal Compliance

`robots.txt` grants technical crawling permission only — it says nothing about usage rights over the content. RTVE's legal notice prohibits reproducing its content without authorization, a standard clause across Spanish media, not specific to RTVE.

Mitigation applied throughout this project:

- The full scraped article text is stored **only** for internal use (generating the summary) and is never shown or published — not in the notebook, the PDF, or the dashboard.
- What's public, for every article, is only: title, category, an AI-generated summary, and a link back to the original RTVE article.
- The citation (source + link) is always built in plain code from the database row, never left for the model to generate from memory, so it cannot be dropped or hallucinated.
- The project is explicitly non-commercial, with no monetization of any kind.

---

## Pipeline Architecture

| Step | Detail |
|---|---|
| 1. Fetch sitemap | `requests` + custom User-Agent → RTVE's live Google News sitemap |
| 2. Parse & filter | `xml.etree.ElementTree` with namespace handling; keeps only articles inside a configurable recency window |
| 3. Persist (phase 1) | SQLite, `Url` as `PRIMARY KEY`, `INSERT OR IGNORE` — idempotent, safe to re-run |
| 4. Scrape (phase 2) | Playwright visits each article: body (`.artBody`), short description (meta tag), and topic category (`data-category` attribute, e.g. `"Economía"`) |
| 5. Select | The N most recent general-news articles, optionally filtered by category |
| 6. Summarize | `Qwen/Qwen2.5-1.5B-Instruct`, one independent call per article, prompt restricted to the scraped text only (no outside knowledge) |
| 7. Build PDF | ReportLab (Platypus API): cover page, then title + category + summary + citation per article |

**Model choice:** a small instruction-tuned LLM was preferred over a heavier one specifically so the whole pipeline (scraping + inference) can run on CPU in a reasonable time, with no GPU dependency and no API cost.

---

## Automation

- **Orchestrated with Prefect**: each pipeline step is an independent task with retries; `Url` as the SQLite primary key makes those retries idempotent, no duplicate rows.
- **Containerized with Docker.**
- **Runs in GitHub Actions**, triggered manually only (`workflow_dispatch`), not on a daily schedule — this project isn't meant to run unattended every day. How a daily cron *would* be configured is documented (commented out) in the workflow file, but intentionally left disabled.
- The image is published to DockerHub as part of the same workflow.

---

## Live Demo and Deployment

**[alex-server.taile13699.ts.net](https://alex-server.taile13699.ts.net/)** — a real, interactive dashboard: choose the number of news, a time window (hours or days), and optionally a category, then click the button. It runs the actual pipeline and returns the resulting PDF. Not a cached or precomputed demo.

The original plan was Hugging Face Spaces. In practice, the account used for this project could not create any Space with compute (Docker, Gradio+CPU, and ZeroGPU were all blocked; only static Spaces were allowed), for reasons that could not be diagnosed (email verified, no notice from HF, no immediate support channel on the free plan). Alternatives were checked concretely, not assumed:

| Alternative | Outcome |
|---|---|
| Streamlit Community Cloud | Free tier is 1GB RAM — too small for the ~2.9GB model |
| Google Colab (`share=True`) | Works, but not always-on (session limits) |
| Oracle Cloud Always Free | Viable, but requires a credit card and manual VM administration |
| **Self-hosting (chosen)** | Free, no card, uses hardware already available |

The dashboard is self-hosted on the author's own machine (Ryzen 7 6800H, 32GB RAM, already running Docker), exposed publicly via **Tailscale Funnel** — free, no credit card, no port-forwarding on the home router. It's only reachable while that machine is on (roughly 9:00-23:00 CET), not 24/7. Deployment code lives in a separate repo: [Automated-News-Report-Spaces](https://github.com/AlejandroBeldaFernandez/Automated-News-Report-Spaces).

---

## Known Limitations

- `.artBody` (the scraped article container) also includes "related articles" boxes mixed into the paragraph text; not filtered out yet.
- The live demo is only reachable while the host machine is on, not 24/7.
- Summaries occasionally exceed the requested word limit; it's a prompt instruction, not a hard constraint.
- The sitemap only reflects RTVE's own publication timing; a genuinely breaking story published minutes ago may not yet be indexed there.

---

## Conclusions

The pipeline reliably turns a live, unstructured news feed into a compact, source-linked report, without assuming anything about the source that wasn't verified directly: the RSS feed's real status, the sitemap's actual freshness behavior, the exact HTML selectors, and the real limits of every hosting option considered were all checked against live data rather than documentation alone.

The most significant finding wasn't a code decision but a compliance one: the original data-source plan (classic RSS) and the original deployment plan (Hugging Face Spaces) both turned out to be unusable for reasons only discoverable by testing against the real service, not by reading its docs. The project's actual architecture is the result of adapting to what was verified to work, not what was assumed to work.

---

## Possible Improvements

- **Clean `.artBody` of embedded "related articles" boxes** before summarization, to reduce noise in the model's input.
- **Cache the model in a smaller quantized form** to speed up inference further on CPU.
- **Add automated tests** around the recency filter and the deduplication logic, beyond the manual verification done during development.
- **Surface `rtve_id`-based defensive logging** (currently a `print`) as a proper log file, for easier debugging of scraping failures over time.

---

## Requirements

```bash
pip install -r requirements.txt
playwright install --with-deps chromium
```

See [`requirements.txt`](requirements.txt) for pinned versions (`requests`, `playwright`, `transformers`, `torch`, `reportlab`, `prefect`, `pymupdf`, `numpy`).

---

*Data source: [rtve.es](https://www.rtve.es) — Radiotelevisión Española*
