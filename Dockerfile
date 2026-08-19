FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Installs the Chromium browser binary AND the OS-level libraries it needs
# (--with-deps), since the base image doesn't have them.
RUN playwright install --with-deps chromium

# Bakes the summarization model into the image at build time, so no
# download happens when the container runs (decided: image size trade-off
# is worth it for a model that would otherwise be re-downloaded on every
# manual run, since there's no daily schedule keeping a cache warm).
RUN python -c "from transformers import AutoTokenizer, AutoModelForCausalLM; \
    AutoTokenizer.from_pretrained('Qwen/Qwen2.5-1.5B-Instruct'); \
    AutoModelForCausalLM.from_pretrained('Qwen/Qwen2.5-1.5B-Instruct')"

COPY automated_news_report.py .

# news.db is created fresh inside the container on each run (no volume):
# the pipeline only ever needs the last 48h from the live sitemap, so
# there's nothing worth persisting between runs.
CMD ["python", "automated_news_report.py"]
