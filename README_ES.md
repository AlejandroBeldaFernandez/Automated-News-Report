# Automated News Report Generator

Pipeline que scrapea las noticias en vivo de RTVE, resume en español los artículos más relevantes con un LLM local, y genera un reporte diario en PDF. Proyecto de portfolio: no es un producto comercial, no hay ninguna monetización prevista.

**[README in English](README.md)**

## Demo en vivo

**[alex-server.taile13699.ts.net](https://alex-server.taile13699.ts.net/)** — un dashboard interactivo de verdad: eliges cuántas noticias, una ventana de tiempo (horas o días) y opcionalmente una categoría, le das al botón, y ejecuta el pipeline real (scraping + resumen con IA local) y muestra el PDF resultante. No es una demo cacheada ni precalculada.

Autoalojado en el hardware propio del autor (no en una plataforma cloud), así que solo está disponible mientras esa máquina esté encendida (aproximadamente de 9:00 a 23:00, hora española). Ver [Despliegue](#despliegue) para el porqué. El código del despliegue vive en un repo separado: [Automated-News-Report-Spaces](https://github.com/AlejandroBeldaFernandez/Automated-News-Report-Spaces).

## Qué hace

1. Descarga el sitemap de Google News de RTVE con un User-Agent propio identificativo.
2. Lo parsea y se queda solo con las noticias de una ventana de tiempo configurable.
3. Scrapea el cuerpo completo, la entradilla y la categoría de cada artículo con Playwright.
4. Guarda todo en SQLite, de forma idempotente.
5. Selecciona las noticias generales más recientes, opcionalmente filtradas por categoría.
6. Resume cada una en español con un LLM local (`Qwen/Qwen2.5-1.5B-Instruct`, en CPU, sin ninguna API externa).
7. Construye un PDF: título, categoría, resumen generado por IA y una cita (fuente + enlace) por cada noticia, construida en código a partir de la base de datos, nunca generada por el modelo.

## Fuente: RTVE

Todo el contenido proviene de **RTVE (Radiotelevisión Española)**, la radiotelevisión pública española. Todos los derechos pertenecen a RTVE. Para información verificada y completa, acude siempre directamente a RTVE (rtve.es); este proyecto genera resúmenes automáticos cortos, no un sustituto del periodismo original.

## Por qué el sitemap, y no el RSS ni el scraping directo

El plan original era el RSS clásico de RTVE vía `feedparser`. Al comprobar el `robots.txt` real de RTVE, ese feed redirige a un host (`api2.rtve.es`) que está desactualizado (fechado en 2022) y bloqueado por su propio `robots.txt`. Este proyecto usa en su lugar el **sitemap de Google News** de RTVE (`rtve.es/sitemaps/sitemaps-news.xml`), explícitamente permitido en el `robots.txt` (`Allow: /sitemaps/*.xml$`), vivo, y con enlace directo a cada artículo. Se usa un User-Agent propio para la descarga del sitemap, ya que el `robots.txt` de RTVE bloquea el de Python por defecto (`Python-urllib`); Playwright, usado para scrapear las páginas de artículo, mantiene en cambio su User-Agent de navegador normal, porque RTVE está detrás de un sistema de gestión de bots de Akamai que es más probable que marque como sospechoso un user-agent atípico en páginas pensadas para humanos.

## Nota legal

El `robots.txt` solo concede permiso técnico de rastreo, no derechos de uso sobre el contenido. El aviso legal de RTVE prohíbe reproducir sus contenidos sin autorización, una cláusula estándar en casi todos los medios españoles, no específica de RTVE. La mitigación de este proyecto: el texto completo scrapeado de cada artículo se guarda solo para uso interno (generar el resumen) y **nunca** se muestra ni se publica, ni en el notebook, ni en el PDF, ni en el dashboard. Lo público es únicamente el título, la categoría, un resumen generado por IA, y un enlace de vuelta al artículo original de RTVE.

## Pipeline y automatización

- Orquestado con **Prefect** (tasks independientes con reintentos; `Url` como clave primaria en SQLite hace esos reintentos idempotentes, sin filas duplicadas).
- Contenerizado con **Docker**.
- Se ejecuta en **GitHub Actions**, con disparo manual únicamente (`workflow_dispatch`), no en un cron diario. Cómo se configuraría un cron diario está documentado (comentado) en el archivo del workflow, pero deliberadamente no activado: este proyecto no está pensado para correr sin supervisión todos los días.
- La imagen se publica en DockerHub como parte del mismo workflow.

## Despliegue

El plan original era Hugging Face Spaces. En la práctica, la cuenta usada para este proyecto no pudo crear ningún Space con cómputo (Docker, Gradio+CPU o ZeroGPU, todos bloqueados; solo se permitían Spaces estáticos), por motivos que no se pudieron diagnosticar (email verificado, sin avisos de HF, sin canal de soporte inmediato en el plan gratuito). Se comprobaron alternativas de forma concreta, no se asumieron: el plan gratuito de Streamlit Community Cloud (1GB de RAM) es insuficiente para el modelo de ~2.9GB; Google Colab con un túnel `share=True` funciona pero no está siempre encendido; el nivel Always Free de Oracle Cloud es viable pero exige tarjeta de crédito y administrar una VM a mano.

El dashboard está autoalojado en su lugar, en la máquina propia del autor (Ryzen 7 6800H, 32GB RAM, con Docker ya instalado), expuesto públicamente con **Tailscale Funnel** (gratis, sin tarjeta de crédito, sin abrir puertos en el router doméstico). Ver el repo [Automated-News-Report-Spaces](https://github.com/AlejandroBeldaFernandez/Automated-News-Report-Spaces) para ese código.

## Limitaciones conocidas

- `.artBody` (el contenedor del artículo scrapeado) también incluye cajas de "noticias relacionadas" mezcladas en el texto de los párrafos; todavía sin filtrar.
- La demo en vivo solo está disponible mientras la máquina que la aloja esté encendida, no 24/7.
- Los resúmenes a veces superan el límite de palabras pedido; es una instrucción del prompt, no una restricción dura.

## Estructura de los repos

- Este repo: el notebook exploratorio (`Automated_news_report.ipynb`) y el pipeline con Prefect independiente (`automated_news_report.py`).
- [Automated-News-Report-Spaces](https://github.com/AlejandroBeldaFernandez/Automated-News-Report-Spaces): el dashboard de Gradio y su Dockerfile, mantenido aparte del código de análisis/pipeline.
