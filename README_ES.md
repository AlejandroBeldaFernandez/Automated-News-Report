# Automated News Report Generator — Pipeline de noticias de RTVE

Pipeline que scrapea las noticias en vivo de RTVE, resume en español los artículos más relevantes con un LLM local, y genera un reporte diario en PDF. Proyecto de portfolio: sin uso comercial, sin ninguna monetización prevista.

- **Problema:** estar al día con las noticias exige visitar varias secciones de una web de noticias y leer artículos completos; no existe un resumen ligero y con enlace a la fuente de solo los titulares relevantes del día
- **Resultado:** un pipeline de extremo a extremo que convierte entre 75 y 190 artículos en vivo de RTVE por ejecución en un número configurable de resúmenes en español generados por IA (título, categoría, cita de la fuente) en un único PDF, en unos 15-20 minutos, totalmente local (sin ninguna API de pago)
- **Valor:** demuestra scraping bajo restricciones reales de cumplimiento (robots.txt, aviso legal), orquestación de flujos con reintentos, inferencia con LLM local, contenerización, y un despliegue real funcionando sin ningún servicio cloud de pago

> [README in English](README.md)

---

## Índice

1. [Planteamiento del problema](#planteamiento-del-problema)
2. [Valor del proyecto](#valor-del-proyecto)
3. [Fuente de datos](#fuente-de-datos)
4. [Elección de la fuente: sitemap vs RSS vs scraping](#elección-de-la-fuente-sitemap-vs-rss-vs-scraping)
5. [Cumplimiento legal](#cumplimiento-legal)
6. [Arquitectura del pipeline](#arquitectura-del-pipeline)
7. [Automatización](#automatización)
8. [Demo en vivo y despliegue](#demo-en-vivo-y-despliegue)
9. [Limitaciones conocidas](#limitaciones-conocidas)
10. [Conclusiones](#conclusiones)
11. [Posibles mejoras](#posibles-mejoras)
12. [Requisitos](#requisitos)

---

## Planteamiento del problema

RTVE, como la mayoría de medios, publica muchos más artículos al día de los que nadie puede leer. Tener una visión del día implica o bien recorrer una portada cargada, o bien leer artículos completos uno a uno. No existe un reporte ligero que diga, en una o dos páginas, "esto es lo que realmente importa hoy, resumido, con un enlace para verificarlo en la fuente".

Este proyecto aborda:

> **Dada una fuente de noticias pública en vivo, ¿puede un pipeline automatizado extraer, filtrar, resumir y empaquetar de forma fiable los artículos más relevantes del día en un reporte con cita a la fuente, sin incumplir las condiciones de rastreo o de uso del contenido de esa fuente?**

---

## Valor del proyecto

Este es un proyecto de portfolio, no un producto. Su valor está en demostrar un pipeline completo, de extremo a extremo, con restricciones reales en vez de un dataset de juguete:

- **Restricciones reales de scraping.** RTVE bloquea el user-agent por defecto de Python, redirige su RSS clásico a un host desactualizado y bloqueado, y sirve el contenido real de la página de forma distinta a lo que esperaría un scraper ingenuo. Cada una de estas cosas se descubrió comprobando el `robots.txt` y el sitio en vivo, no se asumió.
- **Orquestación con reintentos.** El pipeline es idempotente (seguro de reejecutar sin duplicar datos) y está orquestado con Prefect, la misma herramienta que se usa en pipelines de MLOps en producción.
- **Inferencia local, sin API de pago.** El resumen corre sobre un LLM local (`Qwen/Qwen2.5-1.5B-Instruct`), en CPU, con coste cero por petición.
- **Un despliegue real y funcionando**, no solo código que "debería funcionar": un dashboard público e interactivo que ejecuta el pipeline real bajo demanda.

---

## Fuente de datos

- **Fuente:** [RTVE (Radiotelevisión Española)](https://www.rtve.es), la radiotelevisión pública española
- **Feed:** sitemap de Google News de RTVE, `rtve.es/sitemaps/sitemaps-news.xml`
- **Volumen típico:** entre 75 y 190 artículos por descarga, repartidos en varias secciones (`noticias`, `deportes`, `catalunya`, `play`, `rtve`)

Todos los derechos sobre el contenido de los artículos pertenecen a RTVE. Para información verificada y completa, acude siempre directamente a RTVE; este proyecto genera resúmenes automáticos cortos, no un sustituto del periodismo original.

---

## Elección de la fuente: sitemap vs RSS vs scraping

| Opción | Resultado |
|---|---|
| RSS clásico (`feedparser`) | Redirige a `api2.rtve.es`, un host desactualizado (fechado en 2022) **y** bloqueado por su propio `robots.txt` |
| Scraping directo de las páginas de sección | Funciona, pero duplica lo que un sitemap ya da de forma estructurada, sin un permiso de rastreo claro |
| **Sitemap de Google News (elegido)** | Explícitamente permitido en el `robots.txt` (`Allow: /sitemaps/*.xml$`), vivo, con enlace directo a cada artículo |

El sitemap **no** se autolimita a las últimas 48 horas como suelen hacer los sitemaps de Google News: una comprobación en vivo encontró entradas mezcladas desde 2008 hasta la fecha actual. Por eso el filtro de antigüedad se aplica en el propio pipeline, no se asume de la fuente.

Se usa un User-Agent propio e identificativo para la descarga del sitemap, ya que el `robots.txt` de RTVE bloquea explícitamente el de Python por defecto (`Python-urllib`). Playwright, usado para scrapear cada página de artículo, mantiene deliberadamente su User-Agent de navegador normal: el `robots.txt` de RTVE se sirve a través de una ruta `/akamai/`, indicio de una capa de gestión de bots que es más probable que marque como sospechoso un user-agent atípico en páginas pensadas para visitantes humanos.

---

## Cumplimiento legal

El `robots.txt` solo concede permiso técnico de rastreo, no dice nada sobre derechos de uso del contenido. El aviso legal de RTVE prohíbe reproducir sus contenidos sin autorización, una cláusula estándar en casi todos los medios españoles, no específica de RTVE.

Mitigación aplicada en todo el proyecto:

- El texto completo scrapeado de cada artículo se guarda **solo** para uso interno (generar el resumen) y nunca se muestra ni se publica, ni en el notebook, ni en el PDF, ni en el dashboard.
- Lo público, de cada artículo, es únicamente: título, categoría, un resumen generado por IA, y un enlace de vuelta al artículo original de RTVE.
- La cita (fuente + enlace) siempre se construye en código plano a partir de la fila de la base de datos, nunca se deja que el modelo la genere de memoria, para que no se pueda perder ni inventar.
- El proyecto es explícitamente sin ánimo de lucro, sin ningún tipo de monetización.

---

## Arquitectura del pipeline

| Paso | Detalle |
|---|---|
| 1. Descargar sitemap | `requests` + User-Agent propio → sitemap de Google News en vivo de RTVE |
| 2. Parsear y filtrar | `xml.etree.ElementTree` con manejo de namespaces; se queda solo con los artículos dentro de una ventana de antigüedad configurable |
| 3. Persistir (fase 1) | SQLite, `Url` como `PRIMARY KEY`, `INSERT OR IGNORE`, idempotente, seguro de reejecutar |
| 4. Scrapear (fase 2) | Playwright visita cada artículo: cuerpo (`.artBody`), entradilla (etiqueta meta) y categoría (atributo `data-category`, ej. `"Economía"`) |
| 5. Seleccionar | Las N noticias generales más recientes, opcionalmente filtradas por categoría |
| 6. Resumir | `Qwen/Qwen2.5-1.5B-Instruct`, una llamada independiente por artículo, prompt restringido solo al texto scrapeado (sin conocimiento externo) |
| 7. Generar PDF | ReportLab (API Platypus): portada, y luego título + categoría + resumen + cita por cada artículo |

**Elección del modelo:** se prefirió un LLM pequeño instruct-tuned frente a uno más pesado precisamente para que todo el pipeline (scraping + inferencia) pueda correr en CPU en un tiempo razonable, sin depender de GPU ni de coste de API.

---

## Automatización

- **Orquestado con Prefect**: cada paso del pipeline es una task independiente con reintentos; `Url` como clave primaria en SQLite hace esos reintentos idempotentes, sin filas duplicadas.
- **Contenerizado con Docker.**
- **Corre en GitHub Actions**, con disparo manual únicamente (`workflow_dispatch`), no en un cron diario: este proyecto no está pensado para correr sin supervisión todos los días. Cómo se configuraría un cron diario está documentado (comentado) en el archivo del workflow, pero deliberadamente desactivado.
- La imagen se publica en DockerHub como parte del mismo workflow.

---

## Demo en vivo y despliegue

**[alex-server.taile13699.ts.net](https://alex-server.taile13699.ts.net/)** — un dashboard interactivo de verdad: eliges el número de noticias, una ventana de tiempo (horas o días) y opcionalmente una categoría, y le das al botón. Ejecuta el pipeline real y devuelve el PDF resultante. No es una demo cacheada ni precalculada.

El plan original era Hugging Face Spaces. En la práctica, la cuenta usada para este proyecto no pudo crear ningún Space con cómputo (Docker, Gradio+CPU y ZeroGPU, todos bloqueados; solo se permitían Spaces estáticos), por motivos que no se pudieron diagnosticar (email verificado, sin avisos de HF, sin canal de soporte inmediato en el plan gratuito). Se comprobaron alternativas de forma concreta, no se asumieron:

| Alternativa | Resultado |
|---|---|
| Streamlit Community Cloud | El plan gratuito da 1GB de RAM, insuficiente para el modelo de ~2.9GB |
| Google Colab (`share=True`) | Funciona, pero no está siempre encendido (límites de sesión) |
| Oracle Cloud Always Free | Viable, pero exige tarjeta de crédito y administrar una VM a mano |
| **Autoalojamiento (elegido)** | Gratis, sin tarjeta, usa hardware que ya está disponible |

El dashboard está autoalojado en la máquina propia del autor (Ryzen 7 6800H, 32GB RAM, con Docker ya instalado), expuesto públicamente con **Tailscale Funnel**, gratis, sin tarjeta de crédito, sin abrir puertos en el router doméstico. Solo está disponible mientras esa máquina esté encendida (aproximadamente de 9:00 a 23:00), no 24/7. El código de despliegue vive en un repo separado: [Automated-News-Report-Spaces](https://github.com/AlejandroBeldaFernandez/Automated-News-Report-Spaces).

---

## Limitaciones conocidas

- `.artBody` (el contenedor del artículo scrapeado) también incluye cajas de "noticias relacionadas" mezcladas en el texto de los párrafos; todavía sin filtrar.
- La demo en vivo solo está disponible mientras la máquina que la aloja esté encendida, no 24/7.
- Los resúmenes a veces superan el límite de palabras pedido; es una instrucción del prompt, no una restricción dura.
- El sitemap solo refleja el ritmo de publicación propio de RTVE; una noticia de última hora publicada hace minutos puede que todavía no esté indexada ahí.

---

## Conclusiones

El pipeline convierte de forma fiable un feed de noticias en vivo y sin estructurar en un reporte compacto y con enlace a la fuente, sin asumir nada sobre la fuente que no se haya verificado directamente: el estado real del feed RSS, el comportamiento real de frescura del sitemap, los selectores HTML exactos, y los límites reales de cada opción de hosting evaluada se comprobaron todos contra datos en vivo, no solo contra la documentación.

El hallazgo más importante no fue una decisión de código sino de cumplimiento: tanto el plan original de fuente de datos (RSS clásico) como el plan original de despliegue (Hugging Face Spaces) resultaron ser inviables por motivos que solo se pueden descubrir probando contra el servicio real, no leyendo su documentación. La arquitectura final del proyecto es el resultado de adaptarse a lo que se verificó que funcionaba, no a lo que se asumía que funcionaría.

---

## Posibles mejoras

- **Limpiar `.artBody` de las cajas de "noticias relacionadas" incrustadas** antes de resumir, para reducir el ruido en la entrada del modelo.
- **Cachear el modelo en una forma cuantizada más pequeña** para acelerar aún más la inferencia en CPU.
- **Añadir tests automatizados** sobre el filtro de antigüedad y la lógica de deduplicación, más allá de la verificación manual hecha durante el desarrollo.
- **Convertir el aviso defensivo basado en `rtve_id`** (ahora mismo un `print`) en un archivo de log propiamente dicho, para depurar más fácilmente los fallos de scraping a lo largo del tiempo.

---

## Requisitos

```bash
pip install -r requirements.txt
playwright install --with-deps chromium
```

Ver [`requirements.txt`](requirements.txt) para las versiones fijadas (`requests`, `playwright`, `transformers`, `torch`, `reportlab`, `prefect`, `pymupdf`, `numpy`).

---

*Fuente de datos: [rtve.es](https://www.rtve.es) — Radiotelevisión Española*
