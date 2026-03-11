# 📰 Enhanced Multi-Source News Analysis Tool

A location-aware news intelligence tool that searches, filters, and summarizes news articles across multiple sources for urban areas in India. Built for sustainability and ESG risk monitoring.

Developed by [Sustainbility Intelligence Pvt. Ltd.](mailto:Ankan.j@sustaintel.com)

---

## ✨ Features

- **Multi-source ingestion** — Google News RSS, 20+ Indian RSS feeds, and direct web scraping
- **Location-aware filtering** — searches by city, district, and state from a CSV input
- **Keyword-based relevance scoring** — across ESG categories: Labor, Biodiversity, Pollution, Land Rights, Social Cohesion, and more
- **Full-text extraction** — via `newspaper3k`, `BeautifulSoup`, with optional Playwright fallback for JS-heavy pages
- **Google News URL resolution** — unwraps redirect URLs to real publisher links
- **Deduplication & caching** — persists processed URLs and resolved URL maps across runs
- **CLI interface** — fully configurable via command-line arguments
- **Structured output** — per-location CSV files + a JSON summary report

---

## 📁 Project Structure

```
.
├── news_search.py        # Main script
├── Urban_list.csv        # Input: list of urban areas (city, district, state)
├── requirements.txt      # Python dependencies
└── README.md
```

Output is written to a timestamped directory, e.g. `contextual_news_20240311_120000/`:
```
contextual_news_YYYYMMDD_HHMMSS/
├── logs/
│   └── news_analysis_*.log
├── <LocationName>_news.csv
├── processed_urls.json
├── resolved_url_map.json
└── analysis_summary.json
```

---

## 🚀 Installation

### 1. Clone the repository
```bash
git clone https://github.com/ankan-jana1/news_scraping.git
cd news_scraping
```

### 2. Create and activate a virtual environment (recommended)
```bash
python -m venv venv
source venv/bin/activate        # On Windows: venv\Scripts\activate
```

### 3. Install dependencies
```bash
pip install -r requirements.txt
```

### 4. (Optional) Install Playwright for JS URL resolution
```bash
playwright install chromium
```

---

## 🗂️ Input CSV Format

The tool expects a CSV file with at least a `name` column. `district` and `state` improve search accuracy.

```csv
name,district,state
Mumbai,Mumbai City,Maharashtra
Chennai,Chennai,Tamil Nadu
Kolkata,Kolkata,West Bengal
```

Supported name column aliases: `name`, `Name`, `city`, `City`, `urban_area`

---

## ⚙️ Usage

### Basic run (uses defaults)
```bash
python news_search.py
```

### Full example with options
```bash
python news_search.py \
  --urban-csv Urban_list.csv \
  --days-back 30 \
  --min-relevance 30 \
  --sources google_news,rss_feeds \
  --output-prefix my_analysis
```

### Using custom keywords
```bash
python news_search.py --keywords \
  "flood; cyclone; landslide||HEALTH_POPULATION||Natural_Disasters;; \
   deforestation; habitat loss||BIODIVERSITY_CLIMATE||Deforestation & Resource Depletion"
```

---

## 🔧 CLI Arguments

| Argument | Default | Description |
|---|---|---|
| `--urban-csv` | `Urban_list.csv` | Path to urban areas CSV |
| `--days-back` | `30` | How many days back to search |
| `--min-relevance` | `30` | Minimum relevance score to keep an article |
| `--max-locations` | *(all)* | Limit number of locations to process |
| `--sources` | `google_news,rss_feeds,web_scraping` | Comma-separated source list |
| `--output-prefix` | `contextual_news` | Output directory prefix |
| `--disable-scrapers` | `False` | Disable web scraping, use RSS and Google only |
| `--resolve-js` | `False` | Use Playwright for JS-wrapped URL resolution (slower) |
| `--keywords` | *(built-in)* | Custom keywords (see format below) |
| `--use-default-keywords` | `False` | Force use of built-in keyword list |

### Custom `--keywords` format
```
keyword1; keyword2||CATEGORY||SUBCATEGORY;;keyword3||CATEGORY2||SUBCATEGORY2
```

---

## 📊 Output Columns

Each location CSV contains:

| Column | Description |
|---|---|
| `title` | Article headline |
| `url` | Resolved publisher URL |
| `published_date` | Publication date |
| `source_name` | Source (Google News, RSS feed name, etc.) |
| `description` | Article summary/snippet |
| `full_text` | Extracted full article text |
| `relevance_score` | Keyword relevance score (0–100) |
| `category` | Matched ESG category |
| `subcategory` | Matched ESG subcategory |

---

## 🏷️ ESG Keyword Categories (Built-in)

| Category | Subcategories |
|---|---|
| `LABOR_WORKFORCE` | Labor Policies |
| `HEALTH_POPULATION` | Natural Disasters |
| `BIODIVERSITY_CLIMATE` | Deforestation & Resource Depletion, Climate Change, Wildlife Trade |
| `LAND_NATURAL_RESOURCES` | Water Availability, Land Resettlement, Tribal Land |
| `POLLUTION_CONTAMINATION` | Pollution, Legal Action, Contamination & Safety |
| `SOCIAL_COHESION` | Group Grievance, Community Protest |

---

## 📝 License

MIT License — see [LICENSE](LICENSE) for details.
