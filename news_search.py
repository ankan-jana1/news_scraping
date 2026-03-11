# -*- coding: utf-8 -*-
"""
Enhanced Multi-Source News Analysis Tool (with CLI, expanded RSS list, tests-friendly)

Developed by Ankan.j@sustaintel.com
Sustainbility Intelligence Pvt. Ltd.
"""
import argparse
import requests
import pandas as pd
import csv
from datetime import datetime, timedelta
import random
import time
import re
import os
import feedparser
import json
from bs4 import BeautifulSoup
import urllib.parse
from newspaper import Article
from typing import List, Dict, Optional, Set
import logging
from urllib.parse import quote, urlparse
from sumy.parsers.plaintext import PlaintextParser
from sumy.nlp.tokenizers import Tokenizer
from sumy.summarizers.lex_rank import LexRankSummarizer
import hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
import tempfile
import threading

# Configure logging
def setup_logging(output_dir: str = None):
    """Setup comprehensive logging to both file and console"""
    
    # Create logs directory if output_dir is provided
    if output_dir:
        log_dir = os.path.join(output_dir, 'logs')
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, f'news_analysis_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    else:
        log_file = f'news_analysis_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
    )
    
    # Remove existing handlers
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Create file handler
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)  # Save everything to file
    file_handler.setFormatter(formatter)
    
    # Create console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)  # Show INFO+ on console
    console_handler.setFormatter(formatter)
    
    # Setup root logger
    root_logger.setLevel(logging.DEBUG)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    
    return log_file

logger = logging.getLogger(__name__)

@dataclass
class ArticleData:
    """Data class for article information"""
    title: str
    description: str
    url: str
    published_date: str
    source_name: str
    content: str
    full_text: str = ""
    relevance_score: int = 0
    category: str = ""
    subcategory: str = ""

class EnhancedTextExtractor:
    """Enhanced text extraction with multiple fallback methods including optional Playwright JS resolution"""
    
    def __init__(self):
        # Rotate through different user agents to avoid blocking
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/120.0.0.0"
        ]
        self.current_ua_index = 0
        self.session = requests.Session()
        self.blocked_domains = set()  # Track blocked domains
        self.setup_session()

    def setup_session(self):
        """Setup session with rotating headers and retry strategy"""
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry
        
        # Retry strategy
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        self.rotate_user_agent()

    def rotate_user_agent(self):
        """Rotate user agent to avoid detection"""
        self.current_ua_index = (self.current_ua_index + 1) % len(self.user_agents)
        self.session.headers.update({
            'User-Agent': self.user_agents[self.current_ua_index],
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })

    def is_domain_blocked(self, url: str) -> bool:
        """Check if domain is in blocked list"""
        try:
            domain = urlparse(url).netloc
            return domain in self.blocked_domains
        except:
            return False

    def add_blocked_domain(self, url: str):
        """Add domain to blocked list"""
        try:
            domain = urlparse(url).netloc
            self.blocked_domains.add(domain)
            logger.warning(f"Added {domain} to blocked domains list")
        except:
            pass

    def extract_real_url(self, encoded_url: str, use_playwright: bool = False) -> str:
        """
        Resolve Google News wrapper URLs (and other redirect wrappers) to the final article URL.
        Steps:
         - Check query param 'url'
         - Follow redirects (HEAD/GET) and examine chain
         - Inspect static HTML for canonical/og/meta/anchors
         - If still unresolved and use_playwright True, render with Playwright and inspect rendered DOM
        """
        if not encoded_url:
            return encoded_url
        try:
            parsed = urllib.parse.urlparse(encoded_url)
            qs = urllib.parse.parse_qs(parsed.query)
            # fast path: explicit url parameter
            if 'url' in qs and qs['url']:
                try:
                    candidate = urllib.parse.unquote(qs['url'][0])
                    if candidate:
                        return candidate
                except Exception:
                    pass

            # Try HEAD to follow redirects
            try:
                resp = self.session.head(encoded_url, allow_redirects=True, timeout=8)
                final = getattr(resp, 'url', None)
                if final and 'news.google.com' not in final and 'google' not in urllib.parse.urlparse(final).netloc:
                    return final
            except Exception:
                pass

            # Try GET and inspect page
            html = ""
            try:
                resp = self.session.get(encoded_url, allow_redirects=True, timeout=12)
                final = getattr(resp, 'url', None)
                if final and 'news.google.com' not in final and 'google' not in urllib.parse.urlparse(final).netloc:
                    return final

                html = resp.text or ''
                soup = BeautifulSoup(html, 'html.parser')

                # 1) link rel=canonical
                canonical = soup.find('link', rel='canonical')
                if canonical and canonical.get('href'):
                    href = canonical.get('href')
                    if href and 'news.google.com' not in href and 'google' not in urllib.parse.urlparse(href).netloc:
                        return href

                # 2) meta property="og:url"
                og = soup.find('meta', property='og:url')
                if og and og.get('content'):
                    href = og.get('content')
                    if href and 'news.google.com' not in href and 'google' not in urllib.parse.urlparse(href).netloc:
                        return href

                # 3) first external anchor
                for a in soup.find_all('a', href=True):
                    href = a['href']
                    if href.startswith('http') and 'news.google.com' not in href and 'accounts.google' not in href and 'google' not in urllib.parse.urlparse(href).netloc:
                        return href

            except Exception:
                pass

            # If allowed, use Playwright as last resort to execute JS and reveal publisher URL
            if use_playwright:
                try:
                    from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
                    with sync_playwright() as pw:
                        browser = pw.chromium.launch(headless=True)
                        context = browser.new_context(user_agent=self.session.headers.get("User-Agent"))
                        page = context.new_page()
                        try:
                            page.goto(encoded_url, wait_until="networkidle", timeout=30000)
                        except PWTimeout:
                            logger.debug("Playwright navigation timed out; continuing with loaded content")
                        except Exception as e:
                            logger.debug(f"Playwright navigation error: {e}")

                        final_url = page.url
                        if final_url and 'news.google.com' not in final_url and 'google' not in urllib.parse.urlparse(final_url).netloc:
                            browser.close()
                            return final_url

                        content = page.content()
                        soup2 = BeautifulSoup(content, "html.parser")
                        canonical = soup2.find("link", rel="canonical")
                        if canonical and canonical.get("href"):
                            href = canonical.get("href")
                            if href and 'news.google.com' not in href and 'google' not in urllib.parse.urlparse(href).netloc:
                                browser.close()
                                return href

                        og = soup2.find("meta", property="og:url")
                        if og and og.get("content"):
                            href = og.get("content")
                            if href and 'news.google.com' not in href and 'google' not in urllib.parse.urlparse(href).netloc:
                                browser.close()
                                return href

                        for a in soup2.find_all("a", href=True):
                            href = a['href']
                            if href.startswith("http"):
                                netloc = urllib.parse.urlparse(href).netloc or ""
                                if 'news.google.com' not in netloc and 'google' not in netloc:
                                    browser.close()
                                    return href
                        browser.close()
                except Exception as e:
                    logger.debug(f"Playwright resolution failed: {e}")

        except Exception as e:
            logger.debug(f"extract_real_url error for {encoded_url}: {e}")

        return encoded_url

    def get_full_text(self, url: str) -> str:
        """Enhanced text extraction with multiple methods and better error handling"""
        
        # Skip if domain is known to be blocked
        if self.is_domain_blocked(url):
            return "Error: Domain blocked - skipping extraction."
        
        try:
            final_url = url  # assume caller passed in resolved url
            logger.debug(f"Extracting text from: {final_url}")

            # Method 1: newspaper3k with custom config
            try:
                from newspaper import Config
                config = Config()
                config.browser_user_agent = self.user_agents[self.current_ua_index]
                config.request_timeout = 15
                config.number_threads = 1
                
                article = Article(final_url, config=config)
                article.download()
                article.parse()
                
                if article.text and len(article.text.strip()) > 100:
                    logger.debug(f"✓ newspaper3k extracted {len(article.text)} chars")
                    return article.text.strip()
            except Exception as e:
                logger.debug(f"newspaper3k failed: {e}")

            # Method 2: Direct request with enhanced error handling
            self.rotate_user_agent()  # Use fresh user agent
            
            try:
                response = self.session.get(final_url, timeout=20, allow_redirects=True)
                
                # Handle different HTTP errors
                if response.status_code == 403:
                    logger.warning(f"403 Forbidden for {final_url} - keeping URL but skipping text extraction")
                    return "Error: Access forbidden (403) - website blocking requests."
                
                elif response.status_code == 404:
                    return "Error: Article not found (404)."
                
                elif response.status_code == 429:
                    logger.warning(f"Rate limited for {final_url}")
                    time.sleep(5)  # Wait before next request
                    return "Error: Rate limited (429) - too many requests."
                
                elif response.status_code != 200:
                    return f"Error: HTTP {response.status_code}"
                
                response.raise_for_status()
                
            except requests.exceptions.Timeout:
                return "Error: Request timeout - website too slow to respond."
            except requests.exceptions.ConnectionError:
                return "Error: Connection failed - website unreachable."
            except requests.exceptions.TooManyRedirects:
                return "Error: Too many redirects."
            except requests.exceptions.RequestException as e:
                logger.warning(f"Request failed for {final_url}: {e}")
                return f"Error: Request failed - {str(e)[:100]}"

            # Method 3: Enhanced BeautifulSoup with better selectors
            soup = BeautifulSoup(response.text, "html.parser")
            
            # Remove unwanted elements
            for element in soup(["script", "style", "nav", "header", "footer", "aside", "form", "iframe", "noscript"]):
                element.decompose()
            
            # Try multiple extraction strategies with quality scoring
            text_candidates = []
            
            # Strategy 1: Article-specific selectors (weighted by likelihood)
            article_selectors = [
                ('article', 100),
                ('[role="main"]', 90),
                ('.article-content', 85),
                ('.post-content', 85),
                ('.entry-content', 85),
                ('.story-body', 80),
                ('.article-body', 80),
                ('#article-body', 80),
                ('.content', 70),
                ('main', 65),
                ('.main-content', 70),
                ('.news-content', 85),
                ('.story-content', 85)
            ]
            
            for selector, weight in article_selectors:
                try:
                    elements = soup.select(selector)
                    for element in elements:
                        paragraphs = element.find_all('p')
                        if len(paragraphs) >= 3:  # At least 3 paragraphs
                            text = '\n\n'.join([
                                p.get_text(strip=True) for p in paragraphs 
                                if p.get_text(strip=True) and len(p.get_text(strip=True)) > 20
                            ])
                            if len(text) > 200:  # Minimum meaningful length
                                # Calculate quality score
                                quality = weight + min(len(text) / 100, 50)  # Bonus for length
                                text_candidates.append((quality, text))
                except Exception:
                    continue
            
            # Strategy 2: Fallback to all paragraphs if no good candidates
            if not text_candidates:
                all_paragraphs = soup.find_all('p')
                valid_paragraphs = [
                    p.get_text(strip=True) for p in all_paragraphs 
                    if p.get_text(strip=True) and len(p.get_text(strip=True)) > 15
                ]
                if valid_paragraphs:
                    text = '\n\n'.join(valid_paragraphs)
                    if len(text) > 100:
                        text_candidates.append((50, text))  # Lower quality score
            
            # Return the highest quality text
            if text_candidates:
                text_candidates.sort(reverse=True, key=lambda x: x[0])
                selected_text = text_candidates[0][1]
                
                # Basic text cleaning
                selected_text = re.sub(r'\s+', ' ', selected_text)
                selected_text = re.sub(r'\n\s*\n', '\n\n', selected_text)
                
                logger.debug(f"✓ BeautifulSoup extracted {len(selected_text)} chars (quality: {text_candidates[0][0]})")
                return selected_text
            
            # Strategy 3: Last resort - get visible text
            try:
                visible_text = soup.get_text()
                visible_text = re.sub(r'\s+', ' ', visible_text).strip()
                if len(visible_text) > 200:
                    logger.debug(f"✓ Fallback extracted {len(visible_text)} chars")
                    return visible_text[:3000]  # Limit length for last resort
            except Exception as e:
                logger.debug(f"Fallback text extraction failed: {e}")
                
        except Exception as e:
            logger.error(f"Critical error extracting text from {url}: {e}")
        
        return "Error: Could not extract meaningful text from article."

class EnhancedNewsAnalyzer:
    def __init__(self, urban_list_csv: str, output_dir: str = None, news_sources: List[str] = None, resolve_js: bool = False, text_extractor: Optional[EnhancedTextExtractor] = None):
        self.urban_list_csv = urban_list_csv
        self.news_sources = news_sources or ['google_news', 'rss_feeds', 'web_scraping']
        self.text_extractor = text_extractor or EnhancedTextExtractor()
        self.resolve_js = resolve_js  # whether to use Playwright-based resolution
        self.processed_urls: Set[str] = set()  # Track processed URLs
        self.resolved_url_map: Dict[str, str] = {}
        self._map_lock = threading.Lock()
        self.output_dir = output_dir or os.getcwd()
        
        # Load urban areas
        self.urban_areas = self.load_urban_areas()
        
        # Expanded curated RSS sources (deduped)
        self.source_configs = {
            'google_news': {
                'base_url': 'https://news.google.com/rss/search',
                'params': {'hl': 'en-IN', 'gl': 'IN', 'ceid': 'IN:en'}
            },
            'rss_feeds': {
                'sources': list(dict.fromkeys([
                    # national / regional sources
                    'https://feeds.bbci.co.uk/news/world/asia/india/rss.xml',
                    'https://timesofindia.indiatimes.com/rssfeeds/-2128936835.cms',
                    'https://www.thehindu.com/news/national/feeder/default.rss',
                    'https://www.thehindu.com/news/cities/feeder/default.rss',
                    'https://indianexpress.com/section/india/feed/',
                    'https://feeds.feedburner.com/ndtvnews-india-news',
                    'https://feeds.hindustantimes.com/HT-India',
                    'https://thewire.in/feed/',
                    'https://www.firstpost.com/commonfeeds/v1/mfp/rss/web-stories.xml',
                    'https://www.livemint.com/rss/homepage',
                    'https://economictimes.indiatimes.com/feeds/rssfeedsdefault.cms',
                    'https://www.deccanherald.com/rss/top-stories',
                    'https://www.outlookindia.com/rss',
                    'https://www.theprint.in/feed/',
                    'https://scroll.in/feed',
                    'https://www.business-standard.com/rss/home_page_top_stories.rss',
                    'https://www.downtoearth.org.in/rss/news.xml',
                    'https://www.freepressjournal.in/rss',
                    'https://www.telegraphindia.com/rss/326'
                ]))
            }
        }

        # CR Indicators (unchanged)
        self.cr_indicators = {
            #"SECURITY_AND_CONFLICT": {
            #    "Internal_Conflict": ["insurgency", "rebellion", "armed conflict", "sectarian violence"],
            #    "Criminal_Violence": ["organized crime", "gang violence", "kidnapping", "extortion", "drug cartel"],
            #    "Terrorism": ["terrorism", "terrorist attack", "bombing", "suicide bomber", "extremist", "militant group"],
            #    "Security_Forces": ["police brutality", "military abuse"],
            #    "Regional_Neighboring_Instability": ["border conflict", "regional tension interstate dispute", "cross-border dispute"]
            #},
            #"POLITICAL_RISK_GOVERNANCE": {
            #    "Weak_Governance": ["corruption", "government failure", "institutional weakness", "poor governance", "bureaucratic inefficiency"],
            #    "Basic_Services": ["public services", "infrastructure failure", "service delivery", "public utilities", "government services"],
            #    "Trafficking_Trade": ["human trafficking", "illicit trade", "smuggling", "illegal trade", "trafficking network"],
            #},
            "LABOR_WORKFORCE": {
                "Labor_Policies": ["worker rights", "labor dispute",  "labor unrest", "union rights", "strike", "child labor", "forced labor", "labor exploitation", "migratory labor"]
            },
            "HEALTH_POPULATION": {
                #"Food_Security_Health": ["health epidemic", "disease outbreak", "public health"],
                "Natural_Disasters": ["natural disaster", "earthquake", "flood", "cyclone", "landslide", "Erosion", "emergency response"],
                #"Rural_Urban_Disparities": ["sanitation", "water access",],
                #"Forced_Movement": ["political refugee", "Climate Refugee", "forced migration", "resettlement"]
            },
            "BIODIVERSITY_CLIMATE": {
                "Deforestation & Resource Depletion": ["deforestation", "biodiversity protected areas", "environmental destruction", "habitat loss", "resource depletion", "forest fragmentation", "wetland", "mangrove", "human wildlife conflict"],
                "Climate_Change": ["climate change", "climate vulnerability", "sea level rise", "drought"],
                "Wildlife_Trade": ["illegal hunting", "wildlife trafficking", "poaching", "endangered species"]
            },
            "LAND_NATURAL_RESOURCES": {
                "Water_Availability": ["water scarcity", "water crisis",],
                "Land_Resettlement": ["land rights", "land grabbing", "land dispute", "sharecropper", "resettlement", "land acquisition", "forced eviction", "land compensation"],
                "Tribal_Land": ["tribal rights", "tribal land", "tribal communities", "forest rights", "primitive tribal groups", "scheduled area"],
            },
            "POLLUTION_CONTAMINATION": {
                "Pollution": ["air pollution", "water pollution", "critically polluted area", "polluted river", "smog", "industrial pollution"],
                "Legal Action": ["National Green Tribunal", "environmental violation", "environmental fine"],
                "Contamination _Safety": ["contaminated site", "industrial accident", "unsafe workplace", "industrial fatality", "soil contamination", "groundwater contamination"]
            },
            "SOCIAL_COHESION": {
                "Group_Grievance": ["ethnic conflict", "communal violence", "minority rights"],
                #"Workplace_Discrimination": ["workplace discrimination", "job gender inequality", "POSH"],
                "Community_Protest": ["protest", "demonstration", "bandh"]
            }
        }

        # Ensure output_dir exists
        os.makedirs(self.output_dir, exist_ok=True)

        # Load processed URLs persisted from previous runs (if any)
        self._load_processed_urls()
        # Load resolved URL map (wrapper -> publisher) persisted from previous runs
        self._load_resolved_map()

    # ---------------------------
    # Processed URLs persistence
    # ---------------------------
    def _processed_urls_path(self) -> str:
        outdir = self.output_dir or os.getcwd()
        return os.path.join(outdir, "processed_urls.json")

    def _load_processed_urls(self):
        try:
            path = self._processed_urls_path()
            if os.path.exists(path):
                with open(path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.processed_urls = set(data if isinstance(data, list) else [])
                    logger.info(f"Loaded {len(self.processed_urls)} processed URLs from disk")
        except Exception as e:
            logger.debug(f"Could not load processed URLs: {e}")

    def _save_processed_urls(self):
        try:
            path = self._processed_urls_path()
            with open(path, 'w', encoding='utf-8') as f:
                json.dump(list(self.processed_urls), f)
            logger.debug(f"Saved {len(self.processed_urls)} processed URLs to {path}")
        except Exception as e:
            logger.debug(f"Could not save processed URLs: {e}")

    # ---------------------------
    # Resolved URL mapping persistence (wrapper -> resolved publisher)
    # ---------------------------
    def _resolved_map_path(self) -> str:
        outdir = self.output_dir or os.getcwd()
        return os.path.join(outdir, "resolved_url_map.json")

    def _load_resolved_map(self):
        try:
            path = self._resolved_map_path()
            if os.path.exists(path):
                with open(path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.resolved_url_map = data if isinstance(data, dict) else {}
                    logger.info(f"Loaded {len(self.resolved_url_map)} resolved URLs from disk")
            else:
                self.resolved_url_map = {}
        except Exception as e:
            logger.debug(f"Could not load resolved_url_map: {e}")
            self.resolved_url_map = {}

    def _save_resolved_map(self):
        try:
            path = self._resolved_map_path()
            with open(path, 'w', encoding='utf-8') as f:
                json.dump(self.resolved_url_map, f)
            logger.debug(f"Saved {len(self.resolved_url_map)} resolved URLs to {path}")
        except Exception as e:
            logger.debug(f"Could not save resolved_url_map: {e}")

    # ---------------------------
    # Lightweight token-aware helpers
    # ---------------------------
    def _count_word_occurrences(self, text: str, term: str) -> int:
        if not text or not term:
            return 0
        try:
            pattern = r'\b' + re.escape(term.lower()) + r'\b'
            return len(re.findall(pattern, text.lower()))
        except Exception:
            return 0

    def _is_location_in_text(self, text: str, location: Dict) -> bool:
        if not text or not location:
            return False
        text_lc = text.lower()
        for key in ('name', 'district', 'state'):
            term = (location.get(key) or '').strip()
            if term and self._count_word_occurrences(text_lc, term) > 0:
                return True
        return False

    def _lightweight_relevance_filter(self, title: str, summary: str, link: str, location: Dict, keywords: List[str], require_location: bool=True) -> bool:
        combined = " ".join(filter(None, [title, summary, link])).lower()
        location_present = self._is_location_in_text(combined, location)
        kw_present = any(self._count_word_occurrences(combined, kw) > 0 for kw in (keywords or [])[:8])
        if require_location:
            title_summary = " ".join(filter(None, [title, summary])).lower()
            return location_present or (kw_present and self._is_location_in_text(title_summary, location))
        else:
            return location_present or kw_present

    # ---------------------------
    # Other helpers from original (fetchers, processing, summarization ...)
    # ---------------------------
    def load_urban_areas(self) -> List[Dict]:
        """Enhanced urban area loading with better error handling"""
        try:
            if not Path(self.urban_list_csv).exists():
                logger.error(f"Urban list CSV file not found: {self.urban_list_csv}")
                return []
                
            df = pd.read_csv(self.urban_list_csv)
            logger.info(f"Loaded CSV with columns: {df.columns.tolist()}")
            
            # Flexible column name handling
            name_col = None
            for col in ['name', 'Name', 'city', 'City', 'urban_area']:
                if col in df.columns:
                    name_col = col
                    break
            
            if not name_col:
                logger.error("Could not find name column in CSV")
                return []
            
            urban_areas = []
            for _, row in df.iterrows():
                try:
                    # handle NaNs for district/state
                    raw_name = row[name_col] if pd.notna(row[name_col]) else ''
                    clean_name = re.sub(r'\s*(CT|M Cl|M|NP|Corp\.?|OG|\(.*?\)).*$', '', str(raw_name)).strip()
                    
                    district = row.get('district', '')
                    district = '' if pd.isna(district) else str(district)
                    state = row.get('state', '')
                    state = '' if pd.isna(state) else str(state)
                    
                    urban_areas.append({
                        'name': clean_name,
                        'original_name': str(raw_name),
                        'district': district,
                        'state': state,
                        'full_location': f"{clean_name}, {district}, {state}"
                    })
                except Exception as e:
                    logger.warning(f"Error processing row: {e}")
                    continue
            
            logger.info(f"Successfully loaded {len(urban_areas)} urban areas")
            return urban_areas
            
        except Exception as e:
            logger.error(f"Error loading urban areas CSV: {e}")
            return []

    def create_article_hash(self, article: Dict) -> str:
        content = f"{article.get('title', '')}{article.get('url', '')}{article.get('published_date', '')}"
        return hashlib.md5(content.encode()).hexdigest()

    def _extract_candidate_from_entry(self, entry) -> str:
        """
        Extract a likely publisher URL candidate from a feed entry:
         - entry.links[0].href if available
         - else parse entry.summary and pick first <a href=...>
         - else fallback to entry.link
        """
        candidate = ""
        try:
            # feedparser's entry.links is often present with href
            if getattr(entry, 'links', None):
                try:
                    # pick first alternate/href
                    for l in entry.links:
                        if isinstance(l, dict) and l.get('href'):
                            candidate = l.get('href')
                            break
                        elif hasattr(l, 'href'):
                            candidate = l.href
                            break
                except Exception:
                    candidate = ""
            if not candidate:
                summary_html = getattr(entry, 'summary', '') or ''
                if summary_html:
                    soup_entry = BeautifulSoup(summary_html, 'html.parser')
                    a = soup_entry.find('a', href=True)
                    if a:
                        candidate = a['href']
            if not candidate:
                candidate = getattr(entry, 'link', '') or ''
        except Exception:
            candidate = getattr(entry, 'link', '') or ''
        return candidate

    def _normalize_and_extract_url_from_href(self, href: str) -> str:
        """
        For hrefs that are wrappers containing a url= param, try to extract it.
        Otherwise return href unchanged.
        """
        if not href:
            return href
        try:
            parsed = urllib.parse.urlparse(href)
            qs = urllib.parse.parse_qs(parsed.query)
            if 'url' in qs and qs['url']:
                try:
                    return urllib.parse.unquote(qs['url'][0])
                except Exception:
                    return qs['url'][0]
        except Exception:
            pass
        return href

    def fetch_google_news_enhanced(self, query: str, location: Dict, days_back: int = 7, require_location: bool = True) -> List[ArticleData]:
        """Enhanced Google News fetching with publisher-candidate extraction and URL resolution (Playwright optional)"""
        articles = []
        
        location_name = location.get('name', '').strip()
        location_state = location.get('state', '').strip()
        location_district = location.get('district', '').strip()
        
        logger.info(f"Fetching Google News for query: '{query}' in location: {location_name}, {location_state}")
        
        try:
            query_variations = [
                f"{query} \"{location_name}\" {location_state}",
                f"\"{location_name}\" {query}",
                f"{location_district} {query}" if location_district else f"{location_name} news {query}"
            ]
            
            logger.debug(f"Generated {len(query_variations)} query variations: {query_variations}")
            
            for i, search_query in enumerate(query_variations[:3], 1):
                logger.debug(f"Processing query variation {i}/{len(query_variations)}: '{search_query}'")
                
                try:
                    encoded_query = urllib.parse.quote_plus(search_query)
                    url = f"https://news.google.com/rss/search?q={encoded_query}&hl=en-IN&gl=IN&ceid=IN:en"
                    logger.debug(f"Fetching RSS feed from: {url}")
                    
                    headers = {
                        "User-Agent": self.text_extractor.session.headers.get("User-Agent", "Mozilla/5.0")
                    }
                    feed = feedparser.parse(url, request_headers=headers)
                    logger.debug(f"RSS feed parsed. Found {len(feed.entries)} entries")
                    
                    cutoff_date = datetime.now() - timedelta(days=days_back)
                    logger.debug(f"Filtering articles newer than: {cutoff_date}")
                    
                    processed_count = 0
                    keywords = re.findall(r'\w+', query)
                    for entry_idx, entry in enumerate(feed.entries[:40]):
                        try:
                            pub_date = datetime(*entry.published_parsed[:6]) if getattr(entry, 'published_parsed', None) else datetime.now()
                            
                            if pub_date >= cutoff_date:
                                title = getattr(entry, 'title', '') or ''
                                summary = getattr(entry, 'summary', '') or ''
                                raw_link = getattr(entry, 'link', '') or ''

                                # 1) Try to extract a candidate publisher link from the entry (summary or links)
                                candidate_href = self._extract_candidate_from_entry(entry)
                                candidate_href = self._normalize_and_extract_url_from_href(candidate_href)

                                # 2) Prefer cached resolution for candidate, then raw wrapper
                                with self._map_lock:
                                    cached = self.resolved_url_map.get(candidate_href) or self.resolved_url_map.get(raw_link)

                                resolved_link = None
                                if cached:
                                    resolved_link = cached
                                    logger.debug(f"Using cached resolved URL for candidate/raw: {candidate_href} / {raw_link} -> {resolved_link}")
                                else:
                                    # Try light/static resolution on the candidate first
                                    try:
                                        resolved_link = self.text_extractor.extract_real_url(candidate_href, use_playwright=False)
                                    except Exception as e:
                                        logger.debug(f"Candidate static resolution failed for {candidate_href}: {e}")
                                        resolved_link = None

                                    # If candidate didn't resolve to publisher, try resolving raw wrapper
                                    if (not resolved_link or 'news.google.com' in resolved_link or 'google' in urllib.parse.urlparse(resolved_link).netloc) and raw_link and raw_link != candidate_href:
                                        try:
                                            resolved_from_wrapper = self.text_extractor.extract_real_url(raw_link, use_playwright=False)
                                            if resolved_from_wrapper:
                                                resolved_link = resolved_from_wrapper
                                        except Exception:
                                            pass

                                    # If still seems like a google wrapper and Playwright is allowed, try JS resolution candidate->wrapper
                                    if self.resolve_js and (not resolved_link or 'news.google.com' in resolved_link or 'google' in urllib.parse.urlparse(resolved_link).netloc):
                                        try:
                                            resolved_js = self.text_extractor.extract_real_url(candidate_href, use_playwright=True)
                                            if resolved_js and 'news.google.com' not in resolved_js and 'google' not in urllib.parse.urlparse(resolved_js).netloc:
                                                resolved_link = resolved_js
                                        except Exception:
                                            pass
                                        if (not resolved_link or 'news.google.com' in resolved_link or 'google' in urllib.parse.urlparse(resolved_link).netloc) and raw_link and raw_link != candidate_href:
                                            try:
                                                resolved_js2 = self.text_extractor.extract_real_url(raw_link, use_playwright=True)
                                                if resolved_js2 and 'news.google.com' not in resolved_js2 and 'google' not in urllib.parse.urlparse(resolved_js2).netloc:
                                                    resolved_link = resolved_js2
                                            except Exception:
                                                pass

                                    # Save mapping for both candidate and raw when resolved
                                    try:
                                        if resolved_link and resolved_link != candidate_href:
                                            with self._map_lock:
                                                self.resolved_url_map[candidate_href] = resolved_link
                                        if resolved_link and resolved_link != raw_link:
                                            with self._map_lock:
                                                self.resolved_url_map[raw_link] = resolved_link
                                    except Exception:
                                        pass

                                use_link = resolved_link or candidate_href or raw_link

                                logger.debug(f"Final URL used for article: {use_link}")
                                # Use resolved URL for dedupe and storage
                                if use_link and use_link not in self.processed_urls:
                                    # ensure processed_urls and article.url hold the publisher/resolved link
                                    self.processed_urls.add(use_link)
                                    article = ArticleData(
                                        title=title,
                                        description=summary,
                                        url=use_link,
                                        published_date=pub_date.isoformat(),
                                        source_name='Google News',
                                        content=summary
                                    )
                                    articles.append(article)
                                    processed_count += 1
                                    logger.debug(f"Added Google News article {processed_count}: {title[:50]} -> {use_link}")
                                else:
                                    logger.debug(f"Skipped duplicate or missing URL: {use_link}")
                            else:
                                logger.debug(f"Skipped old article from {pub_date}: {getattr(entry, 'title', '')[:50]}...")
                        except Exception as e:
                            logger.debug(f"Error processing entry {entry_idx}: {e}")
                            continue
                    
                    logger.info(f"Query variation {i} yielded {processed_count} articles")
                    # persist resolved map occasionally so subsequent query variations and runs can reuse mappings
                    try:
                        self._save_resolved_map()
                    except Exception:
                        pass
                    time.sleep(1.2)
                    
                except Exception as e:
                    logger.warning(f"Error with query variation '{search_query}': {e}")
                    continue
            
            logger.info(f"Google News fetch completed. Total articles collected: {len(articles)}")
            return articles
            
        except Exception as e:
            logger.error(f"Critical error fetching Google News: {e}")
            return []

    def fetch_rss_feeds_enhanced(self, query_keywords: List[str], location: Dict, days_back: int = 7, require_location: bool = True) -> List[ArticleData]:
        """Enhanced RSS feed fetching with publisher-candidate extraction and URL resolution (Playwright optional)"""
        articles = []
        cutoff_date = datetime.now() - timedelta(days=days_back)
        
        logger.info(f"Fetching RSS feeds for keywords: {query_keywords[:3]} in location: {location.get('name', '')}")
        logger.debug(f"Using {len(self.source_configs['rss_feeds']['sources'])} RSS sources")
        
        for feed_idx, rss_url in enumerate(self.source_configs['rss_feeds']['sources'], 1):
            logger.info(f"Processing RSS feed {feed_idx}/{len(self.source_configs['rss_feeds']['sources'])}: {rss_url}")
            
            try:
                headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                                  "Chrome/120.0.0.0 Safari/537.36"
                }
                feed = feedparser.parse(rss_url, request_headers=headers)

                source_name = getattr(feed.feed, 'title', f'RSS Feed {feed_idx}')
                logger.debug(f"RSS feed parsed: '{source_name}' with {len(feed.entries)} entries")
                
                feed_articles_count = 0
                for entry_idx, entry in enumerate(feed.entries[:60]):
                    try:
                        if getattr(entry, 'published_parsed', None):
                            pub_date = datetime(*entry.published_parsed[:6])
                        elif getattr(entry, 'updated_parsed', None):
                            pub_date = datetime(*entry.updated_parsed[:6])
                        else:
                            pub_date = datetime.now()
                        
                        if pub_date < cutoff_date:
                            logger.debug(f"Skipping old article: {pub_date} < {cutoff_date}")
                            continue
                        
                        title = getattr(entry, 'title', '') or ''
                        summary = getattr(entry, 'summary', '') or ''
                        raw_link = getattr(entry, 'link', '') or ''

                        # Extract candidate publisher link from entry
                        candidate_href = self._extract_candidate_from_entry(entry)
                        candidate_href = self._normalize_and_extract_url_from_href(candidate_href)

                        # Resolve via cache -> static -> playwright (candidate first)
                        with self._map_lock:
                            cached = self.resolved_url_map.get(candidate_href) or self.resolved_url_map.get(raw_link)

                        resolved_link = None
                        if cached:
                            resolved_link = cached
                            logger.debug(f"Using cached resolved URL for RSS entry: {candidate_href} / {raw_link} -> {resolved_link}")
                        else:
                            # try candidate
                            try:
                                resolved_link = self.text_extractor.extract_real_url(candidate_href, use_playwright=False)
                            except Exception:
                                resolved_link = None

                            # try wrapper if candidate didn't resolve
                            if (not resolved_link or 'news.google.com' in resolved_link or 'google' in urllib.parse.urlparse(resolved_link).netloc) and raw_link and raw_link != candidate_href:
                                try:
                                    resolved_from_wrapper = self.text_extractor.extract_real_url(raw_link, use_playwright=False)
                                    if resolved_from_wrapper:
                                        resolved_link = resolved_from_wrapper
                                except Exception:
                                    pass

                            # Playwright fallback if enabled
                            if self.resolve_js and (not resolved_link or 'news.google.com' in resolved_link or 'google' in urllib.parse.urlparse(resolved_link).netloc):
                                try:
                                    resolved_js = self.text_extractor.extract_real_url(candidate_href, use_playwright=True)
                                    if resolved_js and 'news.google.com' not in resolved_js and 'google' not in urllib.parse.urlparse(resolved_js).netloc:
                                        resolved_link = resolved_js
                                except Exception:
                                    pass
                                if (not resolved_link or 'news.google.com' in resolved_link or 'google' in urllib.parse.urlparse(resolved_link).netloc) and raw_link and raw_link != candidate_href:
                                    try:
                                        resolved_js2 = self.text_extractor.extract_real_url(raw_link, use_playwright=True)
                                        if resolved_js2 and 'news.google.com' not in resolved_js2 and 'google' not in urllib.parse.urlparse(resolved_js2).netloc:
                                            resolved_link = resolved_js2
                                    except Exception:
                                        pass

                            # Save mapping
                            try:
                                if resolved_link and resolved_link != candidate_href:
                                    with self._map_lock:
                                        self.resolved_url_map[candidate_href] = resolved_link
                                if resolved_link and resolved_link != raw_link:
                                    with self._map_lock:
                                        self.resolved_url_map[raw_link] = resolved_link
                            except Exception:
                                pass

                        use_link = resolved_link or candidate_href or raw_link

                        # Lightweight relevance filter
                        is_relevant = self._lightweight_relevance_filter(title, summary, use_link, location, query_keywords, require_location=require_location)
                        if not is_relevant:
                            logger.debug(f"Skipped irrelevant article by lightweight filter: {title[:60]}...")
                            continue
                        
                        if use_link and use_link not in self.processed_urls:
                            self.processed_urls.add(use_link)
                            article = ArticleData(
                                title=title,
                                description=summary,
                                url=use_link,
                                published_date=pub_date.isoformat(),
                                source_name=f"{source_name} (RSS)",
                                content=summary
                            )
                            articles.append(article)
                            feed_articles_count += 1
                            logger.debug(f"Added relevant article: {title[:50]} -> {use_link}")
                        else:
                            logger.debug(f"Skipped duplicate or missing URL: {use_link}")
                    
                    except Exception as e:
                        logger.debug(f"Error processing RSS entry {entry_idx} from {rss_url}: {e}")
                        continue
                
                logger.info(f"RSS feed {feed_idx} yielded {feed_articles_count} relevant articles")
                # persist resolved map after each feed for safety
                try:
                    self._save_resolved_map()
                except Exception:
                    pass
                time.sleep(0.6)
                
            except Exception as e:
                logger.warning(f"Error fetching RSS feed {rss_url}: {e}")
                continue
        
        logger.info(f"RSS feeds fetch completed. Total articles collected: {len(articles)}")
        return articles

    def fetch_web_scraping_enhanced(self, location: Dict, keywords: List[str]) -> List[ArticleData]:
        """Enhanced web scraping with multiple site support (site-specific scrapers added)"""
        articles = []
        
        scraping_targets = [
            {
                'name': 'Times of India',
                'search_url': 'https://timesofindia.indiatimes.com/topic/{query}',
                'article_selector': '.uwU81, .times-of-india-story, .byline-stories',
                'title_selector': 'a',
                'base_url': 'https://timesofindia.indiatimes.com'
            },
            {
                'name': 'The Hindu',
                'search_url': 'https://www.thehindu.com/search/?q={query}',
                'article_selector': '.story-card, .search-results .story-card',
                'title_selector': 'h3 a',
                'base_url': 'https://www.thehindu.com'
            },
            {
                'name': 'Indian Express',
                'search_url': 'https://indianexpress.com/?s={query}',
                'article_selector': '.articles, .search-results .article',
                'title_selector': 'h2 a, .title a',
                'base_url': 'https://indianexpress.com'
            },
            {
                'name': 'NDTV',
                'search_url': 'https://www.ndtv.com/search?searchtext={query}',
                'article_selector': '.news_Itm, .story',
                'title_selector': 'a',
                'base_url': 'https://www.ndtv.com'
            },
            {
                'name': 'Hindustan Times',
                'search_url': 'https://www.hindustantimes.com/search?q={query}',
                'article_selector': '.storyCard, .media-body',
                'title_selector': 'a',
                'base_url': 'https://www.hindustantimes.com'
            }
        ]
        
        for target in scraping_targets:
            try:
                search_query = f"{' '.join(keywords[:3])} {location.get('name','')}"
                encoded_query = urllib.parse.quote_plus(search_query)
                url = target['search_url'].format(query=encoded_query)
                
                response = self.text_extractor.session.get(url, timeout=15)
                response.raise_for_status()
                soup = BeautifulSoup(response.content, 'html.parser')
                
                article_elements = soup.select(target['article_selector'])[:12]
                
                for element in article_elements:
                    try:
                        title_elem = element.select_one(target['title_selector'])
                        if not title_elem:
                            title_elem = element.find('a')
                        if title_elem:
                            article_url = title_elem.get('href', '') or title_elem.get('data-url', '')
                            if article_url.startswith('/'):
                                article_url = target['base_url'].rstrip('/') + article_url
                            
                            # Resolve candidate and wrapper like RSS/Google
                            candidate_href = article_url
                            candidate_href = self._normalize_and_extract_url_from_href(candidate_href)

                            with self._map_lock:
                                cached = self.resolved_url_map.get(candidate_href) or self.resolved_url_map.get(article_url)

                            resolved = None
                            if cached:
                                resolved = cached
                            else:
                                try:
                                    resolved = self.text_extractor.extract_real_url(candidate_href, use_playwright=False)
                                except Exception:
                                    resolved = None
                                if (not resolved or 'news.google.com' in resolved or 'google' in urllib.parse.urlparse(resolved).netloc) and article_url and article_url != candidate_href:
                                    try:
                                        resolved2 = self.text_extractor.extract_real_url(article_url, use_playwright=False)
                                        if resolved2:
                                            resolved = resolved2
                                    except Exception:
                                        pass
                                if self.resolve_js and (not resolved or 'news.google.com' in resolved or 'google' in urllib.parse.urlparse(resolved).netloc):
                                    try:
                                        resolved_js = self.text_extractor.extract_real_url(candidate_href, use_playwright=True)
                                        if resolved_js and 'news.google.com' not in resolved_js and 'google' not in urllib.parse.urlparse(resolved_js).netloc:
                                            resolved = resolved_js
                                    except Exception:
                                        pass
                                try:
                                    if resolved and resolved != candidate_href:
                                        with self._map_lock:
                                            self.resolved_url_map[candidate_href] = resolved
                                    if resolved and resolved != article_url:
                                        with self._map_lock:
                                            self.resolved_url_map[article_url] = resolved
                                except Exception:
                                    pass

                            use_link = resolved or candidate_href or article_url
                            
                            title_text = title_elem.get_text().strip()
                            surrounding = element.get_text(" ", strip=True).lower()
                            if location.get('name','').lower() not in surrounding and not any(self._count_word_occurrences(surrounding, kw) > 0 for kw in keywords[:3]):
                                logger.debug(f"Scraped article skipped for location mismatch: {title_text[:60]}...")
                                continue
                            
                            if use_link and use_link not in self.processed_urls:
                                self.processed_urls.add(use_link)
                                articles.append(ArticleData(
                                    title=title_text,
                                    description='',
                                    url=use_link,
                                    published_date=datetime.now().isoformat(),
                                    source_name=f"{target['name']} (Scraped)",
                                    content=''
                                ))
                    except Exception as e:
                        logger.debug(f"Error processing scraped element: {e}")
                        continue
                
                # persist resolved map occasionally
                try:
                    self._save_resolved_map()
                except Exception:
                    pass
                time.sleep(2.0)
                
            except Exception as e:
                logger.warning(f"Error scraping {target['name']}: {e}")
                continue
        
        logger.info(f"Scraped {len(articles)} articles")
        return articles

    def extract_full_text_batch(self, articles: List[ArticleData], max_workers: int = 3) -> List[ArticleData]:
        def extract_single(article: ArticleData) -> ArticleData:
            try:
                # Ensure we use the publisher/resolved URL for extraction
                original_url = article.url or ""
                resolved = None

                # Prefer cached resolution
                try:
                    with self._map_lock:
                        resolved = self.resolved_url_map.get(original_url)
                except Exception:
                    resolved = None

                # If not cached, attempt lightweight/static resolution (optionally Playwright)
                if not resolved:
                    try:
                        # also attempt to extract candidate from article.description if present
                        candidate = original_url
                        try:
                            if article.description:
                                soup_entry = BeautifulSoup(article.description, 'html.parser')
                                a = soup_entry.find('a', href=True)
                                if a:
                                    candidate = self._normalize_and_extract_url_from_href(a['href'])
                        except Exception:
                            candidate = original_url
                        resolved = self.text_extractor.extract_real_url(candidate, use_playwright=self.resolve_js)
                    except Exception as e:
                        logger.debug(f"Resolution attempt failed for {original_url}: {e}")
                        resolved = None

                # If resolution yields a different URL, update article, processed set and cache
                if resolved and resolved != original_url:
                    logger.debug(f"Resolved URL: {original_url} -> {resolved}")
                    article.url = resolved
                    try:
                        with self._map_lock:
                            # cache mapping from wrapper -> resolved publisher
                            self.resolved_url_map[original_url] = resolved
                            # maintain processed_urls set to use resolved URL
                            if original_url in self.processed_urls:
                                self.processed_urls.discard(original_url)
                            self.processed_urls.add(resolved)
                    except Exception:
                        # best-effort; do not break extraction
                        pass

                # The extractor will now operate on article.url (publisher URL where possible)
                article.full_text = self.text_extractor.get_full_text(article.url)

                if article.full_text.startswith("Error:"):
                    if "403" in article.full_text:
                        logger.debug(f"403 Forbidden: {article.url}")
                    elif "timeout" in article.full_text.lower():
                        logger.debug(f"Timeout: {article.url}")
                    elif "blocked" in article.full_text.lower():
                        logger.debug(f"Blocked domain: {article.url}")
                    else:
                        logger.debug(f"Extraction failed: {article.url} - {article.full_text[:50]}")
                else:
                    logger.debug(f"✓ Extracted {len(article.full_text)} characters from {article.url}")

                return article
            except Exception as e:
                logger.error(f"Critical error in text extraction thread for {article.url}: {e}")
                article.full_text = f"Error: Thread execution failed - {str(e)[:100]}"
                return article

        max_workers = min(max_workers, 3)
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_article = {executor.submit(extract_single, article): article for article in articles}
            processed_articles = []
            
            for future in as_completed(future_to_article):
                try:
                    processed_article = future.result(timeout=60)
                    processed_articles.append(processed_article)
                except Exception as e:
                    logger.error(f"Error in thread execution: {e}")
                    failed_article = future_to_article[future]
                    failed_article.full_text = f"Error: Thread failed - {str(e)[:100]}"
                    processed_articles.append(failed_article)
        
        # Persist any new resolved mappings produced during extraction
        try:
            self._save_resolved_map()
        except Exception as e:
            logger.debug(f"Failed to save resolved map after extraction batch: {e}")

        successful = sum(1 for a in processed_articles if not a.full_text.startswith("Error:"))
        failed = len(processed_articles) - successful
        logger.info(f"Text extraction summary: {successful} successful, {failed} failed out of {len(processed_articles)} articles")
        
        return processed_articles

    def analyze_article_relevance_enhanced(self, article: ArticleData, location: Dict, keywords: List[str]) -> Dict:
        title = (article.title or "").lower()
        description = (article.description or "").lower()
        content = (article.content or "").lower()
        full_text = (article.full_text or "").lower()
        
        title_weight = 3
        description_weight = 2
        content_weight = 1
        
        keyword_matches = []
        keyword_score = 0
        
        for keyword in keywords:
            keyword_lower = keyword.lower()
            title_matches = self._count_word_occurrences(title, keyword_lower) * title_weight
            desc_matches = self._count_word_occurrences(description, keyword_lower) * description_weight
            content_matches = (self._count_word_occurrences(content + " " + full_text, keyword_lower)) * content_weight
            
            total_matches = title_matches + desc_matches + content_matches
            if total_matches > 0:
                keyword_matches.append(keyword)
                keyword_score += min(total_matches * 5, 25)
        
        location_matches = []
        location_score = 0
        
        location_terms = [
            (location.get('name','').lower(), 40),
            (location.get('district','').lower(), 20),
            (location.get('state','').lower(), 10)
        ]
        
        combined_text = f"{title} {description} {content} {full_text}"
        
        for term, weight in location_terms:
            if term and self._count_word_occurrences(combined_text, term) > 0:
                location_matches.append(term)
                location_score += weight
        
        if location.get('name','').lower() and self._count_word_occurrences(title, location.get('name','').lower()) > 0:
            location_score += 20
        
        total_relevance = min(keyword_score + location_score, 100)
        
        return {
            'matched_keywords': keyword_matches,
            'location_matches': location_matches,
            'relevance_score': total_relevance,
            'keyword_count': len(keyword_matches),
            'location_score': location_score,
            'keyword_score': keyword_score
        }

    def process_location_enhanced(self, location: Dict, days_back: int = 7, min_relevance: int = 25) -> Optional[str]:
        if not location or not location.get('name'):
            logger.error("Invalid location provided to process_location_enhanced")
            self._save_processed_urls()
            self._save_resolved_map()
            return None
        
        all_processed_articles = []
        location_name_clean = re.sub(r'[^\w\s-]', '', location.get('name','')).replace(' ', '_')
        
        logger.info("="*80)
        logger.info(f"🏙️  PROCESSING LOCATION: {location.get('name')} ({location.get('state')})")
        logger.info(f"District: {location.get('district')}, Full: {location.get('full_location')}")
        logger.info(f"Parameters: days_back={days_back}, min_relevance={min_relevance}")
        logger.info("="*80)
        
        category_summary = {}
        
        for cat_idx, (category, subcategories) in enumerate(self.cr_indicators.items(), 1):
            logger.info(f"\n📂 CATEGORY {cat_idx}/{len(self.cr_indicators)}: {category}")
            logger.info(f"Subcategories to process: {len(subcategories)}")
            
            category_articles_total = 0
            
            for sub_idx, (subcategory, keywords) in enumerate(subcategories.items(), 1):
                logger.info(f"\n  📋 SUBCATEGORY {sub_idx}/{len(subcategories)}: {subcategory}")
                logger.info(f"  Keywords: {keywords[:3]}{'...' if len(keywords) > 3 else ''} ({len(keywords)} total)")
                
                category_articles = []
                
                if 'google_news' in self.news_sources:
                    query = ' OR '.join(keywords[:4])
                    logger.info(f"  🔍 Fetching from Google News with query: '{query}'")
                    google_articles = self.fetch_google_news_enhanced(query, location, days_back, require_location=True)
                    category_articles.extend(google_articles)
                    logger.info(f"  ✅ Google News: {len(google_articles)} articles")
                
                if 'rss_feeds' in self.news_sources:
                    logger.info(f"  🔍 Fetching from RSS feeds")
                    rss_articles = self.fetch_rss_feeds_enhanced(keywords, location, days_back, require_location=True)
                    category_articles.extend(rss_articles)
                    logger.info(f"  ✅ RSS feeds: {len(rss_articles)} articles")
                
                if 'web_scraping' in self.news_sources:
                    logger.info(f"  🔍 Fetching from web scraping")
                    scraped_articles = self.fetch_web_scraping_enhanced(location, keywords)
                    category_articles.extend(scraped_articles)
                    logger.info(f"  ✅ Web scraping: {len(scraped_articles)} articles")
                
                logger.info(f"  📊 Total articles collected for subcategory: {len(category_articles)}")
                
                # Pre-filter to reduce unnecessary extractions
                filtered_candidates = []
                for art in category_articles:
                    try:
                        if self._lightweight_relevance_filter(art.title, art.description, art.url, location, keywords, require_location=True):
                            filtered_candidates.append(art)
                        else:
                            logger.debug(f"Pre-filtered out (low location relevance): {art.title[:80]} - {art.url}")
                    except Exception as e:
                        logger.debug(f"Error during pre-filtering article {art.url}: {e}")
                category_articles = filtered_candidates
                
                if category_articles:
                    logger.info(f"  🔄 Extracting full text from {len(category_articles)} articles...")
                    category_articles = self.extract_full_text_batch(category_articles)
                
                relevant_count = 0
                for art_idx, article in enumerate(category_articles):
                    logger.debug(f"    Analyzing article {art_idx + 1}: {article.title[:50]}...")
                    
                    analysis = self.analyze_article_relevance_enhanced(article, location, keywords)
                    logger.debug(f"    Relevance score: {analysis['relevance_score']}")
                    
                    if analysis['relevance_score'] >= min_relevance:
                        relevant_count += 1
                        
                        extraction_status = "success"
                        extraction_error = ""
                        text_for_analysis = article.full_text
                        
                        if article.full_text.startswith("Error:"):
                            extraction_status = "failed"
                            extraction_error = article.full_text
                            text_for_analysis = f"{article.title} {article.description}"
                            logger.debug(f"    Text extraction failed: {extraction_error[:50]}...")
                        else:
                            logger.debug(f"    Text extraction successful: {len(article.full_text)} characters")
                        
                        processed_data = {
                            'source_name': article.source_name,
                            'published_date': article.published_date,
                            'target_location': location.get('name', ''),
                            'location_district': location.get('district', ''),
                            'location_state': location.get('state', ''),
                            'category': category,
                            'subcategory': subcategory,
                            'search_keywords': ', '.join(keywords),
                            'article_hash': self.create_article_hash(article.__dict__),
                            'title': article.title.strip(),
                            'description': article.description.strip(),
                            'url': article.url,
                            'matched_keywords': ', '.join(analysis['matched_keywords']),
                            'location_matches': ', '.join(analysis['location_matches']),
                            'relevance_score': analysis['relevance_score'],
                            'keyword_match_count': analysis['keyword_count'],
                            'location_relevance_score': analysis['location_score'],
                            'keyword_score': analysis['keyword_score'],
                            'extraction_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                            'full_text': text_for_analysis[:15000] if extraction_status == "success" else "",
                            'summary': self.summarize_text(text_for_analysis, 10) if extraction_status == "success" else "Summary unavailable - text extraction failed",
                            'text_length': len(article.full_text) if extraction_status == "success" else 0,
                            'extraction_status': extraction_status,
                            'extraction_error': extraction_error,
                            'can_retry_extraction': "yes" if "403" not in extraction_error and "blocked" not in extraction_error.lower() else "no"
                        }
                        all_processed_articles.append(processed_data)
                        
                        if extraction_status == "failed":
                            logger.debug(f"    ✅ Kept article with failed extraction: {extraction_error[:30]}...")
                        else:
                            logger.debug(f"    ✅ Kept article with successful extraction")
                    else:
                        logger.debug(f"    ❌ Skipped low relevance article (score: {analysis['relevance_score']})")
                
                logger.info(f"  📈 Relevant articles for subcategory: {relevant_count}/{len(category_articles)}")
                category_articles_total += relevant_count
                
                sleep_time = random.uniform(2, 4)
                logger.info(f"Sleeping for {sleep_time:.2f} seconds before next request...")
                time.sleep(sleep_time)
            
            category_summary[category] = category_articles_total
            logger.info(f"📊 CATEGORY {category} COMPLETE: {category_articles_total} total relevant articles")
        
        total_articles = len(all_processed_articles)
        logger.info(f"\n🏆 LOCATION PROCESSING COMPLETE: {location.get('name')}")
        logger.info(f"Total articles collected: {total_articles}")
        logger.info(f"Category breakdown: {category_summary}")
        
        successful_extractions = len([a for a in all_processed_articles if a['extraction_status'] == 'success'])
        failed_extractions = total_articles - successful_extractions
        logger.info(f"Text extraction: {successful_extractions} success, {failed_extractions} failed")
        
        if all_processed_articles:
            result_file = self.save_results(all_processed_articles, location_name_clean, self.output_dir)
            logger.info(f"💾 Results saved to: {result_file}")
            # Persist processed URLs and resolved map after successful location processing
            self._save_processed_urls()
            self._save_resolved_map()
            return result_file
        else:
            logger.warning(f"⚠️  No relevant articles found for {location.get('name')}")
            # Persist processed URLs and resolved map even if none found to keep state
            self._save_processed_urls()
            self._save_resolved_map()
            return None

    def summarize_text(self, text: str, sentence_count: int = 3) -> str:
        """Extractive summarization using Sumy LexRank with robust fallback."""
        try:
            if not text or len(text.split()) < 30:
                return text

            clean_text = re.sub(r'\s+', ' ', text.strip())
            parser = PlaintextParser.from_string(clean_text, Tokenizer("english"))
            summarizer = LexRankSummarizer()
            summary_sentences = summarizer(parser.document, sentence_count)
            summary = " ".join(str(s) for s in summary_sentences).strip()
            if summary:
                return summary
        except Exception as e:
            logger.warning(f"LexRank summarization failed: {e}")

        sentences = [s.strip() for s in re.split(r'(?<=[.!?])\s+', text)][:sentence_count]
        return " ".join(sentences)

    def save_results(self, articles: List[Dict], location_name: str, output_dir: Optional[str] = None) -> str:
        """
        Save results to CSV. This function now ensures the final CSV contains ONLY
        the requested columns (and in this order):
        source_name, published_date, target_location, location_district, location_state,
        category, subcategory, title, url, full_text, summary
        """
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            safe_location = re.sub(r'[^\w\-_]', '_', location_name)[:120]
            filename = f"cr_indicators_{safe_location}_{timestamp}.csv"
            outdir = output_dir or self.output_dir or os.getcwd()
            os.makedirs(outdir, exist_ok=True)
            fullpath = os.path.join(outdir, filename)
            
            # Create DataFrame from processed articles
            df = pd.DataFrame(articles)
            
            original_count = len(df)
            # Deduplicate by URL first, then title
            if 'url' in df.columns:
                df = df.drop_duplicates(subset=['url'], keep='first')
            url_dedup_count = len(df)
            if 'title' in df.columns:
                df = df.drop_duplicates(subset=['title'], keep='first')
            final_count = len(df)
            
            logger.info(f"Deduplication: {original_count} → {url_dedup_count} (URL) → {final_count} (title)")
            
            # Compute stats on a temp copy before we reduce columns (so we don't lose useful info)
            df_stats = df.copy()
            extraction_stats = df_stats['extraction_status'].value_counts() if 'extraction_status' in df_stats.columns else pd.Series(dtype=int)
            # Top error types (best-effort)
            error_types = pd.Series(dtype=object)
            if 'extraction_error' in df_stats.columns:
                try:
                    error_types = df_stats[df_stats.get('extraction_status', '') == 'failed']['extraction_error'].str.extract(r'Error: ([^-]+)')[0].value_counts()
                except Exception:
                    error_types = pd.Series(dtype=object)
            
            # Add a small rank column to prefer successful extractions when sorting
            df['extraction_status_rank'] = df.get('extraction_status', '').map({'success': 0, 'failed': 1}).fillna(2)
            # Sort primarily by extraction success, then relevance score (if available), then published_date
            sort_cols = ['extraction_status_rank']
            if 'relevance_score' in df.columns:
                sort_cols.append('relevance_score')
            if 'published_date' in df.columns:
                sort_cols.append('published_date')
            df = df.sort_values(sort_cols, ascending=[True, False, False])
            df = df.drop(columns=['extraction_status_rank'], errors='ignore')
            
            # Now select only the requested final columns in the exact order
            final_columns = [
                'source_name',
                'published_date',
                'target_location',
                'location_district',
                'location_state',
                'category',
                'subcategory',
                'title',
                'url',
                'full_text',
                'summary'
            ]
            # Ensure missing columns are added with empty values to avoid errors
            for col in final_columns:
                if col not in df.columns:
                    df[col] = ""
            
            df_final = df[final_columns].copy()
            
            # Write CSV
            df_final.to_csv(fullpath, index=False, encoding='utf-8', quoting=csv.QUOTE_ALL)
            
            total_len = len(df_final)
            stats = {
                'total_articles': total_len,
                'successful_extractions': int(extraction_stats.get('success', 0)),
                'failed_extractions': int(extraction_stats.get('failed', 0)),
                'extraction_success_rate': f"{(int(extraction_stats.get('success', 0)) / total_len * 100):.1f}%" if total_len > 0 else "0.0%",
                'categories_covered': int(df_stats['category'].nunique()) if 'category' in df_stats.columns else 0,
                'avg_relevance_score': float(df_stats['relevance_score'].mean()) if 'relevance_score' in df_stats.columns and total_len > 0 else 0.0,
                'date_range': f"{df_stats['published_date'].min()} to {df_stats['published_date'].max()}" if total_len > 0 and 'published_date' in df_stats.columns else '',
                'top_category': df_stats['category'].mode().iloc[0] if (not df_stats.empty and 'category' in df_stats.columns) else 'N/A',
                'retry_eligible': int(df_stats[df_stats.get('can_retry_extraction', '') == 'yes'].shape[0]) if 'can_retry_extraction' in df_stats.columns else 0,
                'blocked_articles': int(df_stats[df_stats.get('can_retry_extraction', '') == 'no'].shape[0]) if 'can_retry_extraction' in df_stats.columns else 0
            }
            
            logger.info(f"Saved {fullpath} with {len(df_final)} articles")
            logger.info(f"Extraction stats: {stats['successful_extractions']} success, {stats['failed_extractions']} failed ({stats['extraction_success_rate']} success rate)")
            if not error_types.empty:
                logger.info(f"Top error types: {dict(error_types.head(3))}")
            
            return fullpath
            
        except Exception as e:
            logger.error(f"Error saving results: {e}")
            return None

def build_arg_parser():
    p = argparse.ArgumentParser(description="Enhanced multi-source news analysis (location-prioritized)")
    p.add_argument('--urban-csv', default="Urban_list.csv", help="Path to urban list CSV")
    p.add_argument('--days-back', type=int, default=30, help="How many days back to search")
    p.add_argument('--min-relevance', type=int, default=30, help="Minimum relevance score to keep article")
    p.add_argument('--max-locations', type=int, default=None, help="Limit number of locations to process")
    p.add_argument('--sources', type=str, default="google_news,rss_feeds,web_scraping", help="Comma-separated source list (google_news,rss_feeds,web_scraping)")
    p.add_argument('--output-prefix', type=str, default="contextual_news", help="Output directory prefix")
    p.add_argument('--disable-scrapers', action='store_true', help="Disable web scraping targets (only RSS and Google)")
    p.add_argument('--resolve-js', action='store_true', help="Use Playwright to resolve JS-wrapped URLs (slower)")
    p.add_argument('--use-default-keywords', action="store_true", help="Use built-in default keyword list for news search")
    p.add_argument('--keywords', type=str, default=None,
                help=("Custom keywords to use instead of built-in ones. "
                        "Format: keyword_field||CATEGORY||SUBCATEGORY;;keyword2||CAT2||SUB2 "
                        "Within keyword_field you can supply multiple keywords separated by ';' or ',' "
                        "(e.g. \"insurgency; rebellion; armed conflict||SECURITY_AND_CONFLICT||Internal_Conflict\")."))
    return p

def main(argv=None):
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    
    config = {
        'URBAN_LIST_CSV': args.urban_csv,
        'DAYS_BACK': args.days_back,
        'MIN_RELEVANCE': args.min_relevance,
        'MAX_LOCATIONS': args.max_locations,
        'NEWS_SOURCES': [s.strip() for s in args.sources.split(',') if s.strip()],
        'MAX_WORKERS': 3,
        'OUTPUT_DIR_PREFIX': args.output_prefix,
        'RESOLVE_JS': args.resolve_js
    }
    if args.disable_scrapers and 'web_scraping' in config['NEWS_SOURCES']:
        config['NEWS_SOURCES'].remove('web_scraping')
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_dir = f"{config['OUTPUT_DIR_PREFIX']}_{timestamp}"
    os.makedirs(output_dir, exist_ok=True)
    
    log_file = setup_logging(output_dir)
    logger.info(f"Logs will be written to {log_file}")
    logger.info(f"Configuration: {config}")
    
    analyzer = EnhancedNewsAnalyzer(
    urban_list_csv=config['URBAN_LIST_CSV'],
    output_dir=output_dir,
    news_sources=config['NEWS_SOURCES'],
    resolve_js=config['RESOLVE_JS']
    )

    # If custom keywords were provided via --keywords, parse them and override analyzer.cr_indicators
    if args.keywords:
        try:
            custom_input = args.keywords
            entries = [e.strip() for e in custom_input.split(';;') if e.strip()]
            new_cr = {}
            for ent in entries:
                parts = [p.strip() for p in ent.split('||')]
                if len(parts) != 3:
                    logger.warning(f"Skipping malformed keyword entry (expected 3 fields separated by '||'): {ent}")
                    continue
                kw_field, category, subcategory = parts
                # split keyword field on ';' or ',' and strip whitespace
                kws = [k.strip() for k in re.split(r'[;,]', kw_field) if k.strip()]
                if not kws:
                    logger.warning(f"No valid keywords parsed from: {kw_field} in entry: {ent}")
                    continue
                # ensure category/subcategory structure exists and accumulate keywords (deduped, preserve order)
                new_cr.setdefault(category, {})
                existing = new_cr[category].get(subcategory, [])
                existing.extend(kws)
                # dedupe while preserving order
                new_cr[category][subcategory] = list(dict.fromkeys(existing))
            if new_cr:
                analyzer.cr_indicators = new_cr
                logger.info(f"Using custom keywords for analysis. Categories: {list(new_cr.keys())}")
            else:
                logger.warning("No valid custom keyword entries parsed; falling back to built-in keywords.")
        except Exception as e:
            logger.warning(f"Failed to parse --keywords: {e}. Using built-in keywords.")

    if not analyzer.urban_areas:
        logger.error("Could not load urban areas. Please check the CSV file.")
        return

    successful_files = []
    failed_locations = []

    locations_to_process = analyzer.urban_areas[:config['MAX_LOCATIONS']] if config['MAX_LOCATIONS'] else analyzer.urban_areas

    logger.info(f"Starting analysis of {len(locations_to_process)} locations")
    start_time = time.time()

    for i, location in enumerate(locations_to_process, 1):
        logger.info(f"Processing {i}/{len(locations_to_process)}: {location['name']}")
        
        try:
            result_file = analyzer.process_location_enhanced(
                location, config['DAYS_BACK'], config['MIN_RELEVANCE']
            )

            if result_file:
                successful_files.append(result_file)
                logger.info(f"✓ Successfully processed {location['name']} -> {result_file}")
            else:
                failed_locations.append(location['name'])
                logger.warning(f"✗ No articles found for {location['name']}")

        except Exception as e:
            logger.error(f"✗ Failed to process {location['name']}: {e}")
            failed_locations.append(location['name'])

        if i % 10 == 0:
            elapsed = time.time() - start_time
            avg_time = elapsed / i
            estimated_total = avg_time * len(locations_to_process)
            remaining = estimated_total - elapsed
            logger.info(f"Progress: {i}/{len(locations_to_process)} ({i/len(locations_to_process)*100:.1f}%) - "
                       f"ETA: {remaining/60:.1f} minutes")

        time.sleep(1.0)

    total_time = time.time() - start_time
    logger.info(f"\n=== ANALYSIS COMPLETE ===")
    logger.info(f"Total time: {total_time/60:.1f} minutes")
    logger.info(f"Results directory: {output_dir}")
    logger.info(f"Successfully processed: {len(successful_files)} locations")
    logger.info(f"Failed: {len(failed_locations)} locations")
    logger.info(f"Sources used: {config['NEWS_SOURCES']}")
    
    if failed_locations:
        logger.info(f"Failed locations: {failed_locations[:10]}{'...' if len(failed_locations) > 10 else ''}")

    summary_file = os.path.join(output_dir, "analysis_summary.json")
    
    total_articles = 0
    successful_extractions = 0
    failed_extractions = 0
    
    for file_path in successful_files:
        try:
            df_temp = pd.read_csv(file_path)
            total_articles += len(df_temp)
            successful_extractions += len(df_temp[df_temp.get('extraction_status', '') == 'success']) if 'extraction_status' in df_temp.columns else successful_extractions
            failed_extractions += len(df_temp[df_temp.get('extraction_status', '') == 'failed']) if 'extraction_status' in df_temp.columns else failed_extractions
        except Exception:
            continue
    
    summary_data = {
        'analysis_date': datetime.now().isoformat(),
        'configuration': config,
        'results': {
            'total_locations_processed': len(locations_to_process),
            'successful_files': len(successful_files),
            'failed_locations': len(failed_locations),
            'success_rate': (len(successful_files) / len(locations_to_process) * 100) if len(locations_to_process) > 0 else 0,
            'total_articles_collected': total_articles,
            'successful_text_extractions': successful_extractions,
            'failed_text_extractions': failed_extractions,
            'text_extraction_rate': (successful_extractions / total_articles * 100) if total_articles > 0 else 0
        },
        'performance': {
            'total_time_minutes': total_time / 60,
            'avg_time_per_location': total_time / len(locations_to_process) if len(locations_to_process) > 0 else 0
        },
        'data_quality': {
            'articles_with_full_text': successful_extractions,
            'articles_with_metadata_only': failed_extractions,
            'urls_preserved': total_articles,
            'note': 'Final CSV contains only the requested columns; other metadata is used internally.'
        }
    }
    
    with open(summary_file, 'w') as f:
        json.dump(summary_data, f, indent=2)
    
    logger.info(f"Summary report saved: {summary_file}")

if __name__ == "__main__":
    main()