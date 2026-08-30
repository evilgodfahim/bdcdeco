#!/usr/bin/env python3
"""
BD Economics & Finance RSS Feed Processor

Fetches all feeds, deduplicates by link, sends titles to Gemini.
Gemini filters for broad Bangladesh economics and finance news only.
Both Bangla and English titles are evaluated.

Output:  econ_feed.xml
Stats:   econ_stats.json

Changes from previous version:
- Two-stage XML recovery: parse as-is → sanitize → fresh (never silently loses items)
- _sanitize_xml_bytes(): strips forbidden control chars + fixes bare & in URLs
- _safe_text(): escapes & < > in plain text nodes (<link>, <guid>) on write
- errors="replace" on file open guards against encoding corruption
- googlenewsdecoder (PyPI) replaces hand-rolled base64 decoder; retry wrapper
  handles 429s with exponential backoff.
- fetch_all_feeds() now collects all items across all feeds first, then sorts
  globally by parsed pubDate descending before applying age cutoff and cap.
  This ensures Gemini always sees the newest articles regardless of feed order.
- [FIX] Gemini response extraction: safe multi-path text extraction, never
  silently returns "" when response exists; debug logging of raw response.
- [FIX] max_output_tokens: 4096 added to prevent Gemini truncating large batches.
- [FIX] Relaxed HARD RULE: institutional-action titles with hedged language
  (IMF, BB, NBR, Finance Ministry) are SIGNAL, not NOISE.
- [FIX] Rolling seen.json cap (5000 links) — prevents seen-bloat starving Gemini
  of new articles over time.
- [FIX] Exception logging now prints type + full message, not just message.
- [FIX] Gemini batch chunking: splits large article lists into chunks of 80 to
  avoid prompt size issues that cause silent empty responses.
"""

import feedparser
from googlenewsdecoder import new_decoderv1 as _gnews_decoderv1
import html as _html_mod
import json
import os
import re
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
import xml.etree.ElementTree as ET
from google import genai
from email.utils import parsedate_to_datetime
from urllib.parse import urljoin, urlparse
import requests

try:
    from dateutil import parser as dateutil_parser
except Exception:
    dateutil_parser = None

# -- FEEDS ---------------------------------------------------------------------

FEED_URLS = [
    "https://evilgodfahim.github.io/mr/curated_feedb.xml",
    "https://evilgodfahim.github.io/mr/curated_feed.xml",
    "https://evilgodfahim.github.io/mr/curated_feed_edit.xml",
    "https://evilgodfahim.github.io/mr/curated_feed_bdit.xml",
    "https://evilgodfahim.github.io/bdcd/curated_feed.xml",
    "https://evilgodfahim.github.io/bdcdb/curated_feed.xml",
    "https://evilgodfahim.github.io/bdl/final.xml",
    "https://evilgodfahim.github.io/bdlb/final.xml",
    "https://evilgodfahim.github.io/npc/output/merged.xml",
    # Google News — English (BD edition)
    "https://news.google.com/rss/search?q=Bangladesh+economy+OR+economic+OR+GDP+OR+inflation+OR+%22central+bank%22+OR+%22Bangladesh+Bank%22+when:7d&hl=en-BD&gl=BD&ceid=BD:en",
    "https://news.google.com/rss/search?q=Bangladesh+budget+OR+revenue+OR+NBR+OR+tax+OR+export+OR+import+OR+remittance+OR+%22trade+deficit%22+when:7d&hl=en-BD&gl=BD&ceid=BD:en",
    "https://news.google.com/rss/search?q=Bangladesh+DSE+OR+CSE+OR+%22stock+market%22+OR+shares+OR+banking+OR+taka+OR+%22foreign+exchange%22+OR+forex+when:7d&hl=en-BD&gl=BD&ceid=BD:en",
    "https://news.google.com/rss/search?q=Bangladesh+investment+OR+FDI+OR+garments+OR+RMG+OR+EPZ+OR+BIDA+OR+industry+OR+factory+when:7d&hl=en-BD&gl=BD&ceid=BD:en",
    "https://news.google.com/rss/search?q=Bangladesh+IMF+OR+%22World+Bank%22+OR+loan+OR+debt+OR+%22foreign+reserve%22+OR+dollar+OR+ADB+when:7d&hl=en-BD&gl=BD&ceid=BD:en",
    # Google News — Bangla (BD edition)
    "https://news.google.com/rss/search?q=%E0%A6%AC%E0%A6%BE%E0%A6%82%E0%A6%B2%E0%A6%BE%E0%A6%A6%E0%A7%87%E0%A6%B6+%E0%A6%85%E0%A6%B0%E0%A7%8D%E0%A6%A5%E0%A6%A8%E0%A7%80%E0%A6%A4%E0%A6%BF+OR+%E0%A6%AC%E0%A6%BE%E0%A6%9C%E0%A7%87%E0%A6%9F+OR+%E0%A6%B0%E0%A6%BE%E0%A6%9C%E0%A6%B8%E0%A7%8D%E0%A6%AC+OR+%E0%A6%8F%E0%A6%A8%E0%A6%AC%E0%A6%BF%E0%A6%86%E0%A6%B0+OR+%E0%A6%AE%E0%A7%82%E0%A6%B2%E0%A7%8D%E0%A6%AF%E0%A6%B8%E0%A7%8D%E0%A6%AB%E0%A7%80%E0%A6%A4%E0%A6%BF+when:7d&hl=bn-BD&gl=BD&ceid=BD:bn",
    "https://news.google.com/rss/search?q=%E0%A6%AC%E0%A6%BE%E0%A6%82%E0%A6%B2%E0%A6%BE%E0%A6%A6%E0%A7%87%E0%A6%B6+%E0%A6%AC%E0%A7%8D%E0%A6%AF%E0%A6%BE%E0%A6%82%E0%A6%95+OR+%E0%A6%B6%E0%A7%87%E0%A7%9F%E0%A6%BE%E0%A6%B0%E0%A6%AC%E0%A6%BE%E0%A6%9C%E0%A6%BE%E0%A6%B0+OR+%E0%A6%A1%E0%A6%BF%E0%A6%8F%E0%A6%B8%E0%A6%87+OR+%E0%A6%9F%E0%A6%BE%E0%A6%95%E0%A6%BE+OR+%E0%A6%A1%E0%A6%B2%E0%A6%BE%E0%A6%B0+OR+%E0%A6%AC%E0%A7%88%E0%A6%A6%E0%A7%87%E0%A6%B6%E0%A6%BF%E0%A6%95+%E0%A6%AE%E0%A7%81%E0%A6%A6%E0%A7%8D%E0%A6%B0%E0%A6%BE+when:7d&hl=bn-BD&gl=BD&ceid=BD:bn",
    "https://news.google.com/rss/search?q=%E0%A6%AC%E0%A6%BE%E0%A6%82%E0%A6%B2%E0%A6%BE%E0%A6%A6%E0%A7%87%E0%A6%B6+%E0%A6%AC%E0%A6%BF%E0%A6%A8%E0%A6%BF%E0%A7%9F%E0%A7%8B%E0%A6%97+OR+%E0%A6%B0%E0%A6%AA%E0%A7%8D%E0%A6%A4%E0%A6%BE%E0%A6%A8%E0%A6%BF+OR+%E0%A6%86%E0%A6%AE%E0%A6%A6%E0%A6%BE%E0%A6%A8%E0%A6%BF+OR+%E0%A6%B0%E0%A7%87%E0%A6%AE%E0%A6%BF%E0%A6%9F%E0%A7%8D%E0%A6%AF%E0%A6%BE%E0%A6%A8%E0%A7%8D%E0%A6%B8+OR+%E0%A6%AA%E0%A7%8B%E0%A6%B6%E0%A6%BE%E0%A6%95+OR+%E0%A6%B6%E0%A6%BF%E0%A6%B2%E0%A7%8D%E0%A6%AA+when:7d&hl=bn-BD&gl=BD&ceid=BD:bn",
    "https://news.google.com/rss/search?q=%E0%A6%AC%E0%A6%BE%E0%A6%82%E0%A6%B2%E0%A6%BE%E0%A6%A6%E0%A7%87%E0%A6%B6+%E0%A6%86%E0%A6%87%E0%A6%8F%E0%A6%AE%E0%A6%8F%E0%A6%AB+OR+%E0%A6%AC%E0%A6%BF%E0%A6%B6%E0%A7%8D%E0%A6%AC%E0%A6%AC%E0%A7%8D%E0%A6%AF%E0%A6%BE%E0%A6%82%E0%A6%95+OR+%E0%A6%8B%E0%A6%A3+OR+%E0%A6%B0%E0%A6%BF%E0%A6%9C%E0%A6%BE%E0%A6%B0%E0%A7%8D%E0%A6%AD+OR+%E0%A6%AC%E0%A7%88%E0%A6%A6%E0%A7%87%E0%A6%B6%E0%A6%BF%E0%A6%95+%E0%A6%B8%E0%A6%BE%E0%A6%B9%E0%A6%BE%E0%A6%AF%E0%A7%8D%E0%A6%AF+when:7d&hl=bn-BD&gl=BD&ceid=BD:bn",
]

_GNEWS_PREFIXES = (
    "https://news.google.com/rss/articles/",
    "https://news.google.com/read/",
)

KL_API_FEEDS = set()

# -- CONFIG --------------------------------------------------------------------

GEMINI_MODEL          = "gemini-3.6-flash"

SEEN_FILE             = "seen.json"
SELECTED_FILE         = "econ_selected_articles.json"
OUTPUT_XML            = "econ_feed.xml"
EXCLUDED_XML          = "econ_ex.xml"
STATS_FILE            = "econ_stats.json"
MAX_ARTICLES_PER_FEED = 100
MAX_AGE_HOURS         = 25
ALLOW_MISSING_DATES   = True
ALLOW_OLDER           = False
MAX_FEED_ITEMS        = 500
SEEN_ROLLING_CAP      = 5000   # rolling window — prevents seen-bloat
GEMINI_BATCH_SIZE     = 80     # chunk size sent per Gemini call

# -- PROMPT --------------------------------------------------------------------

PROMPT = """ROLE: Binary news classifier. Input is a numbered list of article titles in English or Bengali. Output is a JSON object with one key: "signal", containing a list of 0-based indices of SIGNAL articles.

SIGNAL DEFINITION:
An article is SIGNAL if and only if it reports a concrete, verifiable, national-scale economic or financial development concerning Bangladesh. "Concrete" means a data release, policy decision, regulatory action, or official transaction. "National-scale" means it affects Bangladesh's macroeconomy, a major sector in aggregate, or Bangladesh's position in international finance.

SIGNAL DOMAINS:

Monetary policy — BB policy rate, repo/reverse repo, CRR/SLR, money supply, forex intervention
Exchange rate & reserves — taka rate movement, forex reserve level or trend, BB forex operations
Remittance — aggregate inflow data (monthly/quarterly/annual), BB remittance policy
Inflation — BBS CPI/PPI release, official nationwide inflation figure
Export & import — EPB/BB aggregate trade data, trade balance, current account, BoP figures
Tariff & trade policy — NBR customs/duty changes, trade agreement with national scope
Budget & fiscal policy — national budget, supplementary budget, Finance Minister fiscal statement, NBR tax/VAT structural change, government revenue or expenditure data, fiscal deficit
Public debt — government borrowing programme, T-bill/bond auction results, external debt stock, sovereign bond issuance, debt servicing data
International finance — IMF/World Bank/ADB/IDB/AIIB programme approval, disbursement, or staff-level agreement for Bangladesh; sovereign credit rating change. Includes titles where agreement or progress is strongly implied ("reaches agreement", "concludes review", "unlocks tranche")
FDI — BB/BIDA aggregate FDI inflow or outflow data, national FDI policy change
GDP & macro — BBS GDP/GNI release, national accounts data, per capita income figures
Capital markets — DSE/CSE broad index significant move, BSEC market-wide regulatory decision, circuit breaker, national securities regulation; sovereign or aggregate bond market development
Banking sector systemic — sector-wide NPL data, overall private credit growth, BB prudential regulation affecting all banks, bank merger/nationalisation/licence cancellation, scheduled bank insolvency
Energy & utilities — nationwide fuel price change, electricity or gas tariff adjustment by government or regulator

NOISE — regardless of framing:
Single entity: one bank's earnings, product, branch, CSR, or deposit scheme; one company's revenue, IPO, or investment; one factory's output
Sub-national: city, district, or facility-level event with no stated national aggregate impact
Politics & governance: elections, parliament, cabinet, court, law enforcement — unless the title itself states a direct, named fiscal or monetary consequence
Disaster & crisis: flood, fire, accident — unless title states a quantified national economic impact
Opinion & analysis: editorial, column, forecast, interview, tribute — regardless of subject matter
Human interest: profile, entrepreneur story, award, anniversary

HARD RULE: Classify as NOISE only when a title is entirely speculative with no named actor, institution, or concrete subject matter. Titles that report expected, likely, ongoing, or imminent outcomes from named official bodies — Bangladesh Bank, IMF, NBR, Finance Ministry, BBS, BSEC, EPB, World Bank, ADB — on clearly economic matters ARE SIGNAL. The institutional action is the concrete trigger even when the final outcome is pending. Words like "may", "could", "likely", "expected", "set to", "to raise", "to cut" from a named institution do NOT disqualify a title as SIGNAL. Only classify as NOISE when there is no named institution and no concrete economic subject at all.

OUTPUT FORMAT:
{"signal": [indices]}
Valid JSON only. No markdown. No explanation. No preamble. Empty list if no signal found.

Article titles:
{titles}"""

# -- CONSTANTS -----------------------------------------------------------------

MEDIA_NS = "http://search.yahoo.com/mrss/"
MEDIA_TAG = "{%s}" % MEDIA_NS
ET.register_namespace("media", MEDIA_NS)

BD_TZ = timezone(timedelta(hours=6))

STATS = {
    "per_feed":             {},
    "per_method":           {"KL": 0, "DIRECT": 0},
    "total_fetched":        0,
    "total_passed_age":     0,
    "total_new":            0,
    "total_signal_gemini":  0,
    "total_signal":         0,
    "timestamp":            None,
}

# -- GOOGLE NEWS URL DECODING --------------------------------------------------

def is_google_news_url(url: str) -> bool:
    return any(url.startswith(p) for p in _GNEWS_PREFIXES)


def decode_google_news_url(gnews_url: str, _retries: int = 3) -> str:
    if not gnews_url or not is_google_news_url(gnews_url):
        return gnews_url

    delay = 1.0
    for attempt in range(_retries):
        try:
            result = _gnews_decoderv1(gnews_url, interval=None)
            if result.get("status") and result.get("decoded_url"):
                decoded = result["decoded_url"]
                if decoded.startswith("http"):
                    return decoded

            if attempt < _retries - 1:
                time.sleep(delay)
                delay *= 2

        except Exception as e:
            if attempt < _retries - 1:
                time.sleep(delay)
                delay *= 2
            else:
                print(f"[WARN] gnews decode failed for {gnews_url}: {e}")

    return gnews_url

# -- XML SANITIZATION ----------------------------------------------------------

_CTRL_RE = re.compile(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]')


def _sanitize_xml_bytes(raw: str) -> str:
    raw = _CTRL_RE.sub("", raw)
    raw = re.sub(
        r'&(?!(?:[a-zA-Z][a-zA-Z0-9]*|#[0-9]+|#x[0-9a-fA-F]+);)',
        '&amp;',
        raw,
    )
    return raw


def _safe_text(value: str) -> str:
    if not value:
        return value
    return _html_mod.escape(value, quote=False)

# -- I/O -----------------------------------------------------------------------

def load_seen_links():
    if Path(SEEN_FILE).exists():
        try:
            with open(SEEN_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            links = data.get("links", [])
            # Rolling cap: keep only the most recent N links.
            # Older links drop off, allowing re-evaluation after the window.
            if len(links) > SEEN_ROLLING_CAP:
                links = links[-SEEN_ROLLING_CAP:]
                print(f"[INFO] seen.json trimmed to {SEEN_ROLLING_CAP} entries.")
            return set(links)
        except Exception:
            pass
    return set()


def save_seen_links(seen_links):
    # Always persist sorted so the rolling trim above is stable
    links = sorted(seen_links)
    if len(links) > SEEN_ROLLING_CAP:
        links = links[-SEEN_ROLLING_CAP:]
    with open(SEEN_FILE, "w", encoding="utf-8") as f:
        json.dump({"links": links}, f, indent=2, ensure_ascii=False)


def save_selected_articles(articles):
    existing = []
    if Path(SELECTED_FILE).exists():
        try:
            with open(SELECTED_FILE, "r", encoding="utf-8") as f:
                existing = json.load(f)
        except Exception:
            pass
    existing_links = {a.get("link") for a in existing}
    merged = existing + [a for a in articles if a.get("link") not in existing_links]
    with open(SELECTED_FILE, "w", encoding="utf-8") as f:
        json.dump(merged, f, indent=2, ensure_ascii=False)


def save_stats():
    STATS["timestamp"] = datetime.utcnow().isoformat()
    existing = {}
    if Path(STATS_FILE).exists():
        try:
            with open(STATS_FILE, "r", encoding="utf-8") as f:
                existing = json.load(f)
        except Exception:
            pass
    existing.update(STATS)
    with open(STATS_FILE, "w", encoding="utf-8") as f:
        json.dump(existing, f, indent=2, ensure_ascii=False)

# -- UTILITIES -----------------------------------------------------------------

def normalize_link(link, base=None):
    if not link:
        return ""
    link = link.strip()
    if link.startswith("//"):
        link = "https:" + link
    if base and not urlparse(link).netloc:
        link = urljoin(base, link)
    link = re.sub(r"([?&])utm_[^=]+=[^&]+", r"\1", link)
    link = re.sub(r"([?&])fbclid=[^&]+",    r"\1", link)
    link = re.sub(r"[?&]$", "", link)
    return link.split("#")[0]


def parse_date(entry):
    for key in ("published_parsed", "updated_parsed", "created_parsed", "issued_parsed"):
        st = entry.get(key)
        if st:
            try:
                return datetime.fromtimestamp(time.mktime(st), tz=timezone.utc), False
            except Exception:
                pass

    for key in ("published", "updated", "created", "dc_date", "issued"):
        val = entry.get(key)

        if isinstance(val, str) and val.strip():
            try:
                dt = parsedate_to_datetime(val)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.astimezone(timezone.utc), False
            except Exception:
                pass

            if dateutil_parser:
                try:
                    dt = dateutil_parser.parse(val)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    return dt.astimezone(timezone.utc), False
                except Exception:
                    pass

    if ALLOW_MISSING_DATES:
        return datetime.now(timezone.utc), True

    return None, False


IMG_SRC_RE = re.compile(r'<img[^>]+src=["\']([^"\']+)["\']', re.I)


def find_image_in_html(html_text, base=None):
    if not html_text:
        return None
    m = IMG_SRC_RE.search(html_text)
    if not m:
        return None
    return normalize_link(m.group(1).strip(), base=base)


def get_mime_for_url(url):
    if not url:
        return "image/jpeg"

    path = urlparse(url).path.lower()

    if path.endswith(".png"):
        return "image/png"
    if path.endswith(".gif"):
        return "image/gif"
    if path.endswith(".webp"):
        return "image/webp"
    if path.endswith(".svg"):
        return "image/svg+xml"

    return "image/jpeg"


def extract_image_url(entry, base_link=None):
    mt = entry.get("media_thumbnail")
    if mt:
        if isinstance(mt, list) and mt[0].get("url"):
            return normalize_link(mt[0]["url"], base=base_link)
        if isinstance(mt, dict) and mt.get("url"):
            return normalize_link(mt["url"], base=base_link)

    mc = entry.get("media_content")
    if mc:
        if isinstance(mc, list) and mc[0].get("url"):
            return normalize_link(mc[0]["url"], base=base_link)
        if isinstance(mc, dict) and mc.get("url"):
            return normalize_link(mc["url"], base=base_link)

    enc = entry.get("enclosures")
    if enc and isinstance(enc, list):
        for e in enc:
            href = e.get("href") or e.get("url") or e.get("link")
            typ  = e.get("type", "")
            if href and (
                typ.startswith("image/")
                or re.search(r'\.(jpg|jpeg|png|gif|webp|svg)$', href, re.I)
            ):
                return normalize_link(href, base=base_link)

    links = entry.get("links")
    if links and isinstance(links, list):
        for lnk in links:
            if lnk.get("rel") == "enclosure":
                href = lnk.get("href")
                if href:
                    return normalize_link(href, base=base_link)

    content = entry.get("content")
    if content:
        if isinstance(content, list):
            for c in content:
                if isinstance(c, dict) and c.get("value"):
                    found = find_image_in_html(c.get("value"), base=base_link)
                    if found:
                        return found
        elif isinstance(content, str):
            found = find_image_in_html(content, base=base_link)
            if found:
                return found

    for key in ("summary", "description", "summary_detail", "description_detail"):
        val = entry.get(key)
        if isinstance(val, dict):
            val = val.get("value")

        if isinstance(val, str) and val:
            found = find_image_in_html(val, base=base_link)
            if found:
                return found

    return None

# -- FETCHING ------------------------------------------------------------------

def fetch_via_kl(kl_endpoint, target_feed_url, timeout=20):
    if not kl_endpoint:
        return None

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/xml, text/xml, */*"
    }

    payload = {"url": target_feed_url}

    try:
        resp = requests.post(
            kl_endpoint,
            json=payload,
            headers=headers,
            timeout=timeout,
        )
        if resp.status_code == 200 and resp.text:
            return feedparser.parse(resp.text)
    except Exception:
        pass

    try:
        resp = requests.get(
            kl_endpoint,
            params={"url": target_feed_url},
            headers=headers,
            timeout=timeout,
        )
        if resp.status_code == 200 and resp.text:
            return feedparser.parse(resp.text)
    except Exception:
        pass

    return None


def fetch_feed(url):
    url_norm    = url.strip()
    method_used = "DIRECT"

    if url_norm in KL_API_FEEDS:
        kl_endpoint = os.environ.get("KL")
        feed        = None

        if kl_endpoint:
            feed = fetch_via_kl(kl_endpoint, url_norm)
            if feed:
                method_used = "KL"

        if not feed:
            feed = feedparser.parse(url_norm)
    else:
        feed = feedparser.parse(url_norm)

    entries_count = len(getattr(feed, "entries", []))

    STATS["per_feed"].setdefault(
        url_norm,
        {"fetched": 0, "passed_age": 0, "capped": 0}
    )
    STATS["per_feed"][url_norm]["fetched"] += entries_count

    STATS["per_method"].setdefault(method_used, 0)
    STATS["per_method"][method_used] += entries_count

    STATS["total_fetched"] += entries_count

    return feed


def fetch_all_feeds():
    now          = datetime.now(timezone.utc)
    cutoff       = now - timedelta(hours=MAX_AGE_HOURS)
    all_articles = []

    for url in FEED_URLS:
        feed       = fetch_feed(url)
        feed_items = []

        for e in feed.entries:
            dt, inferred = parse_date(e)

            if not dt:
                continue

            if (not ALLOW_OLDER) and dt < cutoff:
                continue

            desc = ""

            if e.get("summary"):
                desc = e.get("summary")
            elif e.get("description"):
                desc = e.get("description")
            elif e.get("content") and isinstance(e.get("content"), list):
                desc = "\n".join([
                    c.get("value", "")
                    for c in e.get("content")
                    if isinstance(c, dict)
                ])
            else:
                det = e.get("summary_detail") or e.get("description_detail")
                if isinstance(det, dict):
                    desc = det.get("value", "") or ""

            raw_link = normalize_link(e.get("link") or "")

            if is_google_news_url(raw_link):
                real_link = decode_google_news_url(raw_link)
            else:
                real_link = raw_link

            article_id = e.get("id") or real_link or raw_link or ""
            image_url  = extract_image_url(e, base_link=real_link)

            article = {
                "id":          str(article_id),
                "title":       e.get("title", "") or "",
                "link":        real_link,
                "description": desc or "",
                "published":   datetime.strftime(dt, "%a, %d %b %Y %H:%M:%S %z"),
                "source":      url,
                "_dt":         dt,
            }

            if inferred:
                article["published_inferred"] = True

            if image_url:
                article["thumbnail"]      = image_url
                article["thumbnail_type"] = get_mime_for_url(image_url)

            feed_items.append(article)

        passed = len(feed_items)

        STATS["per_feed"][url]["passed_age"] = passed
        STATS["total_passed_age"] += passed

        all_articles.extend(feed_items)

    # Global newest-first ordering
    all_articles.sort(key=lambda a: a["_dt"], reverse=True)

    # Maintain the original per-feed maximum while selecting the newest
    # articles globally.
    per_feed_counts = {}
    selected_articles = []

    for article in all_articles:
        source = article["source"]
        count = per_feed_counts.get(source, 0)

        if count >= MAX_ARTICLES_PER_FEED:
            continue

        selected_articles.append(article)
        per_feed_counts[source] = count + 1

    for url in FEED_URLS:
        STATS["per_feed"][url]["capped"] = per_feed_counts.get(url, 0)

    return selected_articles


def get_new_articles(all_articles, seen_links):
    new = []

    for a in all_articles:
        link = a.get("link")

        if link and link not in seen_links:
            new.append(a)

    return new


def dedup_by_link(articles):
    seen     = set()
    deduped  = []

    for a in articles:
        link = a.get("link", "")

        if link and link not in seen:
            seen.add(link)
            deduped.append(a)
        elif not link:
            deduped.append(a)

    dropped = len(articles) - len(deduped)

    if dropped:
        print(f"Link dedup: removed {dropped} duplicate link(s).")

    return deduped

# -- CLASSIFICATION ------------------------------------------------------------

def extract_signal_indices(text):
    text = text.replace("```json", "").replace("```", "").strip()

    match = re.search(r"\{.*\}", text, flags=re.DOTALL)

    if match:
        try:
            obj = json.loads(match.group(0))

            if isinstance(obj, dict):
                return [
                    i for i in obj.get("signal", [])
                    if isinstance(i, int)
                ]
        except Exception:
            pass

    m = re.search(
        r'"signal"\s*:\s*(\[.*?\])',
        text,
        flags=re.DOTALL
    )

    if m:
        try:
            return [
                i for i in json.loads(m.group(1))
                if isinstance(i, int)
            ]
        except Exception:
            pass

    return []


def _gemini_response_text(response) -> str:
    """
    Safely extract text from a Gemini GenerateContentResponse.

    Gemini SDK can raise on .text if the response was blocked or has no
    text parts (e.g. finish_reason=SAFETY). We try three paths so that
    a blocked response returns "" instead of crashing or silently
    returning all-zeros.
    """
    # Path 1: standard .text property
    try:
        t = response.text
        if t:
            return t
    except Exception:
        pass

    # Path 2: iterate candidates → parts
    try:
        for candidate in response.candidates:
            parts_text = ""
            for part in candidate.content.parts:
                if hasattr(part, "text") and part.text:
                    parts_text += part.text
            if parts_text:
                return parts_text
    except Exception:
        pass

    # Path 3: prompt_feedback / block reason — log and return ""
    try:
        fb = response.prompt_feedback
        if fb:
            print(f"[WARN] Gemini prompt_feedback: {fb}")
    except Exception:
        pass

    return ""


def _classify_batch(client, articles, batch_offset: int) -> list[int]:
    """
    Send one batch of articles to Gemini. Returns absolute indices
    (relative to the full article list, adjusted by batch_offset).
    """
    titles_text = "\n".join([
        f"{i}. {a.get('title', '')}"
        for i, a in enumerate(articles)
    ])

    prompt = PROMPT.replace("{titles}", titles_text)

    response = client.models.generate_content(
        model=GEMINI_MODEL,
        contents=prompt,
        config={
            "response_mime_type": "application/json",
            "max_output_tokens": 4096,
        },
    )

    text = _gemini_response_text(response)

    print(
        f"[DEBUG] Gemini batch offset={batch_offset} "
        f"size={len(articles)} "
        f"response_len={len(text)} "
        f"preview={text[:300]!r}"
    )

    relative_indices = extract_signal_indices(text)

    # Validate and convert to absolute indices
    absolute_indices = []
    for i in relative_indices:
        if 0 <= i < len(articles):
            absolute_indices.append(batch_offset + i)
        else:
            print(f"[WARN] Gemini returned out-of-range index {i} for batch size {len(articles)}")

    return absolute_indices


def classify_with_gemini(articles: list) -> list[int]:
    """
    Classify articles in chunks of GEMINI_BATCH_SIZE.
    Returns a list of absolute signal indices into `articles`.
    """
    api_key = os.environ.get("GEMINI_API_KEY")

    if not api_key:
        print("[ERROR] GEMINI_API_KEY not set.")
        return []

    if not articles:
        return []

    try:
        client = genai.Client(api_key=api_key)
    except Exception as e:
        print(f"[ERROR] Gemini client init failed [{type(e).__name__}]: {e}")
        return []

    all_signal_indices = []
    total_batches = (len(articles) + GEMINI_BATCH_SIZE - 1) // GEMINI_BATCH_SIZE

    for batch_num in range(total_batches):
        start  = batch_num * GEMINI_BATCH_SIZE
        end    = min(start + GEMINI_BATCH_SIZE, len(articles))
        batch  = articles[start:end]

        print(f"  Gemini batch {batch_num + 1}/{total_batches} "
              f"(articles {start}–{end - 1})...")

        try:
            indices = _classify_batch(client, batch, batch_offset=start)
            all_signal_indices.extend(indices)
        except Exception as e:
            print(f"[ERROR] Gemini batch {batch_num + 1} failed "
                  f"[{type(e).__name__}]: {e}")
            # Continue with next batch rather than aborting everything

    return all_signal_indices

# -- XML -----------------------------------------------------------------------

def _fresh_channel(root, feed_title, feed_description):
    channel = ET.SubElement(root, "channel")

    ET.SubElement(channel, "title").text       = feed_title
    ET.SubElement(channel, "link").text        = "https://evilgodfahim.github.io/"
    ET.SubElement(channel, "description").text = feed_description

    return channel


def _load_or_create(output_file, feed_title, feed_description):
    ET.register_namespace("media", MEDIA_NS)

    if Path(output_file).exists():
        try:
            tree    = ET.parse(output_file)
            root    = tree.getroot()
            channel = root.find("channel")

            if channel is not None:
                return tree, root, channel

            channel = _fresh_channel(root, feed_title, feed_description)

            return tree, root, channel

        except ET.ParseError as e:
            print(
                f"[WARN] XML parse failed on first attempt "
                f"({output_file}): {e}"
            )
            print("[INFO] Retrying with sanitized content...")

        try:
            with open(
                output_file,
                "r",
                encoding="utf-8",
                errors="replace"
            ) as fh:
                raw = fh.read()

            clean   = _sanitize_xml_bytes(raw)
            root    = ET.fromstring(clean)
            tree    = ET.ElementTree(root)
            channel = root.find("channel")

            if channel is not None:
                recovered = len(channel.findall("item"))
                print(
                    f"[INFO] Sanitization succeeded — recovered "
                    f"{recovered} existing item(s)."
                )
                return tree, root, channel

            channel = _fresh_channel(root, feed_title, feed_description)

            return tree, root, channel

        except ET.ParseError as e:
            print(
                f"[WARN] XML still unparseable after sanitization "
                f"({output_file}): {e}"
            )
            print(
                "[WARN] Starting a fresh feed — existing items "
                "in this file cannot be recovered."
            )

    root    = ET.Element("rss", {"version": "2.0"})
    tree    = ET.ElementTree(root)
    channel = _fresh_channel(root, feed_title, feed_description)

    return tree, root, channel


def generate_xml_feed(
    articles,
    output_file,
    feed_title=None,
    feed_description=None
):
    feed_title = feed_title or "BD Economics & Finance"
    feed_description = (
        feed_description
        or "AI-curated Bangladesh economics and finance news"
    )

    tree, root, channel = _load_or_create(
        output_file,
        feed_title,
        feed_description
    )

    existing_links = set()

    for item in channel.findall("item"):
        link_el = item.find("link")

        if link_el is not None and link_el.text:
            existing_links.add(link_el.text.strip())

    added = 0

    for a in articles:
        link = (a.get("link") or "").strip()

        if not link or link in existing_links:
            continue

        item = ET.SubElement(channel, "item")

        ET.SubElement(item, "title").text = (
            a.get("title", "") or ""
        )

        ET.SubElement(item, "description").text = (
            a.get("description", "") or ""
        )

        ET.SubElement(item, "link").text = _safe_text(link)

        guid_val = a.get("id") or link
        is_permalink = (
            "true"
            if guid_val.startswith("http")
            else "false"
        )

        ET.SubElement(
            item,
            "guid",
            {"isPermaLink": is_permalink}
        ).text = _safe_text(guid_val)

        if a.get("published"):
            ET.SubElement(item, "pubDate").text = a["published"]

        thumb = a.get("thumbnail")

        if thumb:
            ET.SubElement(
                item,
                MEDIA_TAG + "thumbnail",
                {"url": thumb}
            )

            mime = (
                a.get("thumbnail_type")
                or get_mime_for_url(thumb)
            )

            ET.SubElement(
                item,
                "enclosure",
                {
                    "url": thumb,
                    "type": mime,
                    "length": "0",
                }
            )

        existing_links.add(link)
        added += 1

    all_items = channel.findall("item")
    overflow  = len(all_items) - MAX_FEED_ITEMS

    if overflow > 0:
        for old_item in all_items[:overflow]:
            channel.remove(old_item)

    now_text = datetime.utcnow().strftime(
        "%a, %d %b %Y %H:%M:%S +0000"
    )

    last_build = channel.find("lastBuildDate")

    if last_build is None:
        ET.SubElement(
            channel,
            "lastBuildDate"
        ).text = now_text
    else:
        last_build.text = now_text

    try:
        ET.indent(tree, space="  ")
    except AttributeError:
        pass

    tree.write(
        output_file,
        encoding="unicode",
        xml_declaration=False
    )

    with open(
        output_file,
        "r+",
        encoding="utf-8"
    ) as fh:
        body = fh.read()
        fh.seek(0)

        fh.write(
            '<?xml version="1.0" encoding="UTF-8"?>\n'
            + body
        )

        fh.truncate()

    print(
        f"  → {added} new item(s) written to {output_file} "
        f"(total in feed: {len(channel.findall('item'))})"
    )

    return added

# -- STATS ---------------------------------------------------------------------

def print_stats():
    print("\nFetch statistics:")
    print(f"  Timestamp:        {STATS.get('timestamp')}")
    print(f"  Total fetched:    {STATS['total_fetched']}")
    print(
        f"  Passed age cut:   {STATS['total_passed_age']}  "
        f"(within {MAX_AGE_HOURS}h)"
    )
    print(f"  New (unseen):     {STATS['total_new']}")
    print(
        f"  Signal (Gemini):  {STATS['total_signal']}"
        f"  -> {OUTPUT_XML}"
    )

    print("  Per-method:")

    for method, cnt in STATS["per_method"].items():
        print(f"    {method}: {cnt}")

    print("  Per-feed:")

    for feed, d in STATS["per_feed"].items():
        print(f"    {feed}")
        print(
            f"      fetched={d.get('fetched',0)} "
            f"passed_age={d.get('passed_age',0)} "
            f"capped={d.get('capped',0)}"
        )

    print("")

# -- MAIN ----------------------------------------------------------------------

def main():
    seen_links   = load_seen_links()
    all_articles = fetch_all_feeds()
    new_articles = get_new_articles(all_articles, seen_links)
    new_articles = dedup_by_link(new_articles)

    for a in new_articles:
        a.pop("_dt", None)

    STATS["total_new"] = len(new_articles)

    print(
        f"Sending {len(new_articles)} article(s) "
        f"to Gemini for BD economics/finance filtering..."
    )

    gemini_indices = classify_with_gemini(new_articles)

    gemini_indices = [
        i for i in gemini_indices
        if 0 <= i < len(new_articles)
    ]

    STATS["total_signal_gemini"] = len(gemini_indices)
    STATS["total_signal"]        = len(gemini_indices)

    for a in new_articles:
        link = a.get("link")
        if link:
            seen_links.add(link)

    save_seen_links(seen_links)

    if not gemini_indices:
        print(
            "Gemini returned no signal indices. "
            "Skipping XML writes."
        )
        print_stats()
        return

    signal_indices_set = set(gemini_indices)

    signal_articles = [
        new_articles[i]
        for i in gemini_indices
    ]

    excluded_articles = [
        new_articles[i]
        for i in range(len(new_articles))
        if i not in signal_indices_set
    ]

    generate_xml_feed(
        signal_articles,
        output_file=OUTPUT_XML,
        feed_title="BD Economics & Finance",
        feed_description=(
            "AI-curated Bangladesh national economics "
            "and finance news"
        ),
    )

    generate_xml_feed(
        excluded_articles,
        output_file=EXCLUDED_XML,
        feed_title="Excluded (BD Economics Filter)",
        feed_description=(
            "Articles excluded by BD economics "
            "and finance filter"
        ),
    )

    save_selected_articles(signal_articles)

    STATS["timestamp"] = datetime.utcnow().isoformat()
    save_stats()

    print_stats()


if __name__ == "__main__":
    main()
