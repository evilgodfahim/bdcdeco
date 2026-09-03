#!/usr/bin/env python3
"""
BD Economics & Finance RSS Feed Processor

Fetches all feeds, deduplicates by link, sends titles to Mistral.
Mistral filters for broad Bangladesh economics and finance news only.
Both Bangla and English titles are evaluated.

Output:  econ_feed.xml
Stats:   econ_stats.json
"""

import feedparser
from googlenewsdecoder import new_decoderv1 as _gnews_decoderv1
from mistralai.client import Mistral
import html as _html_mod
import json
import os
import re
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
import xml.etree.ElementTree as ET
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
    "https://news.google.com/rss/search?q=%E0%A6%AC%E0%A6%BE%E0%A6%82%E0%A6%B2%E0%A6%BE%E0%A6%A6%E0%A7%87%E0%A6%B6+%E0%A6%85%E0%A6%B0%E0%A7%8D%E0%A6%A5%E0%A6%A8%E0%A7%80%E0%A6%A4%E0%A6%BF+OR+%E0%A6%AC%E0%A6%BE%E0%A6%9C%E0%A7%87%E0%A6%9F+OR+%E0%A6%B0%E0%A6%BE%E0%A6%9C%E0%A6%B8%E0%A7%8D%E0%A6%AC+OR+%E0%A6%8F%E0%A6%A8%E0%A6%AC%E0%A6%BF%E0%A6%86%E0%A6%B0+OR+%E0%A6%AE%E0%A7%82%E0%A6%B2%E0%A7%8D%E0%A6%AF%E0%A6%BB%E0%A6%B5%E0%A7%80%E0%A6%A4%E0%A6%BF+when:7d&hl=bn-BD&gl=BD&ceid=BD:bn",
    "https://news.google.com/rss/search?q=%E0%A6%AC%E0%A6%BE%E0%A6%82%E0%A6%B2%E0%A6%BE%E0%A6%A6%E0%A7%87%E0%A6%B6+%E0%A6%AC%E0%A7%8D%E0%A6%AF%E0%A6%BE%E0%A6%82%E0%A6%95+OR+%E0%A6%B6%E0%A7%87%E0%A6%AF%E0%A6%BC%E0%A6%BE%E0%A6%B0%E0%A6%AC%E0%A6%BE%E0%A6%9C%E0%A6%BE%E0%A6%B0+OR+%E0%A6%A1%E0%A6%BF%E0%A6%8F%E0%A6%B8%E0%A6%87+OR+%E0%A6%9F%E0%A6%BE%E0%A6%95%E0%A6%BE+OR+%E0%A6%A1%E0%A6%B2%E0%A6%BE%E0%A6%B0+OR+%E0%A6%AC%E0%A7%88%E0%A6%A6%E0%A7%87%E0%A6%B6%E0%A6%BF%E0%A6%95+%E0%A6%AE%E0%A7%81%E0%A6%A6%E0%A7%8D%E0%A6%B0%E0%A6%BE+when:7d&hl=bn-BD&gl=BD&ceid=BD:bn",
    "https://news.google.com/rss/search?q=%E0%A6%AC%E0%A6%BE%E0%A6%82%E0%A6%B2%E0%A6%BE%E0%A6%A6%E0%A7%87%E0%A6%B6+%E0%A6%AC%E0%A6%BF%E0%A6%A8%E0%A6%BF%E0%A6%AF%E0%A6%BC%E0%A7%8B%E0%A6%97+OR+%E0%A6%B0%E0%A6%AA%E0%A7%8D%E0%A6%A4%E0%A6%BE%E0%A6%A8%E0%A6%BF+OR+%E0%A6%86%E0%A6%AE%E0%A6%A6%E0%A6%BE%E0%A6%A8%E0%A6%BF+OR+%E0%A6%B0%E0%A7%87%E0%A6%AE%E0%A6%BF%E0%A6%9C%E0%A6%BC%E0%A6%BE%E0%A6%B8%E0%A7%8D%E0%A6%B8+OR+%E0%A6%AA%E0%A7%8B%E0%A6%B6%E0%A6%BE%E0%A6%95+OR+%E0%A6%B6%E0%A6%BF%E0%A6%B2%E0%A7%8D%E0%A6%AA+when:7d&hl=bn-BD&gl=BD&ceid=BD:bn",
    "https://news.google.com/rss/search?q=%E0%A6%AC%E0%A6%BE%E0%A6%82%E0%A6%B2%E0%A6%BE%E0%A6%A6%E0%A7%87%E0%A6%B6+%E0%A6%86%E0%A6%87%E0%A6%8F%E0%A6%AE%E0%A6%8F%E0%A6%AB+OR+%E0%A6%AC%E0%A6%BF%E0%A6%B6%E0%A7%8D%E0%A6%AC%E0%A6%AC%E0%A7%8D%E0%A6%AF%E0%A6%BE%E0%A6%82%E0%A6%95+OR+%E0%A6%8B%E0%A6%A3+OR+%E0%A6%B0%E0%A6%BF%E0%A6%9C%E0%A6%BE%E0%A6%B0%E0%A7%8D%E0%A6%B2+OR+%E0%A6%AC%E0%A7%88%E0%A6%A6%E0%A7%87%E0%A6%B6%E0%A6%BF%E0%A6%95+%E0%A6%B8%E0%A6%BE%E0%A6%B9%E0%A6%BE%E0%A6%AF%E0%A7%8D%E0%A6%AF+when:7d&hl=bn-BD&gl=BD&ceid=BD:bn",
]

_GNEWS_PREFIXES = (
    "https://news.google.com/rss/articles/",
    "https://news.google.com/read/",
)

KL_API_FEEDS = set()

# -- CONFIG --------------------------------------------------------------------

MISTRAL_MODEL         = "mistral-medium-latest"

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

# -- PROMPT --------------------------------------------------------------------

PROMPT = """ROLE: Strictly classify news headlines in English or Bengali for Bangladesh Macroeconomics and Finance.
Output must be valid JSON only — no markdown formatting, no explanation.

TASK: Identify 0-based indices of SIGNAL articles while strictly excluding NOISE and DEDUPLICATING near-identical coverage.

GUIDELINES FOR SIGNAL:
- Sector-wide or macro significance to Bangladesh's economy/finance:
  1. Central Bank & Monetary Policy: Bangladesh Bank interest rates, policy rate, repo, CRR/SLR, money supply, banking rules.
  2. Currency & Reserves: Taka-dollar exchange rates, foreign exchange reserves, forex market interventions.
  3. External Sector: Remittances, export-import performance, RMG/garments trade, trade deficit, balance of payments.
  4. Fiscal Policy & Taxes: National budget, NBR revenue collection, customs duties, tariffs, VAT, public debt, government borrowing/bonds.
  5. Macro Indicators: GDP growth, CPI inflation, cost of living index, foreign debt, IMF/World Bank/ADB loans or reviews.
  6. Financial & Capital Markets: DSE/CSE stock indices, BSEC capital market regulations, systemic banking sector health (NPLs, liquidity).
  7. Sector-level Infrastructure & Energy: Fuel/gas/electricity tariff adjustments, major national industrial investment policy (BIDA/BEPZA/EPZ).
  8. Expert Macro Analysis: Comprehensive economic outlooks, trade policy analysis, or national economic forecasts.

GUIDELINES FOR NOISE (STRICT EXCLUSIONS):
- Micro-level business routine: Individual company product launches, single branch openings, private business award ceremonies, or minor corporate promotions.
- Localized retail price shifts: Everyday price shifts at local markets without direct government tariff/policy intervention.
- Non-economic topics: Sports, entertainment, crime, political speeches/arguments, local court cases, or accidents without structural economic implications.
- Purely global news with no direct operational or fiscal impact on Bangladesh.

DEDUPLICATION / DEDUCTION RULE (STRICT):
- When multiple articles report on the EXACT SAME news story, event, or announcement (whether across English, Bengali, or different media outlets), select ONLY ONE representative index (preferably the most clear or detailed headline) and OMIT all near-duplicate indices.

DEFAULT RULE: If a headline directly touches Bangladesh's broader economic, monetary, financial, or industrial framework, mark it as SIGNAL unless it duplicates an already selected item.

EXAMPLES:

Example 1:
Input:
0. বাংলাদেশ ব্যাংক নীতি সুদহার ২৫ বেসিস পয়েন্ট বাড়াল
1. Bangladesh Bank raises policy rate by 25 basis points
2. স্কয়ার ফার্মাসিউটিক্যালস সিলেটে নতুন সেলস সেন্টারের উদ্বোধন করল
3. Bangladesh's forex reserves cross $20 billion amid remittance surge
4. রেমিট্যান্সের তোড়ে বৈদেশিক মুদ্রার রিজার্ভ ২০ বিলিয়ন ডলার অতিক্রম করল
5. সাকিব আল হাসান নতুন ব্র্যান্ড অ্যাম্বাসেডর হিসেবে চুক্তিবদ্ধ
6. NBR mandates electronic fiscal devices for all medium retail outlets
7. ঢাকা বিশ্ববিদ্যালয়ে অর্থনীতি বিভাগের নবীন বরণ অনুষ্ঠিত
Output: {"signal": [0, 3, 6]}
(Note: Index 1 duplicates Index 0; Index 4 duplicates Index 3; both are omitted.)

Example 2:
Input:
0. DSE benchmark index drops 45 points amid bank stock sell-off
1. ডিএসইর সূচক ৪৫ পয়েন্ট কমেছে
2. IMF releases third tranche of $1.15 billion loan to Bangladesh
3. আইএমএফের ১.১৫ বিলিয়ন ডলারের তৃতীয় কিস্তি অনুমোদন
4. চট্টগ্রাম বন্দরে নতুন ভিআইপি রেস্টহাউস উদ্বোধন
5. Inflation eases to 9.2% in August as food prices stabilize
6. আগস্টে মূল্যস্ফীতি কমে ৯.২ শতাংশে নেমেছে
7. স্থানীয় কাঁচাবাজারে আলুর দাম কেজিতে ৫ টাকা বাড়ল
8. Commerce Ministry reduces import duty on crude palm oil by 10%
Output: {"signal": [0, 2, 5, 8]}
(Note: Indices 1, 3, and 6 are near-duplicates of 0, 2, and 5 respectively.)

Example 3:
Input:
0. World Bank projects Bangladesh GDP growth at 5.6% for FY25
1. বিশ্বব্যাংক বাংলাদেশের জিডিপি প্রবৃদ্ধি ৫.৬% হতে পারে বলে পূর্বাভাস দিয়েছে
2. BSEC imposes circuit breaker on 5 underperforming stocks
3. Beximco Pharma inaugurates new manufacturing plant in Gazipur
4. Remittance inflow hits $2.2 billion in July, up 15% YoY
5. জুলাইয়ে রেমিট্যান্স এসেছে ২.২ বিলিয়ন ডলার
6. Bangladesh wins cricket match against Sri Lanka by 5 wickets
Output: {"signal": [0, 2, 4]}
(Note: Index 1 duplicates Index 0; Index 5 duplicates Index 4.)

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
    "total_signal_mistral": 0,
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

# -- I/O -----------------------------------------------------------------------

def load_seen_links():
    if Path(SEEN_FILE).exists():
        try:
            with open(SEEN_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            return set(data.get("links", []))
        except Exception:
            pass
    return set()


def save_seen_links(seen_links):
    with open(SEEN_FILE, "w", encoding="utf-8") as f:
        json.dump({"links": sorted(seen_links)}, f, indent=2, ensure_ascii=False)


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
    if path.endswith(".png"):  return "image/png"
    if path.endswith(".gif"):  return "image/gif"
    if path.endswith(".webp"): return "image/webp"
    if path.endswith(".svg"):  return "image/svg+xml"
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
            if href and (typ.startswith("image/") or re.search(r'\.(jpg|jpeg|png|gif|webp|svg)$', href, re.I)):
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
    headers = {"Content-Type": "application/json", "Accept": "application/xml, text/xml, */*"}
    payload = {"url": target_feed_url}
    try:
        resp = requests.post(kl_endpoint, json=payload, headers=headers, timeout=timeout)
        if resp.status_code == 200 and resp.text:
            return feedparser.parse(resp.text)
    except Exception:
        pass
    try:
        resp = requests.get(kl_endpoint, params={"url": target_feed_url}, headers=headers, timeout=timeout)
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
    STATS["per_feed"].setdefault(url_norm, {"fetched": 0, "passed_age": 0, "capped": 0})
    STATS["per_feed"][url_norm]["fetched"] += entries_count
    STATS["per_method"].setdefault(method_used, 0)
    STATS["per_method"][method_used] += entries_count
    STATS["total_fetched"]            += entries_count

    return feed


def fetch_all_feeds():
    now        = datetime.now(timezone.utc)
    cutoff     = now - timedelta(hours=MAX_AGE_HOURS)
    bd_now     = datetime.now(BD_TZ)
    bd_now_str = bd_now.strftime("%a, %d %b %Y %H:%M:%S +0600")

    raw_items = []

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
                desc = "\n".join([c.get("value", "") for c in e.get("content") if isinstance(c, dict)])
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
                "published":   bd_now_str,
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
        STATS["total_passed_age"]           += passed
        raw_items.extend(feed_items)

    raw_items.sort(key=lambda a: a["_dt"], reverse=True)

    pre_cap       = MAX_ARTICLES_PER_FEED * len(FEED_URLS)
    all_articles  = raw_items[:pre_cap]

    included_sources: dict[str, int] = {}
    for a in all_articles:
        included_sources[a["source"]] = included_sources.get(a["source"], 0) + 1
    for url in FEED_URLS:
        STATS["per_feed"][url]["capped"] = included_sources.get(url, 0)

    return all_articles


def get_new_articles(all_articles, seen_links):
    new = []
    for a in all_articles:
        link = a.get("link")
        if link and link not in seen_links:
            new.append(a)
    return new


def dedup_by_link(articles):
    seen    = set()
    deduped = []
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
                return [i for i in obj.get("signal", []) if isinstance(i, int)]
        except Exception:
            pass
    m = re.search(r'"signal"\s*:\s*(\[.*?\])', text, flags=re.DOTALL)
    if m:
        try:
            return [i for i in json.loads(m.group(1)) if isinstance(i, int)]
        except Exception:
            pass
    return []


def send_to_mistral(articles):
    api_key = os.environ.get("MS")
    if not api_key or not articles:
        return []

    try:
        client = Mistral(api_key=api_key)
        titles_text = "\n".join([f"{i}. {a.get('title', '')}" for i, a in enumerate(articles)])
        prompt = PROMPT.replace("{titles}", titles_text)

        response = client.chat.complete(
            model=MISTRAL_MODEL,
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            temperature=0.1,
        )

        text = response.choices[0].message.content or ""
        return extract_signal_indices(text)

    except Exception as e:
        print(f"Mistral classification error: {e}")
        return []

# -- XML -----------------------------------------------------------------------

def _fresh_channel(root, feed_title, feed_description):
    channel = ET.SubElement(root, "channel")
    ET.SubElement(channel, "title").text       = feed_title
    ET.SubElement(channel, "link").text        = "https://evilgodfahim.github.io/"
    ET.SubElement(channel, "description").text = feed_description
    ET.SubElement(channel, "lastBuildDate").text = datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S +0000")
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
            print(f"[WARN] XML parse failed on first attempt ({output_file}): {e}")
            print("[INFO] Retrying with sanitized content…")

        try:
            with open(output_file, "r", encoding="utf-8", errors="replace") as fh:
                raw = fh.read()

            clean   = _sanitize_xml_bytes(raw)
            root    = ET.fromstring(clean)
            tree    = ET.ElementTree(root)
            channel = root.find("channel")

            if channel is not None:
                recovered = len(channel.findall("item"))
                print(f"[INFO] Sanitization succeeded — recovered {recovered} existing item(s).")
                return tree, root, channel

            channel = _fresh_channel(root, feed_title, feed_description)
            return tree, root, channel

        except ET.ParseError as e:
            print(f"[WARN] XML still unparseable after sanitization ({output_file}): {e}")
            print("[WARN] Starting a fresh feed — existing items in this file cannot be recovered.")

    root    = ET.Element("rss", {"version": "2.0"})
    tree    = ET.ElementTree(root)
    channel = _fresh_channel(root, feed_title, feed_description)
    return tree, root, channel


def generate_xml_feed(articles, output_file, feed_title=None, feed_description=None):
    feed_title       = feed_title       or "BD Economics & Finance"
    feed_description = feed_description or "AI-curated Bangladesh economics and finance news"

    tree, root, channel = _load_or_create(output_file, feed_title, feed_description)

    existing_links: set[str] = set()
    for item in channel.findall("item"):
        link_el = item.find("link")
        if link_el is not None and link_el.text:
            existing_links.add(link_el.text.strip())

    first_item_idx = None
    for idx, child in enumerate(list(channel)):
        if child.tag == "item":
            first_item_idx = idx
            break

    added = 0
    for a in articles:
        link = (a.get("link") or "").strip()
        if not link or link in existing_links:
            continue

        item = ET.Element("item")

        ET.SubElement(item, "title").text       = a.get("title", "") or ""
        ET.SubElement(item, "description").text = a.get("description", "") or ""

        ET.SubElement(item, "link").text = link

        guid_val     = a.get("id") or link
        is_permalink = "true" if guid_val.startswith("http") else "false"
        ET.SubElement(item, "guid", {"isPermaLink": is_permalink}).text = guid_val

        if a.get("published"):
            ET.SubElement(item, "pubDate").text = a["published"]

        thumb = a.get("thumbnail")
        if thumb:
            ET.SubElement(item, MEDIA_TAG + "thumbnail", {"url": thumb})
            mime = a.get("thumbnail_type") or get_mime_for_url(thumb)
            ET.SubElement(item, "enclosure", {"url": thumb, "type": mime, "length": "0"})

        if first_item_idx is None:
            channel.append(item)
        else:
            channel.insert(first_item_idx + added, item)

        existing_links.add(link)
        added += 1

    all_items = channel.findall("item")
    overflow  = len(all_items) - MAX_FEED_ITEMS
    if overflow > 0:
        for old_item in all_items[MAX_FEED_ITEMS:]:
            channel.remove(old_item)

    now_text   = datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S +0000")
    last_build = channel.find("lastBuildDate")
    if last_build is None:
        lb = ET.Element("lastBuildDate")
        lb.text = now_text
        channel.insert(3, lb)
    else:
        last_build.text = now_text

    try:
        ET.indent(tree, space="  ")
    except AttributeError:
        pass

    tree.write(output_file, encoding="unicode", xml_declaration=False)
    with open(output_file, "r+", encoding="utf-8") as fh:
        body = fh.read()
        fh.seek(0)
        fh.write('<?xml version="1.0" encoding="UTF-8"?>\n' + body)
        fh.truncate()

    print(f"  → {added} new item(s) written to {output_file}  "
          f"(total in feed: {len(channel.findall('item'))})")
    return added

# -- STATS ---------------------------------------------------------------------

def print_stats():
    print("\nFetch statistics:")
    print(f"  Timestamp:        {STATS.get('timestamp')}")
    print(f"  Total fetched:    {STATS['total_fetched']}")
    print(f"  Passed age cut:   {STATS['total_passed_age']}  (within {MAX_AGE_HOURS}h)")
    print(f"  New (unseen):     {STATS['total_new']}")
    print(f"  Signal (Mistral): {STATS['total_signal_mistral']}  -> {OUTPUT_XML}")
    print("  Per-method:")
    for method, cnt in STATS["per_method"].items():
        print(f"    {method}: {cnt}")
    print("  Per-feed:")
    for feed, d in STATS["per_feed"].items():
        print(f"    {feed}")
        print(f"      fetched={d.get('fetched',0)}  passed_age={d.get('passed_age',0)}  capped={d.get('capped',0)}")
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
    print(f"Sending {len(new_articles)} article(s) to Mistral for BD economics/finance filtering…")

    mistral_indices = send_to_mistral(new_articles)
    mistral_indices = [i for i in mistral_indices if 0 <= i < len(new_articles)]

    STATS["total_signal_mistral"] = len(mistral_indices)
    STATS["total_signal"]         = len(mistral_indices)

    for a in new_articles:
        link = a.get("link")
        if link:
            seen_links.add(link)
    save_seen_links(seen_links)

    signal_articles   = [new_articles[i] for i in mistral_indices]
    excluded_articles = [new_articles[i] for i in range(len(new_articles)) if i not in set(mistral_indices)]

    if signal_articles:
        generate_xml_feed(
            signal_articles,
            output_file=OUTPUT_XML,
            feed_title="BD Economics & Finance",
            feed_description="AI-curated Bangladesh national economics and finance news",
        )
        save_selected_articles(signal_articles)
    else:
        print("Mistral returned no signal indices for main feed.")

    if excluded_articles:
        generate_xml_feed(
            excluded_articles,
            output_file=EXCLUDED_XML,
            feed_title="Excluded (BD Economics Filter)",
            feed_description="Articles excluded by BD economics and finance filter",
        )

    STATS["timestamp"] = datetime.utcnow().isoformat()
    save_stats()
    print_stats()


if __name__ == "__main__":
    main()