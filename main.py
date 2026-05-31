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
import json
import os
import time
import re
from datetime import datetime, timezone, timedelta
from pathlib import Path
import xml.etree.ElementTree as ET
from mistralai.client import Mistral
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
]

KL_API_FEEDS = set()

# -- CONFIG --------------------------------------------------------------------

MISTRAL_MODEL         = "mistral-large-latest"

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

PROMPT = """You are a strict news classification engine. Input: numbered article titles from Bangladeshi and international news outlets. Titles may be in Bengali (Bangla) or English — evaluate both equally. Classify each as SIGNAL or NOISE. Return only SIGNAL indices.

TASK: Select only articles covering broad, national-level Bangladesh economics and finance news. The bar is HIGH.

STEP 1 — INSTANT NOISE. Mark as NOISE immediately if any of:
  - Sports, entertainment, celebrity, lifestyle, health, or human interest
  - Politics, governance, elections, law and order — unless a direct and explicit economic consequence is stated in the title itself
  - Crime, accident, fire, flood, disaster — unless it triggers a stated national economic impact
  - Opinion columns, editorials, tributes, anniversaries, or analysis without concrete data or decisions
  - Any single-entity story: one bank's product launch, deposit scheme, CSR event, or branch opening; one company's earnings, revenue, or IPO; one factory's output; one entrepreneur's story; one NGO's program
  - District-level, city-level, or sub-national economic events with no stated national significance

STEP 2 — SCOPE CHECK. SIGNAL only if ALL three are true:
  (a) Bangladesh-relevant: directly concerns Bangladesh's economy, finance, monetary or fiscal affairs
  (b) National scope: affects the whole country, a major sector, or Bangladesh's position in international economics
  (c) Concrete: a real event, data release, policy decision, or official action — not a speculative feature or profile

SIGNAL categories:
  - Bangladesh Bank: policy rate changes, monetary policy decisions, reserve requirements, forex interventions
  - Foreign exchange: taka exchange rate moves, forex reserve levels and trends, currency policy
  - Inflation: CPI data releases, official inflation figures, nationwide price changes
  - Trade: aggregate export/import data, trade balance, tariff or trade policy changes
  - Remittance: national inflow data, Bangladesh Bank remittance policy, overall trends
  - Budget and fiscal policy: national budget, supplementary budget, NBR tax/VAT decisions, fiscal deficit figures
  - Public debt: government borrowing programme, sovereign bonds, debt-to-GDP figures, debt restructuring
  - International finance: IMF/World Bank/ADB/IDB programmes and loan approvals for Bangladesh, credit rating changes
  - Capital markets: DSE/CSE broad index moves, market-wide circuit breakers, BSEC regulatory decisions affecting the whole market
  - Sectoral aggregate data: total RMG export figures, banking sector NPL ratio, overall FDI data, total private sector credit
  - Energy and utilities: nationwide fuel price changes, electricity or gas tariff adjustments at national level
  - Economic reform and regulation: major national policy reforms, privatisation or nationalisation, broad banking sector regulation

NOISE examples:
  - "Dutch-Bangla Bank launches new savings product"
  - "Robi records 15% revenue growth in Q3"
  - "Garment worker dies in Ashulia factory fire"
  - "Why Bangladesh Must Reform Its Tax System" (opinion)
  - "Ctg port handles record containers in April" (sub-national, single facility)
  - "Small entrepreneur from Rajshahi builds export business" (human interest)

LANGUAGE NOTE: Bengali-language titles must be evaluated by the same criteria.
Key Bengali economic/finance terms: অর্থনীতি, মূল্যস্ফীতি, রপ্তানি, আমদানি, রেমিট্যান্স, বাজেট, বাংলাদেশ ব্যাংক, মুদ্রানীতি, বিনিময় হার, রিজার্ভ, জিডিপি, ঋণ, রাজস্ব, বিনিয়োগ, শেয়ারবাজার, পুঁজিবাজার, ডলার, টাকা, সুদের হার, ব্যাংক খাত, এনপিএল, বৈদেশিক মুদ্রা, আইএমএফ, বিশ্বব্যাংক, মুদ্রাস্ফীতি, বৈদেশিক ঋণ, রাজকোষ, শুল্ক, ভ্যাট, এনবিআর

WHEN IN DOUBT → NOISE.

Output only: {{"signal": [0-based indices]}}. Valid JSON, no markdown, no explanation.

EXAMPLES:

Input:
0. Bangladesh Bank raises policy rate by 50 basis points to curb inflation
1. Grameenphone launches new data bundle for rural users
2. বাংলাদেশের বৈদেশিক মুদ্রার রিজার্ভ ২০ বিলিয়নের নিচে নামল
3. Dutch-Bangla Bank wins CSR award at annual banking summit
4. রপ্তানি আয়ে ১২ শতাংশ প্রবৃদ্ধি, পোশাক খাতে রেকর্ড
5. Government to privatise state-owned jute mills under reform programme
6. জাতীয় রাজস্ব বোর্ড ভ্যাট কাঠামোয় বড় পরিবর্তন আনছে
7. IMF approves $700m tranche for Bangladesh under ECF programme
8. Local entrepreneur builds export business from Rajshahi village
9. DSE general index falls 4% in single session, circuit breaker triggered
10. Bangladesh current account deficit widens to record $8bn
11. Premier Bank opens 10 new branches across the country
12. টাকার বিপরীতে ডলারের দাম আরও বাড়ল, নতুন রেকর্ড
13. কেন্দ্রীয় ব্যাংক সুদের হার বাড়াল, মূল্যস্ফীতি নিয়ন্ত্রণে পদক্ষেপ
14. Dhaka court sentences former minister for corruption
15. Bangladesh foreign debt crosses $100bn for first time
Output: {{"signal": [0, 2, 4, 5, 6, 7, 9, 10, 12, 13, 15]}}

Input:
0. রেমিট্যান্স প্রবাহ কমেছে, টাকার উপর চাপ বাড়ছে
1. Bashundhara Group opens new shopping mall in Sylhet
2. Bangladesh GDP growth falls to 5.1% in FY25, BBS data shows
3. Dhaka Bank MDs speech at annual general meeting
4. এনবিআর আমদানি শুল্ক কাঠামো পরিবর্তনের ঘোষণা দিল
5. International Women's Day celebrated at BRAC office
6. ADB approves $500m loan for Bangladesh infrastructure
7. বাজেট ঘাটতি বাড়ছে, সরকারের ব্যাংক ঋণ রেকর্ড উচ্চতায়
8. Fire breaks out at Tejgaon garment factory, 3 workers injured
9. Bangladesh stock market regulator BSEC bans short selling nationwide
10. ব্র্যাক ব্যাংকের নতুন মোবাইল ব্যাংকিং সেবা চালু
Output: {{"signal": [0, 2, 4, 6, 7, 9]}}

Article titles:
{titles}
"""

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

# -- I/O -----------------------------------------------------------------------

def load_seen_links():
    """Return the set of already-processed article links from seen.json."""
    if Path(SEEN_FILE).exists():
        try:
            with open(SEEN_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            return set(data.get("links", []))
        except Exception:
            pass
    return set()


def save_seen_links(seen_links):
    """Persist the full seen-links set to seen.json."""
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


def find_image_in_html(html, base=None):
    if not html:
        return None
    m = IMG_SRC_RE.search(html)
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
        for l in links:
            if l.get("rel") == "enclosure":
                href = l.get("href")
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
    now          = datetime.now(timezone.utc)
    cutoff       = now - timedelta(hours=MAX_AGE_HOURS)
    bd_now       = datetime.now(BD_TZ)
    bd_now_str   = bd_now.strftime("%a, %d %b %Y %H:%M:%S +0600")
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
                desc = "\n".join([c.get("value", "") for c in e.get("content") if isinstance(c, dict)])
            else:
                det = e.get("summary_detail") or e.get("description_detail")
                if isinstance(det, dict):
                    desc = det.get("value", "") or ""

            link       = normalize_link(e.get("link") or "")
            article_id = e.get("id") or link or ""
            image_url  = extract_image_url(e, base_link=link)

            article = {
                "id":          str(article_id),
                "title":       e.get("title", "") or "",
                "link":        link,
                "description": desc or "",
                "published":   bd_now_str,
                "source":      url,
            }
            if inferred:
                article["published_inferred"] = True
            if image_url:
                article["thumbnail"]      = image_url
                article["thumbnail_type"] = get_mime_for_url(image_url)

            feed_items.append(article)

        passed = len(feed_items)
        capped = min(passed, MAX_ARTICLES_PER_FEED)
        STATS["per_feed"][url]["passed_age"] = passed
        STATS["per_feed"][url]["capped"]     = capped
        STATS["total_passed_age"]           += passed
        all_articles.extend(feed_items[:MAX_ARTICLES_PER_FEED])

    return all_articles


def get_new_articles(all_articles, seen_links):
    """Return articles whose link has not been seen before."""
    new = []
    for a in all_articles:
        link = a.get("link")
        if link and link not in seen_links:
            new.append(a)
    return new


def dedup_by_link(articles):
    """Remove articles sharing an identical link; keep first occurrence."""
    seen  = set()
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
        client      = Mistral(api_key=api_key)
        titles_text = "\n".join([f"{i}. {a.get('title', '')}" for i, a in enumerate(articles)])

        response = client.chat.complete(
            model=MISTRAL_MODEL,
            messages=[{"role": "user", "content": PROMPT.format(titles=titles_text)}],
            response_format={"type": "json_object"},
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
        except ET.ParseError:
            pass
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

    added = 0
    for a in articles:
        link = (a.get("link") or "").strip()
        if not link or link in existing_links:
            continue

        item         = ET.SubElement(channel, "item")
        ET.SubElement(item, "title").text       = a.get("title", "") or ""
        ET.SubElement(item, "link").text        = link
        guid_val     = a.get("id") or link
        is_permalink = "true" if guid_val.startswith("http") else "false"
        ET.SubElement(item, "guid", {"isPermaLink": is_permalink}).text = guid_val
        ET.SubElement(item, "description").text = a.get("description", "") or ""
        if a.get("published"):
            ET.SubElement(item, "pubDate").text = a["published"]

        thumb = a.get("thumbnail")
        if thumb:
            ET.SubElement(item, MEDIA_TAG + "thumbnail", {"url": thumb})
            mime = a.get("thumbnail_type") or get_mime_for_url(thumb)
            ET.SubElement(item, "enclosure", {"url": thumb, "type": mime, "length": "0"})

        existing_links.add(link)
        added += 1

    all_items = channel.findall("item")
    overflow  = len(all_items) - MAX_FEED_ITEMS
    if overflow > 0:
        for old_item in all_items[:overflow]:
            channel.remove(old_item)

    now_text   = datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S +0000")
    last_build = channel.find("lastBuildDate")
    if last_build is None:
        ET.SubElement(channel, "lastBuildDate").text = now_text
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

    # Deduplicate by link within this batch before Mistral
    new_articles = dedup_by_link(new_articles)

    STATS["total_new"] = len(new_articles)
    print(f"Sending {len(new_articles)} article(s) to Mistral for BD economics/finance filtering...")

    mistral_indices = send_to_mistral(new_articles)
    mistral_indices = [i for i in mistral_indices if 0 <= i < len(new_articles)]

    STATS["total_signal_mistral"] = len(mistral_indices)
    STATS["total_signal"]         = len(mistral_indices)

    # Mark all fetched articles as seen regardless of signal/noise
    # so they are never re-sent to the API on the next run
    for a in new_articles:
        link = a.get("link")
        if link:
            seen_links.add(link)
    save_seen_links(seen_links)

    if not mistral_indices:
        print("Mistral returned no signal indices. Skipping XML writes.")
        print_stats()
        return

    signal_articles   = [new_articles[i] for i in mistral_indices]
    excluded_articles = [new_articles[i] for i in range(len(new_articles)) if i not in set(mistral_indices)]

    generate_xml_feed(
        signal_articles,
        output_file=OUTPUT_XML,
        feed_title="BD Economics & Finance",
        feed_description="AI-curated Bangladesh national economics and finance news",
    )

    generate_xml_feed(
        excluded_articles,
        output_file=EXCLUDED_XML,
        feed_title="Excluded (BD Economics Filter)",
        feed_description="Articles excluded by BD economics and finance filter",
    )

    save_selected_articles(signal_articles)

    STATS["timestamp"] = datetime.utcnow().isoformat()
    save_stats()
    print_stats()


if __name__ == "__main__":
    main()
