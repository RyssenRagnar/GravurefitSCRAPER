#!/usr/bin/env python3
"""
Gravurefit python scraper for CommunityScrapers API

- FlareSolverr-only fetcher (configurable FLARESOLVERR_URL via env)
- Persists FlareSolverr cookies to ./config/scraper/Gravure-JP/flaresolverr_session.json
  and reuses them if they are younger than 1 hour.
- Saves raw FlareSolverr JSON under ./config/scraper/Gravure-JP/raw/
- Prunes raw files older than 10 days on startup.
- Reads FRAGMENT JSON from stdin (same interface as other scrapers) and prints a JSON
  object describing the scene to stdout.
"""
from __future__ import annotations
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

try:
    from py_common import log
except ModuleNotFoundError:
    print("You need the 'py_common' module from the CommunityScrapers repo (scrapers/py_common).", file=sys.stderr)
    sys.exit(1)

try:
    import lxml.html
except ModuleNotFoundError:
    print("You need to install lxml: pip install lxml", file=sys.stderr)
    sys.exit(1)

try:
    import requests
except ModuleNotFoundError:
    print("You need to install requests: pip install requests", file=sys.stderr)
    sys.exit(1)


# ---- Configuration (container mounted path) ----
CONFIG_BASE = os.path.join(".", "config", "scraper", "Gravure-JP")
RAW_DIR = os.path.join(CONFIG_BASE, "raw")
OUT_DIR = os.path.join(CONFIG_BASE, "outputs")
SESSION_FILE = os.path.join(CONFIG_BASE, "flaresolverr_session.json")

os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(OUT_DIR, exist_ok=True)
os.makedirs(CONFIG_BASE, exist_ok=True)

# FlareSolverr
FLARESOLVERR_URL = os.environ.get("FLARESOLVERR_URL", "http://127.0.0.1:8191/v1")
FLARESOLVERR_TIMEOUT_MAX = int(os.environ.get("FLARESOLVERR_TIMEOUT_MAX", "60000"))
FLARE_COOKIE_AGE_SECONDS = int(os.environ.get("FLARE_COOKIE_AGE_SECONDS", "3600"))  # 1 hour reuse

# Retention
OUTPUT_RETENTION_DAYS = int(os.environ.get("OUTPUT_RETENTION_DAYS", "10"))

# Site helpers
BASE_SITE = "https://www.gravurefit.com"
DATE_REGEX = re.compile(r"(\d{4})年\s*(\d{1,2})月\s*(\d{1,2})日")
CODE_REGEX = re.compile(r".*?([A-Za-z]{3,6}-\d{2,6}).*")

HEADERS_BROWSER_LIKE = {
    "User-Agent": os.environ.get("GRAVURE_USER_AGENT", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36"),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "ja-JP,ja;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer": BASE_SITE + "/",
}

# -------------------------
# Utility helpers
# -------------------------
def prune_old_outputs(outdir: str = OUT_DIR, days: int = OUTPUT_RETENTION_DAYS) -> None:
    cutoff = time.time() - days * 86400
    for root, _, files in os.walk(outdir):
        for fn in files:
            path = os.path.join(root, fn)
            try:
                if os.path.getmtime(path) < cutoff:
                    os.remove(path)
            except Exception:
                pass

def prune_old_raws(rawdir: str = RAW_DIR, days: int = OUTPUT_RETENTION_DAYS) -> None:
    cutoff = time.time() - days * 86400
    for root, _, files in os.walk(rawdir):
        for fn in files:
            path = os.path.join(root, fn)
            try:
                if os.path.getmtime(path) < cutoff:
                    os.remove(path)
            except Exception:
                pass

def save_json(path: str, obj: Any, pretty: bool = True) -> None:
    try:
        with open(path, "w", encoding="utf-8") as f:
            if pretty:
                json.dump(obj, f, ensure_ascii=False, indent=2)
            else:
                json.dump(obj, f, ensure_ascii=False)
    except Exception as e:
        log.debug(f"Failed to save JSON {path}: {e}")

# FlareSolverr session persistence
def save_flaresolverr_session(session_file: str, cookies: List[Dict[str, Any]]) -> None:
    payload = {"timestamp": int(time.time()), "cookies": cookies}
    try:
        with open(session_file, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except Exception as e:
        log.debug(f"Failed to save FlareSolverr session file: {e}")

def load_flaresolverr_session(session_file: str) -> Optional[Dict[str, Any]]:
    try:
        if not os.path.exists(session_file):
            return None
        with open(session_file, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        log.debug(f"Failed to load FlareSolverr session file: {e}")
        return None

def build_cookie_header_from_list(cookies: List[Dict[str, Any]]) -> Optional[str]:
    parts = []
    for c in cookies:
        name = c.get("name")
        val = c.get("value")
        if name and val is not None:
            parts.append(f"{name}={val}")
    return "; ".join(parts) if parts else None

# -------------------------
# FlareSolverr fetcher (robust to different response shapes)
# -------------------------
def fetch_via_flaresolverr(url: str, flaresolverr_url: str = FLARESOLVERR_URL,
                           headers: Optional[Dict[str, str]] = None,
                           cookie_header: Optional[str] = None,
                           timeout: int = FLARESOLVERR_TIMEOUT_MAX) -> Dict[str, Any]:
    safe_headers = dict(headers) if headers else {}
    if cookie_header and "Cookie" not in safe_headers:
        safe_headers["Cookie"] = cookie_header

    payload = {"cmd": "request.get", "url": url, "maxTimeout": int(timeout)}
    if safe_headers:
        payload["headers"] = safe_headers

    try:
        r = requests.post(flaresolverr_url, json=payload, timeout=(10, 90))
        r.raise_for_status()
    except Exception as e:
        return {"ok": False, "status": None, "html": None, "cookies": [], "raw": {"error": str(e)}}

    try:
        data = r.json()
    except Exception:
        # non-json fallback
        return {"ok": False, "status": None, "html": r.text, "cookies": [], "raw": r.text}

    html_content = None
    cookies: List[Dict[str, Any]] = []
    status = None

    if isinstance(data, dict):
        sol = data.get("solution") or data.get("response") or data

        if isinstance(sol, dict):
            resp = sol.get("response", None)
            if isinstance(resp, dict):
                html_content = resp.get("data") or resp.get("content") or resp.get("html") or html_content
                cookies = resp.get("cookies") or sol.get("cookies") or cookies
                status = resp.get("status") or sol.get("status") or status
            elif isinstance(resp, str):
                html_content = resp
                cookies = sol.get("cookies") or cookies
                status = sol.get("status") or status
            else:
                html_content = sol.get("data") or sol.get("content") or sol.get("html") or html_content
                cookies = sol.get("cookies") or cookies
                status = sol.get("status") or status
        elif isinstance(sol, str):
            html_content = sol

        if not html_content:
            resp2 = data.get("response")
            if isinstance(resp2, dict):
                html_content = resp2.get("content") or resp2.get("data") or resp2.get("html") or html_content
                cookies = resp2.get("cookies") or cookies
                status = resp2.get("status") or status
            elif isinstance(resp2, str):
                html_content = resp2

    return {"ok": bool(html_content), "status": status, "html": html_content, "cookies": cookies, "raw": data}

# -------------------------
# Gravurefit parsing
# -------------------------
def normalize_title(raw_title: Optional[str]) -> Optional[str]:
    if not raw_title:
        return None
    t = raw_title.strip()
    # Remove leading performer like "NAME - "
    t = re.sub(r'^[^－\-\—]+[ \t]*[-－\—][ \t]*', '', t).strip()
    # Remove trailing " / CODE"
    t = re.sub(r'\s*/\s*[A-Za-z0-9_\-()]+$', '', t).strip()
    # Remove leading code "LULU-255：" if present
    t = re.sub(r'^[A-Za-z0-9_\-]+[:：]\s*', '', t).strip()
    return t or None

def find_earliest_date_in_text(text: str) -> Optional[str]:
    dates = []
    for m in DATE_REGEX.finditer(text):
        y, mo, d = m.groups()
        try:
            dt = datetime(int(y), int(mo), int(d), tzinfo=timezone.utc)
            dates.append(dt)
        except Exception:
            continue
    iso_matches = re.findall(r"(\d{4}-\d{2}-\d{2})", text)
    for iso in iso_matches:
        try:
            dt = datetime.fromisoformat(iso).replace(tzinfo=timezone.utc)
            dates.append(dt)
        except Exception:
            pass
    if not dates:
        return None
    earliest = min(dates)
    return earliest.strftime("%Y-%m-%d")

def extract_gravure_fields(html_content: str) -> Dict[str, Any]:
    try:
        doc = lxml.html.fromstring(html_content)
    except Exception:
        try:
            doc = lxml.html.fromstring(str(html_content))
        except Exception:
            return {}

    out: Dict[str, Any] = {}
    # Title
    raw_title_list = doc.xpath("//h1/text()")
    raw_title = raw_title_list[0].strip() if raw_title_list else None
    out["title_raw"] = raw_title
    out["title"] = normalize_title(raw_title)

    # Details / description
    details = doc.xpath("//meta[@name='description']/@content") or doc.xpath("//p[@class='description']/text()")
    out["details"] = details[0].strip() if details else None

    # Date - earliest
    try:
        text = lxml.html.tostring(doc, encoding="unicode", method="text")
    except Exception:
        text = ""
    out["date"] = find_earliest_date_in_text(text)

    # Performers
    performers = doc.xpath("//table//th[contains(text(),'女優')]/following-sibling::td//a/text()")
    if not performers:
        performers = doc.xpath("//a[contains(@href,'/profile')]/text()")
    out["performers"] = [p.strip() for p in performers if p.strip()]

    # Tags: Themes and Plays
    tags: List[str] = []
    themes = doc.xpath("//h2[contains(normalize-space(.),'テーマ')]/following-sibling::ul[1]//a/text()")
    tags += [t.strip() for t in themes if t.strip()]
    plays = doc.xpath("//h2[contains(normalize-space(.),'プレイ')]/following-sibling::ul[1]//a/text() | //h2[contains(normalize-space(.),'服装')]/following-sibling::ul[1]//a/text()")
    tags += [t.strip() for t in plays if t.strip()]
    # dedupe preserve order
    seen = set()
    dedup = []
    for t in tags:
        if t not in seen:
            seen.add(t)
            dedup.append(t)
    out["tags"] = dedup

    # Image
    image = None
    img = doc.xpath("//div[contains(@class,'photopc')]//img[1]/@src")
    if img:
        image = img[0]
    else:
        img = doc.xpath("//img[contains(@class,'package')][1]/@src | //div[contains(@class,'video')]//img[1]/@src")
        if img:
            image = img[0]
        else:
            img = doc.xpath("//meta[@property='og:image']/@content")
            if img:
                image = img[0]
    if image:
        if image.startswith("//"):
            image = "https:" + image
        elif image.startswith("/"):
            image = BASE_SITE.rstrip("/") + image
        out["image"] = image
    else:
        out["image"] = None

    # Studio / maker
    manu = doc.xpath("//table//th[contains(text(),'メーカー')]/following-sibling::td//text()")
    out["studio"] = manu[0].strip() if manu else None

    # Code
    code = None
    canon = doc.xpath("//link[@rel='canonical']/@href") or doc.xpath("//meta[@property='og:url']/@content")
    if canon:
        m = CODE_REGEX.search(canon[0])
        if m:
            code = m.group(1).upper()
        else:
            parts = [p for p in canon[0].split("/") if p]
            if parts:
                code = parts[-1].upper()
    out["code"] = code
    return out

# -------------------------
# Main
# -------------------------
def main():
    # Read FRAGMENT from stdin
    try:
        FRAGMENT = json.loads(sys.stdin.read())
    except Exception:
        log.error("Failed to read input FRAGMENT JSON")
        print(json.dumps({}))
        return

    SCENE_URL = FRAGMENT.get("url")
    # No default URLs: require URL(s)
    if not SCENE_URL:
        log.info("No URL provided in FRAGMENT")
        print(json.dumps({}))
        return

    # Prune old files
    prune_old_raws(RAW_DIR, days=OUTPUT_RETENTION_DAYS)
    prune_old_outputs(OUT_DIR, days=OUTPUT_RETENTION_DAYS)

    # Load saved FlareSolverr cookies if fresh
    session_data = load_flaresolverr_session(SESSION_FILE)
    cookie_header = None
    if session_data:
        ts = session_data.get("timestamp", 0)
        if (time.time() - int(ts)) <= FLARE_COOKIE_AGE_SECONDS:
            cookie_header = build_cookie_header_from_list(session_data.get("cookies", []))

    # Fetch via FlareSolverr only
    log.info(f"Fetching Gravurefit URL via FlareSolverr: {SCENE_URL}")
    fs = fetch_via_flaresolverr(SCENE_URL, flaresolverr_url=FLARESOLVERR_URL, headers=HEADERS_BROWSER_LIKE, cookie_header=cookie_header)

    # Save raw for debugging
    raw_basename = None
    try:
        raw_url = None
        raw_solution = fs.get("raw", {})
        if isinstance(raw_solution, dict):
            raw_url = (raw_solution.get("solution") or {}).get("url") if raw_solution.get("solution") else None
        if not raw_url:
            raw_url = SCENE_URL
        parts = [p for p in raw_url.split("/") if p]
        raw_basename = parts[-1] if parts else "result"
        raw_path = os.path.join(RAW_DIR, f"{raw_basename}_flaresolverr_raw.json")
        save_json(raw_path, fs.get("raw", {}), pretty=True)
        log.info(f"Saved FlareSolverr raw JSON: {raw_path}")
    except Exception:
        log.debug("Failed to save flaresolverr raw")

    # Save cookies returned by FlareSolverr for future reuse
    fl_cookies = fs.get("cookies") or []
    if fl_cookies:
        try:
            save_flaresolverr_session(SESSION_FILE, fl_cookies)
            log.info("Saved FlareSolverr cookies to session store")
        except Exception:
            log.debug("Failed to save FlareSolverr cookies")

    # Parse if HTML present
    parsed: Dict[str, Any] = {}
    if fs.get("ok") and fs.get("html"):
        parsed = extract_gravure_fields(fs.get("html"))
    else:
        # if no html, return raw result for debugging
        log.warning("FlareSolverr did not return HTML or parsing failed")
        parsed = {}

    # Build output object expected by application
    result: Dict[str, Any] = {
        "code": parsed.get("code"),
        "title": parsed.get("title"),
        "date": parsed.get("date"),
        "performers": parsed.get("performers") or [],
        "tags": parsed.get("tags") or [],
        "image": parsed.get("image"),
        "studio": parsed.get("studio"),
        "details": parsed.get("details"),
        "url": SCENE_URL,
        "_flaresolverr_raw": fs.get("raw"),
    }

    # Save to OUT_DIR for traceability (pruned after retention window)
    try:
        if raw_basename:
            out_path = os.path.join(OUT_DIR, f"{raw_basename}.json")
            save_json(out_path, result, pretty=True)
            log.info(f"Wrote parsed output to {out_path}")
    except Exception:
        log.debug("Failed to save output file")

    # Print result JSON to stdout for the application
    print(json.dumps(result, ensure_ascii=False))

if __name__ == "__main__":
    main()