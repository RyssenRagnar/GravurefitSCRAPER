"""JAVLibrary python scraper
Extended: added Gravurefit support + robust FlareSolverr integration
"""
import base64
import json
import re
import sys
import threading
import time
import os
from urllib.parse import urlparse

try:
    from py_common import log
except ModuleNotFoundError:
    print("You need to download the folder 'py_common' from the community repo! (CommunityScrapers/tree/master/scrapers/py_common)", file=sys.stderr)
    sys.exit()

try:
    import lxml.html
except ModuleNotFoundError:
    print("You need to install the lxml module. (https://lxml.de/installation.html#installation)",
     file=sys.stderr)
    print("If you have pip (normally installed with python), run this command in a terminal (cmd): pip install lxml",
     file=sys.stderr)
    sys.exit()

try:
    import requests
except ModuleNotFoundError:
    print("You need to install the requests module. (https://docs.python-requests.org/en/latest/user/install/)",
     file=sys.stderr)
    print("If you have pip (normally installed with python), run this command in a terminal (cmd): pip install requests",
     file=sys.stderr)
    sys.exit()


# GLOBAL VAR ######
JAV_DOMAIN = "Check"
###################

JAV_SEARCH_HTML = None
JAV_MAIN_HTML = None
PROTECTION_CLOUDFLARE = False

# Flaresolverr (default enabled; this integration uses FlareSolverr-only for Gravurefit)
FLARESOLVERR_ENABLED = True
FLARESOLVERR_URL = os.environ.get("FLARESOLVERR_URL", "http://127.0.0.1:8191/v1")
FLARESOLVERR_TIMEOUT_MAX = 60000

JAV_HEADERS = {
    "User-Agent":
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0',
    "Referer": "http://www.javlibrary.com/"
}
# We can't add movie image atm in the same time as Scene
STASH_SUPPORTED = False
# Stash doesn't support Labels yet
STASH_SUPPORT_LABELS = False
# ...and name order too...
STASH_SUPPORT_NAME_ORDER = False
# Tags you don't want to scrape
IGNORE_TAGS = [
    "Features Actress", "Hi-Def", "Beautiful Girl", "Blu-ray",
    "Featured Actress", "VR Exclusive", "MOODYZ SALE 4"
]
# Select preferable name order
NAME_ORDER_JAPANESE = False
# Some performers don't need to be reversed
IGNORE_PERF_REVERSE = ["Lily Heart"]

# Keep the legacy field scheme:
# Actual Code -> Title, actual Title -> Details, actual Details -> /dev/null
LEGACY_FIELDS = True
# Studio Code now in a separate field, so it may (or may not) be stripped from title
# Makes sense only if not LEGACY_FIELDS
KEEP_CODE_IN_TITLE = True

# Tags you want to be added in every scrape
FIXED_TAGS = ""
# Split tags if they contain [,·] ('Best, Omnibus' -> 'Best','Omnibus')
SPLIT_TAGS = False

# Don't fetch the Aliases (Japanese Name)
IGNORE_ALIASES = False
# Always wait for the aliases to load. (Depends on network response)
WAIT_FOR_ALIASES = False
# All javlib sites
SITE_JAVLIB = ["javlibrary", "o58c", "e59f"]

BANNED_WORDS = {
    # (existing banned words map omitted for brevity in this snippet)
}

REPLACE_TITLE = {
    # (existing replacements omitted for brevity)
}

OBFUSCATED_TAGS = {
    "Girl": "Young Girl", # ロリ系 in Japanese
    "Tits": "Small Tits" # 微乳 in Japanese
}


class ResponseHTML:
    content = ""
    html = ""
    status_code = 0
    url = ""

# -------------------------------
# Gravurefit-specific config
# -------------------------------
CONFIG_BASE = os.path.join(".", "config", "scraper", "Gravure-JP")
CONFIG_RAW_DIR = os.path.join(CONFIG_BASE, "raw")
CONFIG_OUT_DIR = os.path.join(CONFIG_BASE, "outputs")
SESSION_FILE = os.path.join(CONFIG_BASE, "flaresolverr_session.json")
# retention: prune raw files older than this (days)
RAW_RETENTION_DAYS = 10
FLARE_COOKIE_AGE_SECONDS = 3600  # reuse cookies within one hour

# Ensure config dirs exist
os.makedirs(CONFIG_BASE, exist_ok=True)
os.makedirs(CONFIG_RAW_DIR, exist_ok=True)
os.makedirs(CONFIG_OUT_DIR, exist_ok=True)

# Gravurefit helpers (regexes / xpaths)
GRAVURE_DATE_REGEX = re.compile(r"(\d{4})年\s*(\d{1,2})月\s*(\d{1,2})日")
GRAVURE_CODE_REGEX = re.compile(r".*?([A-Za-z]{3,6}-\d{2,6}).*")

# -------------------------------
# FlareSolverr helpers (robust)
# -------------------------------
def fetch_via_flaresolverr(url: str, flaresolverr_url: str = FLARESOLVERR_URL,
                           headers: Optional[dict] = None, cookie_header: Optional[str] = None,
                           timeout: int = 60000) -> dict:
    """
    Call FlareSolverr v1 API (cmd=request.get) and return dict:
    { ok: bool, status: optional int, html: optional str, cookies: list, raw: original json }
    Robust to multiple solution shapes.
    """
    safe_headers = dict(headers) if headers else {}
    if cookie_header and "Cookie" not in safe_headers:
        safe_headers["Cookie"] = cookie_header

    payload = {"cmd": "request.get", "url": url, "maxTimeout": timeout}
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
        return {"ok": False, "status": None, "html": r.text, "cookies": [], "raw": r.text}

    html_content = None
    cookies = []
    status = None

    if isinstance(data, dict):
        sol = data.get("solution") or data.get("response") or data

        if isinstance(sol, dict):
            resp = sol.get("response", None)
            # response as dict
            if isinstance(resp, dict):
                html_content = resp.get("data") or resp.get("content") or resp.get("html") or html_content
                cookies = resp.get("cookies") or sol.get("cookies") or cookies
                status = resp.get("status") or sol.get("status") or status
            # response as plain string
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


# Session cookie persistence (for FlareSolverr cookies)
def save_flaresolverr_session(cookies: List[dict]) -> None:
    payload = {"timestamp": int(time.time()), "cookies": cookies}
    try:
        with open(SESSION_FILE, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except Exception:
        log.debug("Failed to save FlareSolverr session file")

def load_flaresolverr_session() -> Optional[dict]:
    try:
        if not os.path.exists(SESSION_FILE):
            return None
        with open(SESSION_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def build_cookie_header_from_list(cookies: List[dict]) -> Optional[str]:
    parts = []
    for c in cookies:
        name = c.get("name")
        val = c.get("value")
        if name and val is not None:
            parts.append(f"{name}={val}")
    return "; ".join(parts) if parts else None

# Prune old raw files older than RAW_RETENTION_DAYS
def prune_old_raws(raw_dir: str = CONFIG_RAW_DIR, days: int = RAW_RETENTION_DAYS) -> None:
    cutoff = time.time() - days * 86400
    for root, _, files in os.walk(raw_dir):
        for fn in files:
            path = os.path.join(root, fn)
            try:
                if os.path.getmtime(path) < cutoff:
                    os.remove(path)
            except Exception:
                pass

# -------------------------------
# Utilities (existing functions reused)
# -------------------------------
def replace_banned_words(matchobj):
    word = matchobj.group(0)
    if word in BANNED_WORDS:
        return BANNED_WORDS[word]
    return word

def cleanup_title(title):
    if title == None:
        return title

    log.info(f"Starting title cleanup for: {title}")
    cleaned_title = False
    for key, value in REPLACE_TITLE.items():
        if key in title:
            title = title.replace(key, value)
            cleaned_title = True
    
    if cleaned_title:
        title = title.strip()
        log.info(f"Found match and using new clean title: {title}")
    return title

def regexreplace(input_replace):
    word_pattern = re.compile(r'(\w|\*)+')
    output = word_pattern.sub(replace_banned_words, input_replace)
    return re.sub(r"[\[\]\"]", "", output)

def getxpath(xpath, tree):
    if not xpath:
        return None
    xpath_result = []
    # It handles the union strangely so it is better to split and get one by one
    if "|" in xpath:
        for xpath_tmp in xpath.split("|"):
            xpath_result.append(tree.xpath(xpath_tmp))
        xpath_result = [val for sublist in xpath_result for val in sublist]
    else:
        xpath_result = tree.xpath(xpath)
    list_tmp = []
    for x_res in xpath_result:
        # for xpaths that don't end with /text()
        if isinstance(x_res, lxml.html.HtmlElement):
            list_tmp.append(x_res.text_content().strip())
        else:
            list_tmp.append(x_res.strip())
    if list_tmp:
        xpath_result = list_tmp
    xpath_result = list(filter(None, xpath_result))
    return xpath_result

# (existing search and parse functions follow... unchanged until bypass_protection)
# SEARCH PAGE
# ... [existing jav_search, jav_search_by_name, buildlist_tagperf, th_request_perfpage, th_imageto_base64] ...
# For brevity we reuse previously defined functions without altering them (they remain above in original file).
# Insert earlier function definitions here unchanged...

# --- Keep existing code for jav_search, jav_search_by_name, buildlist_tagperf, th_request_perfpage, th_imageto_base64
# For brevity, they remain exactly as in the original file above (unchanged).

# -------------------------------
# Overriding bypass_protection to use robust FlareSolverr / session reuse
# -------------------------------
def bypass_protection(url):
    """
    Modify existing bypass_protection logic: use FlareSolverr robust helper,
    inject saved FlareSolverr cookies when present and fresh (within 1 hour),
    save cookies if FlareSolverr returns them.
    """
    global PROTECTION_CLOUDFLARE

    url_domain = re.sub(r"www\.|\.com", "", urlparse(url).netloc)
    log.debug("=== Checking Status of target site ===")
    PROTECTION_CLOUDFLARE = False
    response_html = ResponseHTML
    for site in SITE_JAVLIB:
        url_n = url.replace(url_domain, site)
        try:
            if FLARESOLVERR_ENABLED:
                # prepare cookie header from saved session if fresh
                session_data = load_flaresolverr_session()
                cookie_header = None
                if session_data:
                    ts = session_data.get("timestamp", 0)
                    if (time.time() - int(ts)) <= FLARE_COOKIE_AGE_SECONDS:
                        cookie_header = build_cookie_header_from_list(session_data.get("cookies", []))
                headers = {"Content-Type": "application/json"}
                data = {"cmd": "request.get", "url": url_n, "maxTimeout": FLARESOLVERR_TIMEOUT_MAX}
                log.info(f"Using FlareSolverr: {FLARESOLVERR_URL}")
                log.info(f"Input URL: {url_n}")
                fs_res = fetch_via_flaresolverr(url_n, flaresolverr_url=FLARESOLVERR_URL, headers=headers, cookie_header=cookie_header)
                response_html.content = fs_res.get("html") or ""
                response_html.html = fs_res.get("html") or ""
                response_html.status_code = fs_res.get("status") or (200 if fs_res.get("ok") else 0)
                response_html.url = url_n
                # save returned cookies for reuse
                returned_cookies = fs_res.get("cookies") or []
                if returned_cookies:
                    save_flaresolverr_session(returned_cookies)
                    log.info("Saved FlareSolverr cookies to session store.")
            else:
                response = requests.get(url_n, headers=JAV_HEADERS, timeout=10)
                response_html.content = response.content
                response_html.html = response.text
                response_html.status_code = response.status_code
                response_html.url = response.url
        except Exception as exc_req:
            log.warning(f"Exception error {exc_req} while checking protection for {site}")
            return None, None
        if response_html.url == "https://www.javlib.com/maintenance.html":
            log.error(f"[{site}] Maintenance")
        if "Why do I have to complete a CAPTCHA?" in response_html.html \
            or "Checking your browser before accessing" in response_html.html:
            log.error(f"[{site}] Protected by Cloudflare")
            PROTECTION_CLOUDFLARE = True
        elif response_html.status_code != 200:
            log.error(f"[{site}] Other issue ({response_html.status_code})")
        else:
            log.info(
                    f"[{site}] Using this site for scraping ({response_html.status_code})"
                )
            log.debug("======================================")
            return site, response_html
    log.debug("======================================")
    return None, None

# -------------------------------
# Gravurefit extraction helpers
# -------------------------------
def normalize_gravure_title(raw_title: Optional[str]) -> Optional[str]:
    if not raw_title:
        return None
    t = raw_title.strip()
    # Remove leading performer like "本田瞳 - "
    t = re.sub(r'^[^－\-\—]+[ \t]*[-－\—][ \t]*', '', t).strip()
    # Remove trailing " / CODE"
    t = re.sub(r'\s*/\s*[A-Za-z0-9_\-()]+$', '', t).strip()
    # Remove leading code like "LULU-255："
    t = re.sub(r'^[A-Za-z0-9_\-]+[:：]\s*', '', t).strip()
    return t or None

def find_earliest_date_from_text(text: str) -> Optional[str]:
    if not text:
        return None
    dates = []
    for m in GRAVURE_DATE_REGEX.finditer(text):
        y, mo, d = m.groups()
        try:
            dt = time.strptime(f"{y}-{int(mo):02d}-{int(d):02d}", "%Y-%m-%d")
            dates.append((int(y), int(mo), int(d)))
        except Exception:
            continue
    # also ISO
    iso_matches = re.findall(r"(\d{4}-\d{2}-\d{2})", text)
    for iso in iso_matches:
        try:
            yy, mm, dd = iso.split("-")
            dates.append((int(yy), int(mm), int(dd)))
        except Exception:
            pass
    if not dates:
        return None
    dates_sorted = sorted(dates)
    y, m, d = dates_sorted[0]
    return f"{y}-{m:02d}-{d:02d}"

def extract_gravure_fields(html_content: str) -> dict:
    """Parse Gravurefit page HTML and extract normalized fields per user's spec."""
    try:
        doc = lxml.html.fromstring(html_content)
    except Exception:
        try:
            doc = lxml.html.fromstring(str(html_content))
        except Exception:
            return {}
    out = {}
    # Title
    raw_title = None
    try:
        raw_title = doc.xpath("//h1/text()")
        raw_title = raw_title[0].strip() if raw_title else None
    except Exception:
        raw_title = None
    out["title_raw"] = raw_title
    out["title"] = normalize_gravure_title(raw_title)

    # Details / description
    try:
        details = doc.xpath("//meta[@name='description']/@content") or doc.xpath("//p[@class='description']/text()")
        out["details"] = details[0].strip() if details else None
    except Exception:
        out["details"] = None

    # Date: earliest from document
    try:
        text = lxml.html.tostring(doc, encoding="unicode", method="text")
    except Exception:
        text = ""
    out["date"] = find_earliest_date_from_text(text)

    # Performers
    try:
        performers = doc.xpath("//table//th[text()[contains(.,'女優')]]/following-sibling::td//a/text()")
        if not performers:
            # fallback
            performers = doc.xpath("//a[contains(@href,'/profile')]/text()")
        out["performers"] = [p.strip() for p in performers if p.strip()]
    except Exception:
        out["performers"] = []

    # Tags: combine Themes (テーマ | スタイル) and Plays (プレイ内容 | 服装 | 場所)
    tags = []
    try:
        themes = doc.xpath("//h2[contains(normalize-space(.),'テーマ')]/following-sibling::ul[1]//a/text()")
        tags += [t.strip() for t in themes if t.strip()]
    except Exception:
        pass
    try:
        plays = doc.xpath("//h2[contains(normalize-space(.),'プレイ')]/following-sibling::ul[1]//a/text() | //h2[contains(normalize-space(.),'服装')]/following-sibling::ul[1]//a/text()")
        tags += [t.strip() for t in plays if t.strip()]
    except Exception:
        pass
    # dedupe preserve order
    seen = set()
    dedup = []
    for t in tags:
        if t not in seen:
            seen.add(t)
            dedup.append(t)
    out["tags"] = dedup

    # Image: prefer photopc / package / og, return full https url
    image = None
    try:
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
    except Exception:
        image = None
    if image:
        if image.startswith("//"):
            image = "https:" + image
        elif image.startswith("/"):
            image = "https://www.gravurefit.com" + image
        out["image"] = image
    else:
        out["image"] = None

    # Studio / maker
    try:
        studio = doc.xpath("//table//th[text()[contains(.,'メーカー')]]/following-sibling::td//text()")
        out["studio"] = studio[0].strip() if studio else None
    except Exception:
        out["studio"] = None

    # Code extraction
    code = None
    try:
        canon = doc.xpath("//link[@rel='canonical']/@href") or doc.xpath("//meta[@property='og:url']/@content")
        if canon:
            m = GRAVURE_CODE_REGEX.search(canon[0])
            if m:
                code = m.group(1).upper()
            else:
                parts = [p for p in canon[0].split("/") if p]
                if parts:
                    code = parts[-1].upper()
    except Exception:
        code = None
    out["code"] = code
    return out

# -------------------------------
# Main execution (wrap original script flow, add Gravurefit handling)
# -------------------------------
# (Original script reads FRAGMENT from stdin, etc. We'll reuse that flow)
FRAGMENT = json.loads(sys.stdin.read())

SEARCH_TITLE = FRAGMENT.get("name")
SEARCH_TITLE = cleanup_title(SEARCH_TITLE)
SCENE_URL = FRAGMENT.get("url")

if FRAGMENT.get("title"):
    SCENE_TITLE = FRAGMENT["title"]
    SCENE_TITLE = cleanup_title(SCENE_TITLE)
else:
    SCENE_TITLE = None

if "validSearch" in sys.argv and SCENE_URL is None:
    sys.exit()

# For Gravurefit raw/pruning management
prune_old_raws(CONFIG_RAW_DIR, days=RAW_RETENTION_DAYS)

if "searchName" in sys.argv:
    log.debug(f"Using search with Title: {SEARCH_TITLE}")
    JAV_SEARCH_HTML = send_request(
        f"https://www.javlibrary.com/en/vl_searchbyid.php?keyword={SEARCH_TITLE}",
        JAV_HEADERS)
else:
    if SCENE_URL:
        scene_domain = re.sub(r"www\.|\.com", "", urlparse(SCENE_URL).netloc)
        # Url from Javlib
        if scene_domain in SITE_JAVLIB:
            log.debug(f"Using URL: {SCENE_URL}")
            JAV_MAIN_HTML = send_request(SCENE_URL, JAV_HEADERS)
        else:
            # If scene URL is Gravurefit (or other), still attempt to fetch via bypass_protection (which uses FlareSolverr)
            log.debug(f"Using URL (non-javlib): {SCENE_URL}")
            domain = urlparse(SCENE_URL).netloc
            # Use bypass_protection to obtain HTML (this will use FlareSolverr if enabled)
            site, resp_html = bypass_protection(SCENE_URL)
            if resp_html and resp_html.html:
                JAV_MAIN_HTML = resp_html  # reusing container for compatibility
            else:
                JAV_MAIN_HTML = None
    if JAV_MAIN_HTML is None and SCENE_TITLE:
        log.debug(f"Using search with Title: {SCENE_TITLE}")
        JAV_SEARCH_HTML = send_request(
            f"https://www.javlibrary.com/en/vl_searchbyid.php?keyword={SCENE_TITLE}",
            JAV_HEADERS)

# Existing JavLibrary parsing remains for javlibrary pages
# XPATHs (existing)
jav_xPath_search = {}
jav_xPath_search['url'] = '//div[@class="videos"]/div/a[not(contains(@title,"(Blu-ray"))]/@href'
jav_xPath_search['title'] = '//div[@class="videos"]/div/a[not(contains(@title,"(Blu-ray"))]/@title'
jav_xPath_search['image'] = '//div[@class="videos"]/div/a[not(contains(@title,"(Blu-ray"))]//img/@src'

jav_xPath = {}
jav_xPath["code"] = '//td[@class="header" and text()="ID:"]/following-sibling::td/text()'
jav_xPath["title"] = jav_xPath["code"] if LEGACY_FIELDS else '//div[@id="video_title"]/h3/a/text()'
jav_xPath["details"] = None if not LEGACY_FIELDS else '//div[@id="video_title"]/h3/a/text()'
jav_xPath["url"] = '//meta[@property="og:url"]/@content'
jav_xPath["date"] = '//td[@class="header" and text()="Release Date:"]/following-sibling::td/text()'
jav_xPath["director"] = '//div[@id="video_director"]//td[@class="text"]/span[@class="director"]/a/text()'
jav_xPath["tags"] = '//td[@class="header" and text()="Genre(s):"]/following::td/span[@class="genre"]/a/text()'
jav_xPath["performers"] = '//td[@class="header" and text()="Cast:"]/following::td/span[@class="cast"]/span/a/text()'
jav_xPath["performers_url"] = '//td[@class="header" and text()="Cast:"]/following::td/span[@class="cast"]/span/a/@href'
jav_xPath["studio"] = '//td[@class="header" and text()="Maker:"]/following-sibling::td/span[@class="maker"]/a/text()'
jav_xPath["image"] = '//div[@id="video_jacket"]/img/@src'

jav_result = {}

# If searchName flow
if "searchName" in sys.argv:
    if JAV_SEARCH_HTML:
        if "/en/?v=" in JAV_SEARCH_HTML.url:
            log.debug(f"Scraping the movie page directly ({JAV_SEARCH_HTML.url})")
            jav_tree = lxml.html.fromstring(JAV_SEARCH_HTML.content)
            jav_result["title"] = getxpath(jav_xPath["title"], jav_tree)
            jav_result["details"] = getxpath(jav_xPath["details"], jav_tree)
            jav_result["url"] = getxpath(jav_xPath["url"], jav_tree)
            jav_result["image"] = getxpath(jav_xPath["image"], jav_tree)
            for key, value in jav_result.items():
                if isinstance(value,list):
                    jav_result[key] = value[0]
                if key in ["image", "url"]:
                    jav_result[key] = f"https:{jav_result[key]}".replace("https:https:", "https:")
            jav_result = [jav_result]
        else:
            jav_result = jav_search_by_name(JAV_SEARCH_HTML, jav_xPath_search)
        if jav_result:
            print(json.dumps(jav_result))
        else:
            print(json.dumps([{"title": "The search doesn't return any result."}]))
    else:
        if PROTECTION_CLOUDFLARE:
            print(
                json.dumps([{
                    "title": "Protected by Cloudflare, try later."
                }]))
        else:
            print(
                json.dumps([{
                    "title":
                    "The request has failed to get the page. Check log."
                }]))
    sys.exit()

# If we have Jav main HTML (javlibrary) do normal flow
if JAV_SEARCH_HTML and JAV_MAIN_HTML is None:
    JAV_MAIN_HTML = jav_search(JAV_SEARCH_HTML, jav_xPath_search)

if JAV_MAIN_HTML and hasattr(JAV_MAIN_HTML, "content"):
    jav_tree = lxml.html.fromstring(JAV_MAIN_HTML.content)
    if jav_tree is not None:
        for key, value in jav_xPath.items():
            jav_result[key] = getxpath(value, jav_tree)
        # PostProcess (existing code)
        if jav_result.get("image"):
            tmp = re.sub(r"(http:|https:)", "", jav_result["image"][0])
            jav_result["image"] = "https:" + tmp
            if "now_printing.jpg" in jav_result["image"] or "noimage" in jav_result["image"]:
                log.debug("[Warning][Javlibrary] Image was deleted or failed to load ({jav_result['image']})")
                jav_result["image"] = None
            else:
                imageBase64_jav_thread = threading.Thread(target=th_imageto_base64, args=(jav_result["image"], "JAV",))
                imageBase64_jav_thread.start()
        if jav_result.get("url"):
            jav_result["url"] = "https:" + jav_result["url"][0]
        if jav_result.get("details") and LEGACY_FIELDS:
            jav_result["details"] = re.sub(r"^(.*? ){1}", "", jav_result["details"][0])
        if jav_result.get("title"):
            if LEGACY_FIELDS or KEEP_CODE_IN_TITLE:
                jav_result["title"] = jav_result["title"][0]
            elif not KEEP_CODE_IN_TITLE:
                jav_result["title"] = (re.sub(jav_result['code'][0], "", jav_result["title"][0])).lstrip()
        if jav_result.get("director"):
            jav_result["director"] = jav_result["director"][0]
        if jav_result.get("label"):
            jav_result["label"] = jav_result["label"][0]
        if jav_result.get("performers_url") and IGNORE_ALIASES is False:
            javlibrary_aliases_thread = threading.Thread(target=th_request_perfpage, args=(JAV_MAIN_HTML.url, jav_result["performers_url"],))
            javlibrary_aliases_thread.daemon = True
            javlibrary_aliases_thread.start()

if JAV_MAIN_HTML is None and not SCENE_URL:
    log.info("No results found")
    print(json.dumps({}))
    sys.exit()

# If SCENE_URL points to Gravurefit (or other non-javlib) handle specially
domain = urlparse(SCENE_URL or "").netloc if SCENE_URL else ""
if "gravurefit.com" in domain:
    # We already fetched HTML via bypass_protection earlier, or via fetch_via_flaresolverr directly
    # Attempt to fetch fresh via FlareSolverr (ensuring we inject saved cookies if available)
    session_data = load_flaresolverr_session()
    cookie_header = build_cookie_header_from_list(session_data.get("cookies", [])) if session_data and (time.time() - int(session_data.get("timestamp", 0)) <= FLARE_COOKIE_AGE_SECONDS) else None
    fs = fetch_via_flaresolverr(SCENE_URL, flaresolverr_url=FLARESOLVERR_URL, headers={"User-Agent": JAV_HEADERS["User-Agent"]}, cookie_header=cookie_header)
    # Save raw for debugging
    base_name = None
    parsed = {}
    if fs.get("raw"):
        try:
            # determine basename from returned url or scene path
            raw_url = fs.get("raw", {}).get("solution", {}).get("url") or SCENE_URL
            parts = [p for p in raw_url.split("/") if p]
            base_name = parts[-1] if parts else "result"
            raw_path = os.path.join(CONFIG_RAW_DIR, f"{base_name}_flaresolverr_raw.json")
            with open(raw_path, "w", encoding="utf-8") as f:
                json.dump(fs.get("raw"), f, ensure_ascii=False, indent=2)
        except Exception:
            pass
    # If cookies returned, save for reuse
    if fs.get("cookies"):
        save_flaresolverr_session(fs.get("cookies"))
    if fs.get("ok") and fs.get("html"):
        parsed = extract_gravure_fields(fs.get("html"))
    # Build output in same shape as existing script expects
    result = {
        "code": parsed.get("code"),
        "title": parsed.get("title"),
        "date": parsed.get("date"),
        "performers": parsed.get("performers"),
        "tags": parsed.get("tags"),
        "image": parsed.get("image"),
        "studio": parsed.get("studio"),
        "details": parsed.get("details"),
        "url": SCENE_URL
    }
    # Save a copy under config outputs for debug/traceability (retained only per retention policy)
    if base_name:
        try:
            out_path = os.path.join(CONFIG_OUT_DIR, f"{base_name}.json")
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(result, f, ensure_ascii=False, indent=2)
        except Exception:
            pass
    # Print JSON for the application (stdout)
    print(json.dumps(result, ensure_ascii=False))
    sys.exit(0)

# --- existing javlibrary final assembly (unchanged) ---
log.debug('[JAV] {}'.format(jav_result))

# Time to scrape all data (JavLibrary)
scrape = {}
scrape['code'] = next(iter(jav_result.get('code', []))) if jav_result.get('code') else None
scrape['title'] = jav_result.get('title')
scrape['date'] = next(iter(jav_result.get('date', []))) if jav_result.get('date') else None
scrape['director'] = jav_result.get('director') or None
scrape['url'] = jav_result.get('url')
scrape['details'] = regexreplace(jav_result.get('details', "")) if isinstance(jav_result.get('details', ""), str) else jav_result.get('details')
scrape['studio'] = {'name': next(iter(jav_result.get('studio', []))) } if jav_result.get('studio') else None
scrape['label'] = {'name': jav_result.get('label'), } if jav_result.get('label') else None

if WAIT_FOR_ALIASES and not IGNORE_ALIASES:
    try:
        if javlibrary_aliases_thread.is_alive():
            javlibrary_aliases_thread.join()
    except NameError:
        log.debug("No Jav Aliases Thread")
scrape['performers'] = buildlist_tagperf(jav_result, "perf_jav")
scrape['tags'] = buildlist_tagperf(jav_result.get('tags', []), "tags")
scrape['tags'] = [ {"name": tag_name.strip()} for tag_dict in scrape['tags'] for tag_name in tag_dict["name"].replace('·', ',').split(",")]

try:
    if imageBase64_jav_thread.is_alive() is True:
        imageBase64_jav_thread.join()
    if jav_result.get('image'):
        scrape['image'] = jav_result['image']
except NameError:
    log.debug("No image JAV Thread")

print(json.dumps(scrape))