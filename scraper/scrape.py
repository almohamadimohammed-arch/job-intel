"""
Job Intel v5 — AI Agent Scraper (Gemini-Powered)
For EACH of 351 entities:
  1. Playwright opens their website
  2. Gemini AI reads the page and finds the Careers link
  3. Playwright clicks it
  4. Gemini AI reads the career page and extracts real job titles
  5. Playwright searches LinkedIn for entity name (last 30 days)
"""
import json, hashlib, re, time, random, os, sys
from datetime import date, datetime, timedelta
from pathlib import Path
from urllib.parse import quote_plus, urljoin
import requests

GEMINI_KEY = os.environ.get("GEMINI_API_KEY", "")
if not GEMINI_KEY:
    print("WARNING: No GEMINI_API_KEY found. AI features disabled.")

ENTITIES_PATH = Path(__file__).parent / "entities.json"
ALL_ENTITIES = json.loads(ENTITIES_PATH.read_text(encoding="utf-8")) if ENTITIES_PATH.exists() else []
print(f"Loaded {len(ALL_ENTITIES)} entities")

TIER1 = [
    "SDAIA","NEOM","Qiddiya","Public Investment Fund","stc",
    "Aramco","ROSHN","Red Sea Global","KAUST","Elm",
    "ACWA Power","SABIC","HUMAIN","Savvy Games",
    "Digital Government Authority","National Competitiveness Center",
    "Saudi Space Commission","MISA","RDIA","MCIT","NCA",
    "King Abdullah Financial District","Riyadh Air","New Murabba",
    "Saudi Electricity Company","Saudi Arabian Mining","Diriyah Gate",
    "AlUla Development","Saudi Tourism Authority",
    "General Entertainment Authority","Jeddah Central",
    "Saudi Company for Artificial Intelligence","CEER",
    "Saudi Central Bank","Capital Market Authority",
]


def ask_gemini(prompt, max_tokens=1000):
    """Send a prompt to Gemini Flash and get a response."""
    if not GEMINI_KEY:
        return ""
    try:
        resp = requests.post(
            f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={GEMINI_KEY}",
            json={
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {"maxOutputTokens": max_tokens, "temperature": 0.1}
            },
            timeout=30
        )
        if resp.status_code == 200:
            data = resp.json()
            return data["candidates"][0]["content"]["parts"][0]["text"]
        else:
            print(f"    Gemini error {resp.status_code}: {resp.text[:200]}")
            return ""
    except Exception as e:
        print(f"    Gemini exception: {e}")
        return ""


def simplify_html(html_text, max_chars=8000):
    """Strip scripts, styles, and excess whitespace. Keep links and text."""
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html_text, "html.parser")
    for tag in soup(["script", "style", "svg", "path", "noscript", "iframe", "img", "video", "audio"]):
        tag.decompose()
    links = []
    for a in soup.find_all("a", href=True):
        text = a.get_text(strip=True)
        href = a.get("href", "")
        if text and len(text) < 100:
            links.append(f'<a href="{href}">{text}</a>')
    text = soup.get_text(separator=" ", strip=True)
    text = re.sub(r'\s+', ' ', text)[:3000]
    links_text = "\n".join(links[:150])
    return f"PAGE TEXT:\n{text}\n\nALL LINKS:\n{links_text}"


def fingerprint(title, company):
    return hashlib.sha256(f"{title.lower().strip()}|{company.lower().strip()}".encode()).hexdigest()[:12]


def parse_posted(text):
    if not text: return date.today().isoformat()
    t = text.strip().lower()
    today = date.today()
    for pat, unit in [(r"(\d+)\s*day","d"),(r"(\d+)\s*hour","h"),(r"(\d+)\s*week","w"),(r"(\d+)\s*month","m"),(r"today|just now|الآن","0"),(r"yesterday|أمس","1"),(r"منذ\s*(\d+)\s*يوم","d")]:
        m = re.search(pat, t)
        if m:
            if unit in ("0","h"): return today.isoformat()
            if unit == "1": return (today - timedelta(days=1)).isoformat()
            n = int(m.group(1)) if m.groups() else 1
            d = {"d":timedelta(days=n),"w":timedelta(weeks=n),"m":timedelta(days=n*30)}
            return (today - d.get(unit, timedelta(0))).isoformat()
    return today.isoformat()


def detect_seniority(title):
    t = title.lower()
    if any(w in t for w in ["chief","cto","ceo","cfo","vp","vice president"]): return "executive"
    if any(w in t for w in ["director","head of","general manager","gm "]): return "director"
    if any(w in t for w in ["senior","sr.","lead","principal","manager"]): return "senior"
    if any(w in t for w in ["junior","jr.","associate","intern","trainee"]): return "junior"
    return "mid"


def detect_category(title):
    t = title.lower()
    if any(w in t for w in ["strategy","consulting","consultant","advisory","governance","policy","transformation"]): return "Strategy & Consulting"
    if any(w in t for w in ["technology","product","ai ","data","cyber","digital","cloud","software","engineer","developer","architect"]): return "Technology & Product"
    if any(w in t for w in ["operations","program","project","construction","infrastructure","logistics"]): return "Operations & Execution"
    if any(w in t for w in ["finance","investment","fund","risk","banking","accounting","audit"]): return "Finance & Investment"
    return "Strategy & Consulting"


HIGH_COS = {"pif","neom","qiddiya","stc","aramco","sdaia","bcg","mckinsey","bain","kaust","roshn","humain","elm","acwa","sabic"}


def relevance_score(title, company):
    s = 0.55
    for w in ["innovation","strategy","transformation","director","head","lead","chief","vp"]:
        if w in title.lower(): s += 0.04
    for c in HIGH_COS:
        if c in company.lower(): s += 0.04; break
    return round(min(s, 0.98), 2)


def extract_city(text):
    for c in ["Riyadh","Jeddah","Dammam","Dhahran","NEOM","Jubail","Mecca","Medina","Tabuk","Khobar","Thuwal"]:
        if c.lower() in (text or "").lower(): return c
    return "Riyadh"


def build_job(title, company, city, source, url, posted="", summary=""):
    return {
        "id": fingerprint(title, company),
        "src": source,
        "t": title[:120],
        "co": company[:80],
        "cy": city or "Riyadh",
        "ct": "SA",
        "ca": detect_category(title),
        "tg": [],
        "sg": [],
        "sn": detect_seniority(title),
        "sc": relevance_score(title, company),
        "sm": (summary or "")[:200],
        "st": "new",
        "dt": parse_posted(posted),
        "u": url,
    }


def ai_find_careers_link(page_html, entity_name, page_url):
    """Ask Gemini to find the careers/jobs link on a webpage."""
    simplified = simplify_html(page_html)
    prompt = f"""You are looking at the website of "{entity_name}" ({page_url}).
Your task: find the link that leads to their Careers page, Jobs page, or Vacancies page.

{simplified}

Respond with ONLY the href URL of the careers/jobs link. Nothing else.
If the page is in Arabic, look for وظائف or التوظيف or فرص عمل or انضم إلينا links.
If there is no careers/jobs link on this page, respond with exactly: NONE
Do not explain. Just the URL or NONE."""

    result = ask_gemini(prompt, max_tokens=200)
    result = result.strip().strip('"').strip("'").strip("`")
    if not result or "NONE" in result.upper() or len(result) > 500:
        return None
    if result.startswith("/"):
        result = urljoin(page_url, result)
    if not result.startswith("http"):
        return None
    return result


def ai_extract_jobs(page_html, entity_name, page_url):
    """Ask Gemini to extract job titles from a careers page."""
    simplified = simplify_html(page_html)
    prompt = f"""You are looking at the careers/jobs page of "{entity_name}" ({page_url}).
Your task: extract ALL job vacancy titles listed on this page.

{simplified}

Respond with a JSON array of objects. Each object has:
- "title": the exact job title as shown on the page
- "url": the link to that specific job posting (full URL)

Example: [{{"title": "Senior Data Engineer", "url": "https://example.com/jobs/123"}}]

Rules:
- Only include REAL job vacancy titles (like "Senior Manager", "Data Analyst", "Project Director")
- Do NOT include navigation items like "About", "Home", "Leadership", "Contact"
- Do NOT include department names or categories
- If no job vacancies are found, respond with: []
- Respond with ONLY the JSON array. No explanation."""

    result = ask_gemini(prompt, max_tokens=2000)
    result = result.strip()
    if result.startswith("```"):
        result = re.sub(r'^```\w*\n?', '', result)
        result = re.sub(r'\n?```$', '', result)
    result = result.strip()
    try:
        jobs_data = json.loads(result)
        if isinstance(jobs_data, list):
            return jobs_data
    except Exception:
        pass
    return []


def scrape_entity_ai(browser_page, entity_name, url):
    """AI Agent: navigate entity website, find careers, extract jobs."""
    if not url: return []
    jobs = []
    print(f"    Opening {url}")

    # Step 1: Load the main page
    try:
        browser_page.goto(url, wait_until="domcontentloaded", timeout=20000)
        time.sleep(2)
    except Exception as e:
        print(f"    Could not load: {e}")
        return jobs

    main_html = browser_page.content()
    current_url = browser_page.url

    # Step 2: Ask Gemini to find the careers link
    print(f"    Asking AI to find careers link...")
    careers_url = ai_find_careers_link(main_html, entity_name, current_url)

    if careers_url:
        print(f"    AI found careers link: {careers_url}")
        # Step 3: Navigate to careers page
        try:
            browser_page.goto(careers_url, wait_until="domcontentloaded", timeout=20000)
            time.sleep(3)
            # Scroll to load lazy content
            for _ in range(3):
                browser_page.evaluate("window.scrollBy(0, 800)")
                time.sleep(0.5)
            careers_html = browser_page.content()
            careers_page_url = browser_page.url
        except Exception as e:
            print(f"    Could not load careers page: {e}")
            return jobs

        # Step 4: Ask Gemini to extract job titles
        print(f"    Asking AI to extract jobs...")
        job_list = ai_extract_jobs(careers_html, entity_name, careers_page_url)

        for job_data in job_list:
            title = job_data.get("title", "").strip()
            job_url = job_data.get("url", careers_page_url)
            if not title or len(title) < 5: continue
            if not job_url.startswith("http"):
                job_url = urljoin(careers_page_url, job_url)
            j = build_job(title, entity_name, "Riyadh", "career", job_url)
            jobs.append(j)

        if jobs:
            print(f"    AI extracted {len(jobs)} real jobs")
    else:
        print(f"    AI found no careers link")

    return jobs


def scrape_linkedin_entity(browser_page, entity_name):
    """Search LinkedIn for jobs at this entity, last 30 days."""
    jobs = []
    search_query = quote_plus(entity_name)

    try:
        url = f"https://www.linkedin.com/jobs/search/?keywords={search_query}&location=Saudi%20Arabia&geoId=100459316&f_TPR=r2592000&sortBy=DD"
        browser_page.goto(url, wait_until="domcontentloaded", timeout=20000)
        time.sleep(3)

        # Scroll to load more
        for _ in range(3):
            browser_page.evaluate("window.scrollBy(0, 600)")
            time.sleep(1)

        # Extract job cards
        cards = browser_page.query_selector_all("li")
        for card in cards:
            try:
                title_el = card.query_selector("h3") or card.query_selector(".base-search-card__title")
                if not title_el: continue
                title = title_el.inner_text().strip()
                if not title or len(title) < 5: continue

                company_el = card.query_selector("h4") or card.query_selector(".base-search-card__subtitle")
                company = company_el.inner_text().strip() if company_el else ""

                # Check company matches entity
                if not company: continue
                en = entity_name.lower()
                co = company.lower()
                match = en in co or co in en
                if not match:
                    en_words = [w for w in en.split() if len(w) > 2]
                    match = any(w in co for w in en_words)
                if not match: continue

                link_el = card.query_selector("a[href*='/jobs/view/']") or card.query_selector("a.base-card__full-link")
                href = link_el.get_attribute("href") if link_el else ""
                job_url = href.split("?")[0] if href else ""
                if not job_url.startswith("http"):
                    job_url = f"https://www.linkedin.com{job_url}" if job_url else ""

                loc_el = card.query_selector(".job-search-card__location")
                location = loc_el.inner_text().strip() if loc_el else ""

                time_el = card.query_selector("time")
                posted = time_el.get_attribute("datetime") if time_el else ""

                j = build_job(title, company, extract_city(location), "linkedin", job_url, posted=posted)
                jobs.append(j)

                if len(jobs) >= 25: break
            except Exception: continue

    except Exception as e:
        print(f"    LinkedIn error: {e}")

    return jobs


def linkedin_public_api(entity_name):
    """Fallback: use LinkedIn public API (no browser needed)."""
    jobs = []
    kw = quote_plus(entity_name + " Saudi Arabia")
    try:
        resp = requests.get(
            f"https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?keywords={kw}&location=Saudi%20Arabia&geoId=100459316&start=0&sortBy=DD&f_TPR=r2592000",
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/125.0.0.0 Safari/537.36"},
            timeout=15
        )
        if not resp or resp.status_code != 200: return jobs
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(resp.text, "html.parser")
        for card in soup.select("li"):
            try:
                a = card.select_one("a.base-card__full-link") or card.select_one("a[href*='/jobs/view/']")
                if not a: continue
                t_el = card.select_one("h3") or card.select_one(".base-search-card__title")
                title = t_el.get_text(strip=True) if t_el else ""
                if not title or len(title) < 5: continue
                href = a.get("href","").split("?")[0]
                url = href if href.startswith("http") else f"https://www.linkedin.com{href}"
                co_el = card.select_one("h4") or card.select_one(".base-search-card__subtitle")
                company = co_el.get_text(strip=True) if co_el else ""
                en = entity_name.lower()
                co = company.lower()
                match = en in co or co in en
                if not match:
                    en_words = [w for w in en.split() if len(w) > 2]
                    match = any(w in co for w in en_words)
                if not match: continue
                loc = card.select_one(".job-search-card__location")
                location = loc.get_text(strip=True) if loc else ""
                tm = card.select_one("time")
                posted = tm.get("datetime","") if tm else ""
                j = build_job(title, company, extract_city(location), "linkedin", url, posted=posted)
                jobs.append(j)
            except Exception: continue
    except Exception: pass
    return jobs


def deduplicate(jobs):
    seen = set()
    out = []
    for j in jobs:
        if j["id"] not in seen:
            seen.add(j["id"])
            out.append(j)
    return out


def process_entity(entity, browser_page=None, use_ai=False):
    """Process one entity: AI website scrape + LinkedIn search."""
    name = entity["name"]
    url = entity.get("url","")
    linkedin = entity.get("linkedin","")
    name_ar = entity.get("name_ar","")
    jobs = []

    # Step 1: AI-powered website scraping
    if use_ai and browser_page and url and GEMINI_KEY:
        try:
            website_jobs = scrape_entity_ai(browser_page, name, url)
            jobs.extend(website_jobs)
        except Exception as e:
            print(f"    Website error: {e}")

    # Step 2: LinkedIn search
    if browser_page:
        try:
            li_jobs = scrape_linkedin_entity(browser_page, name)
            jobs.extend(li_jobs)
            if li_jobs:
                print(f"    LinkedIn: {len(li_jobs)} jobs")
        except Exception:
            # Fallback to public API
            li_jobs = linkedin_public_api(name)
            jobs.extend(li_jobs)
    else:
        li_jobs = linkedin_public_api(name)
        jobs.extend(li_jobs)
        if li_jobs:
            print(f"    LinkedIn API: {len(li_jobs)} jobs")

    # Step 3: Fallback — LinkedIn portal link
    if not jobs and linkedin:
        j = build_job(
            f"Open Roles at {name}", name, "Riyadh", "gov",
            linkedin.rstrip("/") + "/jobs/",
            summary=f"{name} ({name_ar}). Check LinkedIn for current openings."
        )
        jobs.append(j)

    # Rate limit for Gemini (15 req/min free tier)
    time.sleep(2)

    return jobs


def main():
    print(f"{'='*60}")
    print(f"JOB INTEL v5 — AI AGENT (Gemini)")
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"Processing {len(ALL_ENTITIES)} entities")
    print(f"Gemini API: {'ACTIVE' if GEMINI_KEY else 'NOT SET'}")
    print(f"{'='*60}")

    all_jobs = []
    tier1_lower = [n.lower() for n in TIER1]

    # Launch browser
    browser_page = None
    try:
        from playwright.sync_api import sync_playwright
        pw = sync_playwright().start()
        browser = pw.chromium.launch(headless=True, args=["--no-sandbox","--disable-dev-shm-usage","--disable-gpu"])
        browser_page = browser.new_page()
        browser_page.set_default_timeout(15000)
        print("Browser ready\n")
    except Exception as e:
        print(f"Playwright not available ({e}), using API only\n")
        browser_page = None

    # TIER 1: Priority entities — full AI + LinkedIn
    print(f"[TIER 1] {len(TIER1)} priority entities (AI agent + LinkedIn)")
    print("="*60)
    tier1_found = set()
    for entity in ALL_ENTITIES:
        name = entity["name"]
        is_tier1 = any(t in name.lower() for t in tier1_lower)
        if not is_tier1: continue
        print(f"\n>> {name}")
        jobs = process_entity(entity, browser_page=browser_page, use_ai=True)
        all_jobs.extend(jobs)
        tier1_found.add(name.lower())
        real = [j for j in jobs if "Open Roles" not in j["t"]]
        print(f"   RESULT: {len(real)} real jobs, {len(jobs)} total")

    # TIER 2: Remaining entities — LinkedIn only (no AI to save quota)
    remaining = [e for e in ALL_ENTITIES if e["name"].lower() not in tier1_found]
    print(f"\n\n[TIER 2] {len(remaining)} remaining entities (LinkedIn only)")
    print("="*60)
    for i, entity in enumerate(remaining):
        name = entity["name"]
        if (i+1) % 25 == 0:
            print(f"\n  [{i+1}/{len(remaining)}] {name}")
        jobs = process_entity(entity, browser_page=browser_page, use_ai=False)
        all_jobs.extend(jobs)

    # Cleanup
    if browser_page:
        try:
            browser_page.close()
            browser.close()
            pw.stop()
        except: pass

    # Deduplicate and sort
    unique = deduplicate(all_jobs)
    unique.sort(key=lambda j: j["sc"], reverse=True)

    real_jobs = [j for j in unique if "Open Roles" not in j["t"]]
    portal_links = [j for j in unique if "Open Roles" in j["t"]]

    print(f"\n\n{'='*60}")
    print(f"FINAL RESULTS:")
    print(f"  Real job postings: {len(real_jobs)}")
    print(f"  Portal links: {len(portal_links)}")
    print(f"  Total unique: {len(unique)}")
    print(f"{'='*60}")

    # Preserve user statuses
    prev = Path("jobs.json")
    sm = {}
    if prev.exists():
        try:
            old = json.loads(prev.read_text())
            for j in old.get("jobs",[]):
                if j.get("st") not in (None,"new"):
                    sm[j["id"]] = j["st"]
        except: pass
    for j in unique:
        if j["id"] in sm:
            j["st"] = sm[j["id"]]

    out = {"updated":datetime.now().isoformat(),"count":len(unique),"jobs":unique[:500]}
    Path("jobs.json").write_text(json.dumps(out, ensure_ascii=False, indent=2))
    print(f"\nWrote {out['count']} jobs to jobs.json")


if __name__ == "__main__":
    main()
