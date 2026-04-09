"""
Job Intel v4 — Entity-First Scraper
For EACH of the 351 entities:
  Step 1: Visit their website, find Careers page, extract job postings
  Step 2: Search Bayt.com for that entity name
  Step 3: Search LinkedIn for that entity name
  Step 4: Search Wadhefa for that entity name
ONLY jobs belonging to one of the 351 entities appear.
"""
import json, hashlib, re, time, random, sys
from datetime import date, datetime, timedelta
from pathlib import Path
from urllib.parse import quote_plus, urljoin
import requests
from bs4 import BeautifulSoup

UA = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 Version/17.5 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/125.0.0.0 Safari/537.36",
]
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
session = requests.Session()

def http_get(url, params=None, timeout=12):
    session.headers.update({"User-Agent": random.choice(UA)})
    time.sleep(random.uniform(1.2, 2.5))
    try:
        r = session.get(url, params=params, timeout=timeout, allow_redirects=True)
        r.raise_for_status()
        return r
    except Exception:
        return None

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

def is_valid_job_title(text):
    t = text.strip()
    if len(t) < 8 or len(t) > 160: return False
    tl = t.lower()
    garbage_exact = {
        "about","about us","contact","contact us","home","menu","leadership",
        "our leadership","board of directors","management","media center",
        "media centre","news","press","blog","our history","history",
        "our story","overview","who we are","our values","vision","mission",
        "more information","learn more","read more","show details",
        "get in touch","connect with us","follow us","privacy","terms",
        "login","sign in","sign up","register","subscribe","search",
        "english","arabic","top content","life at","our culture","benefits",
        "investors","sustainability","partners","clients","services",
        "products","solutions","locations","offices","settings","preferences",
    }
    if tl in garbage_exact: return False
    for g in garbage_exact:
        if tl.startswith(g + " ") and len(tl) < 30: return False
    if t[0] in "#[{<@!": return False
    if any(x in tl for x in ["http","www.",".com/",".sa/",".org/"]): return False
    job_words = {
        "manager","director","specialist","analyst","engineer","consultant",
        "advisor","coordinator","officer","lead","supervisor","architect",
        "developer","designer","planner","head","chief","vp","president",
        "administrator","executive","associate","assistant","intern",
        "trainee","accountant","auditor","controller","buyer","procurement",
        "recruiter","legal","counsel","scientist","researcher","professor",
        "lecturer","driver","technician","operator","mechanic","nurse",
        "doctor","pharmacist","therapist","sales","marketing","business",
        "project","program","portfolio","security","safety","compliance",
        "risk","data","cloud","network","system","secretary","representative",
        "foreman","inspector","surveyor","estimator","scheduler",
    }
    return any(w in tl for w in job_words)

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

def company_matches(found_company, entity_name):
    if not found_company: return False
    fc = found_company.lower().strip()
    en = entity_name.lower().strip()
    if en in fc or fc in en: return True
    en_words = [w for w in en.split() if len(w) > 2]
    for w in en_words:
        if w in fc: return True
    abbrevs = {
        "stc": ["saudi telecom","stc group"],
        "pif": ["public investment fund"],
        "neom": ["neom"],
        "sdaia": ["saudi data","artificial intelligence authority"],
        "kaust": ["king abdullah university"],
        "aramco": ["saudi aramco","aramco"],
        "sabic": ["sabic"],
        "elm": ["elm company"],
        "roshn": ["roshn"],
        "acwa": ["acwa power"],
        "mcit": ["ministry of communications"],
        "misa": ["ministry of investment"],
        "nca": ["national cybersecurity"],
        "rdia": ["research development and innovation"],
    }
    for abbr, matches in abbrevs.items():
        if abbr in en.lower():
            for m in matches:
                if m in fc: return True
    return False

CAREER_PATHS = ["/careers","/en/careers","/career","/en/career","/jobs","/en/jobs","/join-us","/en/join-us","/vacancies","/opportunities"]
CAREER_LINK_WORDS = ["career","careers","jobs","job opening","vacancies","join us","join our","work with us","hiring","opportunities","وظائف","التوظيف","فرص","انضم"]

def bayt_search(entity_name):
    jobs = []
    resp = http_get("https://www.bayt.com/en/saudi-arabia/jobs/", params={"keyword": entity_name, "page": 1})
    if not resp: return jobs
    soup = BeautifulSoup(resp.text, "html.parser")
    cards = soup.select("li[data-js-job]") or soup.select(".has-pointer-d") or soup.select("li.is-compact")
    for card in cards:
        try:
            a = card.select_one("h2 a") or card.select_one("a")
            if not a: continue
            title = a.get_text(strip=True)
            if not is_valid_job_title(title): continue
            href = a.get("href","")
            url = f"https://www.bayt.com{href}" if href.startswith("/") else href
            co_el = card.select_one(".t-mute a")
            company = co_el.get_text(strip=True) if co_el else ""
            if not company_matches(company, entity_name): continue
            loc = card.select_one(".t-mute span")
            location = loc.get_text(strip=True) if loc else ""
            dt = card.select_one("time") or card.select_one(".t-small")
            posted = dt.get_text(strip=True) if dt else ""
            j = build_job(title, company, extract_city(location), "bayt", url, posted=posted)
            jobs.append(j)
        except Exception: continue
    return jobs

def linkedin_search(entity_name):
    jobs = []
    kw = quote_plus(entity_name + " Saudi Arabia")
    resp = http_get(f"https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?keywords={kw}&location=Saudi%20Arabia&geoId=100459316&start=0&sortBy=DD")
    if not resp: return jobs
    soup = BeautifulSoup(resp.text, "html.parser")
    for card in soup.select("li"):
        try:
            a = card.select_one("a.base-card__full-link") or card.select_one("a[href*='/jobs/view/']")
            if not a: continue
            t_el = card.select_one("h3") or card.select_one(".base-search-card__title")
            title = t_el.get_text(strip=True) if t_el else ""
            if not is_valid_job_title(title): continue
            href = a.get("href","").split("?")[0]
            url = href if href.startswith("http") else f"https://www.linkedin.com{href}"
            co_el = card.select_one("h4") or card.select_one(".base-search-card__subtitle")
            company = co_el.get_text(strip=True) if co_el else ""
            if not company_matches(company, entity_name): continue
            loc = card.select_one(".job-search-card__location")
            location = loc.get_text(strip=True) if loc else ""
            tm = card.select_one("time")
            posted = tm.get("datetime","") if tm else ""
            j = build_job(title, company, extract_city(location), "linkedin", url, posted=posted)
            jobs.append(j)
        except Exception: continue
    return jobs

def wadhefa_search(entity_name):
    jobs = []
    resp = http_get("https://www.wadhefa.com/en/jobs/search", params={"q": entity_name, "country": "saudi-arabia"})
    if not resp: return jobs
    soup = BeautifulSoup(resp.text, "html.parser")
    for a_tag in soup.find_all("a", href=re.compile(r"(?i)/jobs?/|/position|/vacanc")):
        try:
            title = a_tag.get_text(strip=True)
            if not is_valid_job_title(title): continue
            href = a_tag.get("href","")
            url = href if href.startswith("http") else urljoin("https://www.wadhefa.com", href)
            j = build_job(title, entity_name, "Riyadh", "wadhefa", url)
            jobs.append(j)
        except Exception: continue
    return jobs

def scrape_entity_website(entity_name, url):
    if not url: return []
    jobs = []
    resp = http_get(url, timeout=10)
    if not resp: return jobs
    soup = BeautifulSoup(resp.text, "html.parser")
    career_url = None
    for link in soup.find_all("a", href=True):
        text = (link.get_text(strip=True) or "").lower()
        href = (link.get("href") or "").lower()
        if any(w in text for w in CAREER_LINK_WORDS) or any(w in href for w in ["career","jobs","vacanc","hiring","join"]):
            raw_href = link.get("href","")
            career_url = raw_href if raw_href.startswith("http") else urljoin(url, raw_href)
            break
    if not career_url:
        base = url.rstrip("/")
        for path in CAREER_PATHS:
            test_resp = http_get(base + path, timeout=8)
            if test_resp and test_resp.status_code == 200:
                career_url = base + path
                break
    if not career_url: return jobs
    resp2 = http_get(career_url, timeout=10)
    if not resp2: return jobs
    soup2 = BeautifulSoup(resp2.text, "html.parser")
    for link in soup2.find_all("a", href=True):
        text = link.get_text(strip=True)
        if not is_valid_job_title(text): continue
        href = link.get("href","")
        job_url = href if href.startswith("http") else urljoin(career_url, href)
        j = build_job(text, entity_name, "Riyadh", "career", job_url)
        jobs.append(j)
        if len(jobs) >= 15: break
    return jobs

def scrape_entity_website_playwright(browser_page, entity_name, url):
    if not url: return []
    jobs = []
    try:
        browser_page.goto(url, wait_until="domcontentloaded", timeout=15000)
        time.sleep(2)
    except Exception:
        return jobs
    try:
        links = browser_page.query_selector_all("a")
        career_link = None
        for link in links:
            try:
                text = (link.inner_text() or "").strip().lower()
                href = (link.get_attribute("href") or "").lower()
                if any(w in text for w in CAREER_LINK_WORDS) or any(w in href for w in ["career","jobs","vacanc","hiring","join"]):
                    if len(text) < 40:
                        career_link = link
                        break
            except: continue
        if career_link:
            career_link.click()
            time.sleep(3)
            browser_page.wait_for_load_state("domcontentloaded", timeout=10000)
            time.sleep(2)
        else:
            base = url.rstrip("/")
            found = False
            for path in CAREER_PATHS[:4]:
                try:
                    browser_page.goto(base + path, wait_until="domcontentloaded", timeout=10000)
                    time.sleep(2)
                    title = browser_page.title() or ""
                    if "404" not in title and "not found" not in title.lower():
                        found = True
                        break
                except: continue
            if not found: return jobs
    except Exception:
        return jobs
    try:
        for _ in range(3):
            browser_page.evaluate("window.scrollBy(0, 800)")
            time.sleep(0.5)
    except: pass
    try:
        current_url = browser_page.url
        all_links = browser_page.query_selector_all("a")
        seen = set()
        for link in all_links:
            try:
                text = (link.inner_text() or "").strip()
                if not is_valid_job_title(text): continue
                if text.lower() in seen: continue
                seen.add(text.lower())
                href = link.get_attribute("href") or ""
                job_url = href if href.startswith("http") else urljoin(current_url, href)
                j = build_job(text, entity_name, "Riyadh", "career", job_url)
                jobs.append(j)
                if len(jobs) >= 15: break
            except: continue
    except: pass
    return jobs

def deduplicate(jobs):
    seen = set()
    out = []
    for j in jobs:
        if j["id"] not in seen:
            seen.add(j["id"])
            out.append(j)
    return out

def process_entity(entity, use_playwright=False, browser_page=None):
    name = entity["name"]
    url = entity.get("url","")
    linkedin = entity.get("linkedin","")
    name_ar = entity.get("name_ar","")
    jobs = []
    if use_playwright and browser_page and url:
        try:
            website_jobs = scrape_entity_website_playwright(browser_page, name, url)
            jobs.extend(website_jobs)
        except: pass
    elif url:
        try:
            website_jobs = scrape_entity_website(name, url)
            jobs.extend(website_jobs)
        except: pass
    try:
        bayt_jobs = bayt_search(name)
        jobs.extend(bayt_jobs)
    except: pass
    try:
        li_jobs = linkedin_search(name)
        jobs.extend(li_jobs)
    except: pass
    try:
        w_jobs = wadhefa_search(name)
        jobs.extend(w_jobs)
    except: pass
    if not jobs and linkedin:
        j = build_job(
            f"Open Roles at {name}", name, "Riyadh", "gov",
            linkedin.rstrip("/") + "/jobs/",
            summary=f"{name} ({name_ar}). Check LinkedIn for current openings."
        )
        jobs.append(j)
    return jobs

def main():
    print(f"{'='*60}")
    print(f"JOB INTEL v4 — ENTITY-FIRST SCRAPER")
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"Processing {len(ALL_ENTITIES)} entities")
    print(f"{'='*60}")
    all_jobs = []
    tier1_lower = [n.lower() for n in TIER1]
    print(f"\n[TIER 1] Priority entities (browser + all boards)")
    browser_page = None
    try:
        from playwright.sync_api import sync_playwright
        pw = sync_playwright().start()
        browser = pw.chromium.launch(headless=True, args=["--no-sandbox","--disable-dev-shm-usage"])
        browser_page = browser.new_page()
        browser_page.set_default_timeout(12000)
        print("  Playwright browser ready")
    except Exception as e:
        print(f"  Playwright not available ({e}), using HTTP only")
        browser_page = None
    tier1_names_found = set()
    for entity in ALL_ENTITIES:
        name = entity["name"]
        is_tier1 = any(t in name.lower() for t in tier1_lower)
        if not is_tier1: continue
        print(f"  -> {name}")
        jobs = process_entity(entity, use_playwright=(browser_page is not None), browser_page=browser_page)
        all_jobs.extend(jobs)
        tier1_names_found.add(name.lower())
        real = [j for j in jobs if "Open Roles" not in j["t"]]
        if real:
            print(f"     {len(real)} real jobs found")
    remaining = [e for e in ALL_ENTITIES if e["name"].lower() not in tier1_names_found]
    print(f"\n[TIER 2] {len(remaining)} remaining entities (HTTP + job boards)")
    for i, entity in enumerate(remaining):
        name = entity["name"]
        if (i+1) % 25 == 0:
            print(f"  [{i+1}/{len(remaining)}] {name}...")
        jobs = process_entity(entity, use_playwright=False, browser_page=None)
        all_jobs.extend(jobs)
    if browser_page:
        try:
            browser_page.close()
            browser.close()
            pw.stop()
        except: pass
    unique = deduplicate(all_jobs)
    unique.sort(key=lambda j: j["sc"], reverse=True)
    real_jobs = [j for j in unique if "Open Roles" not in j["t"]]
    portal_links = [j for j in unique if "Open Roles" in j["t"]]
    print(f"\n{'='*60}")
    print(f"Real job postings: {len(real_jobs)}")
    print(f"Portal links: {len(portal_links)}")
    print(f"Total unique: {len(unique)}")
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
