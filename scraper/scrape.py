import json
import hashlib
import re
import time
import random
from datetime import date, datetime, timedelta
from pathlib import Path
from urllib.parse import quote_plus, urljoin

import requests
from bs4 import BeautifulSoup

# Playwright imported inside functions (lazy load)

UA = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 Version/17.5 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/125.0.0.0 Safari/537.36",
]

KEYWORDS = [
    "innovation manager", "strategy director", "transformation lead",
    "digital transformation", "corporate development", "AI strategy",
    "technology director", "management consultant", "Vision 2030",
    "innovation director", "head of strategy", "product director",
]

ENTITIES_PATH = Path(__file__).parent / "entities.json"
ALL_ENTITIES = json.loads(ENTITIES_PATH.read_text(encoding="utf-8")) if ENTITIES_PATH.exists() else []
print(f"Loaded {len(ALL_ENTITIES)} entities")

session = requests.Session()

# ═══════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════

def get(url, params=None, timeout=15):
    session.headers.update({"User-Agent": random.choice(UA)})
    time.sleep(random.uniform(1.0, 2.0))
    try:
        r = session.get(url, params=params, timeout=timeout)
        r.raise_for_status()
        return r
    except Exception:
        return None

def fp(title, company):
    s = f"{title.lower().strip()}|{company.lower().strip()}"
    return hashlib.sha256(s.encode()).hexdigest()[:12]

def parse_date(text):
    if not text:
        return date.today().isoformat()
    text = text.strip().lower()
    today = date.today()
    for pat, unit in [(r"(\d+)\s*day", "d"), (r"(\d+)\s*hour", "h"), (r"(\d+)\s*week", "w"),
                      (r"(\d+)\s*month", "m"), (r"today|just now", "0"), (r"yesterday", "1"),
                      (r"منذ\s*(\d+)\s*يوم", "d"), (r"منذ\s*(\d+)\s*أسبوع", "w")]:
        m = re.search(pat, text)
        if m:
            if unit in ("0", "h"): return today.isoformat()
            if unit == "1": return (today - timedelta(days=1)).isoformat()
            n = int(m.group(1)) if m.groups() else 1
            delta = {"d": timedelta(days=n), "w": timedelta(weeks=n), "m": timedelta(days=n*30)}
            return (today - delta.get(unit, timedelta(0))).isoformat()
    return today.isoformat()

def seniority(title):
    t = title.lower()
    if any(w in t for w in ["chief","cto","ceo","cfo","vp","vice president"]): return "executive"
    if any(w in t for w in ["director","head of","general manager"]): return "director"
    if any(w in t for w in ["senior","sr.","lead","principal","manager"]): return "senior"
    if any(w in t for w in ["junior","jr.","associate","intern"]): return "junior"
    return "mid"

def category(title, tags):
    t = (title + " " + " ".join(tags)).lower()
    if any(w in t for w in ["strategy","consulting","consultant","advisory","governance","policy"]): return "Strategy & Consulting"
    if any(w in t for w in ["technology","product","ai","data","cyber","digital","cloud","software","engineer"]): return "Technology & Product"
    if any(w in t for w in ["operations","program","project","construction","infrastructure"]): return "Operations & Execution"
    if any(w in t for w in ["finance","investment","fund","risk","banking"]): return "Finance & Investment"
    return "Strategy & Consulting"

HIGH = {"pif","neom","qiddiya","stc","aramco","sdaia","bcg","mckinsey","bain","kaust","roshn","humain","elm","acwa","sabic"}

def score(title, company, tags, signals):
    s = 0.50
    for w in ["innovation","strategy","transformation","director","head","lead","chief"]:
        if w in title.lower(): s += 0.05
    for c in HIGH:
        if c in company.lower(): s += 0.04; break
    for t in ["innovation","strategy","transformation","ai","vision-2030"]:
        if t in tags: s += 0.02
    s += len(signals) * 0.015
    return round(min(s, 0.98), 2)

def make_job(title, company, city, source, url, tags=None, signals=None, posted="", summary=""):
    tg = list(set((tags or [])[:5]))
    sg = list(set((signals or [])[:3]))
    return {
        "id": fp(title, company), "src": source,
        "t": title[:120], "co": company[:80], "cy": city or "Riyadh", "ct": "SA",
        "ca": category(title, tg), "tg": tg, "sg": sg,
        "sn": seniority(title), "sc": score(title, company, tg, sg),
        "sm": (summary or "")[:200], "st": "new",
        "dt": parse_date(posted), "u": url,
    }

def is_junk(text):
    """Filter out non-job links."""
    junk = ["login","sign in","sign up","cookie","privacy","terms","about us",
            "contact","home","menu","close","search","filter","back","next",
            "previous","loading","copyright","©","follow","share","print",
            "english","arabic","العربية","تسجيل","دخول"]
    t = text.lower().strip()
    if len(t) < 5 or len(t) > 150: return True
    return any(j in t for j in junk)

def extract_city(text):
    for c in ["Riyadh","Jeddah","Dammam","Dhahran","NEOM","Jubail","Mecca","Medina","Tabuk","Khobar","Thuwal"]:
        if c.lower() in text.lower(): return c
    return "Riyadh"


# ═══════════════════════════════════════════
# BAYT.COM (HTTP)
# ═══════════════════════════════════════════
def scrape_bayt(keyword, max_pages=2):
    jobs = []
    for page in range(1, max_pages + 1):
        resp = get("https://www.bayt.com/en/saudi-arabia/jobs/", params={"keyword": keyword, "page": page})
        if not resp: break
        soup = BeautifulSoup(resp.text, "html.parser")
        cards = soup.select("li[data-js-job]") or soup.select(".has-pointer-d") or soup.select("li.is-compact")
        if not cards: break
        for card in cards:
            try:
                a = card.select_one("h2 a") or card.select_one("a")
                if not a: continue
                title = a.get_text(strip=True)
                if not title or len(title) < 5: continue
                href = a.get("href", "")
                url = f"https://www.bayt.com{href}" if href.startswith("/") else href
                co = card.select_one(".t-mute a") or card.select_one("[data-automation-id='company']")
                company = co.get_text(strip=True) if co else ""
                loc = card.select_one(".t-mute span")
                location = loc.get_text(strip=True) if loc else ""
                dt = card.select_one("time") or card.select_one(".t-small")
                posted = dt.get_text(strip=True) if dt else ""
                j = make_job(title, company, extract_city(location), "bayt", url,
                            tags=[keyword.lower().replace(" ","-")], posted=posted)
                if j["co"]: jobs.append(j)
            except Exception: continue
    return jobs


# ═══════════════════════════════════════════
# LINKEDIN PUBLIC (HTTP)
# ═══════════════════════════════════════════
def scrape_linkedin(keyword):
    jobs = []
    kw = quote_plus(keyword)
    resp = get(f"https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?keywords={kw}&location=Saudi%20Arabia&geoId=100459316&start=0&sortBy=DD")
    if not resp: return jobs
    soup = BeautifulSoup(resp.text, "html.parser")
    for card in soup.select("li"):
        try:
            a = card.select_one("a.base-card__full-link") or card.select_one("a[href*='/jobs/view/']")
            if not a: continue
            t_el = card.select_one("h3") or card.select_one(".base-search-card__title")
            title = t_el.get_text(strip=True) if t_el else ""
            if not title or len(title) < 5: continue
            href = a.get("href", "").split("?")[0]
            url = href if href.startswith("http") else f"https://www.linkedin.com{href}"
            co = card.select_one("h4") or card.select_one(".base-search-card__subtitle")
            company = co.get_text(strip=True) if co else ""
            loc = card.select_one(".job-search-card__location")
            location = loc.get_text(strip=True) if loc else ""
            tm = card.select_one("time")
            posted = tm.get("datetime", "") if tm else ""
            j = make_job(title, company, extract_city(location), "linkedin", url,
                        tags=[keyword.lower().replace(" ","-")], posted=posted)
            if j["co"]: jobs.append(j)
        except Exception: continue
    return jobs


# ═══════════════════════════════════════════
# SMART ENTITY SCRAPER (PLAYWRIGHT)
# ═══════════════════════════════════════════

# Career page URL patterns to try
CAREER_PATHS = [
    "/careers", "/en/careers", "/ar/careers",
    "/jobs", "/en/jobs",
    "/career", "/en/career",
    "/join-us", "/en/join-us",
    "/work-with-us",
    "/vacancies", "/en/vacancies",
    "/opportunities",
]

# Words that indicate a "careers" link on any page
CAREER_WORDS = [
    "career", "careers", "jobs", "job opening", "vacancies", "vacancy",
    "join us", "join our", "work with us", "hiring", "opportunities",
    "open positions", "current openings", "apply now",
    "وظائف", "التوظيف", "فرص العمل", "انضم", "فرص وظيفية",
]

# Words that indicate an actual job title link on a career page
JOB_LINK_PATTERNS = [
    r"/job[s]?/",
    r"/position[s]?/",
    r"/vacanc",
    r"/opening[s]?/",
    r"/career[s]?/.*\d",
    r"/apply/",
    r"jobId=",
    r"requisition",
    r"/role[s]?/",
]


def scrape_entities_smart(browser):
    """
    For each of the 351 entities:
    1. Open their main website in a real browser
    2. Find and click the Careers/Jobs link
    3. Wait for the career page to load (handles JavaScript)
    4. Extract all job titles and links
    5. Fall back to LinkedIn search if nothing found
    """
    jobs = []
    total = len(ALL_ENTITIES)
    page = browser.new_page()
    page.set_default_timeout(15000)

    for i, ent in enumerate(ALL_ENTITIES):
        name = ent["name"]
        url = ent.get("url", "")
        linkedin = ent.get("linkedin", "")
        name_ar = ent.get("name_ar", "")

        if (i + 1) % 25 == 0 or i == 0:
            print(f"    [{i+1}/{total}] {name}...")

        entity_jobs = []

        # ── STEP 1: Try the main website ──
        if url:
            try:
                entity_jobs = _scrape_entity_website(page, name, url)
            except Exception as e:
                print(f"      [{name}] Website error: {e}")

        # ── STEP 2: If no jobs found, try common career paths ──
        if not entity_jobs and url:
            base = url.rstrip("/")
            for path in CAREER_PATHS[:5]:  # Try top 5 paths
                try:
                    career_url = base + path
                    entity_jobs = _scrape_career_page(page, name, career_url)
                    if entity_jobs:
                        print(f"      [{name}] Found {len(entity_jobs)} jobs at {path}")
                        break
                except Exception:
                    continue

        # ── STEP 3: Try LinkedIn job search ──
        if not entity_jobs and linkedin:
            try:
                linkedin_jobs_url = linkedin.rstrip("/") + "/jobs/"
                entity_jobs = _scrape_linkedin_company(page, name, linkedin_jobs_url)
            except Exception:
                pass

        # ── STEP 4: Fallback — search Bayt for this company ──
        if not entity_jobs and i % 15 == 0:
            try:
                search_name = name.split("(")[0].strip()[:30]
                bayt_jobs = scrape_bayt(search_name, max_pages=1)
                for j in bayt_jobs:
                    j["src"] = "gov"
                entity_jobs = bayt_jobs
            except Exception:
                pass

        # ── STEP 5: Final fallback — portal link ──
        if not entity_jobs:
            portal_url = linkedin.rstrip("/") + "/jobs/" if linkedin else url
            if portal_url:
                j = make_job(f"Open Roles — {name}", name, "Riyadh", "gov", portal_url,
                            tags=["government", "vision-2030"],
                            summary=f"{name} ({name_ar}). Check their portal for openings.")
                entity_jobs = [j]

        for j in entity_jobs:
            if j["src"] not in ("gov", "pif"):
                j["src"] = "gov"
        jobs.extend(entity_jobs)

        # Rate limit
        time.sleep(random.uniform(0.5, 1.5))

    try:
        page.close()
    except Exception:
        pass

    return jobs


def _scrape_entity_website(page, company, url):
    """Open main website, find careers link, click it, extract jobs."""
    jobs = []

    try:
        page.goto(url, wait_until="domcontentloaded", timeout=15000)
        time.sleep(2)
    except Exception:
        return jobs

    # Find career links on the main page
    career_link = None
    try:
        all_links = page.query_selector_all("a")
        for link in all_links:
            try:
                text = (link.inner_text() or "").strip().lower()
                href = (link.get_attribute("href") or "").lower()

                # Check if this link is career-related
                is_career = any(w in text for w in CAREER_WORDS) or any(w in href for w in ["انضم الينا", "our team", "join us", "career", "jobs", "vacanc", "opening", "وظائف", "career", "jobs", "vacanc", "وظائف", "join"])

                if is_career and len(text) < 50:
                    career_link = link
                    break
            except Exception:
                continue
    except Exception:
        pass

    if not career_link:
        return jobs

    # Click the careers link
    try:
        career_link.click()
        time.sleep(3)  # Wait for page to load (JS rendering)
        page.wait_for_load_state("domcontentloaded", timeout=10000)
        time.sleep(2)
    except Exception:
        return jobs

    # Now we're on the career page — extract jobs
    return _extract_jobs_from_page(page, company)


def _scrape_career_page(page, company, career_url):
    """Directly open a career page URL and extract jobs."""
    try:
        page.goto(career_url, wait_until="domcontentloaded", timeout=12000)
        time.sleep(3)
    except Exception:
        return []

    return _extract_jobs_from_page(page, company)


def _extract_jobs_from_page(page, company):
    """Extract job titles and links from whatever page we're on."""
    jobs = []
    seen_titles = set()

    try:
        # Scroll down to load lazy content
        for _ in range(3):
            page.evaluate("window.scrollBy(0, 600)")
            time.sleep(0.5)

        current_url = page.url

        # Strategy 1: Find links that look like individual job postings
        all_links = page.query_selector_all("a")
        for link in all_links:
            try:
                text = (link.inner_text() or "").strip()
                href = link.get_attribute("href") or ""

                # Skip junk
                if is_junk(text):
                    continue

                # Check if href looks like a job link
                is_job_link = any(re.search(p, href, re.IGNORECASE) for p in JOB_LINK_PATTERNS)

                # Or if it's inside a career page and looks like a job title
                is_career_page = any(w in current_url.lower() for w in ["انضم الينا", "our team", "join us", "career", "jobs", "vacanc", "opening", "وظائف"])

                if is_job_link or is_career_page:
                    # This looks like a real job
                    title = text[:120]
                    if title.lower() in seen_titles:
                        continue
                    seen_titles.add(title.lower())

                    job_url = href if href.startswith("http") else urljoin(current_url, href)

                    j = make_job(title, company, "Riyadh", "gov", job_url,
                                tags=["government", "vision-2030"])
                    jobs.append(j)

                    if len(jobs) >= 10:  # Cap per entity
                        break
            except Exception:
                continue

        # Strategy 2: Look for job titles in structured elements (cards, list items, table rows)
        if not jobs:
            selectors = [
                ".job-card", ".job-listing", ".vacancy", ".position",
                "[class*='job']", "[class*='career']", "[class*='vacancy']",
                "article", ".card", "tr",
            ]
            for sel in selectors:
                try:
                    cards = page.query_selector_all(sel)
                    for card in cards:
                        try:
                            a = card.query_selector("a")
                            if not a:
                                continue
                            text = (a.inner_text() or "").strip()
                            href = a.get_attribute("href") or ""

                            if is_junk(text):
                                continue
                            if text.lower() in seen_titles:
                                continue
                            seen_titles.add(text.lower())

                            job_url = href if href.startswith("http") else urljoin(current_url, href)
                            j = make_job(text[:120], company, "Riyadh", "gov", job_url,
                                        tags=["government", "vision-2030"])
                            jobs.append(j)

                            if len(jobs) >= 10:
                                break
                        except Exception:
                            continue
                    if jobs:
                        break
                except Exception:
                    continue

        # Strategy 3: Look for Workable, Greenhouse, Lever embeds (common ATS platforms)
        if not jobs:
            ats_frames = page.query_selector_all("iframe")
            for frame in ats_frames:
                try:
                    src = frame.get_attribute("src") or ""
                    if any(ats in src for ats in ["workable", "greenhouse", "lever", "smartrecruiters", "successfactors"]):
                        # Found an ATS iframe — add as portal link
                        j = make_job(f"Careers at {company}", company, "Riyadh", "gov", src,
                                    tags=["government", "vision-2030"],
                                    summary=f"{company} uses an ATS platform. Apply through their portal.")
                        jobs.append(j)
                        break
                except Exception:
                    continue

    except Exception:
        pass

    return jobs


def _scrape_linkedin_company(page, company, linkedin_jobs_url):
    """Try to scrape LinkedIn company jobs page."""
    jobs = []
    try:
        page.goto(linkedin_jobs_url, wait_until="domcontentloaded", timeout=12000)
        time.sleep(3)

        cards = page.query_selector_all(".base-card, .job-card, li")
        for card in cards:
            try:
                a = card.query_selector("a")
                if not a: continue
                title_el = card.query_selector("h3, .base-search-card__title")
                title = (title_el.inner_text() if title_el else a.inner_text() or "").strip()
                if is_junk(title): continue
                href = (a.get_attribute("href") or "").split("?")[0]
                url = href if href.startswith("http") else f"https://www.linkedin.com{href}"

                j = make_job(title[:120], company, "Riyadh", "gov", url,
                            tags=["government", "vision-2030"])
                jobs.append(j)
                if len(jobs) >= 5: break
            except Exception:
                continue
    except Exception:
        pass
    return jobs


# ═══════════════════════════════════════════
# DEDUP
# ═══════════════════════════════════════════
def deduplicate(jobs):
    seen = set()
    out = []
    for j in jobs:
        if j["id"] not in seen:
            seen.add(j["id"])
            out.append(j)
    return out


# ═══════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════
def main():
    print(f"{'='*60}")
    print(f"JOB INTEL SMART SCRAPER")
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"351 Entities (Playwright) + 3 Job Boards")
    print(f"{'='*60}")

    all_jobs = []

    # 1. Bayt.com
    print("\n[1/4] BAYT.COM")
    for kw in KEYWORDS[:10]:
        try:
            j = scrape_bayt(kw, max_pages=2)
            all_jobs.extend(j)
            print(f"  '{kw}' -> {len(j)}")
        except Exception as e:
            print(f"  '{kw}' ERR: {e}")

    # 2. LinkedIn
    print("\n[2/4] LINKEDIN")
    for kw in KEYWORDS[:4]:
        try:
            j = scrape_linkedin(kw)
            all_jobs.extend(j)
            print(f"  '{kw}' -> {len(j)}")
        except Exception as e:
            print(f"  '{kw}' ERR: {e}")

    # 3. Smart Entity Scraping with Playwright
    print(f"\n[3/4] SMART SCRAPING {len(ALL_ENTITIES)} ENTITIES (Playwright)")
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"]
            )
            entity_jobs = scrape_entities_smart(browser)
            all_jobs.extend(entity_jobs)
            print(f"  Entity jobs found: {len(entity_jobs)}")
            browser.close()
    except ImportError:
        print("  Playwright not installed, falling back to HTTP scraping...")
        # Fallback: use HTTP-only scraping for entities
        for i, ent in enumerate(ALL_ENTITIES):
            name = ent["name"]
            url = ent.get("url", "")
            linkedin = ent.get("linkedin", "")
            portal = linkedin.rstrip("/") + "/jobs/" if linkedin else url
            if portal:
                j = make_job(f"Open Roles — {name}", name, "Riyadh", "gov", portal,
                            tags=["government","vision-2030"],
                            summary=f"{name} ({ent.get('name_ar','')}). Check portal for openings.")
                all_jobs.append(j)
    except Exception as e:
        print(f"  Playwright error: {e}")

    # 4. Additional Bayt searches for top entities
    print("\n[4/4] TARGETED ENTITY SEARCHES")
    top_entities = ["SDAIA", "NEOM", "Qiddiya", "PIF", "stc", "Aramco", "ROSHN",
                    "Red Sea Global", "KAUST", "Elm", "ACWA Power", "SABIC"]
    for name in top_entities:
        try:
            j = scrape_bayt(name, max_pages=1)
            for job in j:
                job["src"] = "gov"
            all_jobs.extend(j)
            print(f"  '{name}' -> {len(j)}")
        except Exception as e:
            print(f"  '{name}' ERR: {e}")

    # Dedup & sort
    unique = deduplicate(all_jobs)
    unique.sort(key=lambda j: j["sc"], reverse=True)
    print(f"\n{'='*60}")
    print(f"RAW: {len(all_jobs)} -> UNIQUE: {len(unique)}")

    # Preserve user statuses
    prev = Path("jobs.json")
    sm = {}
    if prev.exists():
        try:
            old = json.loads(prev.read_text())
            for j in old.get("jobs", []):
                if j.get("st") not in (None, "new"):
                    sm[j["id"]] = j["st"]
        except Exception:
            pass
    for j in unique:
        if j["id"] in sm:
            j["st"] = sm[j["id"]]

    out = {"updated": datetime.now().isoformat(), "count": len(unique), "jobs": unique[:500]}
    Path("jobs.json").write_text(json.dumps(out, ensure_ascii=False, indent=2))
    print(f"Wrote {out['count']} jobs to jobs.json")
    print(f"Updated: {out['updated']}")

if __name__ == "__main__":
    main()
