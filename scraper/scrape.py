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

UA = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 Version/17.5 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0",
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

def get(url, params=None, timeout=15):
    session.headers.update({"User-Agent": random.choice(UA)})
    time.sleep(random.uniform(1.0, 2.5))
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
            if unit in ("0", "h"):
                return today.isoformat()
            if unit == "1":
                return (today - timedelta(days=1)).isoformat()
            n = int(m.group(1)) if m.groups() else 1
            delta = {"d": timedelta(days=n), "w": timedelta(weeks=n), "m": timedelta(days=n * 30)}
            return (today - delta.get(unit, timedelta(0))).isoformat()
    return today.isoformat()

def seniority(title):
    t = title.lower()
    if any(w in t for w in ["chief", "cto", "ceo", "cfo", "vp", "vice president"]):
        return "executive"
    if any(w in t for w in ["director", "head of", "general manager"]):
        return "director"
    if any(w in t for w in ["senior", "sr.", "lead", "principal", "manager"]):
        return "senior"
    if any(w in t for w in ["junior", "jr.", "associate", "intern"]):
        return "junior"
    return "mid"

def category(title, tags):
    t = (title + " " + " ".join(tags)).lower()
    if any(w in t for w in ["strategy", "consulting", "consultant", "advisory", "governance", "policy"]):
        return "Strategy & Consulting"
    if any(w in t for w in ["technology", "product", "ai", "data", "cyber", "digital", "cloud", "software"]):
        return "Technology & Product"
    if any(w in t for w in ["operations", "program", "project", "construction", "infrastructure"]):
        return "Operations & Execution"
    if any(w in t for w in ["finance", "investment", "fund", "risk", "banking"]):
        return "Finance & Investment"
    return "Strategy & Consulting"

HIGH = {"pif", "neom", "qiddiya", "stc", "aramco", "sdaia", "bcg", "mckinsey", "bain", "kaust", "roshn", "humain", "elm"}

def score(title, company, tags, signals):
    s = 0.50
    for w in ["innovation", "strategy", "transformation", "director", "head", "lead", "chief"]:
        if w in title.lower():
            s += 0.05
    for c in HIGH:
        if c in company.lower():
            s += 0.04
            break
    for t in ["innovation", "strategy", "transformation", "ai", "vision-2030"]:
        if t in tags:
            s += 0.02
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

# ── BAYT.COM ──
def scrape_bayt(keyword, max_pages=2):
    jobs = []
    for page in range(1, max_pages + 1):
        resp = get("https://www.bayt.com/en/saudi-arabia/jobs/", params={"keyword": keyword, "page": page})
        if not resp:
            break
        soup = BeautifulSoup(resp.text, "html.parser")
        cards = soup.select("li[data-js-job]") or soup.select(".has-pointer-d") or soup.select("li.is-compact")
        if not cards:
            break
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
                city = "Riyadh"
                for c in ["Riyadh","Jeddah","Dammam","Dhahran","NEOM","Jubail","Mecca","Tabuk","Khobar"]:
                    if c.lower() in location.lower(): city = c; break
                dt = card.select_one("time") or card.select_one(".t-small")
                posted = dt.get_text(strip=True) if dt else ""
                j = make_job(title, company, city, "bayt", url, tags=[keyword.lower().replace(" ", "-")], posted=posted)
                if j["co"]: jobs.append(j)
            except Exception:
                continue
    return jobs

# ── CAREERJET ──
def scrape_careerjet(keyword):
    jobs = []
    resp = get("https://www.careerjet.com.sa/search/jobs", params={"s": keyword, "l": "Saudi Arabia"})
    if not resp: return jobs
    soup = BeautifulSoup(resp.text, "html.parser")
    cards = soup.select("article.job") or soup.select("li.job")
    if not cards:
        links = soup.find_all("a", href=re.compile(r"/jobad/"))
        cards = [l.parent for l in links if l.parent] if links else []
    for card in cards:
        try:
            a = card.select_one("h2 a") or card.select_one("a[href*='/jobad/']")
            if not a: continue
            title = a.get_text(strip=True)
            if not title or len(title) < 5: continue
            href = a.get("href", "")
            url = urljoin("https://www.careerjet.com.sa", href)
            co = card.select_one(".company")
            company = co.get_text(strip=True) if co else ""
            loc = card.select_one(".location")
            location = loc.get_text(strip=True) if loc else ""
            city = "Riyadh"
            for c in ["Riyadh","Jeddah","Dammam","Dhahran","NEOM","Jubail"]:
                if c.lower() in location.lower(): city = c; break
            desc = card.select_one(".desc")
            summary = desc.get_text(strip=True)[:200] if desc else ""
            j = make_job(title, company, city, "careerjet", url, tags=[keyword.lower().replace(" ", "-")], summary=summary)
            if j["co"]: jobs.append(j)
        except Exception:
            continue
    return jobs

# ── LINKEDIN PUBLIC ──
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
            city = "Riyadh"
            for c in ["Riyadh","Jeddah","Dammam","Dhahran","NEOM","Jubail"]:
                if c.lower() in location.lower(): city = c; break
            tm = card.select_one("time")
            posted = tm.get("datetime", "") if tm else ""
            j = make_job(title, company, city, "linkedin", url, tags=[keyword.lower().replace(" ", "-")], posted=posted)
            if j["co"]: jobs.append(j)
        except Exception:
            continue
    return jobs

# ── ALL 351 ENTITIES ──
def scrape_entities():
    jobs = []
    total = len(ALL_ENTITIES)
    for i, ent in enumerate(ALL_ENTITIES):
        name = ent["name"]
        url = ent.get("url", "")
        linkedin = ent.get("linkedin", "")
        name_ar = ent.get("name_ar", "")

        if (i + 1) % 50 == 0:
            print(f"    [{i+1}/{total}] {name}...")

        found = False

        # Every 10th entity: search Bayt
        if i % 10 == 0 and name:
            try:
                search_name = name.split("(")[0].strip()[:40]
                bj = scrape_bayt(search_name, max_pages=1)
                for j in bj:
                    j["src"] = "gov"
                jobs.extend(bj)
                if bj: found = True
            except Exception:
                pass

        # Try career page
        if url and not found:
            try:
                resp = get(url, timeout=10)
                if resp:
                    soup = BeautifulSoup(resp.text, "html.parser")
                    links = soup.find_all("a", href=re.compile(r"(?i)career|job|vacanc|hiring|وظائف|توظيف"))
                    for link in links[:3]:
                        text = link.get_text(strip=True)
                        if text and 5 < len(text) < 120 and not any(x in text.lower() for x in ["login","cookie","privacy","home","about"]):
                            href = link.get("href", "")
                            link_url = href if href.startswith("http") else urljoin(url, href)
                            j = make_job(f"Careers at {name}", name, "Riyadh", "gov", link_url,
                                         tags=["government", "vision-2030"],
                                         summary=f"{name} is hiring. Visit their careers portal.")
                            jobs.append(j)
                            found = True
                            break
            except Exception:
                pass

        # Fallback: LinkedIn jobs link
        if linkedin and not found:
            lj = linkedin.rstrip("/") + "/jobs/"
            j = make_job(f"Open Roles \u2014 {name}", name, "Riyadh", "gov", lj,
                         tags=["government", "vision-2030"],
                         summary=f"{name} ({name_ar}). Check LinkedIn for current openings.")
            jobs.append(j)

    return jobs

# ── DEDUP ──
def deduplicate(jobs):
    seen = set()
    out = []
    for j in jobs:
        if j["id"] not in seen:
            seen.add(j["id"])
            out.append(j)
    return out

# ── MAIN ──
def main():
    print(f"{'='*60}")
    print(f"JOB INTEL \u2014 {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"351 Saudi Entities + 3 Job Boards")
    print(f"{'='*60}")

    all_jobs = []

    print("\n[1/4] BAYT.COM")
    for kw in KEYWORDS[:10]:
        try:
            j = scrape_bayt(kw, max_pages=2)
            all_jobs.extend(j)
            print(f"  '{kw}' \u2192 {len(j)}")
        except Exception as e:
            print(f"  '{kw}' ERR: {e}")

    print("\n[2/4] CAREERJET")
    for kw in KEYWORDS[:6]:
        try:
            j = scrape_careerjet(kw)
            all_jobs.extend(j)
            print(f"  '{kw}' \u2192 {len(j)}")
        except Exception as e:
            print(f"  '{kw}' ERR: {e}")

    print("\n[3/4] LINKEDIN")
    for kw in KEYWORDS[:4]:
        try:
            j = scrape_linkedin(kw)
            all_jobs.extend(j)
            print(f"  '{kw}' \u2192 {len(j)}")
        except Exception as e:
            print(f"  '{kw}' ERR: {e}")

    print(f"\n[4/4] ALL {len(ALL_ENTITIES)} ENTITIES")
    try:
        g = scrape_entities()
        all_jobs.extend(g)
        print(f"  Entity jobs: {len(g)}")
    except Exception as e:
        print(f"  ERR: {e}")

    unique = deduplicate(all_jobs)
    unique.sort(key=lambda j: j["sc"], reverse=True)
    print(f"\nRAW: {len(all_jobs)} \u2192 UNIQUE: {len(unique)}")

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

    out = {"updated": datetime.now().isoformat(), "count": len(unique), "jobs": unique[:300]}
    Path("jobs.json").write_text(json.dumps(out, ensure_ascii=False, indent=2))
    print(f"Wrote {out['count']} jobs to jobs.json")

if __name__ == "__main__":
    main()
