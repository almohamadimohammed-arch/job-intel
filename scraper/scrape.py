"""
Job Intel v3 — Clean Scraper
Strategy: Use 351 entity names to SEARCH job boards.
NOT scraping entity websites (those give garbage).
Sources: Bayt, LinkedIn, Wadhefa, LinkedKSA
"""
import json, hashlib, re, time, random
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
KEYWORDS = [
    "innovation manager Saudi", "strategy director Saudi Arabia",
    "digital transformation Riyadh", "AI strategy Saudi",
    "technology director Saudi", "management consultant Riyadh",
    "Vision 2030 jobs", "innovation director Saudi",
    "head of strategy Riyadh", "product director Saudi Arabia",
]
ENTITIES_PATH = Path(__file__).parent / "entities.json"
ALL_ENTITIES = json.loads(ENTITIES_PATH.read_text(encoding="utf-8")) if ENTITIES_PATH.exists() else []
print(f"Loaded {len(ALL_ENTITIES)} entities")

PRIORITY = [
    "SDAIA","NEOM","Qiddiya","Public Investment Fund","stc",
    "Aramco","ROSHN","Red Sea Global","KAUST","Elm",
    "ACWA Power","SABIC","HUMAIN","Savvy Games",
    "Digital Government Authority","MCIT","NCA",
    "Saudi Space Commission","MISA","RDIA",
    "King Abdullah Financial District","Riyadh Air",
    "Saudi Electricity Company","Saudi Arabian Mining",
    "Diriyah Gate","AlUla","Saudi Tourism Authority",
    "General Entertainment Authority","BCG","McKinsey",
]
session = requests.Session()

def get(url, params=None, timeout=15):
    session.headers.update({"User-Agent": random.choice(UA)})
    time.sleep(random.uniform(1.5, 3.0))
    try:
        r = session.get(url, params=params, timeout=timeout)
        r.raise_for_status()
        return r
    except Exception:
        return None

def fp(title, company):
    return hashlib.sha256(f"{title.lower().strip()}|{company.lower().strip()}".encode()).hexdigest()[:12]

def parse_date(text):
    if not text: return date.today().isoformat()
    text = text.strip().lower()
    today = date.today()
    for pat, unit in [(r"(\d+)\s*day","d"),(r"(\d+)\s*hour","h"),(r"(\d+)\s*week","w"),(r"(\d+)\s*month","m"),(r"today|just now","0"),(r"yesterday","1"),(r"منذ\s*(\d+)\s*يوم","d"),(r"منذ\s*(\d+)\s*أسبوع","w")]:
        m = re.search(pat, text)
        if m:
            if unit in ("0","h"): return today.isoformat()
            if unit == "1": return (today - timedelta(days=1)).isoformat()
            n = int(m.group(1)) if m.groups() else 1
            delta = {"d":timedelta(days=n),"w":timedelta(weeks=n),"m":timedelta(days=n*30)}
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

def is_real_job(title):
    t = title.strip()
    if len(t) < 8 or len(t) > 150: return False
    garbage = [
        "about","about us","contact","contact us","home","menu",
        "leadership","our leadership","board of directors","management",
        "media center","media centre","news","press","blog",
        "our history","history","our story","overview","who we are",
        "our values","vision","mission","our mission",
        "more information","learn more","read more","show details",
        "get in touch","connect with us","follow us",
        "privacy","privacy policy","terms","terms of use","cookie",
        "login","sign in","sign up","register","subscribe",
        "search","filter","sort","back","next","previous",
        "english","arabic","top content","life at","our culture","benefits",
        "linkedin","twitter","facebook","instagram","youtube",
        "copyright","all rights","powered by","settings","preferences",
        "investors","investor relations","annual report",
        "sustainability","csr","community","partners","clients",
        "services","products","solutions","offerings",
        "locations","offices","branches",
    ]
    tl = t.lower()
    for g in garbage:
        if tl == g or tl.startswith(g + " ") or tl.endswith(" " + g): return False
    if t.startswith("#") or t.startswith("[") or t.startswith("{"): return False
    if "http" in tl or "www." in tl or ".com" in tl or ".sa" in tl: return False
    job_words = [
        "manager","director","specialist","analyst","engineer",
        "consultant","advisor","coordinator","officer","lead",
        "supervisor","architect","developer","designer","planner",
        "head","chief","vp","president","administrator",
        "executive","associate","assistant","intern","trainee",
        "accountant","auditor","controller","buyer","procurement",
        "recruiter","legal","counsel","scientist","researcher",
        "professor","driver","technician","operator","mechanic",
        "nurse","doctor","pharmacist","sales","marketing",
        "project","program","portfolio","security","safety",
        "compliance","risk","data","cloud","network","system",
        "senior","junior","مدير","مهندس","محلل","أخصائي","مستشار",
    ]
    return any(w in tl for w in job_words)

def extract_city(text):
    for c in ["Riyadh","Jeddah","Dammam","Dhahran","NEOM","Jubail","Mecca","Medina","Tabuk","Khobar"]:
        if c.lower() in text.lower(): return c
    return "Riyadh"

def make_job(title, company, city, source, url, tags=None, signals=None, posted="", summary=""):
    tg = list(set((tags or [])[:5]))
    sg = list(set((signals or [])[:3]))
    return {"id":fp(title,company),"src":source,"t":title[:120],"co":company[:80],"cy":city or "Riyadh","ct":"SA","ca":category(title,tg),"tg":tg,"sg":sg,"sn":seniority(title),"sc":score(title,company,tg,sg),"sm":(summary or "")[:200],"st":"new","dt":parse_date(posted),"u":url}

def search_bayt(query, max_pages=2):
    jobs = []
    for page in range(1, max_pages + 1):
        resp = get("https://www.bayt.com/en/saudi-arabia/jobs/", params={"keyword": query, "page": page})
        if not resp: break
        soup = BeautifulSoup(resp.text, "html.parser")
        cards = soup.select("li[data-js-job]") or soup.select(".has-pointer-d") or soup.select("li.is-compact")
        if not cards: break
        for card in cards:
            try:
                a = card.select_one("h2 a") or card.select_one("a")
                if not a: continue
                title = a.get_text(strip=True)
                if not is_real_job(title): continue
                href = a.get("href","")
                url = f"https://www.bayt.com{href}" if href.startswith("/") else href
                co = card.select_one(".t-mute a")
                company = co.get_text(strip=True) if co else ""
                loc = card.select_one(".t-mute span")
                location = loc.get_text(strip=True) if loc else ""
                dt = card.select_one("time") or card.select_one(".t-small")
                posted = dt.get_text(strip=True) if dt else ""
                j = make_job(title, company, extract_city(location), "bayt", url, tags=[query.lower().replace(" ","-")[:20]], posted=posted)
                if j["co"]: jobs.append(j)
            except Exception: continue
    return jobs

def search_linkedin(query):
    jobs = []
    kw = quote_plus(query)
    resp = get(f"https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?keywords={kw}&location=Saudi%20Arabia&geoId=100459316&start=0&sortBy=DD")
    if not resp: return jobs
    soup = BeautifulSoup(resp.text, "html.parser")
    for card in soup.select("li"):
        try:
            a = card.select_one("a.base-card__full-link") or card.select_one("a[href*='/jobs/view/']")
            if not a: continue
            t_el = card.select_one("h3") or card.select_one(".base-search-card__title")
            title = t_el.get_text(strip=True) if t_el else ""
            if not is_real_job(title): continue
            href = a.get("href","").split("?")[0]
            url = href if href.startswith("http") else f"https://www.linkedin.com{href}"
            co = card.select_one("h4") or card.select_one(".base-search-card__subtitle")
            company = co.get_text(strip=True) if co else ""
            loc = card.select_one(".job-search-card__location")
            location = loc.get_text(strip=True) if loc else ""
            tm = card.select_one("time")
            posted = tm.get("datetime","") if tm else ""
            j = make_job(title, company, extract_city(location), "linkedin", url, tags=[query.lower().replace(" ","-")[:20]], posted=posted)
            if j["co"]: jobs.append(j)
        except Exception: continue
    return jobs

def search_wadhefa(query):
    jobs = []
    resp = get("https://www.wadhefa.com/en/jobs/search", params={"q": query, "country": "saudi-arabia"})
    if not resp: return jobs
    soup = BeautifulSoup(resp.text, "html.parser")
    for a in soup.find_all("a", href=re.compile(r"(?i)/jobs?/|/position|/vacanc")):
        try:
            title = a.get_text(strip=True)
            if not is_real_job(title): continue
            href = a.get("href","")
            url = href if href.startswith("http") else urljoin("https://www.wadhefa.com", href)
            j = make_job(title, "", "Riyadh", "wadhefa", url, tags=[query.lower().replace(" ","-")[:20]])
            jobs.append(j)
        except Exception: continue
    return jobs

def search_linkedksa(query):
    jobs = []
    resp = get("https://linkedksa.com/", params={"s": query})
    if not resp: return jobs
    soup = BeautifulSoup(resp.text, "html.parser")
    for card in soup.select("article, .post"):
        try:
            a = card.select_one("h2 a, h3 a, .entry-title a, a")
            if not a: continue
            title = a.get_text(strip=True)
            if not is_real_job(title): continue
            href = a.get("href","")
            url = href if href.startswith("http") else urljoin("https://linkedksa.com", href)
            j = make_job(title, "", "Riyadh", "linkedksa", url, tags=[query.lower().replace(" ","-")[:20]])
            jobs.append(j)
        except Exception: continue
    return jobs

def search_all_entities():
    jobs = []
    print("    Phase 1: Priority entities")
    for name in PRIORITY:
        print(f"      {name}")
        bj = search_bayt(name, max_pages=1)
        for j in bj: j["sg"] = list(set(j.get("sg",[]) + ["expansion"]))
        jobs.extend(bj)
        lj = search_linkedin(name)
        jobs.extend(lj)
        wj = search_wadhefa(name)
        jobs.extend(wj)
        found = len(bj)+len(lj)+len(wj)
        if found: print(f"        -> {found} jobs")

    print("    Phase 2: Remaining entities")
    priority_lower = [n.lower() for n in PRIORITY]
    remaining = [e for e in ALL_ENTITIES if not any(p in e["name"].lower() for p in priority_lower)]
    for i, ent in enumerate(remaining):
        if i % 5 != 0: continue
        name = ent["name"]
        if len(name) < 4: continue
        if (i+1) % 50 == 0: print(f"      [{i+1}/{len(remaining)}]")
        try:
            bj = search_bayt(name.split("(")[0].strip()[:35], max_pages=1)
            jobs.extend(bj)
        except Exception: pass

    print("    Phase 3: LinkedIn links for remaining")
    found_cos = set(j["co"].lower() for j in jobs)
    for ent in ALL_ENTITIES:
        name = ent["name"]
        linkedin = ent.get("linkedin","")
        name_ar = ent.get("name_ar","")
        if any(name.lower() in fc for fc in found_cos): continue
        if linkedin:
            j = make_job(f"Open Roles at {name}", name, "Riyadh", "gov",
                        linkedin.rstrip("/")+"/jobs/",
                        tags=["government","vision-2030"],
                        summary=f"{name} ({name_ar}). Check LinkedIn for openings.")
            jobs.append(j)
    return jobs

def deduplicate(jobs):
    seen = set(); out = []
    for j in jobs:
        if j["id"] not in seen: seen.add(j["id"]); out.append(j)
    return out

def main():
    print(f"{'='*60}")
    print(f"JOB INTEL v3 — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"351 Entities x 4 Job Boards — CLEAN (no garbage)")
    print(f"{'='*60}")
    all_jobs = []

    print("\n[1/5] BAYT — Keywords")
    for kw in KEYWORDS:
        j = search_bayt(kw, max_pages=2)
        all_jobs.extend(j)
        print(f"  '{kw}' -> {len(j)}")

    print("\n[2/5] LINKEDIN — Keywords")
    for kw in KEYWORDS[:6]:
        j = search_linkedin(kw)
        all_jobs.extend(j)
        print(f"  '{kw}' -> {len(j)}")

    print("\n[3/5] WADHEFA — Keywords")
    for kw in KEYWORDS[:4]:
        j = search_wadhefa(kw)
        all_jobs.extend(j)
        print(f"  '{kw}' -> {len(j)}")

    print("\n[4/5] LINKEDKSA — Keywords")
    for kw in KEYWORDS[:3]:
        j = search_linkedksa(kw)
        all_jobs.extend(j)
        print(f"  '{kw}' -> {len(j)}")

    print(f"\n[5/5] ENTITY SEARCH — {len(ALL_ENTITIES)} entities")
    entity_jobs = search_all_entities()
    all_jobs.extend(entity_jobs)
    print(f"  Total: {len(entity_jobs)}")

    unique = deduplicate(all_jobs)
    unique.sort(key=lambda j: j["sc"], reverse=True)
    print(f"\n{'='*60}")
    print(f"RAW: {len(all_jobs)} -> UNIQUE: {len(unique)}")

    prev = Path("jobs.json")
    sm = {}
    if prev.exists():
        try:
            old = json.loads(prev.read_text())
            for j in old.get("jobs",[]):
                if j.get("st") not in (None,"new"): sm[j["id"]] = j["st"]
        except Exception: pass
    for j in unique:
        if j["id"] in sm: j["st"] = sm[j["id"]]

    out = {"updated":datetime.now().isoformat(),"count":len(unique),"jobs":unique[:500]}
    Path("jobs.json").write_text(json.dumps(out, ensure_ascii=False, indent=2))
    print(f"Wrote {out['count']} jobs")

if __name__ == "__main__":
    main()
