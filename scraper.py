import requests
from bs4 import BeautifulSoup
import json
from datetime import datetime, timezone
import time

# ─── CONFIG ──────────────────────────────────────────────────────────────────
# Each pipeline has an id, name, url, and a parser type
# Parser types: kinder_morgan, gasquest, tceconnects, tallgrass, energy_transfer_critical,
#               energy_transfer_enbl, enbridge, williams, generic

PIPELINES = [
    { "id": "tgp",       "name": "TGP",           "url": "https://pipeline2.kindermorgan.com/Notices/Notices.aspx?type=C&code=TGP",   "parser": "kinder_morgan" },
    { "id": "sonat",     "name": "Sonat",         "url": "https://pipeline2.kindermorgan.com/Notices/Notices.aspx?type=C&code=SNG",   "parser": "kinder_morgan" },
    { "id": "ngpl",      "name": "NGPL",          "url": "https://pipeline2.kindermorgan.com/Notices/Notices.aspx?type=C&code=NGPL",  "parser": "kinder_morgan" },
    { "id": "mep",       "name": "MEP",           "url": "https://pipeline2.kindermorgan.com/Notices/Notices.aspx?type=C&code=MEP",   "parser": "kinder_morgan" },
    { "id": "txgas",     "name": "Texas Gas",     "url": "https://www.gasquest.com/notices/critical",                                 "parser": "gasquest" },
    { "id": "gulfsouth", "name": "Gulf South",    "url": "https://www.gasquest.com/notices/critical",                                 "parser": "gasquest" },
    { "id": "colgulf",   "name": "Columbia Gulf", "url": "https://ebb.tceconnects.com/infopost/",                                     "parser": "tceconnects" },
    { "id": "anr",       "name": "ANR",           "url": "https://ebb.tceconnects.com/infopost/",                                     "parser": "tceconnects" },
    { "id": "rex",       "name": "REX",           "url": "https://pipeline.tallgrassenergylp.com/Pages/Notices.aspx?pipeline=501&type=CRIT", "parser": "tallgrass" },
    { "id": "rover",     "name": "Rover",         "url": "https://rovermessenger.energytransfer.com/ipost/notice/critical?asset=ROVER",     "parser": "energy_transfer" },
    { "id": "egt",       "name": "EGT",           "url": "https://pipelines.energytransfer.com/ipost/notice/enbl-critical?asset=EGT",       "parser": "energy_transfer" },
    { "id": "fgt",       "name": "FGT",           "url": "https://pipelines.energytransfer.com/ipost/notice/critical?asset=FGT",            "parser": "energy_transfer" },
    { "id": "tetco",     "name": "TETCO",         "url": "https://infopost.enbridge.com/infopost/TEHome.asp?Pipe=TE",                        "parser": "enbridge" },
    { "id": "transco",   "name": "Transco",       "url": "https://www.1line.williams.com/Transco/index.html",                               "parser": "williams" },
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

def fetch(url, timeout=15):
    """Fetch a URL and return BeautifulSoup object, or None on failure."""
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout)
        r.raise_for_status()
        return BeautifulSoup(r.text, "html.parser")
    except Exception as e:
        print(f"  ERROR fetching {url}: {e}")
        return None

def clean(text):
    """Strip and collapse whitespace."""
    if not text:
        return ""
    return " ".join(text.split())

# ─── PARSERS ─────────────────────────────────────────────────────────────────

def parse_kinder_morgan(soup, pipeline):
    """Parse Kinder Morgan EBB notice pages."""
    try:
        rows = soup.select("table tr")
        notices = []
        for row in rows[1:]:  # skip header
            cols = row.find_all("td")
            if len(cols) >= 3:
                title   = clean(cols[0].get_text())
                date    = clean(cols[1].get_text())
                summary = clean(cols[2].get_text())
                link_tag = cols[0].find("a")
                link = ("https://pipeline2.kindermorgan.com" + link_tag["href"]) if link_tag and link_tag.get("href") else pipeline["url"]
                if title:
                    notices.append({"title": title, "date": date, "summary": summary[:300], "url": link})
        if notices:
            return {"status": "active", "posting": notices[0]}
        return {"status": "none", "posting": None}
    except Exception as e:
        print(f"  PARSE ERROR kinder_morgan: {e}")
        return {"status": "error", "posting": None}

def parse_gasquest(soup, pipeline):
    """Parse GasQuest notice pages (Texas Gas, Gulf South)."""
    try:
        rows = soup.select("table tbody tr, .notice-row, tr")
        notices = []
        for row in rows:
            cols = row.find_all("td")
            if len(cols) >= 2:
                title   = clean(cols[0].get_text())
                summary = clean(cols[1].get_text()) if len(cols) > 1 else ""
                date    = clean(cols[-1].get_text()) if len(cols) > 2 else ""
                if title and len(title) > 3:
                    notices.append({"title": title, "date": date, "summary": summary[:300], "url": pipeline["url"]})
        if notices:
            return {"status": "active", "posting": notices[0]}
        return {"status": "none", "posting": None}
    except Exception as e:
        print(f"  PARSE ERROR gasquest: {e}")
        return {"status": "error", "posting": None}

def parse_tceconnects(soup, pipeline):
    """Parse TCE Connects (Columbia Gulf, ANR)."""
    try:
        rows = soup.select("table tr")
        notices = []
        for row in rows[1:]:
            cols = row.find_all("td")
            if len(cols) >= 2:
                title   = clean(cols[0].get_text())
                date    = clean(cols[1].get_text()) if len(cols) > 1 else ""
                summary = clean(cols[2].get_text()) if len(cols) > 2 else ""
                link_tag = cols[0].find("a")
                link = link_tag["href"] if link_tag and link_tag.get("href") else pipeline["url"]
                if title and len(title) > 3:
                    notices.append({"title": title, "date": date, "summary": summary[:300], "url": link})
        if notices:
            return {"status": "active", "posting": notices[0]}
        return {"status": "none", "posting": None}
    except Exception as e:
        print(f"  PARSE ERROR tceconnects: {e}")
        return {"status": "error", "posting": None}

def parse_tallgrass(soup, pipeline):
    """Parse Tallgrass (REX)."""
    try:
        rows = soup.select("table tr, .grid tr")
        notices = []
        for row in rows[1:]:
            cols = row.find_all("td")
            if len(cols) >= 2:
                title   = clean(cols[0].get_text())
                date    = clean(cols[1].get_text()) if len(cols) > 1 else ""
                summary = clean(cols[2].get_text()) if len(cols) > 2 else ""
                link_tag = row.find("a")
                link = link_tag["href"] if link_tag and link_tag.get("href") else pipeline["url"]
                if title and len(title) > 3:
                    notices.append({"title": title, "date": date, "summary": summary[:300], "url": link})
        if notices:
            return {"status": "active", "posting": notices[0]}
        return {"status": "none", "posting": None}
    except Exception as e:
        print(f"  PARSE ERROR tallgrass: {e}")
        return {"status": "error", "posting": None}

def parse_energy_transfer(soup, pipeline):
    """Parse Energy Transfer (Rover, EGT, FGT)."""
    try:
        rows = soup.select("table tr, .notice-table tr")
        notices = []
        for row in rows[1:]:
            cols = row.find_all("td")
            if len(cols) >= 2:
                title   = clean(cols[0].get_text())
                date    = clean(cols[1].get_text()) if len(cols) > 1 else ""
                summary = clean(cols[2].get_text()) if len(cols) > 2 else ""
                link_tag = row.find("a")
                link = link_tag["href"] if link_tag and link_tag.get("href") else pipeline["url"]
                if title and len(title) > 3:
                    notices.append({"title": title, "date": date, "summary": summary[:300], "url": link})
        if notices:
            return {"status": "active", "posting": notices[0]}
        return {"status": "none", "posting": None}
    except Exception as e:
        print(f"  PARSE ERROR energy_transfer: {e}")
        return {"status": "error", "posting": None}

def parse_enbridge(soup, pipeline):
    """Parse Enbridge (TETCO)."""
    try:
        rows = soup.select("table tr")
        notices = []
        for row in rows[1:]:
            cols = row.find_all("td")
            if len(cols) >= 2:
                title   = clean(cols[0].get_text())
                date    = clean(cols[1].get_text()) if len(cols) > 1 else ""
                summary = clean(cols[2].get_text()) if len(cols) > 2 else ""
                link_tag = cols[0].find("a")
                link = link_tag["href"] if link_tag and link_tag.get("href") else pipeline["url"]
                if title and len(title) > 3:
                    notices.append({"title": title, "date": date, "summary": summary[:300], "url": link})
        if notices:
            return {"status": "active", "posting": notices[0]}
        return {"status": "none", "posting": None}
    except Exception as e:
        print(f"  PARSE ERROR enbridge: {e}")
        return {"status": "error", "posting": None}

def parse_williams(soup, pipeline):
    """Parse Williams (Transco)."""
    try:
        rows = soup.select("table tr, .notices tr")
        notices = []
        for row in rows[1:]:
            cols = row.find_all("td")
            if len(cols) >= 2:
                title   = clean(cols[0].get_text())
                date    = clean(cols[1].get_text()) if len(cols) > 1 else ""
                summary = clean(cols[2].get_text()) if len(cols) > 2 else ""
                link_tag = row.find("a")
                link = link_tag["href"] if link_tag and link_tag.get("href") else pipeline["url"]
                if title and len(title) > 3:
                    notices.append({"title": title, "date": date, "summary": summary[:300], "url": link})
        if notices:
            return {"status": "active", "posting": notices[0]}
        return {"status": "none", "posting": None}
    except Exception as e:
        print(f"  PARSE ERROR williams: {e}")
        return {"status": "error", "posting": None}

PARSERS = {
    "kinder_morgan":  parse_kinder_morgan,
    "gasquest":       parse_gasquest,
    "tceconnects":    parse_tceconnects,
    "tallgrass":      parse_tallgrass,
    "energy_transfer": parse_energy_transfer,
    "enbridge":       parse_enbridge,
    "williams":       parse_williams,
}

# ─── MAIN ─────────────────────────────────────────────────────────────────────
def main():
    results = {
        "last_updated": datetime.now(timezone.utc).isoformat(),
        "pipelines": {}
    }

    for pipeline in PIPELINES:
        print(f"Scraping {pipeline['name']}...")
        soup = fetch(pipeline["url"])
        if soup is None:
            results["pipelines"][pipeline["id"]] = {
                "status": "error",
                "posting": None,
                "name": pipeline["name"],
                "url": pipeline["url"]
            }
        else:
            parser = PARSERS.get(pipeline["parser"])
            if parser:
                data = parser(soup, pipeline)
            else:
                data = {"status": "error", "posting": None}
            data["name"] = pipeline["name"]
            data["url"]  = pipeline["url"]
            results["pipelines"][pipeline["id"]] = data
            print(f"  → {data['status']}")
        time.sleep(1)  # be polite between requests

    with open("data.json", "w") as f:
        json.dump(results, f, indent=2)

    print(f"\nDone. Scraped {len(PIPELINES)} pipelines.")
    print(f"Last updated: {results['last_updated']}")

if __name__ == "__main__":
    main()
