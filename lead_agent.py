import os
import re
import time
import random
import asyncio
import requests
import dns.resolver
import csv
from bs4 import BeautifulSoup
from urllib.parse import quote
from email_validator import validate_email, EmailNotValidError
from playwright.async_api import async_playwright

# ============================================================
# CONFIGURATION — EDIT THESE
# ============================================================
SHEET_URL = "YOUR_GOOGLE_APPS_SCRIPT_URL_HERE"
CSV_FILE  = "leads_output.csv"

RUN_HOURS     = 9        # Kaggle max runtime
CYCLE_DELAY   = 45       # seconds between cycles
MAX_PER_CYCLE = 10       # max leads saved per cycle

# ============================================================
# DISPOSABLE DOMAINS
# ============================================================
DISPOSABLE_DOMAINS = {
    "tempmail.com", "guerrillamail.com", "mailinator.com",
    "throwaway.email", "yopmail.com", "10minutemail.com",
    "trashmail.com", "maildrop.cc", "sharklasers.com",
    "dispostable.com", "spamgourmet.com", "fakeinbox.com",
    "mailnull.com", "spamspot.com", "trashmail.me",
    "discard.email", "spamfree24.org", "tempr.email"
}

# ============================================================
# JUNK FILTER
# ============================================================
JUNK_DOMAINS = [
    "example.com", "schema.org", "google.com", "microsoft.com",
    "bing.com", "jquery.com", "cloudflare.com", "amazonaws.com",
    "sentry.io", "wix.com", "wordpress.com", "squarespace.com",
    "apple.com", "youtube.com", "facebook.com", "twitter.com",
    "instagram.com", "tiktok.com", "linkedin.com", "w3.org",
    "gstatic.com", "googleapis.com", "xnxx.com", "pornhub.com"
]

JUNK_PREFIXES = [
    "noreply", "no-reply", "admin", "test", "info", "support",
    "contact", "hello", "team", "sales", "abuse", "postmaster",
    "webmaster", "mailer", "donotreply", "do-not-reply", "newsletter"
]

# ============================================================
# B2B QUERIES — HIGH QUALITY TARGETS
# ============================================================
B2B_QUERIES = [
    '"real estate agency" email CEO owner site:yellowpages.com',
    '"marketing agency" owner founder email site:clutch.co',
    '"ecommerce store" owner email site:manta.com',
    '"restaurant owner" email contact site:yelp.com',
    '"clothing brand" founder email contact',
    '"digital marketing" CEO email site:clutch.co',
    '"web design agency" owner email site:clutch.co',
    '"social media agency" founder email',
    '"SEO agency" CEO founder email contact',
    '"software company" founder CEO email site:crunchbase.com',
    '"IT services" owner email site:yellowpages.com',
    '"accounting firm" owner email site:manta.com',
    '"law firm" partner email contact site:yellowpages.com',
    '"dental clinic" owner email contact',
    '"gym fitness" owner email site:yelp.com',
    '"photography studio" owner email contact',
    '"catering company" owner email',
    '"event management" founder email contact',
    '"logistics company" CEO email',
    '"construction company" owner email site:manta.com',
    '"architecture firm" partner email contact',
    '"interior design" owner email',
    '"printing company" owner email site:yellowpages.com',
    '"recruitment agency" founder email',
    '"HR consulting" CEO email contact',
    '"management consulting" partner email',
    '"fintech startup" founder CEO email site:crunchbase.com',
    '"SaaS company" founder email site:crunchbase.com',
    '"cybersecurity firm" CEO email',
    '"cloud services" founder email site:crunchbase.com',
]

# ============================================================
# B2C QUERIES — REAL CONSUMER SOURCES
# ============================================================
B2C_QUERIES = [
    '"clothing" "fashion" buyer email site:forums.net',
    '"home decor" buyer email blog contact',
    '"beauty products" reviewer email blog',
    '"fitness enthusiast" email blog contact',
    '"travel blogger" email contact site',
    '"food blogger" email contact',
    '"lifestyle blogger" email contact',
    '"mom blogger" email contact',
    '"tech reviewer" email blog contact',
    '"fashion blogger" email contact site',
]

# ============================================================
# FREE PROXY FETCHER
# ============================================================
def get_free_proxies():
    proxies = []
    try:
        res = requests.get(
            "https://proxylist.geonode.com/api/proxy-list?limit=50&page=1&sort_by=lastChecked&sort_type=desc&protocols=http",
            timeout=10
        )
        data = res.json()
        for item in data.get("data", []):
            ip   = item.get("ip")
            port = item.get("port")
            if ip and port:
                proxies.append(f"http://{ip}:{port}")
    except Exception as e:
        print(f"[PROXY] Fetch failed: {e}")
    print(f"[PROXY] Loaded {len(proxies)} proxies")
    return proxies

PROXY_POOL = []
PROXY_INDEX = 0

def get_next_proxy():
    global PROXY_INDEX
    if not PROXY_POOL:
        return None
    proxy = PROXY_POOL[PROXY_INDEX % len(PROXY_POOL)]
    PROXY_INDEX += 1
    return {"http": proxy, "https": proxy}

# ============================================================
# EMAIL UTILITIES
# ============================================================
def extract_emails(text):
    raw = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-z]{2,}', text)
    cleaned = []
    for e in set(raw):
        e = e.lower().strip()
        if len(e) > 60:
            continue
        if any(j in e for j in JUNK_DOMAINS):
            continue
        prefix = e.split("@")[0]
        if any(prefix.startswith(j) for j in JUNK_PREFIXES):
            continue
        cleaned.append(e)
    return cleaned


def verify_email(email):
    # Step 1: Format validation
    try:
        valid = validate_email(email, check_deliverability=False)
        email = valid.email.lower()
    except EmailNotValidError:
        return False, "bad_format"

    domain = email.split("@")[1]

    # Step 2: Disposable check
    if domain in DISPOSABLE_DOMAINS:
        return False, "disposable"

    # Step 3: MX record check
    try:
        dns.resolver.resolve(domain, "MX")
    except Exception:
        return False, "no_mx"

    return True, "valid"


def is_quality_email(email):
    """Prefer personal/role-specific emails over generic ones."""
    prefix = email.split("@")[0].lower()
    generic = ["info", "contact", "hello", "team", "sales",
               "support", "admin", "office", "mail", "enquiry"]
    if any(prefix == g for g in generic):
        return False
    return True

# ============================================================
# SAVE LEAD
# ============================================================
SAVED_EMAILS = set()

def save_lead(email, phone, source, niche, lead_type):
    global SAVED_EMAILS

    if email in SAVED_EMAILS:
        print(f"    [DUPLICATE] {email}")
        return False

    # Verify
    valid, reason = verify_email(email)
    if not valid:
        print(f"    [REJECTED] {email} — {reason}")
        return False

    SAVED_EMAILS.add(email)

    # Save to CSV
    with open(CSV_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([email, phone, source, niche, lead_type, time.strftime("%Y-%m-%d %H:%M")])

    # Save to Google Sheet
    if SHEET_URL and SHEET_URL != "YOUR_GOOGLE_APPS_SCRIPT_URL_HERE":
        try:
            payload = {
                "action"       : "add",
                "email"        : email,
                "phone"        : phone,
                "source"       : source[:100],
                "business_type": niche,
                "lead_type"    : lead_type,
                "location"     : "Global"
            }
            res = requests.post(SHEET_URL, json=payload, timeout=10)
            if "Duplicate" in res.text:
                print(f"    [SHEET DUPLICATE] {email}")
                return False
        except Exception as e:
            print(f"    [SHEET ERROR] {e}")

    print(f"    [SAVED] {email} | {phone} | {niche} | {lead_type}")
    return True

# ============================================================
# BING SCRAPER
# ============================================================
def scrape_bing(query, lead_type="B2B"):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
    }
    saved = 0
    try:
        url  = f"https://www.bing.com/search?q={quote(query)}&count=10"
        prx  = get_next_proxy()
        res  = requests.get(url, headers=headers, proxies=prx, timeout=15)
        soup = BeautifulSoup(res.text, "html.parser")

        urls = []
        for a in soup.select("li.b_algo h2 a"):
            href = a.get("href", "")
            if href.startswith("http") and not any(j in href for j in JUNK_DOMAINS):
                urls.append(href)

        urls = list(set(urls))[:8]
        print(f"    [BING] {len(urls)} URLs found")

        niche = query.split('"')[1] if '"' in query else query.split()[0]

        for site_url in urls:
            if saved >= MAX_PER_CYCLE:
                break
            pages = [
                site_url,
                site_url.rstrip("/") + "/contact",
                site_url.rstrip("/") + "/contact-us",
                site_url.rstrip("/") + "/about",
                site_url.rstrip("/") + "/about-us",
                site_url.rstrip("/") + "/team",
            ]
            for page in pages:
                try:
                    r    = requests.get(page, headers=headers, timeout=8, proxies=get_next_proxy())
                    if r.status_code != 200:
                        continue
                    text   = BeautifulSoup(r.text, "html.parser").get_text()
                    emails = extract_emails(text)
                    for email in emails:
                        if save_lead(email, "N/A", site_url, niche, lead_type):
                            saved += 1
                    if emails:
                        break
                except Exception:
                    continue
            time.sleep(random.uniform(1.5, 3))

    except Exception as e:
        print(f"    [BING ERROR] {e}")

    return saved

# ============================================================
# DUCKDUCKGO SCRAPER
# ============================================================
def scrape_duckduckgo(query, lead_type="B2B"):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
    }
    saved = 0
    try:
        url  = f"https://html.duckduckgo.com/html/?q={quote(query)}"
        res  = requests.get(url, headers=headers, timeout=15)
        soup = BeautifulSoup(res.text, "html.parser")

        urls = []
        for a in soup.select("a.result__url"):
            href = a.get("href", "")
            if href.startswith("http") and not any(j in href for j in JUNK_DOMAINS):
                urls.append(href)

        urls = list(set(urls))[:8]
        print(f"    [DDG] {len(urls)} URLs found")

        niche = query.split('"')[1] if '"' in query else query.split()[0]

        for site_url in urls:
            if saved >= MAX_PER_CYCLE:
                break
            try:
                r    = requests.get(site_url, headers=headers, timeout=8)
                if r.status_code != 200:
                    continue
                text   = BeautifulSoup(r.text, "html.parser").get_text()
                emails = extract_emails(text)
                for email in emails:
                    if save_lead(email, "N/A", site_url, niche, lead_type):
                        saved += 1
            except Exception:
                continue
            time.sleep(random.uniform(1, 2))

    except Exception as e:
        print(f"    [DDG ERROR] {e}")

    return saved

# ============================================================
# YELLOW PAGES SCRAPER
# ============================================================
def scrape_yellowpages(query, location="New York"):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
    }
    saved = 0
    try:
        url  = f"https://www.yellowpages.com/search?search_terms={quote(query)}&geo_location_terms={quote(location)}"
        res  = requests.get(url, headers=headers, timeout=15)
        soup = BeautifulSoup(res.text, "html.parser")

        listings = soup.select("div.info")
        print(f"    [YP] {len(listings)} listings found")

        for listing in listings[:8]:
            if saved >= MAX_PER_CYCLE:
                break
            try:
                # Get website link
                website_el = listing.select_one("a.track-visit-website")
                phone_el   = listing.select_one("div.phones")
                phone      = phone_el.text.strip() if phone_el else "N/A"

                if website_el:
                    site_url = website_el.get("href", "")
                    if not site_url.startswith("http"):
                        continue
                    r    = requests.get(site_url, headers=headers, timeout=8)
                    text = BeautifulSoup(r.text, "html.parser").get_text()
                    emails = extract_emails(text)
                    for email in emails:
                        if save_lead(email, phone, site_url, query, "B2B"):
                            saved += 1
            except Exception:
                continue
            time.sleep(random.uniform(1, 2.5))

    except Exception as e:
        print(f"    [YP ERROR] {e}")

    return saved

# ============================================================
# YELP SCRAPER
# ============================================================
def scrape_yelp(query, location="New York"):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
    }
    saved = 0
    try:
        url  = f"https://www.yelp.com/search?find_desc={quote(query)}&find_loc={quote(location)}"
        res  = requests.get(url, headers=headers, timeout=15)
        soup = BeautifulSoup(res.text, "html.parser")

        links = []
        for a in soup.select("a[href*='/biz/']"):
            href = a.get("href", "")
            if href.startswith("/biz/"):
                links.append("https://www.yelp.com" + href.split("?")[0])

        links = list(set(links))[:8]
        print(f"    [YELP] {len(links)} listings found")

        for biz_url in links:
            if saved >= MAX_PER_CYCLE:
                break
            try:
                r    = requests.get(biz_url, headers=headers, timeout=10)
                soup = BeautifulSoup(r.text, "html.parser")

                # Try to find website link on Yelp page
                website_el = soup.select_one("a[href*='biz_redir']")
                if website_el:
                    site_url = website_el.get("href", "")
                    r2   = requests.get(site_url, headers=headers, timeout=8)
                    text = BeautifulSoup(r2.text, "html.parser").get_text()
                    emails = extract_emails(text)
                    for email in emails:
                        if save_lead(email, "N/A", site_url, query, "B2C"):
                            saved += 1

            except Exception:
                continue
            time.sleep(random.uniform(1, 2))

    except Exception as e:
        print(f"    [YELP ERROR] {e}")

    return saved

# ============================================================
# GOOGLE MAPS SCRAPER (Playwright)
# ============================================================
async def get_emails_from_website(page, url):
    suffixes = ["", "/contact", "/contact-us", "/about", "/about-us", "/team"]
    for suffix in suffixes:
        try:
            full_url = url.rstrip("/") + suffix
            await page.goto(full_url, wait_until="domcontentloaded", timeout=10000)
            await page.wait_for_timeout(1500)
            text   = await page.inner_text("body")
            emails = extract_emails(text)
            if emails:
                return emails
        except Exception:
            continue
    return []


async def scrape_google_maps(page, query, lead_type="B2B"):
    saved = 0
    try:
        maps_url = f"https://www.google.com/maps/search/{quote(query)}"
        await page.goto(maps_url, wait_until="domcontentloaded", timeout=15000)
        await page.wait_for_timeout(4000)

        listings = await page.query_selector_all("div.Nv2PK")
        print(f"    [MAPS] {len(listings)} listings found")

        niche    = query.split()[0]
        location = " ".join(query.split()[1:]) if len(query.split()) > 1 else "Global"

        for i in range(min(len(listings), 8)):
            if saved >= MAX_PER_CYCLE:
                break
            try:
                await page.goto(maps_url, wait_until="domcontentloaded", timeout=15000)
                await page.wait_for_timeout(3000)
                listings = await page.query_selector_all("div.Nv2PK")
                if i >= len(listings):
                    break

                await listings[i].click()
                await page.wait_for_timeout(2500)

                # Phone
                phone    = "N/A"
                phone_el = await page.query_selector("button[data-item-id*='phone']")
                if phone_el:
                    aria = await phone_el.get_attribute("aria-label")
                    if aria:
                        phone = re.sub(r'[^\d+\s\-()]', '', aria).strip()

                # Website
                website_el = await page.query_selector("a[data-item-id='authority']")
                if website_el:
                    website = await website_el.get_attribute("href")
                    if website:
                        emails = await get_emails_from_website(page, website)
                        for email in emails:
                            if save_lead(email, phone, website, niche, lead_type):
                                saved += 1

                await asyncio.sleep(random.uniform(1.5, 3))

            except Exception as e:
                print(f"    [MAPS ERROR] {e}")
                continue

    except Exception as e:
        print(f"    [MAPS FATAL] {e}")

    return saved

# ============================================================
# MANTA SCRAPER (B2B Small Businesses)
# ============================================================
def scrape_manta(query):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
    }
    saved = 0
    try:
        url  = f"https://www.manta.com/search?search={quote(query)}"
        res  = requests.get(url, headers=headers, timeout=15)
        soup = BeautifulSoup(res.text, "html.parser")

        links = []
        for a in soup.select("a[href*='/c/']"):
            href = a.get("href", "")
            if href.startswith("https://www.manta.com/c/"):
                links.append(href)

        links = list(set(links))[:8]
        print(f"    [MANTA] {len(links)} listings found")

        for biz_url in links:
            if saved >= MAX_PER_CYCLE:
                break
            try:
                r    = requests.get(biz_url, headers=headers, timeout=10)
                text = BeautifulSoup(r.text, "html.parser").get_text()
                emails = extract_emails(text)
                for email in emails:
                    niche = query.split()[0]
                    if save_lead(email, "N/A", biz_url, niche, "B2B"):
                        saved += 1
            except Exception:
                continue
            time.sleep(random.uniform(1, 2))

    except Exception as e:
        print(f"    [MANTA ERROR] {e}")

    return saved

# ============================================================
# MAIN AGENT
# ============================================================
async def run_agent():
    global PROXY_POOL

    print("=" * 60)
    print("   LEADS AGENT ACTIVATED")
    print("=" * 60)

    # Setup CSV
    with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Email", "Phone", "Source", "Niche", "Type", "Timestamp"])

    # Load proxies
    PROXY_POOL = get_free_proxies()

    # Shuffle queries
    b2b_queries = B2B_QUERIES.copy()
    b2c_queries = B2C_QUERIES.copy()
    random.shuffle(b2b_queries)
    random.shuffle(b2c_queries)

    b2b_index  = 0
    b2c_index  = 0
    cycle      = 0
    agent_start = time.time()
    total_saved = 0

    locations = [
        "New York", "Los Angeles", "Chicago", "Houston",
        "London", "Dubai", "Toronto", "Sydney", "Miami", "Dallas"
    ]

    maps_queries_b2b = [
        "real estate agency", "marketing agency", "web design company",
        "accounting firm", "law firm", "dental clinic", "gym fitness",
        "restaurant", "clothing store", "photography studio"
    ]

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"]
        )
        page = await browser.new_page()
        await page.set_extra_http_headers({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
        })

        while (time.time() - agent_start) < (RUN_HOURS * 3600):
            cycle += 1
            elapsed = int((time.time() - agent_start) / 60)

            print(f"\n{'='*60}")
            print(f"[CYCLE {cycle}] Elapsed: {elapsed} mins | Total Saved: {total_saved}")
            print(f"{'='*60}")

            location = random.choice(locations)

            # --- B2B Round ---
            b2b_query = b2b_queries[b2b_index % len(b2b_queries)]
            b2b_index += 1
            print(f"\n[B2B BING] {b2b_query}")
            total_saved += scrape_bing(b2b_query, "B2B")

            print(f"\n[B2B DDG] {b2b_query}")
            total_saved += scrape_duckduckgo(b2b_query, "B2B")

            print(f"\n[B2B YELLOW PAGES] {b2b_query[:20]} | {location}")
            total_saved += scrape_yellowpages(b2b_query.split('"')[1] if '"' in b2b_query else b2b_query, location)

            print(f"\n[B2B MANTA] {b2b_query}")
            total_saved += scrape_manta(b2b_query.split('"')[1] if '"' in b2b_query else b2b_query)

            # --- B2C Round ---
            b2c_query = b2c_queries[b2c_index % len(b2c_queries)]
            b2c_index += 1
            print(f"\n[B2C BING] {b2c_query}")
            total_saved += scrape_bing(b2c_query, "B2C")

            print(f"\n[B2C YELP] {b2c_query[:20]} | {location}")
            total_saved += scrape_yelp(b2c_query.split('"')[1] if '"' in b2c_query else b2c_query, location)

            # --- Google Maps Round ---
            maps_q = random.choice(maps_queries_b2b) + " " + location
            print(f"\n[MAPS] {maps_q}")
            total_saved += await scrape_google_maps(page, maps_q, "B2B")

            # Refresh proxies every 10 cycles
            if cycle % 10 == 0:
                print("\n[PROXY] Refreshing proxy pool...")
                PROXY_POOL = get_free_proxies()

            print(f"\n[WAIT] {CYCLE_DELAY}s | Total saved: {total_saved}")
            await asyncio.sleep(CYCLE_DELAY)

        await browser.close()

    print("\n" + "=" * 60)
    print(f"[DONE] Total leads saved: {total_saved}")
    print(f"[FILE] Saved to: {CSV_FILE}")
    print("=" * 60)


# ============================================================
# ENTRY POINT
# ============================================================
if __name__ == "__main__":
    os.system("pip install playwright email-validator dnspython requests beautifulsoup4 -q")
    os.system("playwright install chromium")
    asyncio.run(run_agent())
