import os
import subprocess

subprocess.run(["pip", "install", "playwright", "email-validator",
                "dnspython", "requests", "beautifulsoup4", "-q"], check=True)
subprocess.run(["playwright", "install", "chromium"], check=True)
subprocess.run(["playwright", "install-deps", "chromium"], check=True)

import re
import time
import random
import asyncio
import requests
import dns.resolver
import csv
import smtplib
import socket
from bs4 import BeautifulSoup
from urllib.parse import quote
from email_validator import validate_email, EmailNotValidError
from playwright.async_api import async_playwright

# ============================================================
# CONFIGURATION
# ============================================================
B2B_URL       = "YOUR_GOOGLE_APPS_SCRIPT_URL_HERE"
CSV_FILE      = "leads_output.csv"
SCRAPE_HOURS  = 7
VERIFY_HOURS  = 1
SLEEP_MINUTES = 7
RESTART_HOURS = 8.5
MAX_PER_CYCLE = 10

# ============================================================
# DISPOSABLE DOMAINS
# ============================================================
DISPOSABLE_DOMAINS = {
    "tempmail.com", "guerrillamail.com", "mailinator.com",
    "throwaway.email", "yopmail.com", "10minutemail.com",
    "trashmail.com", "maildrop.cc", "sharklasers.com",
    "dispostable.com", "spamgourmet.com", "fakeinbox.com",
    "mailnull.com", "trashmail.me", "discard.email",
    "spamfree24.org", "tempr.email", "getairmail.com",
    "filzmail.com", "throwam.com", "bccto.me", "chacuo.net"
}

# ============================================================
# JUNK FILTERS
# ============================================================
JUNK_DOMAINS = [
    "example.com", "schema.org", "google.com", "microsoft.com",
    "bing.com", "jquery.com", "cloudflare.com", "amazonaws.com",
    "sentry.io", "wix.com", "wordpress.com", "squarespace.com",
    "apple.com", "youtube.com", "facebook.com", "twitter.com",
    "instagram.com", "tiktok.com", "linkedin.com", "w3.org",
    "gstatic.com", "googleapis.com", "xnxx.com", "pornhub.com",
    "fedex.com", "ups.com", "usps.com", "dhl.com"
]

JUNK_PREFIXES = [
    "noreply", "no-reply", "test", "abuse", "postmaster",
    "webmaster", "mailer", "donotreply", "do-not-reply",
    "newsletter", "unsubscribe", "bounce", "daemon"
]

# ============================================================
# B2B QUERIES — No law/cybersecurity
# ============================================================
B2B_QUERIES = [
    '"real estate agency" owner founder email',
    '"marketing agency" owner founder email site:clutch.co',
    '"ecommerce store" owner founder email site:manta.com',
    '"restaurant owner" email contact site:yelp.com',
    '"digital marketing agency" CEO founder email',
    '"web design agency" owner email site:clutch.co',
    '"social media agency" founder email contact',
    '"SEO agency" CEO founder email',
    '"software company" founder CEO email site:crunchbase.com',
    '"IT services company" owner email site:yellowpages.com',
    '"accounting firm" owner email site:manta.com',
    '"dental clinic" owner email contact',
    '"gym fitness center" owner email',
    '"photography studio" owner email contact',
    '"catering company" owner email contact',
    '"event management company" founder email',
    '"logistics company" CEO owner email',
    '"construction company" owner email site:manta.com',
    '"architecture firm" partner email contact',
    '"interior design studio" owner email',
    '"printing company" owner email site:yellowpages.com',
    '"recruitment agency" founder email',
    '"HR consulting firm" CEO email contact',
    '"management consulting" partner email',
    '"fintech startup" founder CEO email site:crunchbase.com',
    '"SaaS startup" founder email site:crunchbase.com',
    '"cloud services company" founder email',
    '"app development company" CEO founder email',
    '"video production company" owner email contact',
    '"health clinic" owner email contact',
    '"beauty salon" owner email contact',
    '"spa wellness center" owner email',
    '"travel agency" owner email contact',
    '"food distribution company" owner email',
    '"wholesale business" owner email contact',
    '"e-learning company" founder CEO email',
]

# ============================================================
# B2C QUERIES — Online stores focus
# ============================================================
B2C_QUERIES = [
    '"online clothing store" contact email owner',
    '"fashion boutique" online store email contact',
    '"streetwear brand" owner email contact',
    '"activewear brand" founder email',
    '"vintage clothing" online store email',
    '"home decor online store" owner email',
    '"kitchen products store" contact email',
    '"beauty products online" owner email',
    '"skincare brand" founder email contact',
    '"hair products online store" email contact',
    '"fitness equipment store" owner email',
    '"jewelry online store" owner founder email',
    '"handmade products store" email contact',
    '"pet supplies online" owner email',
    '"supplement store online" founder email',
    '"baby products store" founder email',
    '"electronics accessories store" email owner',
    '"sports gear online" contact email owner',
]

MAPS_QUERIES = [
    "real estate agency", "marketing agency", "web design company",
    "accounting firm", "dental clinic", "gym fitness",
    "restaurant", "clothing store", "photography studio",
    "beauty salon", "spa wellness", "catering company",
    "event management", "interior design", "travel agency",
    "digital marketing agency", "health clinic", "printing company"
]

LOCATIONS = [
    "New York", "Los Angeles", "Chicago", "Houston",
    "London", "Dubai", "Toronto", "Sydney", "Miami", "Dallas",
    "San Francisco", "Boston", "Seattle", "Atlanta", "Phoenix"
]

# ============================================================
# PROXY POOL
# ============================================================
PROXY_POOL  = []
PROXY_INDEX = 0

def get_free_proxies():
    proxies = []
    try:
        res = requests.get(
            "https://proxylist.geonode.com/api/proxy-list?limit=50&page=1"
            "&sort_by=lastChecked&sort_type=desc&protocols=http",
            timeout=10
        )
        for item in res.json().get("data", []):
            ip, port = item.get("ip"), item.get("port")
            if ip and port:
                proxies.append(f"http://{ip}:{port}")
    except Exception as e:
        print(f"[PROXY] Fetch failed: {e}")
    print(f"[PROXY] Loaded {len(proxies)} proxies")
    return proxies

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
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
}

def extract_emails(text):
    raw     = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-z]{2,}', text)
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

# ============================================================
# REACHER-STYLE VERIFICATION
# ============================================================
BIG_PROVIDERS = {
    "gmail.com", "hotmail.com", "yahoo.com", "outlook.com",
    "icloud.com", "live.com", "yahoo.co.uk", "googlemail.com",
    "protonmail.com", "aol.com"
}

def get_mx(domain):
    try:
        records = dns.resolver.resolve(domain, "MX")
        return str(sorted(records, key=lambda r: r.preference)[0].exchange).rstrip(".")
    except Exception:
        return None

def smtp_check(email, mx, timeout=10):
    try:
        server = smtplib.SMTP(timeout=timeout)
        server.connect(mx, 25)
        server.helo("verify.local")
        server.mail("check@verify.local")
        code, _ = server.rcpt(str(email))
        server.quit()
        return code
    except smtplib.SMTPConnectError:
        return "CONNECT_ERROR"
    except socket.timeout:
        return "TIMEOUT"
    except Exception:
        return "ERROR"

def is_catch_all(domain, mx):
    fake = f"zzz_fake_{random.randint(10000,99999)}@{domain}"
    return smtp_check(fake, mx, timeout=8) == 250

def reacher_verify(email):
    try:
        valid = validate_email(email, check_deliverability=False)
        email = valid.email.lower()
    except EmailNotValidError:
        return "INVALID", "bad_format"

    domain = email.split("@")[1]

    if domain in DISPOSABLE_DOMAINS:
        return "INVALID", "disposable"

    if domain in BIG_PROVIDERS:
        return "RISKY", "big_provider"

    mx = get_mx(domain)
    if not mx:
        return "INVALID", "no_mx"

    try:
        if is_catch_all(domain, mx):
            return "RISKY", "catch_all"
    except Exception:
        pass

    code = smtp_check(email, mx)
    if code == 250:
        return "VALID", "smtp_confirmed"
    elif code == 550:
        return "INVALID", "mailbox_not_found"
    elif code == "CONNECT_ERROR":
        return "UNKNOWN", "port_25_blocked"
    elif code == "TIMEOUT":
        return "UNKNOWN", "timeout"
    else:
        return "UNKNOWN", f"code_{code}"

# ============================================================
# SAVE LEAD
# ============================================================
SAVED_EMAILS = set()

def save_lead(email, phone, source, niche, lead_type):
    global SAVED_EMAILS
    email = email.lower().strip()
    if email in SAVED_EMAILS:
        return False
    try:
        validate_email(email, check_deliverability=False)
    except EmailNotValidError:
        return False
    domain = email.split("@")[1]
    if domain in DISPOSABLE_DOMAINS:
        return False
    SAVED_EMAILS.add(email)

    phone_str = ("'" + str(phone)) if phone and phone != "N/A" else "N/A"

    with open(CSV_FILE, "a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow([
            email, phone_str, source, niche,
            lead_type, "Pending", time.strftime("%Y-%m-%d %H:%M")
        ])

    if B2B_URL != "YOUR_GOOGLE_APPS_SCRIPT_URL_HERE":
        try:
            requests.post(B2B_URL, json={
                "action": "add", "email": email,
                "phone": phone_str, "source": source[:100],
                "business_type": niche, "lead_type": lead_type,
                "location": "Global"
            }, timeout=10)
        except Exception as e:
            print(f"    [SHEET ERROR] {e}")

    print(f"    [SAVED] {email} | {niche} | {lead_type}")
    return True

def update_csv_status(email, status):
    try:
        rows = []
        with open(CSV_FILE, "r", encoding="utf-8") as f:
            rows = list(csv.reader(f))
        with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            for row in rows:
                if len(row) >= 6 and row[0] == email:
                    row[5] = status
                writer.writerow(row)
    except Exception as e:
        print(f"    [CSV STATUS ERROR] {e}")

def update_sheet_status(email, status):
    if B2B_URL != "YOUR_GOOGLE_APPS_SCRIPT_URL_HERE":
        try:
            requests.post(B2B_URL, json={
                "action": "update_status",
                "email": email, "status": status
            }, timeout=10)
        except Exception:
            pass

# ============================================================
# SCRAPERS
# ============================================================
def scrape_site(site_url, niche, lead_type):
    saved = 0
    for suffix in ["", "/contact", "/contact-us", "/about", "/about-us", "/team"]:
        try:
            r = requests.get(
                site_url.rstrip("/") + suffix,
                headers=HEADERS, timeout=8, proxies=get_next_proxy()
            )
            if r.status_code != 200:
                continue
            emails = extract_emails(BeautifulSoup(r.text, "html.parser").get_text())
            for email in emails:
                if save_lead(email, "N/A", site_url, niche, lead_type):
                    saved += 1
            if emails:
                break
        except Exception:
            continue
    return saved

def scrape_bing(query, lead_type="B2B"):
    saved = 0
    try:
        res  = requests.get(
            f"https://www.bing.com/search?q={quote(query)}&count=10",
            headers=HEADERS, proxies=get_next_proxy(), timeout=15
        )
        soup = BeautifulSoup(res.text, "html.parser")
        urls = list({
            a.get("href", "") for a in soup.select("li.b_algo h2 a")
            if a.get("href", "").startswith("http")
            and not any(j in a.get("href", "") for j in JUNK_DOMAINS)
        })[:8]
        niche = query.split('"')[1] if '"' in query else query.split()[0]
        print(f"    [BING] {len(urls)} URLs")
        for u in urls:
            saved += scrape_site(u, niche, lead_type)
            time.sleep(random.uniform(1.5, 3))
    except Exception as e:
        print(f"    [BING ERROR] {e}")
    return saved

def scrape_duckduckgo(query, lead_type="B2B"):
    saved = 0
    try:
        res  = requests.get(
            f"https://html.duckduckgo.com/html/?q={quote(query)}",
            headers=HEADERS, timeout=15
        )
        soup = BeautifulSoup(res.text, "html.parser")
        urls = list({
            a.get("href", "") for a in soup.select("a.result__url")
            if a.get("href", "").startswith("http")
            and not any(j in a.get("href", "") for j in JUNK_DOMAINS)
        })[:8]
        niche = query.split('"')[1] if '"' in query else query.split()[0]
        print(f"    [DDG] {len(urls)} URLs")
        for u in urls:
            saved += scrape_site(u, niche, lead_type)
            time.sleep(random.uniform(1, 2))
    except Exception as e:
        print(f"    [DDG ERROR] {e}")
    return saved

def scrape_yellowpages(query, location):
    saved = 0
    try:
        res  = requests.get(
            f"https://www.yellowpages.com/search"
            f"?search_terms={quote(query)}&geo_location_terms={quote(location)}",
            headers=HEADERS, timeout=15
        )
        soup = BeautifulSoup(res.text, "html.parser")
        listings = soup.select("div.info")
        print(f"    [YP] {len(listings)} listings")
        for listing in listings[:8]:
            try:
                w = listing.select_one("a.track-visit-website")
                if w:
                    saved += scrape_site(w.get("href", ""), query, "B2B")
            except Exception:
                continue
            time.sleep(random.uniform(1, 2.5))
    except Exception as e:
        print(f"    [YP ERROR] {e}")
    return saved

def scrape_yelp(query, location):
    """
    Scrapes Yelp in 2 ways:
    1. Business website → extract contact emails (B2B)
    2. Reviewer profiles → extract reviewer emails (B2C)
    """
    saved = 0
    try:
        res  = requests.get(
            f"https://www.yelp.com/search?find_desc={quote(query)}&find_loc={quote(location)}",
            headers=HEADERS, timeout=15
        )
        soup  = BeautifulSoup(res.text, "html.parser")
        links = list({
            "https://www.yelp.com" + a.get("href", "").split("?")[0]
            for a in soup.select("a[href*='/biz/']")
            if a.get("href", "").startswith("/biz/")
        })[:8]
        print(f"    [YELP] {len(links)} listings")

        for biz in links:
            try:
                r = requests.get(biz, headers=HEADERS, timeout=10)
                s = BeautifulSoup(r.text, "html.parser")

                # ── B2B: Business website email ──────────────
                wel = s.select_one("a[href*='biz_redir']")
                if wel:
                    saved += scrape_site(wel.get("href", ""), query, "B2B")

                # ── B2C: Reviewer profile emails ─────────────
                reviewer_links = list({
                    "https://www.yelp.com" + a.get("href", "").split("?")[0]
                    for a in s.select("a[href*='/user_details']")
                    if a.get("href", "").startswith("/user_details")
                })[:5]

                for reviewer_url in reviewer_links:
                    try:
                        rr   = requests.get(reviewer_url, headers=HEADERS, timeout=8)
                        rs   = BeautifulSoup(rr.text, "html.parser")
                        text = rs.get_text()
                        emails = extract_emails(text)
                        for email in emails:
                            if save_lead(email, "N/A", reviewer_url, query, "B2C"):
                                saved += 1
                        # Also check reviewer website if listed
                        web_el = rs.select_one("a[href*='http'][class*='website']")
                        if not web_el:
                            web_el = rs.select_one("p.external-website a")
                        if web_el:
                            saved += scrape_site(web_el.get("href", ""), query, "B2C")
                    except Exception:
                        continue
                    time.sleep(random.uniform(1, 1.5))

            except Exception:
                continue
            time.sleep(random.uniform(1, 2))

    except Exception as e:
        print(f"    [YELP ERROR] {e}")
    return saved

def scrape_manta(query):
    saved = 0
    try:
        res  = requests.get(
            f"https://www.manta.com/search?search={quote(query)}",
            headers=HEADERS, timeout=15
        )
        soup  = BeautifulSoup(res.text, "html.parser")
        links = list({
            a.get("href", "") for a in soup.select("a[href*='/c/']")
            if a.get("href", "").startswith("https://www.manta.com/c/")
        })[:8]
        niche = query.split('"')[1] if '"' in query else query.split()[0]
        print(f"    [MANTA] {len(links)} listings")
        for biz in links:
            saved += scrape_site(biz, niche, "B2B")
            time.sleep(random.uniform(1, 2))
    except Exception as e:
        print(f"    [MANTA ERROR] {e}")
    return saved

async def get_emails_playwright(page, url):
    for suffix in ["", "/contact", "/contact-us", "/about", "/about-us", "/team"]:
        try:
            await page.goto(url.rstrip("/") + suffix,
                            wait_until="domcontentloaded", timeout=10000)
            await page.wait_for_timeout(1500)
            emails = extract_emails(await page.inner_text("body"))
            if emails:
                return emails
        except Exception:
            continue
    return []

async def scrape_maps(page, query, lead_type="B2B"):
    saved    = 0
    maps_url = f"https://www.google.com/maps/search/{quote(query)}"
    try:
        await page.goto(maps_url, wait_until="domcontentloaded", timeout=15000)
        await page.wait_for_timeout(4000)
        listings = await page.query_selector_all("div.Nv2PK")
        niche    = query.split()[0]
        print(f"    [MAPS] {len(listings)} listings")
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
                phone    = "N/A"
                phone_el = await page.query_selector("button[data-item-id*='phone']")
                if phone_el:
                    aria = await phone_el.get_attribute("aria-label")
                    if aria:
                        phone = re.sub(r'[^\d+\s\-()]', '', aria).strip()
                web_el = await page.query_selector("a[data-item-id='authority']")
                if web_el:
                    website = await web_el.get_attribute("href")
                    if website:
                        emails = await get_emails_playwright(page, website)
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
# VERIFICATION PHASE
# ============================================================
def run_verification_phase():
    print("\n" + "=" * 60)
    print("[PHASE 2] VERIFICATION — 1 hour")
    print("=" * 60)

    verify_start            = time.time()
    valid_c, invalid_c, unknown_c = 0, 0, 0

    pending = []
    try:
        with open(CSV_FILE, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            next(reader)
            for row in reader:
                if len(row) >= 6 and row[5] == "Pending":
                    pending.append(row[0])
    except Exception as e:
        print(f"[VERIFY] Read error: {e}")
        return

    print(f"[VERIFY] {len(pending)} pending emails")

    for email in pending:
        if (time.time() - verify_start) >= (VERIFY_HOURS * 3600):
            print("[VERIFY] 1 hour limit — stopping")
            break

        status, reason = reacher_verify(email)
        update_csv_status(email, status)
        update_sheet_status(email, status)

        if status == "VALID":
            valid_c += 1
            print(f"    [VALID]   {email} — {reason}")
        elif status == "INVALID":
            invalid_c += 1
            print(f"    [INVALID] {email} — {reason}")
        else:
            unknown_c += 1
            print(f"    [{status}] {email} — {reason}")

        time.sleep(1.5)

    print(f"\n[VERIFY DONE] Valid:{valid_c} Invalid:{invalid_c} Unknown/Risky:{unknown_c}")

# ============================================================
# MAIN AGENT
# ============================================================
async def run_agent():
    global PROXY_POOL

    print("=" * 60)
    print("   LEADS AGENT — Full Cycle Mode")
    print("   7h Scrape → 1h Verify → 7min Sleep → Repeat @ 8.5h")
    print("=" * 60)

    with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow([
            "Email", "Phone", "Source", "Niche",
            "Type", "Status", "Timestamp"
        ])

    PROXY_POOL = get_free_proxies()

    b2b_q = B2B_QUERIES.copy()
    b2c_q = B2C_QUERIES.copy()
    random.shuffle(b2b_q)
    random.shuffle(b2c_q)

    master_cycle = 0
    grand_total  = 0

    while True:
        master_cycle += 1
        cycle_start   = time.time()

        print(f"\n{'='*60}")
        print(f"[MASTER CYCLE {master_cycle}]")
        print(f"{'='*60}")

        # ── PHASE 1: SCRAPING ────────────────────────────────
        print(f"\n[PHASE 1] Scraping {SCRAPE_HOURS}h...")
        scrape_start = time.time()
        total_saved  = 0
        b2b_i = b2c_i = sub_cycle = 0

        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-setuid-sandbox",
                      "--disable-dev-shm-usage"]
            )
            page = await browser.new_page()
            await page.set_extra_http_headers({"User-Agent": HEADERS["User-Agent"]})

            while (time.time() - scrape_start) < (SCRAPE_HOURS * 3600):
                sub_cycle += 1
                elapsed    = int((time.time() - scrape_start) / 60)
                print(f"\n[SUB {sub_cycle}] {elapsed}min | Saved: {total_saved}")

                location  = random.choice(LOCATIONS)
                b2b_query = b2b_q[b2b_i % len(b2b_q)]
                b2c_query = b2c_q[b2c_i % len(b2c_q)]
                b2b_i    += 1
                b2c_i    += 1
                q_b2b     = b2b_query.split('"')[1] if '"' in b2b_query else b2b_query
                q_b2c     = b2c_query.split('"')[1] if '"' in b2c_query else b2c_query

                print(f"\n[B2B BING]  {b2b_query[:50]}")
                total_saved += scrape_bing(b2b_query, "B2B")

                print(f"\n[B2B DDG]   {b2b_query[:50]}")
                total_saved += scrape_duckduckgo(b2b_query, "B2B")

                print(f"\n[B2B YP]    {q_b2b[:30]} | {location}")
                total_saved += scrape_yellowpages(q_b2b, location)

                print(f"\n[B2B MANTA] {q_b2b[:30]}")
                total_saved += scrape_manta(q_b2b)

                print(f"\n[B2C BING]  {b2c_query[:50]}")
                total_saved += scrape_bing(b2c_query, "B2C")

                print(f"\n[B2C YELP]  {q_b2c[:30]} | {location}")
                total_saved += scrape_yelp(q_b2c, location)

                maps_q = random.choice(MAPS_QUERIES) + " " + location
                print(f"\n[MAPS]      {maps_q}")
                total_saved += await scrape_maps(page, maps_q, "B2B")

                if sub_cycle % 10 == 0:
                    print("\n[PROXY] Refreshing...")
                    PROXY_POOL = get_free_proxies()

                print(f"\n[WAIT] 45s | Total: {total_saved}")
                await asyncio.sleep(45)

            await browser.close()

        grand_total += total_saved
        print(f"\n[PHASE 1 DONE] This session: {total_saved} | Grand total: {grand_total}")

        # ── PHASE 2: VERIFICATION ────────────────────────────
        run_verification_phase()

        # ── SLEEP ────────────────────────────────────────────
        print(f"\n[SLEEP] {SLEEP_MINUTES} minutes...")
        time.sleep(SLEEP_MINUTES * 60)

        # ── WAIT FOR FULL 8.5h CYCLE ─────────────────────────
        elapsed_total = time.time() - cycle_start
        remaining     = (RESTART_HOURS * 3600) - elapsed_total
        if remaining > 0:
            print(f"[WAIT] {int(remaining/60)}min until next cycle...")
            time.sleep(remaining)

        print(f"\n[RESTART] Cycle {master_cycle} done → Starting cycle {master_cycle+1}")


if __name__ == "__main__":
    asyncio.run(run_agent())
