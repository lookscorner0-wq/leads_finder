import os
print("Installing Playwright and browsers...")
os.system('pip install playwright')
os.system('playwright install chromium')
os.system('playwright install --with-deps chromium')
import asyncio
import nest_asyncio
import re
import time
import random
import smtplib
import dns.resolver
import requests
from bs4 import BeautifulSoup
from urllib.parse import quote
from kaggle_secrets import UserSecretsClient

nest_asyncio.apply()

# ============================================================
# CONFIGURATION
# ============================================================
B2B_URL = "https://script.google.com/macros/s/AKfycbyQF4wMuCc2fUU-PFW7vrmHuDDUeJzsXYtQwyZC3d03THFYg01tqOuBtQ82qOfEDhqr/exec"
print(f"Connecting to: {B2B_URL}") # Taake logs mein confirm ho jaye

START_TIME = time.time()
SEVEN_HOURS = 7 * 3600
ONE_HOUR = 1 * 3600

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
}

JUNK = [
    "example.com", "schema.org", "google.com", "microsoft.com",
    "xnxx", "porn", "adult", "sex", "xxx", "forum", "noreply",
    "no-reply", "support", "info", "admin", "testbing.com", "jquery.com",
    "cloudflare.com", "amazonaws.com",
    "sentry.io", "wix.com", "wordpress.com", "squarespace.com",
    "apple.com", "youtube.com", "facebook.com", "twitter.com",
    "instagram.com", "tiktok.com", "linkedin.com", "w3.org",
    "duckduckgo.com", "maps.google.com", "gstatic.com", "googleapis.com"
]

# ============================================================
# QUERIES
# ============================================================
QUERIES = [
    '"HVAC company" "New York" "contact us"',
    '"solar energy company" "California" "email us"',
    '"plumbing services" "London" "contact"',
    '"roofing contractor" "Texas" "get in touch"',
    '"dentist clinic" "Dubai" "contact us"',
    '"gym fitness center" "Toronto" "email"',
    '"interior design studio" "Sydney" "contact us"',
    '"car detailing service" "Chicago" "contact"',
    '"digital marketing agency" "New York" "contact us"',
    '"SEO agency" "London" "email us"',
    '"software house" "Toronto" "contact us"',
    '"recruitment agency" "Dubai" "get in touch"',
    '"accounting firm" "Sydney" "contact"',
    '"IT services company" "Chicago" "email us"',
    '"cleaning services" "Los Angeles" "contact us"',
    '"landscaping company" "Houston" "email"',
    '"photography studio" "Miami" "contact us"',
    '"catering company" "London" "get in touch"',
    '"event management company" "Dubai" "contact"',
    '"real estate agency" "New York" "email us"',
    '"insurance agency" "California" "contact us"',
    '"web design agency" "Toronto" "contact"',
    '"ecommerce store" "Sydney" "email us"',
    '"logistics company" "Chicago" "contact us"',
    '"trucking company" "Texas" "get in touch"',
    '"SaaS startup" "New York" "contact us"',
    '"AI company" "London" "email us"',
    '"cybersecurity firm" "Toronto" "contact"',
    '"cloud services company" "Dubai" "contact us"',
    '"data analytics company" "Sydney" "email"',
    '"construction company" "Houston" "contact us"',
    '"architecture firm" "Miami" "get in touch"',
    '"electrical contractor" "Chicago" "contact"',
    '"pest control company" "Los Angeles" "email us"',
    '"moving company" "New York" "contact us"',
    '"auto repair shop" "Texas" "contact"',
    '"beauty salon" "London" "email us"',
    '"spa wellness center" "Dubai" "contact us"',
    '"hotel management company" "Sydney" "contact"',
    '"travel agency" "New York" "email us"',
    '"printing company" "Chicago" "contact us"',
    '"graphic design studio" "Miami" "contact"',
    '"video production company" "London" "email"',
    '"animation studio" "Toronto" "contact us"',
    '"app development company" "Dubai" "get in touch"',
    '"machine learning company" "Sydney" "contact"',
    '"fintech startup" "New York" "email us"',
    '"health clinic" "California" "contact us"',
    '"physiotherapy clinic" "London" "contact"',
    '"veterinary clinic" "Toronto" "email us"',
    '"optometry clinic" "Chicago" "contact us"',
    '"tutoring center" "Dubai" "get in touch"',
    '"e-learning company" "Sydney" "contact"',
    '"HR consulting firm" "New York" "email"',
    '"management consulting" "London" "contact us"',
    '"supply chain company" "Texas" "contact"',
    '"manufacturing company" "Chicago" "email us"',
    '"food distribution company" "Miami" "contact us"',
    '"wholesale business" "Toronto" "get in touch"',
    '"drone services company" "Dubai" "contact"',
    '"3D printing company" "Sydney" "email us"',
]

# ============================================================
# UTILITIES
# ============================================================
def extract_emails(text):
    emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-z]{2,}', text)
    return [
        e for e in set(emails)
        if not any(j in e.lower() for j in JUNK + JUNK_KEYWORDS)
        and len(e) < 60
    ]

def clean_phone(phone):
    if not phone or phone == "N/A":
        return "N/A"
    cleaned = re.sub(r'[^\d+]', '', str(phone))
    if len(cleaned) < 7:
        return "N/A"
    return cleaned

def save_lead(email, phone, source, niche, location):
    payload = {
        "action": "add",
        "title": "Business Owner",
        "location": location,
        "business_type": niche,
        "source": source[:100],
        "email": email,
        "phone": clean_phone(phone)
    }
    try:
        res = requests.post(B2B_URL, json=payload, timeout=10)
        if "Added" in res.text:
            print(f"    [SAVED] {email} | {clean_phone(phone)}")
            return True
        elif "Duplicate" in res.text:
            print(f"    [DUPLICATE] {email}")
    except Exception as e:
        print(f"    [SHEET ERROR] {e}")
    return False

def update_sheet(email, action):
    try:
        requests.post(B2B_URL, json={"action": action, "email": email}, timeout=10)
    except:
        pass

# ============================================================
# BING SCRAPER
# ============================================================
def scrape_bing(query):
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    try:
        url = f"https://www.bing.com/search?q={quote(query)}&count=10"
        res = requests.get(url, headers=headers, timeout=15)
        soup = BeautifulSoup(res.text, "html.parser")

        urls = []
        for cite in soup.select("cite"):
            text = cite.text.strip()
            if text.startswith("http"):
                urls.append(text.split(" ")[0])
            elif "." in text:
                clean = text.split("›")[0].strip()
                if clean:
                    urls.append("https://" + clean)

        urls = list(set(urls))
        urls = [u for u in urls if "bing.com" not in u and "microsoft.com" not in u]
        print(f"    [BING] URLs: {len(urls)}")

        niche = query.split('"')[1] if '"' in query else query.split()[0]
        location = query.split('"')[3] if len(query.split('"')) > 3 else "Global"

        for site_url in urls[:6]:
            try:
                pages = [site_url] + [site_url.rstrip("/") + s for s in ["/contact", "/contact-us", "/about"]]
                for page in pages:
                    try:
                        r = requests.get(page, headers=headers, timeout=8)
                        if r.status_code != 200:
                            continue
                        text = BeautifulSoup(r.text, "html.parser").get_text()
                        emails = extract_emails(text)
                        if emails:
                            for email in emails:
                                # Direct save — no collection
                                save_lead(email, "N/A", site_url, niche, location)
                            break
                    except:
                        continue
                time.sleep(random.uniform(1, 2))
            except:
                continue

    except Exception as e:
        print(f"    [BING ERROR] {e}")
# ============================================================
# WEBSITE EMAIL EXTRACTOR (Playwright)
# ============================================================
async def get_emails_from_website(page, url):
    suffixes = ["", "/contact", "/contact-us", "/about", "/about-us"]
    for suffix in suffixes:
        try:
            full_url = url.rstrip("/") + suffix
            await page.goto(full_url, wait_until="domcontentloaded", timeout=10000)
            await page.wait_for_timeout(1500)
            text = await page.inner_text("body")
            emails = extract_emails(text)
            if emails:
                return emails
        except:
            continue
    return []

# ============================================================
# GOOGLE MAPS SCRAPER
# ============================================================
async def scrape_google_maps(page, query):
    leads = []
    try:
        maps_url = f"https://www.google.com/maps/search/{quote(query)}"
        await page.goto(maps_url, wait_until="domcontentloaded", timeout=15000)
        await page.wait_for_timeout(4000)

        listings = await page.query_selector_all("div.Nv2PK")
        print(f"    [MAPS] Listings: {len(listings)}")

        niche = query.split()[0]
        location = " ".join(query.split()[1:])

        for i in range(min(len(listings), 8)):
            try:
                await page.goto(maps_url, wait_until="domcontentloaded", timeout=15000)
                await page.wait_for_timeout(3000)
                listings = await page.query_selector_all("div.Nv2PK")
                if i >= len(listings):
                    break

                await listings[i].click()
                await page.wait_for_timeout(2500)

                phone_el = await page.query_selector("button[data-item-id*='phone']")
                phone = "N/A"
                if phone_el:
                    aria = await phone_el.get_attribute("aria-label")
                    if aria:
                        phone = re.sub(r'[^\d+]', '', aria)

                website_el = await page.query_selector("a[data-item-id='authority']")
                website = await website_el.get_attribute("href") if website_el else None

                if website:
                    emails = await get_emails_from_website(page, website)
                    for email in emails:
                        leads.append({
                            "email": email,
                            "phone": phone,
                            "source": website,
                            "niche": niche,
                            "location": location
                        })
                        print(f"    [MAPS] {email} | {phone}")

                await page.wait_for_timeout(random.randint(1500, 3000))

            except Exception as e:
                print(f"    [MAPS ERROR] {e}")
                continue

    except Exception as e:
        print(f"    [MAPS FATAL] {e}")
    return leads

# ============================================================
# VERIFICATION
# ============================================================
def run_verification():
    print("\n[VERIFICATION] Fetching pending emails...")
    try:
        res = requests.get(B2B_URL, timeout=10)
        pending = res.json()
        print(f"[VERIFICATION] {len(pending)} pending")
        for item in pending:
            email = item if isinstance(item, str) else item.get("email", "")
            if not email:
                continue
            try:
                domain = email.split("@")[1]
                records = dns.resolver.resolve(domain, "MX")
                mx = str(records[0].exchange)
                server = smtplib.SMTP(timeout=10)
                server.connect(mx)
                server.helo("verify.com")
                server.mail("verify@verify.com")
                code, _ = server.rcpt(email)
                server.quit()
                if code == 250:
                    update_sheet(email, "update")
                    print(f"    [VALID] {email}")
                else:
                    update_sheet(email, "delete")
                    print(f"    [REMOVED] {email}")
            except:
                update_sheet(email, "delete")
                print(f"    [REMOVED] {email}")
            time.sleep(1)
    except Exception as e:
        print(f"[VERIFICATION ERROR] {e}")

# ============================================================
# MAIN AGENT
# ============================================================
async def run_agent():
    from playwright.async_api import async_playwright

    queries = QUERIES.copy()
    random.shuffle(queries)
    query_index = 0
    cycle = 0
    agent_start = time.time()

    async with async_playwright() as p:
    browser = await p.chromium.launch(
        headless=True,
        args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"]
    )
    page = await browser.new_page()  # ✅ FIX 1

    while (time.time() - agent_start) < SEVEN_HOURS:
        cycle += 1
        elapsed = int((time.time() - agent_start) / 60)
        print(f"\n{'='*50}")
        print(f"[CYCLE {cycle}] Elapsed: {elapsed} mins")

        query = queries[query_index % len(queries)]
        query_index += 1

        # Bing Round
        print(f"[BING] {query}")
        scrape_bing(query)

        await asyncio.sleep(random.uniform(2, 4))  # ✅ FIX 2 (no page needed)

        # Maps Round
        maps_query = re.sub(r'"', '', query).replace('contact us', '').replace('email us', '').replace('get in touch', '').replace('contact', '').strip()
        print(f"[MAPS] {maps_query}")
        maps_leads = await scrape_google_maps(page, maps_query)
        for lead in maps_leads:
            save_lead(lead["email"], lead["phone"],
                     lead["source"], lead["niche"], lead["location"])

        wait = random.randint(30, 60)
        print(f"[WAIT] {wait}s")
        await asyncio.sleep(wait)  # ✅ FIX 3

    await browser.close()

    # Verification Phase
    print("\n" + "="*50)
    print("[VERIFICATION PHASE] 1 hour...")
    verify_start = time.time()
    while (time.time() - verify_start) < ONE_HOUR:
        run_verification()
        time.sleep(300)

    print("\n[SLEEP] 10 minutes...")
    time.sleep(600)
    print("\n[RESTART]")
    asyncio.run(run_agent())

if __name__ == "__main__":
    print("="*50)
    print("B2B LEAD AGENT ACTIVATED")
    print("="*50)
    asyncio.run(run_agent())
