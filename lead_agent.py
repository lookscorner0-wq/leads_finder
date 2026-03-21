import asyncio
import nest_asyncio
import re
import time
import random
import smtplib
import dns.resolver
import requests
from playwright.async_api import async_playwright
from kaggle_secrets import UserSecretsClient

nest_asyncio.apply()

# ============================================================
# CONFIGURATION
# ============================================================
secrets = UserSecretsClient()
B2B_URL = secrets.get_secret("B2B_SCRIPT_URL")

START_TIME = time.time()
SEVEN_HOURS = 7 * 3600
ONE_HOUR = 1 * 3600

JUNK = [
    "example.com", "schema.org", "google.com", "microsoft.com",
    "bing.com", "jquery.com", "cloudflare.com", "amazonaws.com",
    "sentry.io", "wix.com", "wordpress.com", "squarespace.com",
    "apple.com", "youtube.com", "facebook.com", "twitter.com",
    "instagram.com", "tiktok.com", "linkedin.com", "w3.org",
    "duckduckgo.com", "maps.google.com"
]

# ============================================================
# 50+ DDG QUERIES
# ============================================================
DDG_QUERIES = [
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
    '"restaurant catering" "Toronto" "get in touch"',
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
]

# ============================================================
# EMAIL UTILITIES
# ============================================================
def extract_emails(text):
    emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-z]{2,}', text)
    return [e for e in set(emails) if not any(j in e for j in JUNK) and len(e) < 60]

def verify_email_smtp(email):
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
        return code == 250
    except:
        return False

def save_lead(email, phone, source, niche, location):
    payload = {
        "action": "add",
        "title": "Business Owner",
        "location": location,
        "business_type": niche,
        "source": source,
        "email": email,
        "phone": phone
    }
    try:
        res = requests.post(B2B_URL, json=payload, timeout=10)
        if "Added" in res.text:
            print(f"    [SAVED] {email}")
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
# WEBSITE EMAIL EXTRACTOR
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
# DDG SCRAPER
# ============================================================
async def scrape_ddg(page, query):
    leads = []
    try:
        url = f"https://duckduckgo.com/?q={requests.utils.quote(query)}&ia=web"
        await page.goto(url, wait_until="domcontentloaded", timeout=15000)
        await page.wait_for_timeout(random.randint(2000, 4000))

        results = await page.query_selector_all("article[data-testid='result']")
        print(f"    DDG results: {len(results)}")

        urls = []
        for result in results[:8]:
            a = await result.query_selector("a[href]")
            if a:
                href = await a.get_attribute("href")
                if href and href.startswith("http"):
                    urls.append(href)

        for site_url in urls:
            try:
                emails = await get_emails_from_website(page, site_url)
                if emails:
                    niche = query.split('"')[1]
                    location = query.split('"')[3] if len(query.split('"')) > 3 else "Global"
                    for email in emails:
                        leads.append({
                            "email": email,
                            "phone": "N/A",
                            "source": site_url,
                            "niche": niche,
                            "location": location
                        })
                await page.wait_for_timeout(random.randint(1000, 2000))
            except:
                continue

    except Exception as e:
        print(f"    [DDG ERROR] {e}")
    return leads

# ============================================================
# GOOGLE MAPS SCRAPER
# ============================================================
async def scrape_google_maps(page, query):
    leads = []
    try:
        maps_url = f"https://www.google.com/maps/search/{requests.utils.quote(query)}"
        await page.goto(maps_url, wait_until="domcontentloaded", timeout=15000)
        await page.wait_for_timeout(4000)

        listings = await page.query_selector_all("div.Nv2PK")
        print(f"    Maps listings: {len(listings)}")

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
                phone = await phone_el.get_attribute("aria-label") if phone_el else "N/A"
                if phone and "Phone" in phone:
                    phone = phone.replace("Phone:", "").strip()

                website_el = await page.query_selector("a[data-item-id='authority']")
                website = await website_el.get_attribute("href") if website_el else None

                if website:
                    emails = await get_emails_from_website(page, website)
                    niche = query.split()[0]
                    location = " ".join(query.split()[1:])
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
                print(f"    [MAPS LISTING ERROR] {e}")
                continue

    except Exception as e:
        print(f"    [MAPS ERROR] {e}")
    return leads

# ============================================================
# VERIFICATION ENGINE
# ============================================================
def run_verification():
    print("\n[VERIFICATION] Fetching pending emails...")
    try:
        res = requests.get(B2B_URL, timeout=10)
        pending = res.json()
        print(f"[VERIFICATION] {len(pending)} emails to verify")
        for item in pending:
            email = item if isinstance(item, str) else item.get("email", "")
            if not email:
                continue
            is_valid = verify_email_smtp(email)
            if is_valid:
                update_sheet(email, "update")
                print(f"    [VALID] {email}")
            else:
                update_sheet(email, "delete")
                print(f"    [REMOVED] {email}")
            time.sleep(1)
    except Exception as e:
        print(f"[VERIFICATION ERROR] {e}")

# ============================================================
# MAIN AGENT
# ============================================================
async def run_agent():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        queries = DDG_QUERIES.copy()
        random.shuffle(queries)
        query_index = 0

        cycle = 0
        agent_start = time.time()

        while (time.time() - agent_start) < SEVEN_HOURS:
            cycle += 1
            elapsed = int((time.time() - agent_start) / 60)
            print(f"\n{'='*50}")
            print(f"[CYCLE {cycle}] Elapsed: {elapsed} mins")

            # DDG Round
            query = queries[query_index % len(queries)]
            query_index += 1
            print(f"[DDG] Query: {query}")
            ddg_leads = await scrape_ddg(page, query)
            for lead in ddg_leads:
                save_lead(lead["email"], lead["phone"],
                         lead["source"], lead["niche"], lead["location"])

            await page.wait_for_timeout(random.randint(2000, 4000))

            # Maps Round
            maps_query = query.replace('"', '').replace('contact us', '').replace('email us', '').strip()
            print(f"[MAPS] Query: {maps_query}")
            maps_leads = await scrape_google_maps(page, maps_query)
            for lead in maps_leads:
                save_lead(lead["email"], lead["phone"],
                         lead["source"], lead["niche"], lead["location"])

            wait = random.randint(30, 60)
            print(f"[WAIT] {wait}s")
            await page.wait_for_timeout(wait * 1000)

        await browser.close()

    # Verification Phase
    print("\n" + "="*50)
    print("[VERIFICATION PHASE] Running for 1 hour...")
    verify_start = time.time()
    while (time.time() - verify_start) < ONE_HOUR:
        run_verification()
        time.sleep(300)

    # Sleep
    print("\n[SLEEP] 10 minutes...")
    time.sleep(600)

    # Restart
    print("\n[RESTART] Restarting agent...")
    asyncio.run(run_agent())

if __name__ == "__main__":
    print("="*50)
    print("B2B LEAD AGENT ACTIVATED")
    print("="*50)
    asyncio.run(run_agent())
