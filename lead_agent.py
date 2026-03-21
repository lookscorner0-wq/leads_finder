import os
import time
import requests
import re
import random

from kaggle_secrets import UserSecretsClient
secrets = UserSecretsClient()

try:
    B2B_URL = secrets.get_secret("B2B_SCRIPT_URL")
    B2C_URL = secrets.get_secret("B2C_SCRIPT_URL")
    SENDER_EMAIL = secrets.get_secret("SENDER_EMAIL")
except Exception as e:
    print(f"⚠️ Secret Error: {e}")
    B2B_URL = None
    B2C_URL = None
    SENDER_EMAIL = "lookscorner080@gmail.com"

# --- TIME LIMITS ---
START_TIME = time.time()
SEVEN_HOURS = 7 * 3600 

# --- MASSIVE NICHE BANK ---
B2B_NICHES = ["HVAC", "Solar Energy", "Plumbers", "Roofer", "Logistics", "Trucking", "Law Firms", "Dentists", "Gym Owners", "Interior Designers", "Car Detailing", "SaaS Startups", "E-com Stores", "Digital Marketing", "SEO Agencies", "Software Houses", "Recruitment Firms"]
B2C_NICHES = ["Weight Loss", "Keto Diet", "Yoga Enthusiasts", "Skincare Lovers", "Luxury Fashion", "Home Decor", "Parenting Tips", "Pet Training", "Study Abroad IELTS", "Personal Finance", "Real Estate Investors", "Career Coaching", "Gaming eSports"]
LOCATIONS = ["London", "New York", "Dubai", "Toronto", "Sydney", "Karachi", "Texas", "Florida"]
PLATFORMS = ["site:facebook.com", "site:instagram.com", "site:urlebird.com", "site:reddit.com", "site:picuki.com"]

def generate_batch_queries(count=5):
    """Generates 5 random search queries from the massive bank"""
    queries = []
    email_providers = ["@gmail.com", "@yahoo.com", "@hotmail.com"]
    for _ in range(count):
        mode = random.choice(["B2B", "B2C"])
        domain = random.choice(email_providers)
        if mode == "B2B":
            n = random.choice(B2B_NICHES)
            l = random.choice(LOCATIONS)
            queries.append({"query": f'"{n}" {l} "contact us" "{domain}"', "mode": "B2B"})
        else:
            n = random.choice(B2C_NICHES)
            p = random.choice(PLATFORMS)
            queries.append({"query": f'{p} "{n}" "{domain}"', "mode": "B2C"})
    return queries

def extract_leads(text):
    """Finds emails and phones using regex"""
    emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-z]{2,}', text)
    phones = re.findall(r'\+?\d{10,13}', text)
    return list(set(emails)), list(set(phones))

def hunter(query, category):
    # Select the right URL based on category
    target_url = B2B_URL if category == "B2B" else B2C_URL
    
    print(f"🔎 Searching {category}: {query}")
    try:
        search_url = f"https://www.bing.com/search?q={query}"
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        res = requests.get(search_url, headers=headers, timeout=15)
        
        emails, phones = extract_leads(res.text)
        
        for email in emails:
            # Data to send to Google Script
            payload = {
                "action": "add",
                "location": "Global/Web",
                "platform": "Search/Social",
                "email": email,
                "phone": phones[0] if phones else "N/A",
                "title": query[:40]
            }
            
            # Send to the SELECTED Sheet (B2B or B2C)
            try:
                response = requests.post(target_url, json=payload, timeout=10)
                if "Added" in response.text:
                    print(f"✅ Saved to {category} Sheet: {email}")
                elif "Duplicate" in response.text:
                    print(f"⏭️ Duplicate in {category}: {email}")
            except:
                print(f"⚠️ Error connecting to {category} Sheet")
            
            time.sleep(2) 

    except Exception as e:
        print(f"⚠️ Search Error: {e}")
            

    except Exception as e:
        print(f"⚠️ Error: {e}")

if __name__ == "__main__":
    print("🚀 Lead Agent Activated (7-Hour Mode)")
    
    while (time.time() - START_TIME) < SEVEN_HOURS:
        batch = generate_batch_queries(5)
        for q in batch:
            hunter(q["query"], q["mode"])
            # Slow & Smooth gap between queries
            wait = random.randint(45, 90)
            print(f"😴 Waiting {wait}s...")
            time.sleep(wait)

    print("🏁 7 Hours Complete. Verification Mode Triggered...")
    # Add your SMTP verification function call here
    
    print("😴 Hibernating for 10 mins...")
    time.sleep(600)
