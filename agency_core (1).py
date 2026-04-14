"""
agency_core.py
==============
Agency — FILE 2
Contains: Manager + Analyzer + Researcher
LangGraph powered — PostgreSQL checkpoints + NOTIFY event system

Manager    → PostgreSQL NOTIFY (event-driven, always listening)
Analyzer   → PostgreSQL NOTIFY (wakes when OOS saved by watcher)
Researcher → CRON (daily) + optional YouTube via Manager command
"""

import os
import json
import time
import select
import random
import imaplib
import email
import smtplib
import logging
import threading
import schedule
import subprocess
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from typing import TypedDict, Annotated, Optional, Literal
from concurrent.futures import ThreadPoolExecutor, as_completed
import operator

import psycopg2
import psycopg2.extensions
import psycopg2.pool
import requests
import redis
from github import Github

from openai import OpenAI, APIError, APITimeoutError, RateLimitError
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.postgres import PostgresSaver

from playwright.sync_api import sync_playwright
from playwright_stealth import stealth_sync

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s — %(message)s",
)
log = logging.getLogger("agency-core")

openai_client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])

# ============================================================
# CONFIG & SECRETS
# ============================================================

OPENAI_MODEL         = os.environ.get("OPENAI_MODEL_ID",
                        "ft:gpt-4o-mini-2024-07-18:personal:final-brain-1:DREfTesR")
DATABASE_URL         = os.environ["DATABASE_URL"]
WPPCONNECT_URL       = os.environ.get("WPPCONNECT_URL", "http://127.0.0.1:3000")
CEO_WHATSAPP         = os.environ["CEO_WHATSAPP_NUMBER"]          # e.g. 923001234567
GMAIL_USER           = os.environ["GMAIL_USER"]
GMAIL_APP_PASSWORD   = os.environ["GMAIL_APP_PASSWORD"]
GOOGLE_SHEET_URL     = os.environ["GOOGLE_SHEET_URL"]
GITHUB_TOKEN         = os.environ["GITHUB_TOKEN"]
GITHUB_REPO          = os.environ["GITHUB_REPO"]                  # "org/repo-name"

# Browserless + Tor (same stack as watchers_executors)
BROWSERLESS_URL      = os.environ.get("BROWSERLESS_URL", "http://localhost:3001")
TOR_PROXY            = os.environ.get("TOR_PROXY_URL", "socks5://127.0.0.1:9050")

# Redis — Researcher 20hr rolling cache only
REDIS_URL            = os.environ.get("REDIS_URL", "redis://localhost:6379")
RESEARCH_CACHE_TTL   = 20 * 60 * 60   # 20 hours in seconds

TWITTER_COOKIES_RAW  = os.environ.get("TWITTER_COOKIES", "[]")
REDDIT_COOKIES_RAW   = os.environ.get("REDDIT_COOKIES", "[]")

# Researcher ke alag accounts — watcher accounts alag rehte hain
RESEARCHER_TWITTER_COOKIES_RAW  = os.environ.get("RESEARCHER_TWITTER_COOKIES",
                                    os.environ.get("TWITTER_COOKIES", "[]"))
RESEARCHER_REDDIT_COOKIES_RAW   = os.environ.get("RESEARCHER_REDDIT_COOKIES",
                                    os.environ.get("REDDIT_COOKIES", "[]"))
RESEARCHER_LINKEDIN_COOKIES_RAW = os.environ.get("RESEARCHER_LINKEDIN_COOKIES", "[]")

RESEARCH_KEYWORDS = [
    "AI automation agency", "chatbot development trends",
    "lead generation AI", "no-code automation tools",
    "n8n make.com workflow", "AI agent market",
]


# ============================================================
# CONNECTION POOL  (shared with watchers_executors if same process,
#                   or independent pool here)
# ============================================================

_pool: psycopg2.pool.ThreadedConnectionPool | None = None
_pool_lock = threading.Lock()


def _get_pool() -> psycopg2.pool.ThreadedConnectionPool:
    global _pool
    if _pool is None:
        with _pool_lock:
            if _pool is None:
                _pool = psycopg2.pool.ThreadedConnectionPool(
                    minconn=2, maxconn=10, dsn=DATABASE_URL
                )
    return _pool


def get_conn():
    return _get_pool().getconn()


def put_conn(conn):
    try:
        _get_pool().putconn(conn)
    except Exception:
        pass


# ============================================================
# LANGGRAPH STATE SCHEMAS
# ============================================================

class ManagerState(TypedDict):
    event_type:    str          # "whatsapp_msg" | "agent_msg" | "new_lead" | "gmail" | "sheet_check"
    event_payload: dict
    actions_taken: Annotated[list, operator.add]
    errors:        Annotated[list, operator.add]
    done:          bool


class AnalyzerState(TypedDict):
    oos_id:       str
    oos_data:     dict
    decision:     str           # "approach" | "skip"
    reason:       str
    actions_taken: Annotated[list, operator.add]
    errors:        Annotated[list, operator.add]
    done:          bool


class ResearcherState(TypedDict):
    query:          str
    platform:       str         # "twitter" | "reddit" | "google" | "youtube"
    results:        list
    summary:        str
    youtube_enabled: bool
    errors:         Annotated[list, operator.add]
    done:           bool


# ============================================================
# DATABASE UTILITIES
# ============================================================

def db_fetch(query: str, params: tuple = ()) -> list:
    conn = get_conn()
    cur  = None
    try:
        cur = conn.cursor()
        cur.execute(query, params)
        return cur.fetchall()
    except Exception as exc:
        log.error(f"db_fetch error: {exc}")
        return []
    finally:
        if cur:
            cur.close()
        put_conn(conn)


def db_execute(query: str, params: tuple = ()):
    conn = get_conn()
    cur  = None
    try:
        cur = conn.cursor()
        cur.execute(query, params)
        conn.commit()
    except Exception as exc:
        conn.rollback()
        log.error(f"db_execute error: {exc}")
    finally:
        if cur:
            cur.close()
        put_conn(conn)


def db_notify(channel: str, payload: str = ""):
    conn = get_conn()
    cur  = None
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT pg_notify('{channel}', %s)", (payload,))
        conn.commit()
    except Exception as exc:
        conn.rollback()
        log.error(f"db_notify error: {exc}")
    finally:
        if cur:
            cur.close()
        put_conn(conn)


# ── Agent messages ─────────────────────────────────────────

def get_unread_agent_messages(to_agent: str) -> list[dict]:
    rows = db_fetch("""
        SELECT id, from_agent, message_type, payload, related_id, related_type
        FROM agent_messages
        WHERE to_agent=%s AND is_read=false
        ORDER BY created_at ASC
    """, (to_agent,))
    results = []
    for row in rows:
        results.append({
            "id": row[0], "from_agent": row[1],
            "message_type": row[2],
            "payload": row[3] if isinstance(row[3], dict) else json.loads(row[3] or "{}"),
            "related_id": row[4], "related_type": row[5],
        })
    return results


def mark_agent_message_read(msg_id: int):
    db_execute(
        "UPDATE agent_messages SET is_read=true WHERE id=%s", (msg_id,)
    )


def send_agent_message(from_agent, to_agent, message_type, payload,
                       related_id="", related_type="system"):
    db_execute("""
        INSERT INTO agent_messages
            (from_agent, to_agent, message_type, payload, related_id, related_type)
        VALUES (%s,%s,%s,%s,%s,%s)
    """, (from_agent, to_agent, message_type,
          json.dumps(payload), related_id, related_type))
    db_notify("agent_channel", to_agent)


# ── Leads & conversations ──────────────────────────────────

def get_lead(lead_id: str) -> dict | None:
    rows = db_fetch(
        "SELECT lead_id, name, platform, status, watcher_notes FROM leads WHERE lead_id=%s",
        (lead_id,)
    )
    if not rows:
        return None
    r = rows[0]
    return {"lead_id": r[0], "name": r[1], "platform": r[2],
            "status": r[3], "watcher_notes": r[4]}


def get_conversation(lead_id: str) -> list:
    rows = db_fetch(
        "SELECT messages FROM watcher_conversations WHERE lead_id=%s", (lead_id,)
    )
    if not rows:
        return []
    msgs = rows[0][0]
    return msgs if isinstance(msgs, list) else json.loads(msgs or "[]")


def update_lead_status(lead_id: str, status: str):
    db_execute(
        "UPDATE leads SET status=%s, last_updated=NOW() WHERE lead_id=%s",
        (status, lead_id)
    )


def get_new_leads() -> list[dict]:
    """Leads that are ready for Manager first WhatsApp contact."""
    rows = db_fetch("""
        SELECT l.lead_id, l.name, l.platform, l.watcher_notes,
               wc.messages
        FROM leads l
        LEFT JOIN watcher_conversations wc ON l.lead_id = wc.lead_id
        WHERE l.status = 'discovery_in_progress'
          AND l.whatsapp IS NOT NULL
          AND l.manager_contacted = false
        ORDER BY l.created_at ASC
        LIMIT 10
    """)
    results = []
    for r in rows:
        msgs = r[4] if isinstance(r[4], list) else json.loads(r[4] or "[]")
        results.append({
            "lead_id": r[0], "name": r[1], "platform": r[2],
            "watcher_notes": r[3], "conversation": msgs,
        })
    return results


def get_pending_oos() -> list[dict]:
    rows = db_fetch("""
        SELECT lead_id, platform, post_url, post_content, username, source_type
        FROM oos_temp
        WHERE status='pending_analyzer'
        ORDER BY created_at ASC
        LIMIT 20
    """)
    return [
        {"lead_id": r[0], "platform": r[1], "post_url": r[2],
         "post_content": r[3], "username": r[4], "source_type": r[5]}
        for r in rows
    ]


def update_oos_status(lead_id: str, status: str):
    db_execute(
        "UPDATE oos_temp SET status=%s WHERE lead_id=%s", (status, lead_id)
    )


def get_payment_sheet_data() -> list[dict]:
    """Read Google Sheet via CSV export (no API key needed)."""
    try:
        import csv, io
        sheet_id = GOOGLE_SHEET_URL.split("/d/")[1].split("/")[0]
        csv_url  = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv"
        resp     = requests.get(csv_url, timeout=15)
        resp.raise_for_status()
        reader  = csv.DictReader(io.StringIO(resp.text))
        results = []
        for row in reader:
            results.append({k.strip().lower().replace(" ", "_"): v.strip()
                            for k, v in row.items()})
        return results
    except Exception as exc:
        log.error(f"Google Sheet read error: {exc}")
        return []


def log_agent_error(agent_name, error_type, error_message):
    db_execute("""
        INSERT INTO agent_errors
            (agent_name, error_type, error_message, self_heal_attempts, status)
        VALUES (%s,%s,%s,0,'open')
    """, (agent_name, error_type, error_message))


def get_researcher_youtube_enabled() -> bool:
    """Check if Manager has enabled YouTube research via agent_messages."""
    rows = db_fetch("""
        SELECT payload FROM agent_messages
        WHERE to_agent='researcher'
          AND message_type='command'
        ORDER BY created_at DESC
        LIMIT 1
    """)
    if not rows:
        return False
    try:
        payload = rows[0][0]
        if isinstance(payload, str):
            payload = json.loads(payload)
        return payload.get("youtube_enabled", False)
    except Exception:
        return False


# ============================================================
# LLM UTILITIES  (with retry + backoff)
# ============================================================

def call_llm(messages: list, max_tokens: int = 500) -> str | None:
    for attempt in range(3):
        try:
            resp = openai_client.chat.completions.create(
                model=OPENAI_MODEL,
                messages=messages,
                temperature=0.7,
                max_tokens=max_tokens,
            )
            return resp.choices[0].message.content.strip()
        except (APITimeoutError, RateLimitError) as exc:
            wait = (2 ** attempt) + random.uniform(0, 1)
            log.warning(f"LLM retry {attempt+1}/3 in {wait:.1f}s: {exc}")
            time.sleep(wait)
        except APIError as exc:
            log.error(f"LLM API error: {exc}")
            return None
        except Exception as exc:
            log.error(f"LLM unexpected: {exc}")
            return None
    return None


# ============================================================
# WHATSAPP UTILITIES  (WPPConnect bridge)
# ============================================================

def wpp_send(number: str, message: str, typing: bool = True) -> bool:
    """Send WhatsApp message via local WPPConnect bridge."""
    endpoint = "/send-typing" if typing else "/send"
    try:
        resp = requests.post(
            f"{WPPCONNECT_URL}{endpoint}",
            json={"number": number, "message": message, "typing_sec": 2},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        if not data.get("ok"):
            log.warning(f"WPP send failed: {data}")
            return False
        return True
    except Exception as exc:
        log.error(f"WPP send error to {number}: {exc}")
        return False


def wpp_get_messages(number: str, limit: int = 20) -> list[dict]:
    """Get recent messages from a WhatsApp number."""
    try:
        resp = requests.get(
            f"{WPPCONNECT_URL}/messages/{number}",
            params={"limit": limit},
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json().get("messages", [])
    except Exception as exc:
        log.error(f"WPP get messages error: {exc}")
        return []


def wpp_status() -> bool:
    try:
        resp = requests.get(f"{WPPCONNECT_URL}/status", timeout=5)
        return resp.json().get("ok", False)
    except Exception:
        return False


# ============================================================
# GMAIL UTILITIES  (IMAP read + SMTP send)
# ============================================================

def gmail_get_unread(limit: int = 10) -> list[dict]:
    """Fetch unread emails from Gmail via IMAP."""
    results = []
    try:
        mail = imaplib.IMAP4_SSL("imap.gmail.com")
        mail.login(GMAIL_USER, GMAIL_APP_PASSWORD)
        mail.select("inbox")
        _, data = mail.search(None, "UNSEEN")
        ids = data[0].split()[-limit:]   # last N unread
        for eid in ids:
            _, msg_data = mail.fetch(eid, "(RFC822)")
            msg = email.message_from_bytes(msg_data[0][1])
            subject = msg.get("Subject", "")
            sender  = msg.get("From", "")
            body    = ""
            if msg.is_multipart():
                for part in msg.walk():
                    if part.get_content_type() == "text/plain":
                        body = part.get_payload(decode=True).decode("utf-8", errors="ignore")
                        break
            else:
                body = msg.get_payload(decode=True).decode("utf-8", errors="ignore")
            results.append({
                "id": eid.decode(), "subject": subject,
                "sender": sender, "body": body[:2000],
            })
        mail.logout()
    except Exception as exc:
        log.error(f"Gmail IMAP error: {exc}")
    return results


def gmail_send(to: str, subject: str, body: str) -> bool:
    """Send email via Gmail SMTP."""
    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = GMAIL_USER
        msg["To"]      = to
        msg.attach(MIMEText(body, "plain"))

        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(GMAIL_USER, GMAIL_APP_PASSWORD)
            server.sendmail(GMAIL_USER, to, msg.as_string())
        return True
    except Exception as exc:
        log.error(f"Gmail SMTP send error: {exc}")
        return False


def gmail_reply(to: str, subject: str, original_body: str,
                reply_text: str) -> bool:
    """LLM-generated reply to an email."""
    reply = call_llm([{
        "role": "user",
        "content": (
            f"Email from: {to}\nSubject: {subject}\n"
            f"Their message:\n{original_body}\n\n"
            "Generate a professional reply email. Plain text only."
        )
    }], max_tokens=400)
    if not reply:
        return False
    return gmail_send(to, f"Re: {subject}", reply)


# ============================================================
# GITHUB UTILITIES
# ============================================================

def github_push_file(repo_path: str, file_path: str,
                     content: str, commit_msg: str) -> bool:
    """Push/update a file in the GitHub repo."""
    try:
        gh   = Github(GITHUB_TOKEN)
        repo = gh.get_repo(GITHUB_REPO)
        try:
            existing = repo.get_contents(file_path)
            repo.update_file(
                file_path, commit_msg, content, existing.sha
            )
        except Exception:
            repo.create_file(file_path, commit_msg, content)
        log.info(f"[GitHub] Pushed: {file_path}")
        return True
    except Exception as exc:
        log.error(f"GitHub push error: {exc}")
        return False


def github_get_file(file_path: str) -> str | None:
    """Read a file from the GitHub repo."""
    try:
        gh      = Github(GITHUB_TOKEN)
        repo    = gh.get_repo(GITHUB_REPO)
        content = repo.get_contents(file_path)
        return content.decoded_content.decode("utf-8")
    except Exception as exc:
        log.error(f"GitHub read error: {exc}")
        return None


# ============================================================
# PLAYWRIGHT UTILITIES  (for Researcher)
# ============================================================

# ============================================================
# REDIS — Researcher cache (20hr TTL)
# ============================================================

_redis_client: redis.Redis | None = None
_redis_lock = threading.Lock()


def _get_redis() -> redis.Redis:
    global _redis_client
    if _redis_client is None:
        with _redis_lock:
            if _redis_client is None:
                _redis_client = redis.from_url(REDIS_URL, decode_responses=True)
    return _redis_client


def research_cache_set(platform: str, query: str, results: list):
    """Cache scrape results for 20 hours."""
    try:
        key = f"research:{platform}:{hash(query) & 0xFFFFFFFF}"
        _get_redis().setex(key, RESEARCH_CACHE_TTL, json.dumps(results))
        log.info(f"[Redis] Cached {platform}:{query[:30]} ({len(results)} items, TTL 20hr)")
    except Exception as exc:
        log.warning(f"[Redis] Cache set failed: {exc}")


def research_cache_get(platform: str, query: str) -> list | None:
    """Retrieve cached results if still within 20hr window."""
    try:
        key = f"research:{platform}:{hash(query) & 0xFFFFFFFF}"
        raw = _get_redis().get(key)
        if raw:
            log.info(f"[Redis] Cache HIT {platform}:{query[:30]}")
            return json.loads(raw)
        return None
    except Exception as exc:
        log.warning(f"[Redis] Cache get failed: {exc}")
        return None


def research_collect_all_cached(platform: str) -> list:
    """
    Collect ALL cached research results for a platform after 20hr window.
    Called at end of research cycle to build final summary.
    """
    try:
        r    = _get_redis()
        keys = r.keys(f"research:{platform}:*")
        all_results = []
        for key in keys:
            raw = r.get(key)
            if raw:
                try:
                    all_results.extend(json.loads(raw))
                except Exception:
                    pass
        log.info(f"[Redis] Collected {len(all_results)} cached items for {platform}")
        return all_results
    except Exception as exc:
        log.warning(f"[Redis] Collect failed: {exc}")
        return []


# ============================================================
# PLAYWRIGHT + BROWSERLESS + TOR + STEALTH  (Researcher)
# Same stack as watchers_executors — consistent across system
# ============================================================

def _new_research_context(playwright, cookies_raw: str = ""):
    """Connect to Browserless with Tor proxy + stealth."""
    browser = playwright.chromium.connect_over_cdp(
        f"{BROWSERLESS_URL}?stealth=true&--proxy-server={TOR_PROXY}"
    )
    context = browser.new_context(
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        viewport={"width": 1280, "height": 900},
        locale="en-US",
        timezone_id="America/New_York",
    )
    stealth_sync(context)
    if cookies_raw:
        try:
            cookies = json.loads(cookies_raw)
            if cookies:
                context.add_cookies(cookies)
        except Exception as exc:
            log.warning(f"Cookie load: {exc}")
    return browser, context


def playwright_scrape_twitter(query: str) -> list[dict]:
    """Scrape Twitter via Browserless + Tor + stealth. Results cached in Redis."""
    cached = research_cache_get("twitter", query)
    if cached is not None:
        return cached
    results = []
    try:
        with sync_playwright() as pw:
            browser, ctx = _new_research_context(pw, RESEARCHER_TWITTER_COOKIES_RAW)
            page = ctx.new_page()
            page.goto(
                f"https://twitter.com/search?q={query.replace(' ','%20')}&src=typed_query&f=live",
                timeout=20000,
            )
            page.wait_for_timeout(4000)
            for art in page.query_selector_all("article[data-testid='tweet']")[:10]:
                try:
                    text_el = art.query_selector("[data-testid='tweetText']")
                    user_el = art.query_selector("[data-testid='User-Name'] span")
                    results.append({
                        "platform": "twitter",
                        "text":   text_el.inner_text() if text_el else "",
                        "author": user_el.inner_text() if user_el else "unknown",
                        "query":  query,
                    })
                except Exception:
                    continue
            browser.close()
    except Exception as exc:
        log.error(f"[Researcher] Twitter scrape: {exc}")
    research_cache_set("twitter", query, results)
    return results


def playwright_scrape_reddit(query: str) -> list[dict]:
    """Scrape Reddit via Browserless + Tor + stealth. Results cached in Redis."""
    cached = research_cache_get("reddit", query)
    if cached is not None:
        return cached
    results = []
    try:
        with sync_playwright() as pw:
            browser, ctx = _new_research_context(pw, RESEARCHER_REDDIT_COOKIES_RAW)
            page = ctx.new_page()
            page.goto(
                f"https://www.reddit.com/search/?q={query.replace(' ','+')}&sort=new",
                timeout=20000,
            )
            page.wait_for_timeout(4000)
            for p in page.query_selector_all("div[data-testid='post-container']")[:10]:
                try:
                    title = p.query_selector("h3")
                    results.append({
                        "platform": "reddit",
                        "text":   title.inner_text() if title else "",
                        "author": "reddit_post",
                        "query":  query,
                    })
                except Exception:
                    continue
            browser.close()
    except Exception as exc:
        log.error(f"[Researcher] Reddit scrape: {exc}")
    research_cache_set("reddit", query, results)
    return results


def playwright_scrape_google(query: str) -> list[dict]:
    """Scrape Google via Browserless + Tor + stealth. Results cached in Redis."""
    cached = research_cache_get("google", query)
    if cached is not None:
        return cached
    results = []
    try:
        with sync_playwright() as pw:
            browser, ctx = _new_research_context(pw)
            page = ctx.new_page()
            page.goto(
                f"https://www.google.com/search?q={query.replace(' ','+')}&tbs=qdr:d",
                timeout=20000,
            )
            page.wait_for_timeout(3000)
            for item in page.query_selector_all("div.g")[:8]:
                try:
                    title   = item.query_selector("h3")
                    snippet = item.query_selector(".VwiC3b, .s3v9rd")
                    link_el = item.query_selector("a")
                    results.append({
                        "platform": "google",
                        "text": (
                            (title.inner_text() if title else "") + " — " +
                            (snippet.inner_text() if snippet else "")
                        ),
                        "url":   link_el.get_attribute("href") if link_el else "",
                        "query": query,
                    })
                except Exception:
                    continue
            browser.close()
    except Exception as exc:
        log.error(f"[Researcher] Google scrape: {exc}")
    research_cache_set("google", query, results)
    return results


def playwright_scrape_linkedin(query: str) -> list[dict]:
    """Scrape LinkedIn posts via Browserless + Tor + stealth. Results cached in Redis."""
    cached = research_cache_get("linkedin", query)
    if cached is not None:
        return cached
    results = []
    try:
        with sync_playwright() as pw:
            browser, ctx = _new_research_context(pw, RESEARCHER_LINKEDIN_COOKIES_RAW)
            page = ctx.new_page()
            search_url = (
                f"https://www.linkedin.com/search/results/content/"
                f"?keywords={query.replace(' ','%20')}&sortBy=date_posted"
            )
            page.goto(search_url, timeout=25000)
            page.wait_for_timeout(5000)
            page.evaluate("window.scrollBy(0, 1000)")
            page.wait_for_timeout(2000)
            for post in page.query_selector_all("div.feed-shared-update-v2")[:10]:
                try:
                    text_el   = post.query_selector("span.break-words")
                    author_el = post.query_selector("span.feed-shared-actor__name")
                    results.append({
                        "platform": "linkedin",
                        "text":   text_el.inner_text()[:500] if text_el else "",
                        "author": author_el.inner_text() if author_el else "unknown",
                        "query":  query,
                    })
                except Exception:
                    continue
            browser.close()
    except Exception as exc:
        log.error(f"[Researcher] LinkedIn scrape: {exc}")
    research_cache_set("linkedin", query, results)
    return results


def scrape_youtube_transcript(query: str) -> list[dict]:
    """
    Optional — only loads when Manager enables youtube_enabled.
    Uses yt-dlp for info + openai-whisper for transcription.
    Heavy: ~1GB RAM. Runs only when commanded.
    """
    results = []
    try:
        import yt_dlp
        import whisper

        ydl_opts = {
            "quiet": True,
            "extract_flat": True,
            "default_search": "ytsearch5",
        }
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(f"ytsearch5:{query}", download=False)
            entries = info.get("entries", [])[:3]

        whisper_model = whisper.load_model("small")   # ~250MB
        for entry in entries:
            url   = entry.get("url") or f"https://youtube.com/watch?v={entry.get('id','')}"
            title = entry.get("title", "")
            try:
                audio_opts = {
                    "format": "bestaudio/best",
                    "outtmpl": f"/tmp/yt_{entry.get('id','tmp')}.%(ext)s",
                    "quiet": True,
                    "postprocessors": [{
                        "key": "FFmpegExtractAudio",
                        "preferredcodec": "mp3",
                    }],
                }
                with yt_dlp.YoutubeDL(audio_opts) as ydl:
                    ydl.download([url])
                audio_file = f"/tmp/yt_{entry.get('id','tmp')}.mp3"
                transcript = whisper_model.transcribe(audio_file)
                results.append({
                    "platform": "youtube",
                    "title": title, "url": url,
                    "text": transcript["text"][:2000],
                    "query": query,
                })
                # Clean up
                import os as _os
                try:
                    _os.remove(audio_file)
                except Exception:
                    pass
            except Exception as exc:
                log.warning(f"YouTube transcript error for {url}: {exc}")
                results.append({
                    "platform": "youtube",
                    "title": title, "url": url,
                    "text": title,   # fallback: just title
                    "query": query,
                })
    except ImportError:
        log.error("YouTube deps not installed: pip install yt-dlp openai-whisper")
    except Exception as exc:
        log.error(f"YouTube scrape error: {exc}")
    return results


# ============================================================
# ══════════════════════════════════════════════════════════
#  MANAGER NODES
# ══════════════════════════════════════════════════════════
# ============================================================

def manager_listen_node(state: ManagerState) -> ManagerState:
    """
    Collects all pending events and sets event_type + event_payload.
    Priority: agent_messages > new_leads > whatsapp > gmail > sheet
    """
    log.info("[Manager] Checking events...")

    # 1. Agent messages (highest priority)
    msgs = get_unread_agent_messages("manager")
    if msgs:
        msg = msgs[0]
        mark_agent_message_read(msg["id"])
        return {
            **state,
            "event_type": "agent_msg",
            "event_payload": msg,
        }

    # 2. New leads ready for WhatsApp contact
    new_leads = get_new_leads()
    if new_leads:
        return {
            **state,
            "event_type": "new_lead",
            "event_payload": {"leads": new_leads},
        }

    # 3. WhatsApp replies from clients/CEO
    if wpp_status():
        wa_msgs = wpp_get_messages(CEO_WHATSAPP, limit=5)
        unread_wa = [m for m in wa_msgs if not m.get("fromMe")]
        if unread_wa:
            return {
                **state,
                "event_type": "whatsapp_msg",
                "event_payload": {"messages": unread_wa, "from": "ceo"},
            }

    # 4. Gmail unread
    emails = gmail_get_unread(limit=5)
    if emails:
        return {
            **state,
            "event_type": "gmail",
            "event_payload": {"emails": emails},
        }

    # 5. Payment sheet check
    return {
        **state,
        "event_type": "sheet_check",
        "event_payload": {},
    }


def _route_manager(state: ManagerState):
    t = state["event_type"]
    if t == "agent_msg":    return "handle_agent_msg"
    if t == "new_lead":     return "handle_new_lead"
    if t == "whatsapp_msg": return "handle_whatsapp"
    if t == "gmail":        return "handle_gmail"
    if t == "sheet_check":  return "handle_sheet"
    return END


def manager_handle_agent_msg_node(state: ManagerState) -> ManagerState:
    msg      = state["event_payload"]
    msg_type = msg.get("message_type", "")
    payload  = msg.get("payload", {})
    from_agent = msg.get("from_agent", "unknown")
    actions  = []

    log.info(f"[Manager] Agent message: {msg_type} from {from_agent}")

    # OOS decision request from watcher
    if msg_type == "oos_decision_request":
        lead_id   = payload.get("lead_id", "")
        post_text = payload.get("post", "")
        decision  = call_llm([{
            "role": "user",
            "content": (
                f"OOS post from watcher:\n{post_text}\n\n"
                "Should we approach this as a potential partner/future client? "
                "Reply: APPROACH or SKIP. Then one sentence reason."
            )
        }], max_tokens=80)
        if decision and "APPROACH" in decision.upper():
            update_oos_status(lead_id, "approach")
            send_agent_message("manager", from_agent, "oos_approved",
                               {"lead_id": lead_id}, lead_id)
            actions.append(f"OOS approved for approach: {lead_id}")
        else:
            update_oos_status(lead_id, "skipped")
            actions.append(f"OOS skipped: {lead_id}")

    # Executor report — project done
    elif msg_type == "executor_report":
        project_id  = payload.get("project_id", "")
        client_name = payload.get("client", "")
        status      = payload.get("status", "")
        deploy_note = payload.get("deploy_note", "")

        ceo_msg = call_llm([{
            "role": "user",
            "content": (
                f"Project complete.\nClient: {client_name}\n"
                f"Project ID: {project_id}\nStatus: {status}\n"
                f"Deploy note: {deploy_note}\n\n"
                "Write a brief WhatsApp update for the CEO."
            )
        }], max_tokens=200)
        if ceo_msg:
            wpp_send(CEO_WHATSAPP, ceo_msg)
            actions.append(f"CEO notified about project {project_id}")

    # Executor blocked — missing info
    elif msg_type == "missing_info":
        project_id = payload.get("project_id", "")
        detail     = payload.get("detail", "")
        ceo_alert  = (
            f"⚠️ Project {project_id} blocked.\n"
            f"Missing info:\n{detail[:300]}\n\n"
            "Please provide the missing credentials/details."
        )
        wpp_send(CEO_WHATSAPP, ceo_alert)
        actions.append(f"CEO alerted: missing info for {project_id}")

    # Error flag from any agent
    elif msg_type == "error_flag":
        detail = payload.get("detail", "")
        wpp_send(CEO_WHATSAPP, f"🚨 Agent error:\n{detail[:400]}")
        actions.append("CEO alerted: agent error")

    # Researcher daily summary
    elif msg_type == "research_summary":
        summary = payload.get("summary", "")
        wpp_send(CEO_WHATSAPP, f"📊 Daily Research Summary:\n\n{summary[:1000]}")
        actions.append("Research summary sent to CEO")

    # Researcher command (enable/disable youtube)
    elif msg_type == "researcher_command":
        send_agent_message("manager", "researcher", "command", payload)
        actions.append(f"Researcher command forwarded: {payload}")

    else:
        log.info(f"[Manager] Unhandled message type: {msg_type}")

    return {**state, "actions_taken": actions, "done": True}


def manager_handle_new_lead_node(state: ManagerState) -> ManagerState:
    leads   = state["event_payload"].get("leads", [])
    actions = []

    for lead in leads:
        lead_id  = lead["lead_id"]
        name     = lead["name"]
        platform = lead["platform"]
        notes    = lead["watcher_notes"] or ""
        conv     = lead["conversation"]

        # Generate first WhatsApp message via LLM
        conv_text = "\n".join(
            f"{m['role'].upper()}: {m['text']}" for m in conv[-5:]
        )
        first_msg = call_llm([{
            "role": "user",
            "content": (
                f"Lead name: {name}\nPlatform: {platform}\n"
                f"Context: {notes[:300]}\n"
                f"Recent conversation:\n{conv_text}\n\n"
                "Generate first WhatsApp outreach message. "
                "Professional, friendly, brief."
            )
        }], max_tokens=300)

        if first_msg:
            # Get lead WhatsApp from DB
            rows = db_fetch(
                "SELECT whatsapp FROM leads WHERE lead_id=%s", (lead_id,)
            )
            if rows and rows[0][0]:
                wa_number = rows[0][0]
                ok = wpp_send(wa_number, first_msg)
                if ok:
                    db_execute(
                        "UPDATE leads SET manager_contacted=true WHERE lead_id=%s",
                        (lead_id,)
                    )
                    update_lead_status(lead_id, "manager_in_contact")
                    actions.append(f"WhatsApp sent to {name} ({lead_id})")
                    # Notify CEO
                    wpp_send(
                        CEO_WHATSAPP,
                        f"📥 New lead contacted: {name} ({platform})\n"
                        f"Lead ID: {lead_id}"
                    )

    return {**state, "actions_taken": actions, "done": True}


def manager_handle_whatsapp_node(state: ManagerState) -> ManagerState:
    messages = state["event_payload"].get("messages", [])
    source   = state["event_payload"].get("from", "")
    actions  = []

    for msg in messages:
        body      = msg.get("body", "").strip()
        from_num  = msg.get("from", "").replace("@c.us", "")
        if not body:
            continue

        log.info(f"[Manager] WhatsApp from {from_num}: {body[:80]}")

        # CEO message — parse commands
        if source == "ceo" or from_num == CEO_WHATSAPP.replace("@c.us", ""):
            response = call_llm([{
                "role": "user",
                "content": (
                    f"CEO WhatsApp message: {body}\n\n"
                    "You are the agency manager. Parse this command and respond. "
                    "If it's a project approval, extract project details. "
                    "If it's a researcher command, note it. "
                    "Reply with your action taken."
                )
            }], max_tokens=300)
            if response:
                wpp_send(CEO_WHATSAPP, response)
                actions.append(f"CEO command handled: {body[:60]}")

                # Check if CEO approved a project
                if any(kw in body.lower() for kw in ["approve", "start", "go ahead"]):
                    rows = db_fetch("""
                        SELECT project_id, executor_type FROM projects
                        WHERE status='pending_ceo'
                        ORDER BY created_at DESC LIMIT 1
                    """)
                    if rows:
                        project_id, exec_type = rows[0]
                        db_execute(
                            "UPDATE projects SET status='approved' WHERE project_id=%s",
                            (project_id,)
                        )
                        db_notify("executor_channel",
                                  f"{exec_type}:{project_id}")
                        actions.append(f"Project {project_id} approved → executor notified")

        else:
            # Client message — find lead, generate reply
            rows = db_fetch(
                "SELECT lead_id, name FROM leads WHERE whatsapp=%s LIMIT 1",
                (from_num,)
            )
            if rows:
                lead_id, name = rows[0]
                conv = get_conversation(lead_id)
                conv_text = "\n".join(
                    f"{m['role'].upper()}: {m['text']}" for m in conv[-6:]
                )
                reply = call_llm([{
                    "role": "user",
                    "content": (
                        f"Client: {name}\n"
                        f"Conversation history:\n{conv_text}\n\n"
                        f"Client's new WhatsApp message: {body}\n\n"
                        "Generate manager reply. If client is ready to proceed, "
                        "ask for project details/brief."
                    )
                }], max_tokens=350)
                if reply:
                    wpp_send(from_num, reply)
                    actions.append(f"Client {name} replied via WhatsApp")

    return {**state, "actions_taken": actions, "done": True}


def manager_handle_gmail_node(state: ManagerState) -> ManagerState:
    emails  = state["event_payload"].get("emails", [])
    actions = []

    for em in emails:
        sender  = em["sender"]
        subject = em["subject"]
        body    = em["body"]

        log.info(f"[Manager] Gmail from {sender}: {subject}")

        replied = gmail_reply(sender, subject, body, "")
        if replied:
            actions.append(f"Gmail replied to {sender}: {subject[:50]}")
        # Also notify CEO for important emails
        if any(kw in subject.lower() for kw in
               ["payment", "invoice", "urgent", "contract"]):
            wpp_send(
                CEO_WHATSAPP,
                f"📧 Important email from {sender}:\n"
                f"Subject: {subject}\n{body[:300]}"
            )
            actions.append(f"CEO alerted about email: {subject}")

    return {**state, "actions_taken": actions, "done": True}


def manager_handle_sheet_node(state: ManagerState) -> ManagerState:
    """Check payment sheet and alert CEO on overdue/pending payments."""
    actions = []
    try:
        rows = get_payment_sheet_data()
        overdue = [
            r for r in rows
            if r.get("status", "").lower() in ("overdue", "pending", "unpaid")
        ]
        if overdue:
            summary = "\n".join(
                f"• {r.get('client_name','?')} — {r.get('amount','?')} "
                f"({r.get('status','?')})"
                for r in overdue[:5]
            )
            wpp_send(
                CEO_WHATSAPP,
                f"💰 Payment Alert:\n\n{summary}"
            )
            actions.append(f"CEO alerted: {len(overdue)} pending payments")
        else:
            log.info("[Manager] Sheet check — no overdue payments")
    except Exception as exc:
        log.error(f"[Manager] Sheet check error: {exc}")
        actions.append(f"Sheet check error: {exc}")

    return {**state, "actions_taken": actions, "done": True}


# ============================================================
# ══════════════════════════════════════════════════════════
#  ANALYZER NODES
# ══════════════════════════════════════════════════════════
# ============================================================

def analyzer_fetch_node(state: AnalyzerState) -> AnalyzerState:
    """
    FIX — if oos_id already set via NOTIFY payload, fetch that specific item.
    Otherwise scan DB for next pending (fallback path).
    """
    if state.get("oos_id"):
        # Direct lookup — oos_id came from NOTIFY payload
        rows = db_fetch("""
            SELECT lead_id, platform, post_url, post_content, username, source_type
            FROM oos_temp
            WHERE lead_id=%s AND status='pending_analyzer'
        """, (state["oos_id"],))
        if not rows:
            log.info(f"[Analyzer] OOS {state['oos_id']} not found or already processed")
            return {**state, "done": True}
        r = rows[0]
        item = {"lead_id": r[0], "platform": r[1], "post_url": r[2],
                "post_content": r[3], "username": r[4], "source_type": r[5]}
    else:
        pending = get_pending_oos()
        if not pending:
            log.info("[Analyzer] No pending OOS items")
            return {**state, "done": True}
        item = pending[0]

    log.info(f"[Analyzer] Processing OOS: {item['lead_id']}")
    return {
        **state,
        "oos_id": item["lead_id"],
        "oos_data": item,
    }


def _route_after_fetch(state: AnalyzerState) -> Literal["analyze", END]:
    return END if state.get("done") else "analyze"


def analyzer_analyze_node(state: AnalyzerState) -> AnalyzerState:
    """LLM analyzes OOS post and decides: approach or skip."""
    data     = state["oos_data"]
    post     = data.get("post_content", "")
    platform = data.get("platform", "")
    username = data.get("username", "")

    result = call_llm([{
        "role": "user",
        "content": (
            f"Platform: {platform}\nUser: {username}\n"
            f"Post:\n{post}\n\n"
            "This post was flagged as OOS (Out of Scope) — "
            "not directly needing our AI/automation services, "
            "but potentially interesting.\n\n"
            "Decide:\n"
            "APPROACH — worth contacting (partner, future client, collaboration)\n"
            "SKIP — not relevant at all\n\n"
            "Format:\nDECISION: APPROACH or SKIP\n"
            "REASON: one sentence"
        )
    }], max_tokens=100)

    decision = "SKIP"
    reason   = "LLM unavailable"
    if result:
        lines = result.strip().split("\n")
        for line in lines:
            if line.startswith("DECISION:"):
                decision = "APPROACH" if "APPROACH" in line.upper() else "SKIP"
            if line.startswith("REASON:"):
                reason = line.replace("REASON:", "").strip()

    return {**state, "decision": decision, "reason": reason}


def analyzer_act_node(state: AnalyzerState) -> AnalyzerState:
    """Based on decision — update DB and notify Manager."""
    oos_id   = state["oos_id"]
    decision = state["decision"]
    reason   = state["reason"]
    actions  = []

    if decision == "APPROACH":
        update_oos_status(oos_id, "approach")
        send_agent_message(
            "analyzer", "manager", "oos_decision_request",
            {"lead_id": oos_id, "decision": "APPROACH", "reason": reason},
            oos_id,
        )
        actions.append(f"OOS {oos_id} → APPROACH, Manager notified")
    else:
        update_oos_status(oos_id, "skipped")
        actions.append(f"OOS {oos_id} → SKIP")

    log.info(f"[Analyzer] {oos_id}: {decision} — {reason}")
    return {**state, "actions_taken": actions, "done": True}


# ============================================================
# ══════════════════════════════════════════════════════════
#  RESEARCHER NODES
# ══════════════════════════════════════════════════════════
# ============================================================

def researcher_scrape_node(state: ResearcherState) -> ResearcherState:
    """
    Scrape platform for this query — results stored in Redis (20hr TTL).
    scrape functions handle cache check internally:
      cache HIT  → return cached, no re-scrape
      cache MISS → scrape → store in Redis → return
    """
    query    = state["query"]
    platform = state["platform"]
    results  = []

    log.info(f"[Researcher] Scraping {platform}: {query}")

    if platform == "twitter":
        results = playwright_scrape_twitter(query)
    elif platform == "reddit":
        results = playwright_scrape_reddit(query)
    elif platform == "google":
        results = playwright_scrape_google(query)
    elif platform == "linkedin":
        results = playwright_scrape_linkedin(query)
    elif platform == "youtube" and state.get("youtube_enabled"):
        results = scrape_youtube_transcript(query)

    return {**state, "results": results}


def researcher_summarize_node(state: ResearcherState) -> ResearcherState:
    """
    After individual query scrape — just return done.
    Real summary is built in researcher_final_summary_node
    after ALL queries across ALL platforms are cached.
    """
    results = state["results"]
    query   = state["query"]
    if not results:
        log.info(f"[Researcher] No results for {query} — cached as empty")
    return {**state, "summary": "", "done": True}


def researcher_report_node(state: ResearcherState) -> ResearcherState:
    """Per-query node — nothing to save here, final summary done separately."""
    log.info(f"[Researcher] Query complete: {state['query']} ({state['platform']})")
    return {**state, "done": True}


def researcher_build_final_summary():
    """
    Called ONCE after all per-query graphs complete (end of daily cycle).
    Collects ALL cached results from Redis across all platforms,
    builds one LLM summary, saves to PostgreSQL, notifies Manager → CEO.
    """
    log.info("━━━━ Researcher: Building Final Summary from Redis cache ━━━━")
    platforms = ["twitter", "reddit", "google", "youtube"]
    all_results = []
    for platform in platforms:
        items = research_collect_all_cached(platform)
        all_results.extend(items)

    if not all_results:
        log.warning("[Researcher] No cached results found for final summary")
        return

    combined = "\n\n".join(
        f"[{r.get('platform','?')}] {r.get('text','')[:400]}"
        for r in all_results[:40]   # up to 40 items across all platforms
    )

    summary = call_llm([{
        "role": "user",
        "content": (
            f"20-hour market research data from Twitter, Reddit, Google, YouTube.\n\n"
            f"Research queries covered: {', '.join(RESEARCH_KEYWORDS)}\n\n"
            f"Scraped content:\n{combined}\n\n"
            "Write a comprehensive market intelligence report (10-15 bullet points). "
            "Cover: trends, client pain points, competitor moves, opportunities, "
            "recommended outreach angles for our AI automation agency."
        )
    }], max_tokens=1000)

    if not summary:
        log.error("[Researcher] Final summary LLM call failed")
        return

    # Save to PostgreSQL via agent_messages → Manager picks up
    send_agent_message(
        "researcher", "manager", "research_summary",
        {
            "summary":  summary,
            "platform": "all",
            "queries":  RESEARCH_KEYWORDS,
            "items":    len(all_results),
        },
    )
    log.info(f"[Researcher] Final summary saved ({len(all_results)} items → DB)")


# ============================================================
# LANGGRAPH GRAPH BUILDERS  (single checkpointer)
# ============================================================

_CHECKPOINTER = PostgresSaver.from_conn_string(DATABASE_URL)
try:
    _CHECKPOINTER.setup()   # Creates checkpoint tables on first run (idempotent)
except Exception as _ce:
    log.warning(f"[Checkpointer] setup() skipped (may already exist): {_ce}")


def build_manager_graph():
    g = StateGraph(ManagerState)
    g.add_node("listen",            manager_listen_node)
    g.add_node("handle_agent_msg",  manager_handle_agent_msg_node)
    g.add_node("handle_new_lead",   manager_handle_new_lead_node)
    g.add_node("handle_whatsapp",   manager_handle_whatsapp_node)
    g.add_node("handle_gmail",      manager_handle_gmail_node)
    g.add_node("handle_sheet",      manager_handle_sheet_node)

    g.set_entry_point("listen")
    g.add_conditional_edges("listen", _route_manager, {
        "handle_agent_msg":  "handle_agent_msg",
        "handle_new_lead":   "handle_new_lead",
        "handle_whatsapp":   "handle_whatsapp",
        "handle_gmail":      "handle_gmail",
        "handle_sheet":      "handle_sheet",
        END: END,
    })
    for node in ["handle_agent_msg", "handle_new_lead",
                 "handle_whatsapp", "handle_gmail", "handle_sheet"]:
        g.add_edge(node, END)

    return g.compile(checkpointer=_CHECKPOINTER)


def build_analyzer_graph():
    g = StateGraph(AnalyzerState)
    g.add_node("fetch",   analyzer_fetch_node)
    g.add_node("analyze", analyzer_analyze_node)
    g.add_node("act",     analyzer_act_node)

    g.set_entry_point("fetch")
    g.add_conditional_edges("fetch", _route_after_fetch,
                            {"analyze": "analyze", END: END})
    g.add_edge("analyze", "act")
    g.add_edge("act", END)
    return g.compile(checkpointer=_CHECKPOINTER)


def build_researcher_graph():
    g = StateGraph(ResearcherState)
    g.add_node("scrape",    researcher_scrape_node)
    g.add_node("summarize", researcher_summarize_node)
    g.add_node("report",    researcher_report_node)

    g.set_entry_point("scrape")
    g.add_edge("scrape",    "summarize")
    g.add_edge("summarize", "report")
    g.add_edge("report",    END)
    return g.compile(checkpointer=_CHECKPOINTER)


MANAGER_GRAPH    = build_manager_graph()
ANALYZER_GRAPH   = build_analyzer_graph()
RESEARCHER_GRAPH = build_researcher_graph()


# ============================================================
# RUN HELPERS
# ============================================================

_CORE_POOL = ThreadPoolExecutor(max_workers=4, thread_name_prefix="core")


def run_manager_cycle():
    """
    FIX — Manager uses a STABLE thread_id ('manager_main') so LangGraph
    checkpointing is meaningful and state carries across cycles.
    Each invocation is a fresh graph run but with checkpoint continuity.
    """
    config    = {"configurable": {"thread_id": "manager_main"}}
    initial: ManagerState = {
        "event_type": "", "event_payload": {},
        "actions_taken": [], "errors": [], "done": False,
    }
    try:
        MANAGER_GRAPH.invoke(initial, config)
    except Exception as exc:
        log.error(f"[Manager] Cycle error: {exc}")
        log_agent_error("manager", "crash", str(exc))


def run_analyzer(oos_id: str = ""):
    """
    FIX — oos_id passed via NOTIFY payload so checkpoint resume works.
    If oos_id provided, thread_id is stable (same crash-resume key).
    If empty fallback, analyzer fetches next pending from DB itself.
    """
    thread_id = f"analyzer_{oos_id}" if oos_id else f"analyzer_scan_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
    config    = {"configurable": {"thread_id": thread_id}}
    initial: AnalyzerState = {
        "oos_id": oos_id, "oos_data": {},
        "decision": "", "reason": "",
        "actions_taken": [], "errors": [], "done": False,
    }
    try:
        ANALYZER_GRAPH.invoke(initial, config)
    except Exception as exc:
        log.error(f"[Analyzer] Run error: {exc}")
        log_agent_error("analyzer", "crash", str(exc))


def run_researcher(query: str, platform: str, youtube_enabled: bool = False):
    thread_id = (f"researcher_{platform}_"
                 f"{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}")
    config    = {"configurable": {"thread_id": thread_id}}
    initial: ResearcherState = {
        "query": query, "platform": platform,
        "results": [], "summary": "",
        "youtube_enabled": youtube_enabled,
        "errors": [], "done": False,
    }
    try:
        RESEARCHER_GRAPH.invoke(initial, config)
    except Exception as exc:
        log.error(f"[Researcher] Run error: {exc}")
        log_agent_error("researcher", "crash", str(exc))


def researcher_daily_cycle():
    """
    Run research across all platforms for all keywords.
    Each query scrapes and stores results in Redis (20hr TTL).
    After ALL queries complete -> one final LLM summary -> DB -> Manager -> CEO.
    """
    log.info("━━━━ Researcher Daily Cycle Start ━━━━")
    youtube_on = get_researcher_youtube_enabled()
    platforms  = ["twitter", "reddit", "google", "linkedin"]
    if youtube_on:
        platforms.append("youtube")
        log.info("[Researcher] YouTube enabled by Manager")

    futures = []
    for kw in RESEARCH_KEYWORDS:
        for platform in platforms:
            futures.append(
                _CORE_POOL.submit(run_researcher, kw, platform, youtube_on)
            )
            time.sleep(random.uniform(5, 15))  # polite delay between scrapes

    # Wait for all scrapes — collect errors
    for f in as_completed(futures):
        exc = f.exception()
        if exc:
            log.error(f"[Researcher] Future error: {exc}")

    log.info("━━━━ Researcher: All scrapes done — building final summary ━━━━")
    # Collect everything from Redis -> one LLM summary -> PostgreSQL -> Manager -> CEO
    researcher_build_final_summary()
    log.info("━━━━ Researcher Daily Cycle Done ━━━━")


# ============================================================
# POSTGRESQL NOTIFY LISTENER  (Manager + Analyzer)
# select.select() based — true event-driven, auto-reconnect
# ============================================================

def _make_notify_conn():
    conn = psycopg2.connect(DATABASE_URL)
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    with conn.cursor() as cur:
        cur.execute("LISTEN agent_channel")
        cur.execute("LISTEN oos_channel")
    return conn


def core_notify_listener():
    """
    Listens on:
      agent_channel  → payload 'manager'  → run_manager_cycle()
      oos_channel    → any payload        → run_analyzer()
    Watcher notifies 'agent_channel' with 'manager' when it saves OOS.
    Watcher also notifies 'oos_channel' directly after save_oos_temp().
    """
    log.info("[Core] NOTIFY listener started")
    backoff = 5
    conn    = None

    while True:
        try:
            if conn is None:
                conn    = _make_notify_conn()
                backoff = 5
                log.info("[Core] NOTIFY listener connected")

            r, _, _ = select.select([conn], [], [], 60)
            if not r:
                try:
                    with conn.cursor() as _ka:
                        _ka.execute("SELECT 1")   # keepalive ping
                except Exception:
                    raise
                continue

            conn.poll()
            while conn.notifies:
                notify  = conn.notifies.pop(0)
                channel = notify.channel
                payload = notify.payload.strip()

                log.info(f"[Core] NOTIFY on '{channel}': '{payload}'")

                if channel == "agent_channel" and payload == "manager":
                    _CORE_POOL.submit(run_manager_cycle)

                elif channel == "oos_channel":
                    # FIX — pass oos_id from payload so analyzer resumes correctly
                    _CORE_POOL.submit(run_analyzer, payload)  # payload = lead_id

        except Exception as exc:
            log.error(f"[Core] NOTIFY error: {exc} — reconnecting in {backoff}s")
            try:
                if conn:
                    conn.close()
            except Exception:
                pass
            conn = None
            time.sleep(backoff)
            backoff = min(backoff * 2, 120)


# ============================================================
# STARTUP MESSAGE  (CEO WhatsApp — system online)
# ============================================================

def send_startup_message():
    """
    ADDED — On server start, Manager sends CEO a WhatsApp message immediately.
    Waits up to 30s for WPP bridge to be ready, then sends once.
    """
    log.info("[Manager] Waiting for WPP bridge before startup message...")
    for _ in range(6):          # try for up to 30 seconds
        if wpp_status():
            break
        time.sleep(5)

    if wpp_status():
        startup_text = (
            "✅ *Agency System Online*\n\n"
            "Manager, Analyzer & Researcher are active.\n"
            "Watchers are running — I'll notify you on new leads, "
            "project updates, and any issues.\n\n"
            "Reply anytime to give commands."
        )
        wpp_send(CEO_WHATSAPP, startup_text, typing=False)
        log.info("[Manager] ✅ Startup message sent to CEO")
    else:
        log.warning("[Manager] WPP bridge not ready after 30s — startup message skipped")


# ============================================================
# MAIN
# ============================================================

def _validate_env():
    """Fail fast on startup if critical env vars are missing."""
    required = [
        "OPENAI_API_KEY", "DATABASE_URL", "CEO_WHATSAPP_NUMBER",
        "GMAIL_USER", "GMAIL_APP_PASSWORD", "GOOGLE_SHEET_URL",
        "GITHUB_TOKEN", "GITHUB_REPO",
    ]
    missing = [k for k in required if not os.environ.get(k)]
    if missing:
        raise EnvironmentError(
            f"[Startup] Missing required environment variables: {', '.join(missing)}"
        )
    log.info("[Startup] All required environment variables present ✓")


def main():
    log.info("══════════════════════════════════════")
    log.info("  Agency Core — Manager + Analyzer + Researcher")
    log.info("══════════════════════════════════════")

    _validate_env()

    # Startup WhatsApp message to CEO
    threading.Thread(target=send_startup_message, daemon=True).start()

    # NOTIFY listener — Manager + Analyzer wake up on events
    notify_thread = threading.Thread(
        target=core_notify_listener,
        daemon=True,
        name="core-notify-listener",
    )
    notify_thread.start()

    # Manager also polls every 60s as fallback
    # (catches WhatsApp replies, Gmail — not triggered by NOTIFY)
    schedule.every(60).seconds.do(
        lambda: _CORE_POOL.submit(run_manager_cycle)
    )

    # Researcher — daily at 8 AM
    schedule.every().day.at("08:00").do(
        lambda: _CORE_POOL.submit(researcher_daily_cycle)
    )

    # Run researcher once on startup
    _CORE_POOL.submit(researcher_daily_cycle)

    # Keep main thread alive
    while True:
        schedule.run_pending()
        time.sleep(30)


if __name__ == "__main__":
    main()
