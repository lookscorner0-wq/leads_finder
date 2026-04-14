"""
agency_core.py
==============
Agency: NoError
Contains: Manager + Analyzer + Researcher
LangGraph powered — PostgreSQL checkpoints + NOTIFY event system

FIXES APPLIED:
  1. Manager sirf NOTIFY pe wake up — schedule polling removed
  2. Gmail optional — CEO "gmail on"/"gmail off" se control
  3. OpenAI sirf zarurat pe — rule-based filter pehle
  4. Startup message DB mein store — restart pe dobara nahi aata
  5. Schema: task_path + manager_contacted columns added
  6. Browserless + Tor removed — replaced with requests + BeautifulSoup
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
from bs4 import BeautifulSoup
import redis
from github import Github

from openai import OpenAI, APIError, APITimeoutError, RateLimitError
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.postgres import PostgresSaver

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s — %(message)s",
)
log = logging.getLogger("noerror-core")

openai_client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])

# ============================================================
# CONFIG & SECRETS
# ============================================================

AGENCY_NAME          = "NoError"
OPENAI_MODEL         = "ft:gpt-4o-mini-2024-07-18:personal:final-brain-1:DREfTesR"
DATABASE_URL         = os.environ["DATABASE_URL"]

EVOLUTION_URL        = os.environ.get("EVOLUTION_URL", "")
EVOLUTION_API_KEY    = os.environ.get("EVOLUTION_API_KEY", "")
EVOLUTION_INSTANCE   = os.environ.get("EVOLUTION_INSTANCE", "whatsapp")

CEO_WHATSAPP         = os.environ["CEO_WHATSAPP_NUMBER"]
GMAIL_USER           = os.environ["GMAIL_USER"]
GMAIL_APP_PASSWORD   = os.environ["GMAIL_APP_PASSWORD"]
GOOGLE_SHEET_URL     = os.environ["GOOGLE_SHEET_URL"]
GITHUB_TOKEN         = os.environ["GITHUB_TOKEN"]
GITHUB_REPO          = os.environ["GITHUB_REPO"]

REDIS_URL            = os.environ.get("REDIS_URL", "redis://localhost:6379")
RESEARCH_CACHE_TTL   = 20 * 60 * 60

RESEARCH_KEYWORDS = [
    "recent launches of ai", "agentic orchestration",
    "lead generation AI", "best ai frameworks",
    "ai recent updates", "solutions for devlopers",
]

# ── Gmail relevant senders/subjects for rule-based filter ───
GMAIL_IMPORTANT_SUBJECTS = ["payment", "invoice", "urgent", "contract", "project", "client"]
GMAIL_IMPORTANT_SENDERS  = []  # add client emails here e.g. ["client@example.com"]

# ── Human-like request headers ───────────────────────────────
_SCRAPE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

def _human_delay():
    """Random human-like delay between requests."""
    time.sleep(random.uniform(2.5, 6.0))

# ============================================================
# CONNECTION POOL
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
# FIX 5 — SCHEMA: task_path + manager_contacted added
# ============================================================

SCHEMA_SQL = """
-- LangGraph checkpoint tables
CREATE TABLE IF NOT EXISTS checkpoints (
    thread_id            TEXT        NOT NULL,
    checkpoint_ns        TEXT        NOT NULL DEFAULT '',
    checkpoint_id        TEXT        NOT NULL,
    parent_checkpoint_id TEXT,
    type                 TEXT,
    checkpoint           JSONB,
    metadata             JSONB,
    PRIMARY KEY (thread_id, checkpoint_ns, checkpoint_id)
);

CREATE TABLE IF NOT EXISTS checkpoint_blobs (
    thread_id     TEXT   NOT NULL,
    checkpoint_ns TEXT   NOT NULL DEFAULT '',
    channel       TEXT   NOT NULL,
    version       TEXT   NOT NULL,
    type          TEXT   NOT NULL,
    blob          BYTEA,
    PRIMARY KEY (thread_id, checkpoint_ns, channel, version)
);

CREATE TABLE IF NOT EXISTS checkpoint_migrations (
    v INTEGER PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS checkpoint_writes (
    thread_id     TEXT   NOT NULL,
    checkpoint_ns TEXT   NOT NULL DEFAULT '',
    checkpoint_id TEXT   NOT NULL,
    task_id       TEXT   NOT NULL,
    idx           SERIAL NOT NULL,
    channel       TEXT   NOT NULL,
    type          TEXT,
    blob          BYTEA  NOT NULL,
    task_path     TEXT,
    PRIMARY KEY (thread_id, checkpoint_ns, checkpoint_id, task_id, idx)
);

-- Agency tables
CREATE TABLE IF NOT EXISTS leads (
    lead_id           TEXT PRIMARY KEY,
    name              TEXT,
    platform          TEXT,
    status            TEXT DEFAULT 'new',
    whatsapp          TEXT,
    watcher_notes     TEXT,
    manager_contacted BOOLEAN DEFAULT FALSE,
    last_updated      TIMESTAMPTZ DEFAULT NOW(),
    created_at        TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS watcher_conversations (
    lead_id   TEXT PRIMARY KEY,
    messages  JSONB DEFAULT '[]'
);

CREATE TABLE IF NOT EXISTS oos_temp (
    lead_id      TEXT PRIMARY KEY,
    platform     TEXT,
    post_url     TEXT,
    post_content TEXT,
    username     TEXT,
    source_type  TEXT,
    status       TEXT DEFAULT 'pending_analyzer',
    created_at   TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS agent_messages (
    id            SERIAL PRIMARY KEY,
    from_agent    TEXT,
    to_agent      TEXT,
    message_type  TEXT,
    payload       JSONB,
    related_id    TEXT,
    related_type  TEXT,
    is_read       BOOLEAN DEFAULT FALSE,
    created_at    TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS agent_errors (
    id                  SERIAL PRIMARY KEY,
    agent_name          TEXT,
    error_type          TEXT,
    error_message       TEXT,
    self_heal_attempts  INT DEFAULT 0,
    status              TEXT DEFAULT 'open',
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS projects (
    project_id    TEXT PRIMARY KEY,
    executor_type TEXT,
    status        TEXT DEFAULT 'pending_ceo',
    client        TEXT,
    deploy_note   TEXT,
    created_at    TIMESTAMPTZ DEFAULT NOW()
);

-- FIX 4: Startup message flag table
CREATE TABLE IF NOT EXISTS system_flags (
    key        TEXT PRIMARY KEY,
    value      TEXT,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- FIX 2: Gmail toggle flag table
CREATE TABLE IF NOT EXISTS agent_settings (
    key        TEXT PRIMARY KEY,
    value      TEXT,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Migration: add task_path if old table exists without it
ALTER TABLE checkpoint_writes ADD COLUMN IF NOT EXISTS task_path TEXT;

-- Migration: add manager_contacted if old table exists without it
ALTER TABLE leads ADD COLUMN IF NOT EXISTS manager_contacted BOOLEAN DEFAULT FALSE;
"""


def setup_database():
    log.info(f"[{AGENCY_NAME}] Setting up database tables...")
    conn = None
    try:
        conn = psycopg2.connect(DATABASE_URL)
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(SCHEMA_SQL)
        log.info(f"[{AGENCY_NAME}] Database setup complete ✓")
    except Exception as exc:
        log.error(f"[{AGENCY_NAME}] Database setup error: {exc}")
        raise
    finally:
        if conn:
            conn.close()


# ============================================================
# LANGGRAPH STATE SCHEMAS
# ============================================================

class ManagerState(TypedDict):
    event_type:    str
    event_payload: dict
    actions_taken: Annotated[list, operator.add]
    errors:        Annotated[list, operator.add]
    done:          bool


class AnalyzerState(TypedDict):
    oos_id:        str
    oos_data:      dict
    decision:      str
    reason:        str
    actions_taken: Annotated[list, operator.add]
    errors:        Annotated[list, operator.add]
    done:          bool


class ResearcherState(TypedDict):
    query:           str
    platform:        str
    results:         list
    summary:         str
    youtube_enabled: bool
    errors:          Annotated[list, operator.add]
    done:            bool


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


# ── FIX 2: Gmail on/off from DB ─────────────────────────────

def get_gmail_enabled() -> bool:
    rows = db_fetch("SELECT value FROM agent_settings WHERE key='gmail_enabled'")
    if not rows:
        return False
    return rows[0][0].lower() == "true"


def set_gmail_enabled(enabled: bool):
    db_execute("""
        INSERT INTO agent_settings (key, value, updated_at)
        VALUES ('gmail_enabled', %s, NOW())
        ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value, updated_at=NOW()
    """, ("true" if enabled else "false",))


# ── FIX 4: Startup message DB flag ──────────────────────────

def get_startup_sent() -> bool:
    rows = db_fetch("SELECT value FROM system_flags WHERE key='startup_msg_sent'")
    if not rows:
        return False
    return rows[0][0].lower() == "true"


def set_startup_sent():
    db_execute("""
        INSERT INTO system_flags (key, value, updated_at)
        VALUES ('startup_msg_sent', 'true', NOW())
        ON CONFLICT (key) DO UPDATE SET value='true', updated_at=NOW()
    """)


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
    db_execute("UPDATE agent_messages SET is_read=true WHERE id=%s", (msg_id,))


def send_agent_message(from_agent, to_agent, message_type, payload,
                       related_id="", related_type="system"):
    db_execute("""
        INSERT INTO agent_messages
            (from_agent, to_agent, message_type, payload, related_id, related_type)
        VALUES (%s,%s,%s,%s,%s,%s)
    """, (from_agent, to_agent, message_type,
          json.dumps(payload), related_id, related_type))
    db_notify("agent_channel", to_agent)


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
    rows = db_fetch("""
        SELECT l.lead_id, l.name, l.platform, l.watcher_notes, wc.messages
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
    db_execute("UPDATE oos_temp SET status=%s WHERE lead_id=%s", (status, lead_id))


def get_payment_sheet_data() -> list[dict]:
    try:
        import csv, io
        sheet_id = GOOGLE_SHEET_URL.split("/d/")[1].split("/")[0]
        csv_url  = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv"
        resp     = requests.get(csv_url, timeout=15)
        resp.raise_for_status()
        reader  = csv.DictReader(io.StringIO(resp.text))
        return [{k.strip().lower().replace(" ", "_"): v.strip()
                 for k, v in row.items()} for row in reader]
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
    rows = db_fetch("""
        SELECT payload FROM agent_messages
        WHERE to_agent='researcher' AND message_type='command'
        ORDER BY created_at DESC LIMIT 1
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
# LLM UTILITIES
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
# EVOLUTION API — WhatsApp Utilities
# ============================================================

def _evo_headers() -> dict:
    return {
        "Content-Type": "application/json",
        "apikey": EVOLUTION_API_KEY,
    }


def wpp_send(number: str, message: str, typing: bool = True) -> bool:
    clean_number = number.replace("+", "").replace("-", "").replace(" ", "")
    if not clean_number.endswith("@s.whatsapp.net"):
        jid = f"{clean_number}@s.whatsapp.net"
    else:
        jid = clean_number

    url = f"{EVOLUTION_URL}/message/sendText/{EVOLUTION_INSTANCE}"
    payload = {"number": jid, "text": message}
    try:
        resp = requests.post(url, json=payload, headers=_evo_headers(), timeout=30)
        resp.raise_for_status()
        return True
    except Exception as exc:
        log.error(f"[Evolution] Send error to {number}: {exc}")
        return False


def wpp_get_messages(number: str, limit: int = 20) -> list[dict]:
    clean_number = number.replace("+", "").replace("-", "").replace(" ", "")
    jid = f"{clean_number}@s.whatsapp.net"
    url = f"{EVOLUTION_URL}/chat/findMessages/{EVOLUTION_INSTANCE}"
    payload = {"where": {"key": {"remoteJid": jid}}, "limit": limit}
    try:
        resp = requests.post(url, json=payload, headers=_evo_headers(), timeout=15)
        resp.raise_for_status()
        data = resp.json()
        messages = []
        records  = data if isinstance(data, list) else data.get("messages", {}).get("records", [])
        for m in records:
            msg_content = m.get("message", {})
            text = (
                msg_content.get("conversation") or
                msg_content.get("extendedTextMessage", {}).get("text") or ""
            )
            messages.append({
                "body":   text,
                "from":   m.get("key", {}).get("remoteJid", ""),
                "fromMe": m.get("key", {}).get("fromMe", False),
                "id":     m.get("key", {}).get("id", ""),
            })
        return messages
    except Exception as exc:
        log.error(f"[Evolution] Get messages error: {exc}")
        return []


def wpp_status() -> bool:
    try:
        url  = f"{EVOLUTION_URL}/instance/connectionState/{EVOLUTION_INSTANCE}"
        resp = requests.get(url, headers=_evo_headers(), timeout=8)
        data = resp.json()
        state = data.get("instance", {}).get("state", "") or data.get("state", "")
        return state == "open"
    except Exception:
        return False


# ============================================================
# FIX 2 — GMAIL: rule-based filter + optional
# ============================================================

def gmail_get_unread(limit: int = 10) -> list[dict]:
    results = []
    try:
        mail = imaplib.IMAP4_SSL("imap.gmail.com")
        mail.login(GMAIL_USER, GMAIL_APP_PASSWORD)
        mail.select("inbox")
        _, data = mail.search(None, "UNSEEN")
        ids = data[0].split()[-limit:]
        for eid in ids:
            _, msg_data = mail.fetch(eid, "(RFC822)")
            msg     = email.message_from_bytes(msg_data[0][1])
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


def _is_important_email(em: dict) -> bool:
    subject = em.get("subject", "").lower()
    sender  = em.get("sender", "").lower()
    if any(kw in subject for kw in GMAIL_IMPORTANT_SUBJECTS):
        return True
    if GMAIL_IMPORTANT_SENDERS:
        if any(s in sender for s in GMAIL_IMPORTANT_SENDERS):
            return True
    return False


def gmail_send(to: str, subject: str, body: str) -> bool:
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


def gmail_reply(to: str, subject: str, original_body: str) -> bool:
    reply = call_llm([{
        "role": "user",
        "content": (
            f"Email from: {to}\nSubject: {subject}\n"
            f"Their message:\n{original_body}\n\n"
            f"You are Manager of {AGENCY_NAME} AI automation agency. "
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
    try:
        gh   = Github(GITHUB_TOKEN)
        repo = gh.get_repo(GITHUB_REPO)
        try:
            existing = repo.get_contents(file_path)
            repo.update_file(file_path, commit_msg, content, existing.sha)
        except Exception:
            repo.create_file(file_path, commit_msg, content)
        log.info(f"[GitHub] Pushed: {file_path}")
        return True
    except Exception as exc:
        log.error(f"GitHub push error: {exc}")
        return False


def github_get_file(file_path: str) -> str | None:
    try:
        gh      = Github(GITHUB_TOKEN)
        repo    = gh.get_repo(GITHUB_REPO)
        content = repo.get_contents(file_path)
        return content.decoded_content.decode("utf-8")
    except Exception as exc:
        log.error(f"GitHub read error: {exc}")
        return None


# ============================================================
# REDIS — Researcher cache
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
    try:
        key = f"research:{platform}:{hash(query) & 0xFFFFFFFF}"
        _get_redis().setex(key, RESEARCH_CACHE_TTL, json.dumps(results))
    except Exception as exc:
        log.warning(f"[Redis] Cache set failed: {exc}")


def research_cache_get(platform: str, query: str) -> list | None:
    try:
        key = f"research:{platform}:{hash(query) & 0xFFFFFFFF}"
        raw = _get_redis().get(key)
        return json.loads(raw) if raw else None
    except Exception as exc:
        log.warning(f"[Redis] Cache get failed: {exc}")
        return None


def research_collect_all_cached(platform: str) -> list:
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
        return all_results
    except Exception as exc:
        log.warning(f"[Redis] Collect failed: {exc}")
        return []


# ============================================================
# FIX 6 — SCRAPING: requests + BeautifulSoup (Browserless/Tor removed)
# ============================================================

def _get_session() -> requests.Session:
    """Fresh session with human-like headers."""
    session = requests.Session()
    session.headers.update(_SCRAPE_HEADERS)
    return session


def playwright_scrape_twitter(query: str) -> list[dict]:
    cached = research_cache_get("twitter", query)
    if cached is not None:
        return cached
    results = []
    try:
        session = _get_session()
        _human_delay()
        url  = f"https://nitter.net/search?q={requests.utils.quote(query)}&f=tweets"
        resp = session.get(url, timeout=15)
        soup = BeautifulSoup(resp.text, "html.parser")
        for item in soup.select(".timeline-item")[:10]:
            try:
                text_el   = item.select_one(".tweet-content")
                author_el = item.select_one(".username")
                if text_el:
                    results.append({
                        "platform": "twitter",
                        "text":   text_el.get_text(strip=True),
                        "author": author_el.get_text(strip=True) if author_el else "unknown",
                        "query":  query,
                    })
            except Exception:
                continue
            _human_delay()
    except Exception as exc:
        log.error(f"[Researcher] Twitter scrape: {exc}")
    research_cache_set("twitter", query, results)
    return results


def playwright_scrape_reddit(query: str) -> list[dict]:
    cached = research_cache_get("reddit", query)
    if cached is not None:
        return cached
    results = []
    try:
        session = _get_session()
        _human_delay()
        url  = f"https://www.reddit.com/search.json?q={requests.utils.quote(query)}&sort=new&limit=10"
        resp = session.get(url, timeout=15)
        data = resp.json()
        posts = data.get("data", {}).get("children", [])
        for post in posts[:10]:
            d = post.get("data", {})
            results.append({
                "platform": "reddit",
                "text":   d.get("title", ""),
                "author": d.get("author", "reddit_post"),
                "query":  query,
            })
            _human_delay()
    except Exception as exc:
        log.error(f"[Researcher] Reddit scrape: {exc}")
    research_cache_set("reddit", query, results)
    return results


def playwright_scrape_google(query: str) -> list[dict]:
    cached = research_cache_get("google", query)
    if cached is not None:
        return cached
    results = []
    try:
        session = _get_session()
        _human_delay()
        url  = f"https://www.google.com/search?q={requests.utils.quote(query)}&tbs=qdr:d"
        resp = session.get(url, timeout=15)
        soup = BeautifulSoup(resp.text, "html.parser")
        for item in soup.select("div.g")[:8]:
            try:
                title_el   = item.select_one("h3")
                snippet_el = item.select_one(".VwiC3b, .s3v9rd")
                link_el    = item.select_one("a")
                results.append({
                    "platform": "google",
                    "text": (
                        (title_el.get_text(strip=True) if title_el else "") + " — " +
                        (snippet_el.get_text(strip=True) if snippet_el else "")
                    ),
                    "url":   link_el.get("href", "") if link_el else "",
                    "query": query,
                })
            except Exception:
                continue
            _human_delay()
    except Exception as exc:
        log.error(f"[Researcher] Google scrape: {exc}")
    research_cache_set("google", query, results)
    return results


def playwright_scrape_linkedin(query: str) -> list[dict]:
    """LinkedIn public search via requests — no login required for basic results."""
    cached = research_cache_get("linkedin", query)
    if cached is not None:
        return cached
    results = []
    try:
        session = _get_session()
        _human_delay()
        url  = f"https://www.linkedin.com/search/results/content/?keywords={requests.utils.quote(query)}&sortBy=date_posted"
        resp = session.get(url, timeout=20)
        soup = BeautifulSoup(resp.text, "html.parser")
        for item in soup.select("div.search-results__list li")[:10]:
            try:
                text_el = item.select_one("p, span.break-words")
                results.append({
                    "platform": "linkedin",
                    "text":   text_el.get_text(strip=True)[:500] if text_el else "",
                    "author": "linkedin_post",
                    "query":  query,
                })
            except Exception:
                continue
            _human_delay()
    except Exception as exc:
        log.error(f"[Researcher] LinkedIn scrape: {exc}")
    research_cache_set("linkedin", query, results)
    return results


def scrape_youtube_transcript(query: str) -> list[dict]:
    results = []
    try:
        import yt_dlp, whisper
        ydl_opts = {"quiet": True, "extract_flat": True, "default_search": "ytsearch5"}
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info    = ydl.extract_info(f"ytsearch5:{query}", download=False)
            entries = info.get("entries", [])[:3]
        whisper_model = whisper.load_model("small")
        for entry in entries:
            url   = entry.get("url") or f"https://youtube.com/watch?v={entry.get('id','')}"
            title = entry.get("title", "")
            try:
                audio_opts = {
                    "format": "bestaudio/best",
                    "outtmpl": f"/tmp/yt_{entry.get('id','tmp')}.%(ext)s",
                    "quiet": True,
                    "postprocessors": [{"key": "FFmpegExtractAudio", "preferredcodec": "mp3"}],
                }
                with yt_dlp.YoutubeDL(audio_opts) as ydl:
                    ydl.download([url])
                audio_file = f"/tmp/yt_{entry.get('id','tmp')}.mp3"
                transcript = whisper_model.transcribe(audio_file)
                results.append({
                    "platform": "youtube", "title": title, "url": url,
                    "text": transcript["text"][:2000], "query": query,
                })
                try:
                    os.remove(audio_file)
                except Exception:
                    pass
            except Exception as exc:
                log.warning(f"YouTube transcript error for {url}: {exc}")
                results.append({
                    "platform": "youtube", "title": title, "url": url,
                    "text": title, "query": query,
                })
    except ImportError:
        log.error("YouTube deps not installed: pip install yt-dlp openai-whisper")
    except Exception as exc:
        log.error(f"YouTube scrape error: {exc}")
    return results


# ============================================================
# MANAGER NODES
# ============================================================

def manager_listen_node(state: ManagerState) -> ManagerState:
    log.info(f"[{AGENCY_NAME} Manager] Checking events...")

    msgs = get_unread_agent_messages("manager")
    if msgs:
        msg = msgs[0]
        mark_agent_message_read(msg["id"])
        return {**state, "event_type": "agent_msg", "event_payload": msg}

    new_leads = get_new_leads()
    if new_leads:
        return {**state, "event_type": "new_lead", "event_payload": {"leads": new_leads}}

    if wpp_status():
        wa_msgs = wpp_get_messages(CEO_WHATSAPP, limit=5)
        unread  = [m for m in wa_msgs if not m.get("fromMe")]
        if unread:
            return {**state, "event_type": "whatsapp_msg",
                    "event_payload": {"messages": unread, "from": "ceo"}}

    # FIX 2: Gmail sirf tab check ho jab enabled ho
    if get_gmail_enabled():
        emails = gmail_get_unread(limit=5)
        important = [e for e in emails if _is_important_email(e)]
        if important:
            return {**state, "event_type": "gmail",
                    "event_payload": {"emails": important}}

    return {**state, "event_type": "sheet_check", "event_payload": {}}


def _route_manager(state: ManagerState):
    t = state["event_type"]
    if t == "agent_msg":    return "handle_agent_msg"
    if t == "new_lead":     return "handle_new_lead"
    if t == "whatsapp_msg": return "handle_whatsapp"
    if t == "gmail":        return "handle_gmail"
    if t == "sheet_check":  return "handle_sheet"
    return END


def manager_handle_agent_msg_node(state: ManagerState) -> ManagerState:
    msg        = state["event_payload"]
    msg_type   = msg.get("message_type", "")
    payload    = msg.get("payload", {})
    from_agent = msg.get("from_agent", "unknown")
    actions    = []

    log.info(f"[{AGENCY_NAME} Manager] Agent message: {msg_type} from {from_agent}")

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
            actions.append(f"OOS approved: {lead_id}")
        else:
            update_oos_status(lead_id, "skipped")
            actions.append(f"OOS skipped: {lead_id}")

    elif msg_type == "executor_report":
        project_id  = payload.get("project_id", "")
        client_name = payload.get("client", "")
        status      = payload.get("status", "")
        deploy_note = payload.get("deploy_note", "")
        ceo_msg = call_llm([{
            "role": "user",
            "content": (
                f"You are manager of {AGENCY_NAME} AI automation agency.\n"
                f"Project complete.\nClient: {client_name}\n"
                f"Project ID: {project_id}\nStatus: {status}\n"
                f"Deploy note: {deploy_note}\n\n"
                "Write a brief WhatsApp update for the CEO."
            )
        }], max_tokens=200)
        if ceo_msg:
            wpp_send(CEO_WHATSAPP, ceo_msg)
            actions.append(f"CEO notified about project {project_id}")

    elif msg_type == "missing_info":
        project_id = payload.get("project_id", "")
        detail     = payload.get("detail", "")
        wpp_send(CEO_WHATSAPP,
                 f"⚠️ [{AGENCY_NAME}] Project {project_id} blocked.\n"
                 f"Missing info:\n{detail[:300]}\n\nPlease provide the missing details.")
        actions.append(f"CEO alerted: missing info for {project_id}")

    elif msg_type == "error_flag":
        detail = payload.get("detail", "")
        wpp_send(CEO_WHATSAPP, f"🚨 [{AGENCY_NAME}] Agent error:\n{detail[:400]}")
        actions.append("CEO alerted: agent error")

    elif msg_type == "researcher_command":
            cmd = payload.get("command", "")
            if cmd == "ondemand":
                platforms = payload.get("platforms", ["linkedin"])
                _CORE_POOL.submit(researcher_ondemand_cycle, platforms)
                actions.append(f"On-demand research started: {platforms}")
            else:
                send_agent_message("manager", "researcher", "command", payload)
                actions.append(f"Researcher command forwarded: {payload}")

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

        conv_text = "\n".join(
            f"{m['role'].upper()}: {m['text']}" for m in conv[-5:]
        )
        first_msg = call_llm([{
            "role": "user",
            "content": (
                f"You are the Manager of {AGENCY_NAME}, an AI automation agency.\n"
                f"Lead name: {name}\nPlatform: {platform}\n"
                f"Context: {notes[:300]}\n"
                f"Recent conversation:\n{conv_text}\n\n"
                "Generate first WhatsApp outreach message. "
                "Professional, friendly, brief. Mention NoError naturally."
            )
        }], max_tokens=300)

        if first_msg:
            rows = db_fetch("SELECT whatsapp FROM leads WHERE lead_id=%s", (lead_id,))
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
                    wpp_send(CEO_WHATSAPP,
                             f"📥 [{AGENCY_NAME}] New lead contacted: {name} ({platform})\n"
                             f"Lead ID: {lead_id}")

    return {**state, "actions_taken": actions, "done": True}


def manager_handle_whatsapp_node(state: ManagerState) -> ManagerState:
    messages = state["event_payload"].get("messages", [])
    source   = state["event_payload"].get("from", "")
    actions  = []

    for msg in messages:
        body     = msg.get("body", "").strip()
        from_num = msg.get("from", "").replace("@s.whatsapp.net", "").replace("@c.us", "")
        if not body:
            continue

        log.info(f"[{AGENCY_NAME} Manager] WhatsApp from {from_num}: {body[:80]}")

        if source == "ceo" or from_num == CEO_WHATSAPP.replace("+", ""):

            body_lower = body.lower().strip()
            if body_lower in ("gmail on", "gmail enable"):
                set_gmail_enabled(True)
                wpp_send(CEO_WHATSAPP, "✅ Gmail monitoring enabled.")
                actions.append("Gmail enabled by CEO")
                continue
            elif body_lower in ("gmail off", "gmail disable"):
                set_gmail_enabled(False)
                wpp_send(CEO_WHATSAPP, "🔕 Gmail monitoring disabled.")
                actions.append("Gmail disabled by CEO")
                continue

            response = call_llm([{
                "role": "user",
                "content": (
                    f"You are the Manager of {AGENCY_NAME} AI automation agency.\n"
                    f"CEO WhatsApp message: {body}\n\n"
                    "Parse this command and respond with action taken. "
                    "If project approval, extract project details."
                )
            }], max_tokens=300)
            if response:
                wpp_send(CEO_WHATSAPP, response)
                actions.append(f"CEO command handled: {body[:60]}")

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
                        db_notify("executor_channel", f"{exec_type}:{project_id}")
                        actions.append(f"Project {project_id} approved")
        else:
            rows = db_fetch(
                "SELECT lead_id, name FROM leads WHERE whatsapp=%s LIMIT 1",
                (from_num,)
            )
            if rows:
                lead_id, name = rows[0]
                conv      = get_conversation(lead_id)
                conv_text = "\n".join(
                    f"{m['role'].upper()}: {m['text']}" for m in conv[-6:]
                )
                reply = call_llm([{
                    "role": "user",
                    "content": (
                        f"You are Manager of {AGENCY_NAME} AI automation agency.\n"
                        f"Client: {name}\n"
                        f"Conversation history:\n{conv_text}\n\n"
                        f"Client's new WhatsApp message: {body}\n\n"
                        "Generate manager reply."
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
        log.info(f"[{AGENCY_NAME} Manager] Gmail from {sender}: {subject}")
        replied = gmail_reply(sender, subject, body)
        if replied:
            actions.append(f"Gmail replied to {sender}: {subject[:50]}")
        if any(kw in subject.lower() for kw in ["payment", "invoice", "urgent", "contract"]):
            wpp_send(CEO_WHATSAPP,
                     f"📧 [{AGENCY_NAME}] Important email from {sender}:\n"
                     f"Subject: {subject}\n{body[:300]}")
            actions.append(f"CEO alerted about email: {subject}")
    return {**state, "actions_taken": actions, "done": True}


def manager_handle_sheet_node(state: ManagerState) -> ManagerState:
    actions = []
    try:
        rows    = get_payment_sheet_data()
        overdue = [r for r in rows
                   if r.get("status", "").lower() in ("overdue", "pending", "unpaid")]
        if overdue:
            summary = "\n".join(
                f"• {r.get('client_name','?')} — {r.get('amount','?')} ({r.get('status','?')})"
                for r in overdue[:5]
            )
            wpp_send(CEO_WHATSAPP, f"💰 [{AGENCY_NAME}] Payment Alert:\n\n{summary}")
            actions.append(f"CEO alerted: {len(overdue)} pending payments")
    except Exception as exc:
        log.error(f"[{AGENCY_NAME} Manager] Sheet check error: {exc}")
    return {**state, "actions_taken": actions, "done": True}


# ============================================================
# ANALYZER NODES
# ============================================================

def analyzer_fetch_node(state: AnalyzerState) -> AnalyzerState:
    if state.get("oos_id"):
        rows = db_fetch("""
            SELECT lead_id, platform, post_url, post_content, username, source_type
            FROM oos_temp
            WHERE lead_id=%s AND status='pending_analyzer'
        """, (state["oos_id"],))
        if not rows:
            return {**state, "done": True}
        r    = rows[0]
        item = {"lead_id": r[0], "platform": r[1], "post_url": r[2],
                "post_content": r[3], "username": r[4], "source_type": r[5]}
    else:
        pending = get_pending_oos()
        if not pending:
            return {**state, "done": True}
        item = pending[0]

    return {**state, "oos_id": item["lead_id"], "oos_data": item}


def _route_after_fetch(state: AnalyzerState) -> Literal["analyze", "__end__"]:
    return "__end__" if state.get("done") else "analyze"


def analyzer_analyze_node(state: AnalyzerState) -> AnalyzerState:
    data     = state["oos_data"]
    post     = data.get("post_content", "")
    platform = data.get("platform", "")
    username = data.get("username", "")

    result = call_llm([{
        "role": "user",
        "content": (
            f"Agency: {AGENCY_NAME}\nPlatform: {platform}\n"
            f"User: {username}\nPost:\n{post}\n\n"
            "This post was flagged as OOS. Decide:\n"
            "APPROACH — worth contacting\nSKIP — not relevant\n\n"
            "Format:\nDECISION: APPROACH or SKIP\nREASON: one sentence"
        )
    }], max_tokens=100)

    decision = "SKIP"
    reason   = "LLM unavailable"
    if result:
        for line in result.strip().split("\n"):
            if line.startswith("DECISION:"):
                decision = "APPROACH" if "APPROACH" in line.upper() else "SKIP"
            if line.startswith("REASON:"):
                reason = line.replace("REASON:", "").strip()

    return {**state, "decision": decision, "reason": reason}


def analyzer_act_node(state: AnalyzerState) -> AnalyzerState:
    oos_id   = state["oos_id"]
    decision = state["decision"]
    reason   = state["reason"]
    actions  = []

    if decision == "APPROACH":
        update_oos_status(oos_id, "approach")
        send_agent_message("analyzer", "manager", "oos_decision_request",
                           {"lead_id": oos_id, "decision": "APPROACH", "reason": reason},
                           oos_id)
        actions.append(f"OOS {oos_id} → APPROACH")
    else:
        update_oos_status(oos_id, "skipped")
        actions.append(f"OOS {oos_id} → SKIP")

    return {**state, "actions_taken": actions, "done": True}


# ============================================================
# RESEARCHER NODES
# ============================================================

def researcher_scrape_node(state: ResearcherState) -> ResearcherState:
    query    = state["query"]
    platform = state["platform"]
    results  = []

    log.info(f"[{AGENCY_NAME} Researcher] Scraping {platform}: {query}")

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
    return {**state, "summary": "", "done": True}


def researcher_report_node(state: ResearcherState) -> ResearcherState:
    log.info(f"[{AGENCY_NAME} Researcher] Query done: {state['query']} ({state['platform']})")
    return {**state, "done": True}


def researcher_build_final_summary():
    log.info(f"[{AGENCY_NAME} Researcher] Building final summary...")
    platforms   = ["twitter", "reddit", "google", "youtube"]
    all_results = []
    for platform in platforms:
        all_results.extend(research_collect_all_cached(platform))

    if not all_results:
        log.warning(f"[{AGENCY_NAME} Researcher] No cached results found")
        return

    combined = "\n\n".join(
        f"[{r.get('platform','?')}] {r.get('text','')[:400]}"
        for r in all_results[:40]
    )
    summary = call_llm([{
        "role": "user",
        "content": (
            f"You are the Researcher agent for {AGENCY_NAME} AI automation agency.\n"
            f"20-hour market research data.\n"
            f"Research queries: {', '.join(RESEARCH_KEYWORDS)}\n\n"
            f"Scraped content:\n{combined}\n\n"
            "Write a comprehensive market intelligence report (10-15 bullet points). "
            "Cover: trends, client pain points, competitor moves, opportunities, "
            "recommended outreach angles."
        )
    }], max_tokens=1000)

    if not summary:
        return

    send_agent_message("researcher", "manager", "research_summary",
                       {"summary": summary, "platform": "all",
                        "queries": RESEARCH_KEYWORDS, "items": len(all_results)})
    log.info(f"[{AGENCY_NAME} Researcher] Summary sent to Manager")


# ============================================================
# LANGGRAPH GRAPH BUILDERS
# ============================================================

from psycopg_pool import ConnectionPool

_conn_pool   = ConnectionPool(DATABASE_URL, max_size=5, open=False)
_conn_pool.open(wait=True)
_CHECKPOINTER = PostgresSaver(_conn_pool)
try:
    _CHECKPOINTER.setup()
except Exception as _ce:
    log.warning(f"[Checkpointer] setup() skipped (tables exist): {_ce}")


def build_manager_graph():
    g = StateGraph(ManagerState)
    g.add_node("listen",           manager_listen_node)
    g.add_node("handle_agent_msg", manager_handle_agent_msg_node)
    g.add_node("handle_new_lead",  manager_handle_new_lead_node)
    g.add_node("handle_whatsapp",  manager_handle_whatsapp_node)
    g.add_node("handle_gmail",     manager_handle_gmail_node)
    g.add_node("handle_sheet",     manager_handle_sheet_node)

    g.set_entry_point("listen")
    g.add_conditional_edges("listen", _route_manager, {
        "handle_agent_msg": "handle_agent_msg",
        "handle_new_lead":  "handle_new_lead",
        "handle_whatsapp":  "handle_whatsapp",
        "handle_gmail":     "handle_gmail",
        "handle_sheet":     "handle_sheet",
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
    config   = {"configurable": {"thread_id": "manager_main"}}
    initial: ManagerState = {
        "event_type": "", "event_payload": {},
        "actions_taken": [], "errors": [], "done": False,
    }
    try:
        MANAGER_GRAPH.invoke(initial, config)
    except Exception as exc:
        log.error(f"[{AGENCY_NAME} Manager] Cycle error: {exc}")
        log_agent_error("manager", "crash", str(exc))


def run_analyzer(oos_id: str = ""):
    thread_id = (f"analyzer_{oos_id}" if oos_id
                 else f"analyzer_scan_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}")
    config    = {"configurable": {"thread_id": thread_id}}
    initial: AnalyzerState = {
        "oos_id": oos_id, "oos_data": {},
        "decision": "", "reason": "",
        "actions_taken": [], "errors": [], "done": False,
    }
    try:
        ANALYZER_GRAPH.invoke(initial, config)
    except Exception as exc:
        log.error(f"[{AGENCY_NAME} Analyzer] Run error: {exc}")
        log_agent_error("analyzer", "crash", str(exc))


def run_researcher(query: str, platform: str, youtube_enabled: bool = False):
    thread_id = f"researcher_{platform}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
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
        log.error(f"[{AGENCY_NAME} Researcher] Run error: {exc}")
        log_agent_error("researcher", "crash", str(exc))

def researcher_ondemand_cycle(platforms: list[str] = None, keywords: list[str] = None):
    """CEO command se trigger hoga — LinkedIn, YouTube, ya custom platforms."""
    youtube_on = get_researcher_youtube_enabled()
    target_platforms = platforms or ["linkedin", "youtube"]
    target_keywords  = keywords or RESEARCH_KEYWORDS

    futures = []
    for kw in target_keywords:
        for platform in target_platforms:
            if platform == "youtube" and not youtube_on:
                continue
            futures.append(_CORE_POOL.submit(run_researcher, kw, platform, youtube_on))
            time.sleep(random.uniform(5, 15))

    for f in as_completed(futures):
        exc = f.exception()
        if exc:
            log.error(f"[{AGENCY_NAME} Researcher] On-demand error: {exc}")

    researcher_build_final_summary()
    log.info(f"[{AGENCY_NAME} Researcher] Daily cycle done")


# ============================================================
# FIX 4 — STARTUP MESSAGE: DB flag — restart pe dobara nahi
# ============================================================

def send_startup_message():
    if get_startup_sent():
        log.info(f"[{AGENCY_NAME}] Startup message already sent (DB flag). Skipping.")
        return

    log.info(f"[{AGENCY_NAME}] Waiting for WhatsApp connection...")
    for _ in range(24):
        if wpp_status():
            break
        time.sleep(5)

    if wpp_status():
        startup_text = (
            f"✅ *{AGENCY_NAME} System Online*\n\n"
            "Manager, Analyzer & Researcher are active.\n"
            "Watchers are running — I'll notify you on new leads, "
            "project updates, and any issues.\n\n"
            "Commands:\n"
            "• *gmail on* / *gmail off* — Gmail monitoring toggle\n"
            "• *approve* — Approve pending project\n\n"
            "Reply anytime to give commands. 🚀"
        )
        ok = wpp_send(CEO_WHATSAPP, startup_text)
        if ok:
            set_startup_sent()
            log.info(f"[{AGENCY_NAME}] ✅ Startup message sent to CEO")
        else:
            log.warning(f"[{AGENCY_NAME}] Startup message failed to send")
    else:
        log.warning(f"[{AGENCY_NAME}] WhatsApp not connected after 120s — startup message skipped")


# ============================================================
# POSTGRESQL NOTIFY LISTENER
# ============================================================

def _make_notify_conn():
    conn = psycopg2.connect(DATABASE_URL)
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    with conn.cursor() as cur:
        cur.execute("LISTEN agent_channel")
        cur.execute("LISTEN oos_channel")
    return conn


def core_notify_listener():
    log.info(f"[{AGENCY_NAME}] NOTIFY listener started")
    backoff = 5
    conn    = None

    while True:
        try:
            if conn is None:
                conn    = _make_notify_conn()
                backoff = 5
                log.info(f"[{AGENCY_NAME}] NOTIFY listener connected")

            r, _, _ = select.select([conn], [], [], 60)
            if not r:
                try:
                    with conn.cursor() as _ka:
                        _ka.execute("SELECT 1")
                except Exception:
                    raise
                continue

            conn.poll()
            while conn.notifies:
                notify  = conn.notifies.pop(0)
                channel = notify.channel
                payload = notify.payload.strip()

                log.info(f"[{AGENCY_NAME}] NOTIFY on '{channel}': '{payload}'")

                if channel == "agent_channel" and payload == "manager":
                    _CORE_POOL.submit(run_manager_cycle)
                elif channel == "oos_channel":
                    _CORE_POOL.submit(run_analyzer, payload)

        except Exception as exc:
            log.error(f"[{AGENCY_NAME}] NOTIFY error: {exc} — reconnecting in {backoff}s")
            try:
                if conn:
                    conn.close()
            except Exception:
                pass
            conn = None
            time.sleep(backoff)
            backoff = min(backoff * 2, 120)


# ============================================================
# ENV VALIDATION
# ============================================================

def _validate_env():
    required = [
        "OPENAI_API_KEY", "DATABASE_URL", "CEO_WHATSAPP_NUMBER",
        "GMAIL_USER", "GMAIL_APP_PASSWORD", "GOOGLE_SHEET_URL",
        "GITHUB_TOKEN", "GITHUB_REPO", "EVOLUTION_URL",
        "EVOLUTION_API_KEY", "EVOLUTION_INSTANCE",
    ]
    missing = [k for k in required if not os.environ.get(k)]
    if missing:
        raise EnvironmentError(
            f"[{AGENCY_NAME}] Missing env variables: {', '.join(missing)}"
        )
    log.info(f"[{AGENCY_NAME}] All env variables present ✓")


# ============================================================
# MAIN — FIX 1: schedule polling removed, sirf NOTIFY
# ============================================================

def main():
    log.info("══════════════════════════════════════")
    log.info(f"  {AGENCY_NAME} — Manager + Analyzer + Researcher")
    log.info("══════════════════════════════════════")

    _validate_env()
    setup_database()

    threading.Thread(target=send_startup_message, daemon=True,
                     name="startup-msg").start()

    threading.Thread(target=core_notify_listener, daemon=True,
                     name="notify-listener").start()

    schedule.every().day.at("08:00").do(
        lambda: _CORE_POOL.submit(researcher_daily_cycle)
    )

    log.info(f"[{AGENCY_NAME}] All systems running ✅")
    log.info(f"[{AGENCY_NAME}] Gmail: {'ON' if get_gmail_enabled() else 'OFF (send gmail on to enable)'}")

    while True:
        schedule.run_pending()
        time.sleep(30)


if __name__ == "__main__":
    main()
