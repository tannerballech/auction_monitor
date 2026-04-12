"""
gmail_reader.py — Eagle Creek Auction Monitor
Reads sale announcement emails from known senders (Scott and Rowan KY)
using the Gmail API, then sends them to Claude for structured extraction.

Both Scott KY (kgross@carrowaylaw.com) and Rowan KY (budsalyer4@gmail.com)
send listing data as attachments rather than in the email body:
  - Scott: always a PDF
  - Rowan: PDF or Word doc (.docx); sometimes sends update emails with no
           useful attachment — Claude returns 0 listings for those naturally

Attachment handling:
  - PDF  → pdfminer.six (already in project for Madison KY)
  - DOCX → python-docx (pip install python-docx)
  - Body fallback → only used if no supported attachment is found AND the
                    body matches sale keywords (preserves old behavior)

claude_parse_listings() returns Street/City/State/Zip as separate fields.
No address parsing is done in this file.
"""

from __future__ import annotations
import os
import base64
import re
import io
from datetime import datetime, timedelta

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

from scrapers.base import claude_parse_listings
from config import (
    GOOGLE_CREDENTIALS_PATH,
    GMAIL_TOKEN_PATH,
    EMAIL_SOURCES,
    GMAIL_LOOKBACK_DAYS,
)

SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]

# MIME types we know how to extract text from
_PDF_MIME_TYPES  = {"application/pdf"}
_WORD_MIME_TYPES = {
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "application/msword",
}
_SUPPORTED_MIME_TYPES = _PDF_MIME_TYPES | _WORD_MIME_TYPES


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

def _get_gmail_service():
    """Authenticate and return a Gmail API service object."""
    creds = None

    if os.path.exists(GMAIL_TOKEN_PATH):
        creds = Credentials.from_authorized_user_file(GMAIL_TOKEN_PATH, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                GOOGLE_CREDENTIALS_PATH, SCOPES
            )
            creds = flow.run_local_server(port=0)
        with open(GMAIL_TOKEN_PATH, "w") as token_file:
            token_file.write(creds.to_json())

    return build("gmail", "v1", credentials=creds)


# ---------------------------------------------------------------------------
# Attachment extraction
# ---------------------------------------------------------------------------

def _find_attachments(payload: dict) -> list[dict]:
    """
    Recursively walk a Gmail message payload and return all parts that
    have a filename and a supported MIME type.
    Returns list of dicts: {filename, mime_type, attachment_id, data}
      - attachment_id is set when the data is stored externally (large files)
      - data is set when the content is inline (small files)
    """
    found = []
    mime_type = payload.get("mimeType", "")
    filename  = payload.get("filename", "")
    body      = payload.get("body", {})

    if filename and mime_type in _SUPPORTED_MIME_TYPES:
        found.append({
            "filename":      filename,
            "mime_type":     mime_type,
            "attachment_id": body.get("attachmentId"),
            "data":          body.get("data"),  # inline data (rare for PDFs)
        })

    for part in payload.get("parts", []):
        found.extend(_find_attachments(part))

    return found


def _download_attachment(service, message_id: str, attachment_info: dict) -> bytes | None:
    """Download attachment bytes from Gmail API."""
    if attachment_info["data"]:
        # Inline data — already base64 encoded
        return base64.urlsafe_b64decode(attachment_info["data"])

    if attachment_info["attachment_id"]:
        try:
            result = service.users().messages().attachments().get(
                userId="me",
                messageId=message_id,
                id=attachment_info["attachment_id"],
            ).execute()
            return base64.urlsafe_b64decode(result["data"])
        except Exception as e:
            print(f"  [Gmail] Failed to download attachment: {e}")
            return None

    return None


def _extract_pdf_text(raw_bytes: bytes) -> str:
    """Extract plain text from PDF bytes using pdfminer.six."""
    try:
        from pdfminer.high_level import extract_text
        return extract_text(io.BytesIO(raw_bytes))
    except ImportError:
        print("  [Gmail] pdfminer.six not installed — run: pip install pdfminer.six")
        return ""
    except Exception as e:
        print(f"  [Gmail] PDF text extraction failed: {e}")
        return ""


def _extract_docx_text(raw_bytes: bytes) -> str:
    """Extract plain text from Word doc bytes using python-docx."""
    try:
        import docx
        doc = docx.Document(io.BytesIO(raw_bytes))
        return "\n".join(p.text for p in doc.paragraphs)
    except ImportError:
        print("  [Gmail] python-docx not installed — run: pip install python-docx")
        return ""
    except Exception as e:
        print(f"  [Gmail] DOCX text extraction failed: {e}")
        return ""


def _extract_attachment_text(service, message_id: str, attachment_info: dict) -> str:
    """Download and extract text from a supported attachment."""
    raw = _download_attachment(service, message_id, attachment_info)
    if not raw:
        return ""

    mime = attachment_info["mime_type"]
    if mime in _PDF_MIME_TYPES:
        return _extract_pdf_text(raw)
    elif mime in _WORD_MIME_TYPES:
        return _extract_docx_text(raw)
    return ""


# ---------------------------------------------------------------------------
# Body extraction (fallback)
# ---------------------------------------------------------------------------

def _decode_message_body(msg_payload) -> str:
    """Extract plain text body from a Gmail message payload."""
    body = ""

    def _extract(payload):
        nonlocal body
        mime_type = payload.get("mimeType", "")
        if mime_type == "text/plain":
            data = payload.get("body", {}).get("data", "")
            if data:
                body += base64.urlsafe_b64decode(data).decode("utf-8", errors="replace")
        elif "parts" in payload:
            for part in payload["parts"]:
                _extract(part)

    _extract(msg_payload)
    return body


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def scrape_emails() -> list[dict]:
    """
    Fetch recent emails from all configured senders and extract auction listings.
    Returns a flat list of listing dicts across all email sources.
    Listings have Street/City/State/Zip as separate fields (from claude_parse_listings).
    """
    all_listings = []

    try:
        service = _get_gmail_service()
    except Exception as e:
        print(f"  [Gmail] Authentication failed: {e}")
        print(f"  [Gmail] Run the script interactively once to complete OAuth.")
        return []

    cutoff_date = (datetime.today() - timedelta(days=GMAIL_LOOKBACK_DAYS)).strftime("%Y/%m/%d")

    for source in EMAIL_SOURCES:
        county = source["county"]
        state  = source["state"]
        sender = source["sender"]

        print(f"  [Gmail] Checking {county} ({sender})...")

        query = f"from:{sender} after:{cutoff_date}"

        try:
            results = service.users().messages().list(
                userId="me", q=query, maxResults=20
            ).execute()
            messages = results.get("messages", [])
        except Exception as e:
            print(f"  [Gmail] Error searching {county}: {e}")
            continue

        if not messages:
            print(f"  [Gmail/{county}] Found 0 listings.")
            continue

        county_listings = []

        for msg_ref in messages:
            try:
                msg = service.users().messages().get(
                    userId="me", id=msg_ref["id"], format="full"
                ).execute()

                headers  = {h["name"]: h["value"] for h in msg["payload"].get("headers", [])}
                subject  = headers.get("Subject", "(no subject)")
                date_str = headers.get("Date", "")

                # ── Try attachments first ──────────────────────────────────
                attachments   = _find_attachments(msg["payload"])
                text_to_parse = ""

                for att in attachments:
                    att_text = _extract_attachment_text(service, msg_ref["id"], att)
                    if att_text.strip():
                        text_to_parse += att_text + "\n\n"

                # ── Fall back to body if no useful attachment found ─────────
                if not text_to_parse.strip():
                    body = _decode_message_body(msg["payload"])
                    if body.strip() and re.search(
                        r"sale|auction|foreclosure|commissioner|vs\.?|address",
                        body,
                        re.IGNORECASE,
                    ):
                        text_to_parse = body

                if not text_to_parse.strip():
                    continue

                context = f"Email Subject: {subject}\nEmail Date: {date_str}\n\n{text_to_parse}"
                parsed  = claude_parse_listings(context, county, state, f"Gmail: {sender}")
                county_listings.extend(parsed)

            except Exception as e:
                print(f"  [Gmail] Error parsing message for {county}: {e}")
                continue

        # Deduplicate by case number, falling back to street address
        seen = set()
        for listing in county_listings:
            key = listing.get("Case Number", "") or listing.get("Street", "")
            if key and key not in seen:
                seen.add(key)
                all_listings.append(listing)

        print(f"  [Gmail/{county}] Found {len(seen)} listings.")

    return all_listings