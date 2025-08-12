import os, json, uuid, logging
from flask import Flask, request, abort, jsonify

logging.basicConfig(level=logging.INFO)
app = Flask(__name__)

# ------------------ Config ------------------
DIFY_API_BASE = os.environ.get("DIFY_API", "https://api.dify.ai").rstrip("/")
DIFY_DATASET_ID = os.environ.get("DIFY_DATASET_ID", "").strip()
DIFY_API_KEY = os.environ.get("DIFY_API_KEY", "").strip()

WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "").strip()   # e.g., https://<service>.run.app
CHANNEL_TOKEN = os.environ.get("CHANNEL_TOKEN", "secret-123")

# Optional: restrict to a single Drive folder (parents contains this ID)
TARGET_FOLDER_ID = os.environ.get("TARGET_FOLDER_ID", "").strip()

# ------------------ Lazy clients ------------------
def _clients():
    """
    Create Google Drive + Firestore clients lazily (so startup never fails).
    """
    from google.cloud import firestore
    import google.auth
    from googleapiclient.discovery import build

    creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/drive.readonly"])
    drive = build("drive", "v3", credentials=creds, cache_discovery=False)
    db = firestore.Client()
    return drive, db

# ------------------ Firestore state helpers ------------------
def _get_state(db):
    doc = db.collection("state").document("drive").get()
    return doc.to_dict() if doc.exists else {}

def _set_state(db, payload):
    db.collection("state").document("drive").set(payload, merge=True)

# ------------------ Drive download/export ------------------
def _export_or_download(drive, file_id: str, mime_type: str, name_hint: str):
    """
    Export Google Docs/Sheets/Slides to standard formats, otherwise download as-is.
    Returns (filename, bytes).
    """
    if mime_type.startswith("application/vnd.google-apps"):
        if mime_type.endswith("document"):
            out = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
            filename = f"{(name_hint or file_id)}.docx"
        elif mime_type.endswith("spreadsheets"):
            out = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            filename = f"{(name_hint or file_id)}.xlsx"
        else:
            out = "application/pdf"
            filename = f"{(name_hint or file_id)}.pdf"
        data = drive.files().export(fileId=file_id, mimeType=out).execute()
        return filename, data
    else:
        data = drive.files().get_media(fileId=file_id).execute()
        meta = drive.files().get(fileId=file_id, fields="name").execute()
        return meta.get("name", file_id), data

# ------------------ Dify upload ------------------
def _upload_to_dify(filename: str, content_bytes: bytes):
    import requests

    if not DIFY_API_KEY or not DIFY_DATASET_ID:
        raise RuntimeError("Missing DIFY_API_KEY or DIFY_DATASET_ID")

    headers = {"Authorization": f"Bearer {DIFY_API_KEY}"}

    # 1) Minimal payload (most robust)
    files = {
        "file": (filename, content_bytes, "application/octet-stream"),
    }
    r = requests.post(
        f"{DIFY_API_BASE}/v1/datasets/{DIFY_DATASET_ID}/document/create-by-file",
        headers=headers,
        files=files,
        timeout=120,
    )
    if r.status_code == 400:
        # 2) Retry with light processing options (pass as plain string field, not JSON part)
        process_opts = {
            "indexing_technique": "high_quality",
            # "process_rule": {"rules": {"segmentation": {"max_tokens": 800}}}  # enable later if you want
        }
        files = {
            "file": (filename, content_bytes, "application/octet-stream"),
            "data": (None, json.dumps(process_opts)),
        }
        r = requests.post(
            f"{DIFY_API_BASE}/v1/datasets/{DIFY_DATASET_ID}/document/create-by-file",
            headers=headers,
            files=files,
            timeout=120,
        )

    try:
        r.raise_for_status()
    except Exception:
        app.logger.error("Dify upload failed: %s %s", r.status_code, r.text[:1000])
        raise
    return r.json()

# ------------------ Process changes ------------------
def _process_changes(drive, db, page_token: str):
    """
    Pull changes from Drive, optionally filter by TARGET_FOLDER_ID, upload to Dify, then advance pageToken.
    """
    result = drive.changes().list(
        pageToken=page_token,
        includeItemsFromAllDrives=True,
        supportsAllDrives=True
    ).execute()

    for ch in result.get("changes", []):
        if ch.get("removed"):
            continue
        file = ch.get("file") or {}
        file_id = file.get("id")
        if not file_id:
            continue

        meta = drive.files().get(
            fileId=file_id,
            fields="id,name,mimeType,trashed,parents"
        ).execute()
        if meta.get("trashed"):
            continue

        # Optional: filter to one folder
        if TARGET_FOLDER_ID:
            parents = set(meta.get("parents") or [])
            if TARGET_FOLDER_ID not in parents:
                app.logger.info("Skipping %s (outside target folder)", meta.get("name"))
                continue

        name = meta.get("name")
        mime = meta.get("mimeType")

        fname, bytes_ = _export_or_download(drive, file_id, mime, name)
        app.logger.info("Uploading to Dify: %s (id=%s)", fname, file_id)
        _upload_to_dify(fname, bytes_)

    new_token = result.get("newStartPageToken") or result.get("nextPageToken")
    if new_token:
        _set_state(db, {"pageToken": new_token})

# ------------------ Routes ------------------
@app.get("/")
def health():
    return "ok"

@app.get("/debug/info")
def debug_info():
    info = {
        "project": os.environ.get("GOOGLE_CLOUD_PROJECT"),
        "webhook_url": WEBHOOK_URL,
        "has_dify_key": bool(DIFY_API_KEY),
        "has_dataset_id": bool(DIFY_DATASET_ID),
        "channel_token_set": bool(CHANNEL_TOKEN),
        "target_folder_id": TARGET_FOLDER_ID or None,
    }
    try:
        drive, db = _clients()
        state = _get_state(db)
        info["stored_page_token"] = state.get("pageToken")
        return jsonify(info)
    except Exception as e:
        app.logger.exception("debug_info failed")
        info["error"] = str(e)
        return jsonify(info), 500

@app.post("/debug/pull")
def debug_pull():
    try:
        drive, db = _clients()
        state = _get_state(db)
        pt = state.get("pageToken")
        if not pt:
            return jsonify({"ok": False, "error": "no pageToken; call /init first"}), 400
        _process_changes(drive, db, pt)
        return jsonify({"ok": True})
    except Exception as e:
        app.logger.exception("manual pull failed")
        return jsonify({"ok": False, "error": str(e)}), 500

@app.get("/init")
def init_watch():
    try:
        if not WEBHOOK_URL or not WEBHOOK_URL.startswith("https://"):
            raise RuntimeError("WEBHOOK_URL missing or invalid (must be https Cloud Run URL)")
        drive, db = _clients()
        token = drive.changes().getStartPageToken().execute()["startPageToken"]
        _set_state(db, {"pageToken": token})

        channel_id = str(uuid.uuid4())
        body = {
            "id": channel_id,
            "type": "web_hook",
            "address": WEBHOOK_URL.rstrip("/") + "/drive-webhook",
            "token": CHANNEL_TOKEN,
        }
        drive.changes().watch(
            pageToken=token,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
            body=body
        ).execute()

        return jsonify({"ok": True, "channel_id": channel_id, "startPageToken": token})
    except Exception as e:
        app.logger.exception("/init failed")
        return jsonify({"ok": False, "error": str(e)}), 500

@app.post("/drive-webhook")
def drive_webhook():
    try:
        app.logger.info("drive-webhook called")
        if request.headers.get("X-Goog-Channel-Token") != CHANNEL_TOKEN:
            app.logger.warning("channel token mismatch")
            abort(403)
        drive, db = _clients()
        state = _get_state(db)
        page_token = state.get("pageToken")
        if not page_token:
            abort(500, "Missing pageToken; open /init once.")
        _process_changes(drive, db, page_token)
        return ("", 204)
    except Exception as e:
        app.logger.exception("/drive-webhook failed")
        return jsonify({"ok": False, "error": str(e)}), 500
