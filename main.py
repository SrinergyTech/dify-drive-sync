import os, json, uuid, requests
from flask import Flask, request, abort
from googleapiclient.discovery import build
import google.auth
from google.cloud import firestore

# ---- Config ----
DIFY_API = "https://api.dify.ai/v1"
DATASET_ID = os.environ["DIFY_DATASET_ID"]
DIFY_KEY = os.environ["DIFY_API_KEY"]
WEBHOOK_URL = os.environ["WEBHOOK_URL"]        # set after deploy
CHANNEL_TOKEN = os.environ.get("CHANNEL_TOKEN","secret-123")  # any random string

# Firestore: store Drive pageToken between notifications
db = firestore.Client()
STATE_DOC = db.collection("state").document("drive")

# Use Cloud Run service account identity to call Drive
creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/drive.readonly"])
drive = build("drive", "v3", credentials=creds, cache_discovery=False)

app = Flask(__name__)

def get_state():
    snap = STATE_DOC.get()
    return snap.to_dict() if snap.exists else {}

def set_state(d):
    STATE_DOC.set(d, merge=True)

def export_or_download(file_id, mime_type, name_hint):
    # Export native Google files; download others
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

def upload_to_dify(filename, content_bytes):
    files = {
        "file": (filename, content_bytes),
        "data": (
            None,
            json.dumps({
                "indexing_technique": "high_quality",
                "process_rule": {"rules": {"segmentation": {"max_tokens": 800}}}
            }),
            "application/json",
        ),
    }
    r = requests.post(
        f"{DIFY_API}/datasets/{DATASET_ID}/document/create-by-file",
        headers={"Authorization": f"Bearer {DIFY_KEY}"},
        files=files,
        timeout=120,
    )
    r.raise_for_status()
    return r.json()

def process_changes(page_token):
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
            fields="id,name,mimeType,trashed"
        ).execute()
        if meta.get("trashed"):
            continue

        name = meta.get("name")
        mime = meta.get("mimeType")

        fname, bytes_ = export_or_download(file_id, mime, name)
        upload_to_dify(fname, bytes_)

    new_token = result.get("newStartPageToken") or result.get("nextPageToken")
    if new_token:
        set_state({"pageToken": new_token})

@app.get("/")
def health():
    return "ok"

@app.get("/init")
def init_watch():
    # 1) get startPageToken and store it
    token = drive.changes().getStartPageToken().execute()["startPageToken"]
    set_state({"pageToken": token})

    # 2) create a webhook channel pointing to this service
    channel_id = str(uuid.uuid4())
    body = {
        "id": channel_id,
        "type": "web_hook",
        "address": WEBHOOK_URL + "/drive-webhook",
        "token": CHANNEL_TOKEN,  # echoed back in header on notifications
    }
    drive.changes().watch(body=body).execute()
    return {"ok": True, "channel_id": channel_id, "startPageToken": token}

@app.post("/drive-webhook")
def drive_webhook():
    # Validate the shared secret
    if request.headers.get("X-Goog-Channel-Token") != CHANNEL_TOKEN:
        abort(403)

    state = get_state()
    page_token = state.get("pageToken")
    if not page_token:
        abort(500, "Missing pageToken; open /init once.")

    process_changes(page_token)
    return ("", 204)
