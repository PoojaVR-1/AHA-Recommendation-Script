import json
import requests
import gspread
from google.oauth2.service_account import Credentials

# --- Setup ---
SHEET_NAME = "Web Series Metadata"
RATING_TAB_NAME = "Rating Chart"
GOOGLE_SHEET_ID = "1pg0NqkjzQ2uv4y5p4wZVDamS4WulAS8fobJVt6_LW9k"
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
SERVICE_ACCOUNT_FILE = 'credentials/client_secret.json'

# Authenticate
credentials = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
client = gspread.authorize(credentials)

# Open sheets
sheet = client.open_by_key(GOOGLE_SHEET_ID)
ws = sheet.worksheet(SHEET_NAME)
ratings_ws = sheet.worksheet(RATING_TAB_NAME)

# --- Read headers from main sheet and make them lowercase ---
headers = ws.row_values(1)
lower_headers = [h.strip().lower() for h in headers]
print("🧾 Sheet Headers (lowercased):", lower_headers)

# Required headers (lowercased)
required_keys = ["id", "type", "title", "description", "languagecode", "primarycategory",
                 "actors", "director", "content_region", "rating_index", "rg-srv url", "payload"]

# Check for missing required headers
missing_headers = [key for key in required_keys if key not in lower_headers]
if missing_headers:
    print(f"Missing required headers: {missing_headers}")
    exit(1)

# Get actual column indices
def col_index(key):
    return lower_headers.index(key) + 1

payload_col = col_index("payload")
url_col = col_index("rg-srv url")
full_url_col = col_index("full url")

# --- Process each row ---
rows = ws.get_all_values()[1:]
batch_updates = []

for idx, row in enumerate(rows, start=2):
    try:
        if len(row) < payload_col or str(row[payload_col - 1]).strip():
            print(f"[Row {idx}] ⏭ Skipped: Already has payload or row too short.")
            continue

        def safe_get(key):
            i = lower_headers.index(key)
            return str(row[i]).strip() if i < len(row) else ""

        payload = {
            "ID": safe_get("id"),
            "TYPE": safe_get("type"),
            "TITLE": safe_get("title"),
            "DESCRIPTION": safe_get("description"),
            "LANGUAGECODE": safe_get("languagecode"),
            "PRIMARYCATEGORY": f"[{safe_get('primarycategory')}]" if safe_get("primarycategory") else "[]",
            "ACTORS": safe_get("actors"),
            "DIRECTOR": safe_get("director"),
            "CONTENT_REGION": safe_get("content_region"),
            "RATING_INDEX": int(safe_get("rating_index")) if safe_get("rating_index").isdigit() else "NR"
        }

        payload_json = json.dumps(payload, ensure_ascii=False)
        
        # POST to Recommendation API
        recommendation_api_url = "https://recommendation-engine.api.aha.firstlight.ai/online/recommend"
        try:
            rec_response = requests.post(recommendation_api_url, data=payload_json, headers={"Content-Type": "application/json"})
            if rec_response.status_code != 200:
                print(f"[Row {idx}] Recommendation API failed ({rec_response.status_code}): {rec_response.text}")
                continue
            else:
                print(f"[Row {idx}] Recommendation API call successful")
        except Exception as e:
            print(f"[Row {idx}] Recommendation API call error: {e}")
            continue
            
        # POST to rg-srv URL
        url = safe_get("rg-srv url")
        response = requests.post(url, data=payload_json, headers={"Content-Type": "application/json"})

        # Queue payload update
        batch_updates.append({
            "range": f"{gspread.utils.rowcol_to_a1(idx, payload_col)}",
            "values": [[payload_json]]
        })

        # Generate and queue full URL
        content_id = safe_get("id")
        language_code = safe_get("languagecode")
        full_url = f"https://rg-srv.api.aha.firstlight.ai/recommendation/more-like-this/{content_id}?acl={language_code}"

        batch_updates.append({
            "range": f"{gspread.utils.rowcol_to_a1(idx, full_url_col)}",
            "values": [[full_url]]
        })

        # Push updates every 50 cells (25 rows * 2 fields) or near end
        if len(batch_updates) >= 50:
            ws.batch_update(batch_updates)
            batch_updates.clear()

        print(f"[Row {idx}] Payload sent successfully")

    except Exception as e:
        print(f"[Row {idx}] Error processing row: {e}")

# Final flush after loop
if batch_updates:
    ws.batch_update(batch_updates)
