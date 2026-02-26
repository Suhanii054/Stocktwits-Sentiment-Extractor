import requests
from requests.auth import HTTPBasicAuth
import gzip
import json
import os
from datetime import datetime, timedelta

USERNAME = "your_username"
PASSWORD = "your_password"

BASE_FOLDER = r"E:\StockTwits data"
ACTIVITY_FOLDER = os.path.join(BASE_FOLDER, "activity")
MESSAGE_FOLDER = os.path.join(BASE_FOLDER, "message")

EXPECTED_FIELDS = {
    "activity": [
        "action", "created_at", "object", "object_id", "subject", "subject_id"
    ], 
    "message": [
        "id", "body", "created_at", "user", "source"
    ],
}

def ensure_folder(path):
    if not os.path.exists(path):
        os.makedirs(path)

def download_stocktwits_backup(file_type, year, month, day, username, password, folder):
    url = f"https://firestream.stocktwits.com/backups/{file_type}/{year}/{month:02d}/{day:02d}"
    filename = f"stocktwits_{file_type}_{year}_{month:02d}_{day:02d}.gz"
    filepath = os.path.join(folder, filename)
    if os.path.exists(filepath):
        print(f"File {filepath} already exists, skipping download.")
        return filepath
    print(f"Downloading {url} as {filepath}...")
    resp = requests.get(url, auth=HTTPBasicAuth(username, password), stream=True)
    if resp.status_code != 200:
        print(f"Error response for {file_type} {year}-{month:02d}-{day:02d}: {resp.status_code} {resp.reason}")
        return None
    try:
        with open(filepath, 'wb') as f:
            for chunk in resp.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
        print(f"Downloaded to {filepath}")
        return filepath
    except Exception as e:
        print(f"Error saving file stream: {e}")
        return None

def read_gz_json_lines(filename, n=5):
    data = []
    if not os.path.exists(filename):
        print(f"File {filename} does not exist.")
        return data
    try:
        with gzip.open(filename, "rb") as f:
            for i, line in enumerate(f):
                if i >= n:
                    break
                try:
                    message = json.loads(line)
                    data.append(message)
                except Exception as e:
                    print(f"Error parsing line {i+1} in {filename}: {e}")
    except Exception as e:
        print(f"Error reading {filename}: {e}")
    return data

def check_fields(records, expected_fields):
    """Check if all expected fields are present in each record."""
    for idx, rec in enumerate(records):
        missing = [f for f in expected_fields if f not in rec]
        if missing:
            print(f"Record {idx+1} missing fields: {missing}")
        else:
            print(f"Record {idx+1} contains all expected fields.")

def main():
    ensure_folder(ACTIVITY_FOLDER)
    ensure_folder(MESSAGE_FOLDER)
    start_date = datetime(2009, 1, 1)
    end_date = datetime.today()
    current_date = start_date
    while current_date <= end_date:
        y, m, d = current_date.year, current_date.month, current_date.day
        for file_type, folder in [('activity', ACTIVITY_FOLDER), ('message', MESSAGE_FOLDER)]:
            filepath = download_stocktwits_backup(file_type, y, m, d, USERNAME, PASSWORD, folder)
            if filepath:
                # Debugging: Read and check first 3 records
                records = read_gz_json_lines(filepath, n=3)
                print(f"\nSample records from {filepath}:")
                for idx, record in enumerate(records, 1):
                    print(f"\n--- Record {idx} ---")
                    print(json.dumps(record, indent=2))
                # Field check for debugging only
                if records:
                    print(f"\nChecking fields for {file_type} {y}-{m:02d}-{d:02d}:")
                    check_fields(records, EXPECTED_FIELDS[file_type])
                print("="*40)
        current_date += timedelta(days=1)

if __name__ == "__main__":
    main()