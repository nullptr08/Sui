import argparse
import json
import os
import sys
import time
import threading
import logging
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup

# ================= GLOBAL CONFIGURATION =================
BASE_URL = "https://daleel.admission.gov.sd/result2026/Result_2026.aspx" # See note below about localhost
START_ID = 20000000
END_ID   = START_ID + 100000

RESULTS_FILE = "results.jsonl"
PROGRESS_FILE = "progress.txt"
LOG_FILE = "scraper.log"

MAX_THREADS = 5
REQUEST_DELAY = 0.5
REQUEST_TIMEOUT = 15
MAX_RETRIES = 2

CATEGORIES = {"1": "قبول عام", "3": "قبول على النفقة الخاصة", "5": "قبول أبناء العاملين"}
SUBMIT_VALUE = "عرض النتيجة"
ERROR_KEYWORDS = ["لا توجد نتيجة", "غير موجود", "خطأ", "not found", "error", "invalid", "no result", "does not exist"]

# ================= CI/CD DETECTION =================
IS_CI = os.environ.get("CI") == "true" or os.environ.get("GITHUB_ACTIONS") == "true"

# ================= THREAD-LOCAL STORAGE =================
thread_local = threading.local()
file_lock = threading.Lock()

def get_session():
    if not hasattr(thread_local, "session"):
        thread_local.session = requests.Session()
        thread_local.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept-Language": "ar,en-US;q=0.7,en;q=0.3",
        })
    return thread_local.session

# ================= LOGGING =================
# In CI, we only log to stdout to keep it clean. Locally, we log to file too.
log_handlers = [logging.StreamHandler(sys.stdout)]
if not IS_CI:
    log_handlers.append(logging.FileHandler(LOG_FILE, encoding="utf-8"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=log_handlers
)

# ================= CORE LOGIC =================
def is_valid_result(soup):
    name_label = soup.find(id="StudentNameLabel")
    if name_label and len(name_label.get_text(strip=True)) > 2:
        return True
    container = soup.find("div", {"class": "result-wrap"})
    if container:
        text = container.get_text(" ", strip=True).lower()
        if not any(err in text for err in ERROR_KEYWORDS):
            return True
    return False

def process_id(application_id):
    session = get_session()
    for category, label in CATEGORIES.items():
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                response = session.get(BASE_URL, timeout=REQUEST_TIMEOUT)
                response.raise_for_status()
                soup = BeautifulSoup(response.text, "html.parser")

                viewstate = soup.find("input", {"name": "__VIEWSTATE"})
                eventvalidation = soup.find("input", {"name": "__EVENTVALIDATION"})
                captcha_span = soup.find("span", {"id": "CaptchaDiv"})

                if not all([viewstate, eventvalidation, captcha_span]):
                    time.sleep(1)
                    continue

                payload = {
                    "__VIEWSTATE": viewstate.get("value", ""),
                    "__EVENTVALIDATION": eventvalidation.get("value", ""),
                    "DropDownList1": category,
                    "TextBox1": str(application_id),
                    "CaptchaInput": captcha_span.get_text(strip=True),
                    "Button1": SUBMIT_VALUE
                }

                result_resp = session.post(BASE_URL, data=payload, headers={"Referer": BASE_URL}, timeout=REQUEST_TIMEOUT)
                
                if result_resp.status_code == 429:
                    return "rate_limited"

                result_resp.raise_for_status()
                result_soup = BeautifulSoup(result_resp.text, "html.parser")

                if is_valid_result(result_soup):
                    student_name = result_soup.find(id="StudentNameLabel")
                    faculty_name = result_soup.find(id="FacultyNameLabel")
                    
                    result_data = {
                        "application_id": str(application_id),
                        "category": label,
                        "student_name": student_name.get_text(strip=True) if student_name else "Unknown",
                        "faculty": faculty_name.get_text(strip=True) if faculty_name else "Unknown",
                    }
                    
                    with file_lock:
                        with open(RESULTS_FILE, "a", encoding="utf-8") as f:
                            f.write(json.dumps(result_data, ensure_ascii=False) + "\n")
                        with open(PROGRESS_FILE, "a", encoding="utf-8") as f:
                            f.write(f"{application_id}\n")
                    return "success"
                
                break 
            except requests.RequestException:
                if attempt == MAX_RETRIES:
                    return "network_error"
                time.sleep(attempt * 2)
        time.sleep(REQUEST_DELAY)
    return "not_found"

# ================= MAIN =================
def load_completed_ids():
    completed = set()
    if Path(PROGRESS_FILE).exists():
        with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
            completed = set(line.strip() for line in f if line.strip())
    return completed

def main():
    parser = argparse.ArgumentParser(description="CI/CD Compatible ASP.NET Result Scraper")
    parser.add_argument("--start", type=int, default=START_ID)
    parser.add_argument("--end", type=int, default=END_ID)
    parser.add_argument("--threads", type=int, default=MAX_THREADS)
    parser.add_argument("--yes", action="store_true", help="Skip interactive confirmation")
    args = parser.parse_args()

    if args.start > args.end:
        sys.exit(1)

    total_ids = args.end - args.start + 1
    
    # FIX 1: Skip interactive prompt if in CI or if --yes is passed
    if total_ids > 10000 and not IS_CI and not args.yes:
        logging.warning(f"⚠️  LARGE RANGE DETECTED: {total_ids:,} IDs.")
        confirm = input("Type 'YES' to continue, or Ctrl+C to abort: ")
        if confirm.strip().upper() != "YES":
            sys.exit(0)

    completed_ids = load_completed_ids()
    pending_ids = [str(i) for i in range(args.start, args.end + 1) if str(i) not in completed_ids]

    logging.info(f"Server: {BASE_URL} | Range: {args.start}-{args.end} | Pending: {len(pending_ids):,} | Threads: {args.threads}")

    if not pending_ids:
        logging.info("No pending IDs. Exiting.")
        return

    stats = {"success": 0, "not_found": 0, "errors": 0, "processed": 0}
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=args.threads) as executor:
        future_to_id = {executor.submit(process_id, app_id): app_id for app_id in pending_ids}
        
        for future in as_completed(future_to_id):
            app_id = future_to_id[future]
            try:
                status = future.result()
                if status == "success": stats["success"] += 1
                elif status == "not_found": stats["not_found"] += 1
                else: stats["errors"] += 1
            except Exception:
                stats["errors"] += 1
            
            stats["processed"] += 1
            
            # FIX 2: Clean CI Progress Logging (Print every 500 IDs instead of spamming)
            if IS_CI and stats["processed"] % 500 == 0:
                elapsed = time.time() - start_time
                speed = stats["processed"] / elapsed if elapsed > 0 else 0
                logging.info(f"Progress: {stats['processed']}/{len(pending_ids)} | Found: {stats['success']} | Speed: {speed:.1f} ID/s")

    logging.info(f"COMPLETE | Found: {stats['success']} | Not Found: {stats['not_found']} | Errors: {stats['errors']}")

if __name__ == "__main__":
    main()