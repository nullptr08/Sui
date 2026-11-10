import requests
from bs4 import BeautifulSoup
import json
import time
import random
import threading
import os
import sys
import argparse
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import urllib3

try:
    from tqdm import tqdm
except ImportError:
    tqdm = None

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ================= CONFIGURATION =================
TARGET_URL = "https://daleel.admission.gov.sd/result2026/Result_2026.aspx"
START_ID = 21039800
END_ID = 21039900
THREADS = 6

PROXY_LIST = [] 

# ================= CONFIGURATION =================
# ... (keep URL, START_ID, END_ID, THREADS, etc. the same) ...

CATEGORIES = {
    "13": "منح التعليم الاهلي"
}

RESULTS_FILE = "results.jsonl"
PROGRESS_FILE = "progress.txt"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "ar,en-US;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
    "Cache-Control": "max-age=0"
}

file_lock = threading.Lock()
IS_CI = os.environ.get("CI") == "true" or os.environ.get("GITHUB_ACTIONS") == "true"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

def get_session():
    session = requests.Session()
    session.headers.update(HEADERS)
    if PROXY_LIST:
        session.proxies = {"http": random.choice(PROXY_LIST), "https": random.choice(PROXY_LIST)}
    return session

def check_id(app_id, url):
    # Allow up to 2 attempts per ID (handles actual server soft-throttling)
    for attempt in range(1, 3):
        session = get_session()
        is_anomaly = False
        
        # WE MUST CHECK EACH CATEGORY WITH A FRESH CAPTCHA
        for cat_id, cat_name in CATEGORIES.items():
            try:
                # 1. FRESH GET FOR EVERY CATEGORY ATTEMPT
                res = session.get(url, timeout=12, verify=False)
                res.raise_for_status()
                soup = BeautifulSoup(res.text, "html.parser")
                
                vs = soup.find("input", {"name": "__VIEWSTATE"})
                ev = soup.find("input", {"name": "__EVENTVALIDATION"})
                captcha = soup.find("span", {"id": "CaptchaDiv"})
                
                if not all([vs, ev, captcha]):
                    is_anomaly = True
                    break # Form missing, trigger full retry
                    
                # 2. POST with the FRESH captcha
                payload = {
                    "__VIEWSTATE": vs.get("value", ""),
                    "__EVENTVALIDATION": ev.get("value", ""),
                    "DropDownList1": cat_id,
                    "TextBox1": str(app_id),
                    "CaptchaInput": captcha.get_text(strip=True),
                    "Button1": "عرض النتيجة"
                }
                
                post_res = session.post(url, data=payload, timeout=12, verify=False)
                post_soup = BeautifulSoup(post_res.text, "html.parser")
                
                # 3. Check for HIT
                name_lbl = post_soup.find(id="StudentNameLabel")
                if name_lbl and len(name_lbl.get_text(strip=True)) > 3:
                    faculty = post_soup.find(id="FacultyNameLabel")
                    result = {
                        "application_id": str(app_id),
                        "category": cat_name,
                        "student_name": name_lbl.get_text(strip=True),
                        "faculty": faculty.get_text(strip=True) if faculty else "Unknown"
                    }
                    
                    with file_lock:
                        with open(RESULTS_FILE, "a", encoding="utf-8") as f:
                            f.write(json.dumps(result, ensure_ascii=False) + "\n")
                        with open(PROGRESS_FILE, "a", encoding="utf-8") as f:
                            f.write(f"{app_id}\n")
                    return True # SUCCESS! Stop checking other categories.
                    
                # 4. Check for TRUE MISS (Safe to try next category)
                msg_lbl = post_soup.find(id="LabelMessage")
                msg_text = msg_lbl.get_text(strip=True) if msg_lbl else ""
                
                if "رقم الاستمارة غير صحيح" in msg_text:
                    continue # True miss for this category, loop to next category
                    
                # 5. Check for SOFT FAILURE (Server throttling/blocking)
                if "رمز التحقق غير صحيح" in msg_text or "انتهت" in msg_text or "جلسة" in msg_text:
                    is_anomaly = True
                    break # Captcha failed unexpectedly, break and retry whole ID
                    
            except Exception:
                is_anomaly = True
                break
                
        # If we finished all categories and no anomaly occurred, it's a verified miss
        if not is_anomaly:
            return False 
            
        # JITTER before retrying the whole ID
        time.sleep(random.uniform(0.15, 0.35))
        
    return False

def load_completed():
    if not os.path.exists(PROGRESS_FILE):
        return set()
    with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
        return set(line.strip() for line in f if line.strip())

def main():
    global TARGET_URL, START_ID, END_ID, THREADS
    
    parser = argparse.ArgumentParser(description="Resilient ASP.NET Result Scraper")
    parser.add_argument("--start", type=int, default=START_ID)
    parser.add_argument("--end", type=int, default=END_ID)
    parser.add_argument("--threads", type=int, default=THREADS)
    parser.add_argument("--url", type=str, default=TARGET_URL)
    args = parser.parse_args()

    TARGET_URL = args.url
    START_ID = args.start
    END_ID = args.end
    THREADS = args.threads

    logging.info(f"Server: {TARGET_URL} | Range: {START_ID}-{END_ID} | Threads: {THREADS}")
    
    completed = load_completed()
    pending_ids = [i for i in range(START_ID, END_ID + 1) if str(i) not in completed]
    total_pending = len(pending_ids)
    
    logging.info(f"Pending: {total_pending:,} | Already done: {len(completed):,}")
    
    stats = {"found": 0, "processed": 0}
    start_time = time.time()
    
    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        futures = {executor.submit(check_id, app_id, TARGET_URL): app_id for app_id in pending_ids}
        
        if IS_CI or tqdm is None:
            for future in as_completed(futures):
                if future.result():
                    stats["found"] += 1
                stats["processed"] += 1
                
                if stats["processed"] % 500 == 0:
                    elapsed = time.time() - start_time
                    speed = stats["processed"] / elapsed if elapsed > 0 else 0
                    logging.info(f"Progress: {stats['processed']}/{total_pending} | Found: {stats['found']} | Speed: {speed:.1f} ID/s")
        else:
            for future in tqdm(as_completed(futures), total=total_pending, desc="Scanning"):
                if future.result():
                    stats["found"] += 1
                
    logging.info("==========================================================")
    logging.info(f"SCAN COMPLETE | Found: {stats['found']} | Not Found: {total_pending - stats['found']} | Errors: 0")
    logging.info("==========================================================")

if __name__ == "__main__":
    main()
