import os, io, time, socket, mimetypes
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests, openpyxl, boto3
from botocore.config import Config
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime

# ---- Cloudflare R2 CREDENTIALS (from GitHub Secrets) ----
ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID")
ACCESS_KEY = os.getenv("R2_ACCESS_KEY")
SECRET_KEY = os.getenv("R2_SECRET_KEY")

# ---- BUCKET AND PATHS ----
BUCKET      = "lihan"
EXCEL_KEY   = "Desktop/Excel file/Symbols.xlsx"
UPLOAD_PREF = "Desktop/All pdfs download/"
ENDPOINT    = f"https://{ACCOUNT_ID}.r2.cloudflarestorage.com"

# ---- SETTINGS ----
MAX_WORKERS = 8
REQUEST_TIMEOUT = 120

RETRY = Retry(
    total=3,
    backoff_factor=1.0,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET", "HEAD"]
)
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Referer": "https://www.nseindia.com/"
}

def is_connected(host="8.8.8.8", port=53, timeout=3):
    try:
        socket.setdefaulttimeout(timeout)
        socket.socket(socket.AF_INET, socket.SOCK_STREAM).connect((host, port))
        return True
    except OSError:
        return False

def build_session():
    s = requests.Session()
    s.headers.update(HEADERS)
    adapter = HTTPAdapter(max_retries=RETRY, pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s

def s3_client_r2():
    cfg = Config(signature_version="s3v4", retries={"max_attempts": 3, "mode": "standard"})
    return boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        config=cfg,
        region_name="auto"
    )

def pick_filename(url, resp_headers):
    cd = resp_headers.get("Content-Disposition", "") or ""
    name = ""
    if "filename=" in cd:
        name = cd.split("filename=")[-1].strip().strip(";").strip('"')
        name = os.path.basename(name)
    if not name:
        name = os.path.basename(urlparse(url).path) or "download.pdf"
    if not os.path.splitext(name)[1]:
        ctype = (resp_headers.get("Content-Type") or "").split(";")[0].strip()
        ext = mimetypes.guess_extension(ctype) or ".pdf"
        if not name.endswith(ext):
            name += ext
    return name

def read_urls_from_r2_excel(s3, bucket, key):
    obj = s3.get_object(Bucket=bucket, Key=key)
    data = obj["Body"].read()
    wb = openpyxl.load_workbook(filename=io.BytesIO(data), read_only=True, data_only=True)
    sheet = wb["Sheet1"]
    urls = []
    for row in range(2, sheet.max_row + 1):
        val = sheet[f"A{row}"].value
        if val and str(val).strip():
            urls.append(str(val).strip())
    return urls

def download_and_upload(session, s3, bucket, url):
    while not is_connected():
        print("❌ No internet. Retrying in 10 seconds...")
        time.sleep(10)
    try:
        with session.get(url, stream=True, timeout=REQUEST_TIMEOUT, allow_redirects=True) as r:
            if r.status_code != 200:
                return f"FAIL {url} -> HTTP {r.status_code}"
            filename = pick_filename(url, r.headers)
            key = f"{UPLOAD_PREF}{filename}"
            r.raw.decode_content = True
            s3.upload_fileobj(
                Fileobj=r.raw,
                Bucket=bucket,
                Key=key,
                ExtraArgs={"ContentType": r.headers.get("Content-Type", "application/pdf")}
            )
            return f"OK   {url} -> r2://{bucket}/{key}"
    except Exception as e:
        return f"FAIL {url} -> {e}"

def main():
    start = datetime.now()
    s3 = s3_client_r2()
    print("📘 Reading Excel from R2...")
    urls = read_urls_from_r2_excel(s3, BUCKET, EXCEL_KEY)
    if not urls:
        print("No URLs found. Exiting.")
        return
    print(f"Found {len(urls)} URLs. Starting downloads...")

    session = build_session()
    results = []
    from concurrent.futures import ThreadPoolExecutor, as_completed
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futs = {pool.submit(download_and_upload, session, s3, BUCKET, url): url for url in urls}
        for fut in as_completed(futs):
            msg = fut.result()
            results.append(msg)
            print(msg)

    ok = sum(s.startswith("OK") for s in results)
    fail = len(results) - ok

    os.makedirs("output", exist_ok=True)
    log = f"output/log_{start.strftime('%Y%m%d_%H%M%S')}.txt"
    with open(log, "w", encoding="utf-8") as f:
        f.write("\n".join(results))
        f.write(f"\n\n✅ Done. Success: {ok}, Failed: {fail}\n")
    print(f"\n✅ Done. Success: {ok}, Failed: {fail}")
    print(f"🕒 Log saved as: {log}")

if __name__ == "__main__":
    main()
