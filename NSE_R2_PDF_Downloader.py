# NSE_R2_PDF_Downloader.py
# Purpose: Read URLs from an Excel stored in Cloudflare R2 and stream-download each PDF back to R2.
# Notes:
# - Uses env vars for R2 creds in CI (GitHub Actions). Defaults for bucket/paths are set below.
# - Memory light: no full-file buffering; uses upload_fileobj(stream).

import os, io, time, socket, mimetypes, datetime
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests, openpyxl, boto3
from botocore.config import Config
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------- Read credentials from environment (set in GitHub Actions) ----------
ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID")
ACCESS_KEY = os.getenv("R2_ACCESS_KEY_ID")
SECRET_KEY = os.getenv("R2_SECRET_ACCESS_KEY")

# ---------- Bucket and object paths (safe defaults; override via env if desired) ----------
BUCKET      = os.getenv("R2_BUCKET", "lihan")
EXCEL_KEY   = os.getenv("R2_EXCEL_KEY", "Desktop/Excel file/Symbols.xlsx")
UPLOAD_PREF = os.getenv("R2_UPLOAD_PREFIX", "Desktop/All pdfs download/")

# ---------- General settings ----------
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "8"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "120"))
ENDPOINT = f"https://{ACCOUNT_ID}.r2.cloudflarestorage.com" if ACCOUNT_ID else None

RETRY = Retry(
    total=3,
    backoff_factor=1.0,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET", "HEAD"],
)
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Referer": "https://www.nseindia.com/",
}

# ---------- Simple logger that writes to file and prints ----------
os.makedirs("output", exist_ok=True)
LOG_PATH = os.path.join("output", f"log_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt")

def log(msg: str) -> None:
    with open(LOG_PATH, "a", encoding="utf-8") as f:
        f.write(msg.rstrip() + "\n")
    print(msg, flush=True)

# ---------- Helpers ----------
def is_connected(host="8.8.8.8", port=53, timeout=3) -> bool:
    """Quick internet check."""
    try:
        socket.setdefaulttimeout(timeout)
        socket.socket(socket.AF_INET, socket.SOCK_STREAM).connect((host, port))
        return True
    except OSError:
        return False

def build_session() -> requests.Session:
    """HTTP session with retry and connection pooling."""
    s = requests.Session()
    s.headers.update(HEADERS)
    adapter = HTTPAdapter(max_retries=RETRY, pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s

def s3_client_r2():
    """Cloudflare R2 S3-compatible client."""
    if not all([ACCOUNT_ID, ACCESS_KEY, SECRET_KEY]):
        missing = [n for n, v in [("R2_ACCOUNT_ID", ACCOUNT_ID),
                                  ("R2_ACCESS_KEY_ID", ACCESS_KEY),
                                  ("R2_SECRET_ACCESS_KEY", SECRET_KEY)] if not v]
        raise RuntimeError(f"Missing R2 secrets: {', '.join(missing)}")
    cfg = Config(signature_version="s3v4", retries={"max_attempts": 3, "mode": "standard"})
    return boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        config=cfg,
        region_name="auto",
    )

def pick_filename(url: str, resp_headers: dict) -> str:
    """Prefer server filename; else derive from URL; ensure extension."""
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

def read_urls_from_r2_excel(s3, bucket: str, key: str) -> list:
    """Read URLs from Excel file in R2 (Sheet1, column A, from row 2)."""
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

def download_and_upload(session: requests.Session, s3, bucket: str, url: str) -> str:
    """Stream download one URL and upload directly to R2."""
    while not is_connected():
        time.sleep(5)

    try:
        with session.get(url, stream=True, timeout=REQUEST_TIMEOUT, allow_redirects=True) as r:
            if r.status_code != 200:
                return f"FAIL {url} -> HTTP {r.status_code}"

            filename = pick_filename(url, r.headers)
            key = f"{UPLOAD_PREF}{filename}"

            r.raw.decode_content = True
            content_type = r.headers.get("Content-Type", "application/pdf")

            s3.upload_fileobj(
                Fileobj=r.raw,
                Bucket=bucket,
                Key=key,
                ExtraArgs={"ContentType": content_type},
            )
            return f"OK   {url} -> r2://{bucket}/{key}"
    except Exception as e:
        return f"FAIL {url} -> {e}"

# ---------- Main ----------
def main():
    log("Starting NSE R2 PDF Downloader")
    log(f"Bucket: {BUCKET}")
    log(f"Excel key: {EXCEL_KEY}")
    log(f"Upload prefix: {UPLOAD_PREF}")

    s3 = s3_client_r2()

    # Sanity check: Excel is reachable
    log("Reading Excel from R2 ...")
    try:
        urls = read_urls_from_r2_excel(s3, BUCKET, EXCEL_KEY)
    except Exception as e:
        log(f"ERROR reading Excel: {e}")
        raise

    if not urls:
        log("No URLs found in Excel. Exiting.")
        return
    log(f"Found {len(urls)} URLs. Starting downloads ...")

    session = build_session()
    results = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futs = {pool.submit(download_and_upload, session, s3, BUCKET, url): url for url in urls}
        for fut in as_completed(futs):
            msg = fut.result()
            results.append(msg)
            log(msg)

    ok = sum(x.startswith("OK") for x in results)
    fail = len(results) - ok
    log(f"Done. Success: {ok}, Failed: {fail}")
    log(f"Log saved: {LOG_PATH}")

if __name__ == "__main__":
    main()
