import asyncio
import httpx
import hashlib
import os
import random
import re
import aiosqlite
from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime

BASE_URL = "https://broniewski.edu.pl/planylekcji/"
ALT_BASE_URL = "https://broniewski.edu.pl/plan/"
PORTAL_URL = "https://broniewski.edu.pl/index.php/plan-lekcji-2026-2027"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_FILE = os.path.join(BASE_DIR, "scanner_state.db")
LOG_FILE = os.path.join(BASE_DIR, "changes.log")
SUCCESS_FILE = os.path.join(BASE_DIR, "znalezione_plany_2027.txt")
DOWNLOAD_DIR = os.path.join(BASE_DIR, "downloads")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

# --- POBIERANIE WEBHOOKÓW Z ZMIENNYCH ŚRODOWISKOWYCH (GITHUB SECRETS) ---
WEBHOOK_PLANY = os.getenv("DISCORD_WEBHOOK_URL_1")    # #plany-lekcji
WEBHOOK_STATUS = os.getenv("DISCORD_WEBHOOK_URL_3")   # #wymuś-skan
IS_MANUAL_RUN = os.getenv("GITHUB_EVENT_NAME") == "workflow_dispatch"

# --- KONFIGURACJA WYDAJNOŚCIOWA I FUNKCJONALNA ---
DAEMON_MODE = False                 # False = jeden skan i koniec (tryb dla GitHub Actions)
CHECK_INTERVAL_SECONDS = 10800      # Czas pauzy w trybie demona (3 godziny)
MAX_CONCURRENT_REQUESTS = 50        # Maksymalny limit współbieżności
CHECK_TEACHERS = True               # True = skanuje katalogi nauczycieli (nauczyciele/, n/)
CHECK_4DIGIT_FOLDERS = True         # True = skanuje masowo sekwencje 4-cyfrowe (0001-9999)
ONLY_CURRENT_AND_NEXT_MONTH = True  # True = sprawdza bieżący oraz następny miesiąc

# --- OPCJE ULTIMATE (WARIANTY STRUKTUR I ARCHIWUM) ---
CHECK_POLISH_MONTH_NAMES = True     # True = sprawdza słowne nazwy miesięcy (np. 'maj', 'styczen')
CHECK_UNDERSCORE_FORMATS = True     # True = sprawdza formaty z podkreśleniem (np. '2027_maj', '2027_05')
CHECK_ALT_BASE_PLAN_PATH = True     # True = sprawdza ścieżki z przedrostkiem /plan/
CHECK_STUDENTS_FOLDER = True        # True = sprawdza katalogi uczniów (uczniowie/, uczen/)
CHECK_SHORT_STUDENTS_FOLDER = True  # True = sprawdza krótki katalog uczniów (u/)
CHECK_ROOMS = True                  # True = sprawdza katalogi sal lekcyjnych (sale/, s/)

# --- DUBLOWANIE, POTRAJANIE I CZTEROKROTNE SŁOWA KLUCZOWE ---
CHECK_KEYWORD_DOUBLING = False      # True = sprawdza dublowanie katalogów (np. /oddzialy/oddzialy/)
CHECK_KEYWORD_TRIPLING = False      # True = sprawdza potrajanie katalogów (np. /oddzialy/oddzialy/oddzialy/)
CHECK_KEYWORD_QUADRUPLING = False   # True = sprawdza czterokrotne katalogi (np. /oddzialy/oddzialy/oddzialy/oddzialy/)

# --- EKSTREMALNE FORMATY DAT I NOWE OPCJE ---
CHECK_DDMMYYYY_DATE_FOLDER = True   # True = sprawdza datę DDMMYYYY
CHECK_DDMMYY_DATE_FOLDER = True     # True = sprawdza datę DDMMYY
CHECK_COMPACT_DATE_FOLDERS = True   # True = sprawdza formaty typu DMMYY / DMRR
CHECK_DATE_RANGES = False           # True = sprawdza zakresy dat (np. 05-09, 05.09)

# --- DODATKOWE STRUKTURY EKSPORTU I PLIKI RAMKOWE ---
CHECK_HTML_SUBFOLDER = False        # True = sprawdza podkatalog html/
CHECK_GENERATED_EXPORT = False      # True = sprawdza katalogi generated/ oraz export/
CHECK_FRAME_INDEX_FILES = False     # True = sprawdza pliki ramkowe index_n.htm, index_o.htm, index_s.htm

# --- OPCJE SPECJALNE ---
CHECK_PLIKI_PLAN_PATH = True        # True = sprawdza ścieżki z przedrostkiem /pliki/[rok]/plan/
CHECK_LEGACY_UCZNIOWIETH = True     # True = sprawdza archiwalny system /plan/uczniowieth/ (z 2019r.)

POLISH_MONTHS_MAP = {
    1: "styczen", 2: "luty", 3: "marzec", 4: "kwiecien", 
    5: "maj", 6: "czerwiec", 7: "lipiec", 8: "sierpien", 
    9: "wrzesien", 10: "pazdziernik", 11: "listopad", 12: "grudzien"
}

SMOKE_TEST_URLS = [
    'https://www.broniewski.edu.pl/planylekcji/2027/0805/o/',
    'https://www.broniewski.edu.pl/planylekcji/2027/05/o/',
    'https://www.broniewski.edu.pl/planylekcji/2027/1902/oddzialy/',
    'https://www.broniewski.edu.pl/planylekcji/2027/1901/oddzialy/'
]

KEY_MONITOR_URLS = [
    'https://www.broniewski.edu.pl/planylekcji',
    'https://www.broniewski.edu.pl/planylekcji/2027/oddzialy/oddzialy.htm',
    'https://www.broniewski.edu.pl/planylekcji/2027/nauczyciele/nauczyciele.htm'
]

# --- OBSŁUGA DISCORDA ---
async def send_discord_msg(webhook_url, content):
    if not webhook_url:
        return
    try:
        async with httpx.AsyncClient() as client:
            await client.post(webhook_url, json={"content": content}, timeout=10.0)
    except Exception as e:
        print(f"[!] Błąd wysyłania powiadomienia Discord: {e}")

# --- ADAPTIVE RATE LIMITER (Zarządzanie tempem w locie) ---
class AdaptiveLimiter:
    def __init__(self, initial_concurrency=20):
        self.concurrency = initial_concurrency
        self.semaphore = asyncio.Semaphore(self.concurrency)
        self.lock = asyncio.Lock()

    async def report_success(self):
        async with self.lock:
            if self.concurrency < MAX_CONCURRENT_REQUESTS:
                self.concurrency += 1
                self.semaphore = asyncio.Semaphore(self.concurrency)

    async def report_error(self):
        async with self.lock:
            if self.concurrency > 5:
                self.concurrency = max(5, self.concurrency // 2)
                self.semaphore = asyncio.Semaphore(self.concurrency)

adaptive_limiter = AdaptiveLimiter(initial_concurrency=30)

# --- OPERACJE NA BAZIE SQLITE ---
async def init_db():
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS state (
                url TEXT PRIMARY KEY,
                hash TEXT,
                size INTEGER,
                last_modified TEXT,
                last_checked TEXT
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS portal_links (
                link TEXT PRIMARY KEY
            )
        """)
        await db.commit()

async def get_state_from_db(url):
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute("SELECT hash, size, last_modified, last_checked FROM state WHERE url = ?", (url,)) as cursor:
            row = await cursor.fetchone()
            if row:
                return {"hash": row[0], "size": row[1], "last_modified": row[2], "last_checked": row[3]}
    return None

async def save_state_to_db(url, data):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("""
            INSERT OR REPLACE INTO state (url, hash, size, last_modified, last_checked)
            VALUES (?, ?, ?, ?, ?)
        """, (url, data.get("hash"), data.get("size"), data.get("last_modified"), data.get("last_checked")))
        await db.commit()

async def save_portal_links_to_db(links):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("DELETE FROM portal_links")
        for link in links:
            await db.execute("INSERT OR IGNORE INTO portal_links (link) VALUES (?)", (link,))
        await db.commit()

async def get_portal_links_from_db():
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute("SELECT link FROM portal_links") as cursor:
            rows = await cursor.fetchall()
            return [row[0] for row in rows]

# --- FUNKCJE POMOCNICZE ---
def format_eta(seconds):
    if seconds < 0:
        return "00:00:00"
    return str(timedelta(seconds=int(seconds)))

def get_active_folders():
    folders = ["oddzialy", "o"]
    if CHECK_TEACHERS:
        folders.extend(["nauczyciele", "n"])
    if CHECK_STUDENTS_FOLDER:
        folders.extend(["uczniowie", "uczen"])
    if CHECK_SHORT_STUDENTS_FOLDER:
        folders.append("u")
    if CHECK_ROOMS:
        folders.extend(["sale", "s"])
        
    extra_folders = []
    for f in folders:
        if CHECK_KEYWORD_DOUBLING:
            extra_folders.append(f"{f}/{f}")
        if CHECK_KEYWORD_TRIPLING:
            extra_folders.append(f"{f}/{f}/{f}")
        if CHECK_KEYWORD_QUADRUPLING:
            extra_folders.append(f"{f}/{f}/{f}/{f}")
            
    return folders + extra_folders

def is_soft_404(page_text):
    if not page_text or len(page_text.strip()) < 200:
        return True
    
    text_lower = page_text.lower()
    error_keywords = [
        'nie znaleziono', 'błąd 404', 'strona nie istnieje', 
        'error 404', 'podana strona nie', 'nie ma takiej strony'
    ]
    for keyword in error_keywords:
        if keyword in text_lower:
            return True
    return False

def extract_links_from_html(html_content):
    if not html_content:
        return []
    cleaned_html = re.sub(r'https?://(?:www\.)?broniewski\.edu\.pl', '', html_content, flags=re.IGNORECASE)
    found = re.findall(r'href=["\']([^"\']*(?:planylekcji|plan|pliki)[^"\']*)["\']', cleaned_html, re.IGNORECASE)
    
    cleaned_links = set()
    for link in found:
        if link.startswith("http"):
            cleaned_links.add(link)
        elif link.startswith("/"):
            cleaned_links.add("https://broniewski.edu.pl" + link)
        else:
            cleaned_links.add(BASE_URL + link)
    return list(cleaned_links)

def get_target_months():
    now = datetime.now()
    cur_m = now.month
    next_m = (cur_m % 12) + 1
    if ONLY_CURRENT_AND_NEXT_MONTH:
        return [cur_m, next_m]
    return list(range(1, 13))

def generate_priority_structures():
    branches = set()
    years = ["2027"]
    short_years = [y[-2:] for y in years] 
    folder_mapping = get_active_folders()
    target_months = get_target_months()

    for year in years:
        for folder in folder_mapping:
            branches.add(f"{year}/{folder}/")

        if CHECK_HTML_SUBFOLDER:
            branches.add(f"{year}/html/")
            for folder in folder_mapping:
                branches.add(f"{year}/html/{folder}/")

        if CHECK_GENERATED_EXPORT:
            for sub in ["generated", "export"]:
                branches.add(f"{year}/{sub}/")
                for folder in folder_mapping:
                    branches.add(f"{year}/{sub}/{folder}/")

        for day in range(1, 32):
            for month in target_months:
                date_ddmm = f"{day:02d}{month:02d}"
                
                for folder in folder_mapping:
                    branches.add(f"{year}/{date_ddmm}/{folder}/")

                if CHECK_DATE_RANGES and day <= 27:
                    range_hyphen = f"{day:02d}-{day+4:02d}"
                    range_dot = f"{day:02d}.{day+4:02d}"
                    for folder in folder_mapping:
                        branches.add(f"{year}/{range_hyphen}/{folder}/")
                        branches.add(f"{year}/{range_dot}/{folder}/")

                if CHECK_DDMMYYYY_DATE_FOLDER:
                    date_ddmmyyyy = f"{day:02d}{month:02d}{year}"
                    for folder in folder_mapping:
                        branches.add(f"{year}/{date_ddmmyyyy}/{folder}/")

                if CHECK_DDMMYY_DATE_FOLDER:
                    for sy in short_years:
                        date_ddmmyy = f"{day:02d}{month:02d}{sy}"
                        for folder in folder_mapping:
                            branches.add(f"{year}/{date_ddmmyy}/{folder}/")

                if CHECK_COMPACT_DATE_FOLDERS:
                    date_dmmyy = f"{day}{month:02d}{year[-2:]}"
                    date_dmryy = f"{day}{month}{year[-2:]}"
                    for folder in folder_mapping:
                        branches.add(f"{year}/{date_dmmyy}/{folder}/")
                        branches.add(f"{year}/{date_dmryy}/{folder}/")

        for month in target_months:
            m_name = POLISH_MONTHS_MAP.get(month, "")
            m_num_str = f"{month:02d}"
            
            if CHECK_POLISH_MONTH_NAMES:
                for folder in folder_mapping:
                    branches.add(f"{year}/{m_name}/{folder}/")
            
            if CHECK_UNDERSCORE_FORMATS:
                for folder in folder_mapping:
                    branches.add(f"{year}_{m_name}/{folder}/")
                    branches.add(f"{year}_{m_num_str}/{folder}/")

    return list(branches)

def generate_brute_structures():
    branches = set()
    years = ["2027"]
    folder_mapping = get_active_folders()

    for year in years:
        for i in range(1, 100):
            num = f"{i:02d}"
            for folder in folder_mapping:
                branches.add(f"{year}/{num}/{folder}/")
                
        for i in range(1, 1000):
            num = f"{i:03d}"
            for folder in folder_mapping:
                branches.add(f"{year}/{num}/{folder}/")

        if CHECK_4DIGIT_FOLDERS:
            for i in range(1, 10000):
                num = f"{i:04d}"
                for folder in folder_mapping:
                    branches.add(f"{year}/{num}/{folder}/")

    return list(branches)

def log_change(message):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_entry = f"[{timestamp}] {message}\n"
    print(log_entry.strip())
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(log_entry)
        
    # Powiadomienie na Discord (#plany-lekcji) w przypadku wykrycia zmiany
    if WEBHOOK_PLANY:
        asyncio.create_task(send_discord_msg(WEBHOOK_PLANY, f"🚨 **Skaner Planów Lekcji**: {message}"))

def save_success_url(url):
    if os.path.exists(SUCCESS_FILE):
        with open(SUCCESS_FILE, "r", encoding="utf-8") as f:
            existing = f.read().splitlines()
        if url in existing:
            return
    with open(SUCCESS_FILE, "a", encoding="utf-8") as f:
        f.write(url + "\n")

def save_png_locally(url, raw_bytes):
    try:
        if ALT_BASE_URL in url:
            relative_path = url.replace(ALT_BASE_URL, "alt_plan/")
        elif "pliki/" in url:
            relative_path = url.replace("https://broniewski.edu.pl/pliki/", "pliki_plan/")
        else:
            relative_path = url.replace(BASE_URL, "")
            
        local_file_path = os.path.join(DOWNLOAD_DIR, relative_path)
        os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
        with open(local_file_path, "wb") as f:
            f.write(raw_bytes)
    except Exception as e:
        print(f"[!] Błąd zapisu pliku lokalnego {url}: {e}")

# --- ZAPYTANIA Z HTTP/2 ORAZ ADAPTIVE RATE LIMITING ---
async def throttled_get(client, url, timeout=7.0, retries=3, backoff=0.5):
    async with adaptive_limiter.semaphore:
        for attempt in range(retries + 1):
            try:
                response = await client.get(url, headers=HEADERS, timeout=timeout, follow_redirects=True)
                
                if response.status_code == 429:
                    await adaptive_limiter.report_error()
                    retry_after = response.headers.get('Retry-After')
                    base_wait = int(retry_after) if retry_after and retry_after.isdigit() else 15
                    
                    wait_time = base_wait + random.uniform(2.0, 7.0)
                    print(f"\n[!] Blokada/Ograniczenie (Kod 429) dla {url}. Czekam {wait_time:.1f}s...")
                    await asyncio.sleep(wait_time)
                    continue
                
                elif response.status_code in (500, 502, 503, 504):
                    await adaptive_limiter.report_error()
                    if attempt < retries:
                        await asyncio.sleep(backoff * (2 ** attempt))
                        continue
                
                if response.status_code not in (200, 404, 403):
                    print(f"[INFO] Nietypowy kod {response.status_code} dla adresu: {url}")

                await adaptive_limiter.report_success()
                return url, response
            
            except (httpx.TimeoutException, httpx.NetworkError, httpx.RequestError):
                await adaptive_limiter.report_error()
                if attempt == retries:
                    return url, None
                await asyncio.sleep(backoff * (attempt + 1))
            except Exception:
                return url, None
        return url, None

async def throttled_head(client, url, timeout=5.0):
    async with adaptive_limiter.semaphore:
        try:
            response = await client.head(url, headers=HEADERS, timeout=timeout, follow_redirects=True)
            if response.status_code in (403, 404, 405):
                return await throttled_get(client, url, timeout=timeout)
            
            if response.status_code not in (200, 404, 403):
                print(f"[INFO] (HEAD) Nietypowy kod {response.status_code} dla adresu: {url}")

            await adaptive_limiter.report_success()
            return url, response
        except Exception:
            await adaptive_limiter.report_error()
            return url, None

async def run_scan_cycle():
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"\n[*] [{current_time}] Uruchamianie skanera ULTIMATE v11...")
    
    # Jeśli uruchomiono skan na żądanie, wyślij komunikat na kanał #wymuś-skan
    if IS_MANUAL_RUN and WEBHOOK_STATUS:
        await send_discord_msg(WEBHOOK_STATUS, "⚙️ **Rozpoczynam skanowanie na żądanie planów lekcji...**")

    await init_db()
    changes_detected = False
    timeout_urls_temp = []
    active_links = []

    start_time = datetime.now()

    async with httpx.AsyncClient(http2=True) as client:
        
        # KROK 0: TEST WSTĘPNY ORAZ KLUCZOWE STRONY
        print(f"\n[*] Krok 0: Test wstępny i weryfikacja kluczowych stron...")
        s0_start = datetime.now()
        smoke_tasks = [throttled_get(client, url, timeout=5.0) for url in SMOKE_TEST_URLS]
        for future in asyncio.as_completed(smoke_tasks):
            await future

        key_tasks = [throttled_get(client, url, timeout=6.0) for url in KEY_MONITOR_URLS]
        for future in asyncio.as_completed(key_tasks):
            url, response = await future
            if response and response.status_code == 200:
                content_hash = hashlib.md5(response.content).hexdigest()
                current_data = {"hash": content_hash, "size": len(response.content), "last_checked": current_time}
                
                previous_data = await get_state_from_db(url)
                await save_state_to_db(url, current_data)

                # Sprawdzenie warunku Last-Modified dla 2 pierwszych kluczowych stron (<= 48h)
                if url in KEY_MONITOR_URLS[:2]:
                    last_mod_header = response.headers.get("Last-Modified")
                    if last_mod_header:
                        try:
                            mod_dt = parsedate_to_datetime(last_mod_header)
                            now_utc = datetime.now(mod_dt.tzinfo)
                            time_diff = now_utc - mod_dt
                            
                            if timedelta(0) <= time_diff <= timedelta(hours=48):
                                log_change(f"ALARM (Świeża modyfikacja <= 48h): Strona {url} była modyfikowana {time_diff.total_seconds() / 3600:.1f}h temu!")
                                changes_detected = True
                        except Exception as e:
                            print(f"[!] Nie udało się sparsować Last-Modified dla {url}: {e}")

                if previous_data and previous_data.get("hash") != content_hash:
                    log_change(f"ALARM: Zmiana na kluczowej stronie: {url}")
                    changes_detected = True
        
        s0_duration = (datetime.now() - s0_start).total_seconds()
        print(f"[*] [Krok 0] Zakończony w {s0_duration:.2f}s")

        # KROK 1: PORTAL I ŚCIEŻKI PRIORYTETOWE
        print(f"\n[*] Krok 1: Analiza portalu z planami i ścieżek priorytetowych...")
        _, portal_response = await throttled_get(client, PORTAL_URL)
        portal_extracted_links = []
        if portal_response and portal_response.status_code == 200:
            portal_extracted_links = extract_links_from_html(portal_response.text)
            await save_portal_links_to_db(portal_extracted_links)

        priority_branches = generate_priority_structures()
        for portal_link in portal_extracted_links:
            if "planylekcji/" in portal_link:
                sub_path = portal_link.split("planylekcji/")[-1]
                if sub_path and sub_path not in priority_branches:
                    priority_branches.append(sub_path)
            elif "plan/" in portal_link:
                sub_path = portal_link.split("plan/")[-1]
                if sub_path and sub_path not in priority_branches:
                    priority_branches.append(sub_path)

        built_tasks = []
        task_branch_mapping = {}

        for branch in priority_branches:
            suffix = branch if branch.endswith(".htm") or branch.endswith("/") else branch + "/oddzialy.htm"
            
            url_std = BASE_URL + suffix
            built_tasks.append(throttled_get(client, url_std))
            task_branch_mapping[url_std] = branch

            if CHECK_ALT_BASE_PLAN_PATH:
                url_alt = ALT_BASE_URL + suffix
                built_tasks.append(throttled_get(client, url_alt))
                task_branch_mapping[url_alt] = branch

            if CHECK_PLIKI_PLAN_PATH:
                url_pliki = f"https://broniewski.edu.pl/pliki/2027/plan/{branch}"
                built_tasks.append(throttled_get(client, url_pliki))
                task_branch_mapping[url_pliki] = f"pliki/2027/plan/{branch}"

        if CHECK_LEGACY_UCZNIOWIETH:
            legacy_base = "https://broniewski.edu.pl/plan/uczniowieth/"
            built_tasks.append(throttled_get(client, legacy_base + "index.html"))
            task_branch_mapping[legacy_base + "index.html"] = "uczniowieth"
            for prefix in ["a", "b", "c", "e"]:
                for i in range(1, 10):
                    l_url = f"{legacy_base}{prefix}{i}.html"
                    built_tasks.append(throttled_get(client, l_url))
                    task_branch_mapping[l_url] = "uczniowieth"

        total_priority = len(built_tasks)
        print(f"[*] [Krok 1] Liczba linków do przetestowania: {total_priority}")
        
        completed_p = 0
        step1_start = datetime.now()

        for future in asyncio.as_completed(built_tasks):
            index_url, resp = await future
            completed_p += 1
            
            if completed_p % 200 == 0 or completed_p == total_priority:
                elapsed = (datetime.now() - step1_start).total_seconds()
                if elapsed > 0 and completed_p > 0:
                    rate = completed_p / elapsed
                    eta_sec = int((total_priority - completed_p) / rate) if rate > 0 else 0
                    percent = (completed_p / total_priority) * 100
                    print(f"[*] [Krok 1] Postęp: {completed_p}/{total_priority} ({percent:.1f}%) | ETA: {format_eta(eta_sec)} | Wątki: {adaptive_limiter.concurrency}")

            if resp and resp.status_code == 200 and not is_soft_404(resp.text):
                branch = task_branch_mapping.get(index_url)
                h = hashlib.md5(resp.content).hexdigest()
                await save_state_to_db(index_url, {"hash": h, "size": len(resp.content), "last_checked": current_time})
                if branch and branch not in active_links:
                    active_links.append(branch)
                    print(f"\n[+] [ZNALEZIONO 200 OK] {index_url}")

        step1_duration = (datetime.now() - step1_start).total_seconds()
        s1_speed = (total_priority / step1_duration) if step1_duration > 0 else 0
        print(f"[*] [Krok 1] Zakończony w {step1_duration:.2f}s | Średnio: {s1_speed:.2f} adresów/s")

        # KROK 2: SKAN SEKWENCYJNY
        brute_branches = generate_brute_structures()
        total_brute = len(brute_branches) * (2 if CHECK_ALT_BASE_PLAN_PATH else 1)
        print(f"\n[*] Krok 2: Masowy skan sekwencyjny dla 2027...")
        print(f"[*] [Krok 2 - Skan] Liczba linków do przetestowania: {total_brute}")
        
        b_tasks = []
        b_branch_to_url = {}
        for branch in brute_branches:
            u_std = BASE_URL + branch + "oddzialy.htm"
            b_tasks.append(throttled_get(client, u_std))
            b_branch_to_url[u_std] = branch
            
            if CHECK_ALT_BASE_PLAN_PATH:
                u_alt = ALT_BASE_URL + branch + "oddzialy.htm"
                b_tasks.append(throttled_get(client, u_alt))
                b_branch_to_url[u_alt] = branch

        completed_b = 0
        step2_start = datetime.now()
        for future in asyncio.as_completed(b_tasks):
            index_url, response = await future
            completed_b += 1
            
            if completed_b % 200 == 0 or completed_b == total_brute:
                elapsed = (datetime.now() - step2_start).total_seconds()
                if elapsed > 0 and completed_b > 0:
                    rate = completed_b / elapsed
                    eta_sec = int((total_brute - completed_b) / rate) if rate > 0 else 0
                    percent = (completed_b / total_brute) * 100
                    print(f"[*] [Krok 2 - Skan] Postęp: {completed_b}/{total_brute} ({percent:.1f}%) | ETA: {format_eta(eta_sec)} | Wątki: {adaptive_limiter.concurrency}")

            if response is None:
                timeout_urls_temp.append(index_url)
            elif response.status_code == 200 and not is_soft_404(response.text):
                branch = b_branch_to_url.get(index_url)
                h = hashlib.md5(response.content).hexdigest()
                await save_state_to_db(index_url, {"hash": h, "size": len(response.content), "last_checked": current_time})
                if branch and branch not in active_links:
                    active_links.append(branch)
                    print(f"\n[+] [ZNALEZIONO 200 OK] {index_url}")

        step2_duration = (datetime.now() - step2_start).total_seconds()
        s2_speed = (total_brute / step2_duration) if step2_duration > 0 else 0
        print(f"[*] [Krok 2 - Skan] Zakończony w {step2_duration:.2f}s | Średnio: {s2_speed:.2f} adresów/s")
        print(f"[*] Skan sekwencyjny zakończony. Unikalnych aktywnych gałęzi: {len(active_links)}")

        # GŁĘBOKI SKAN ZASOBÓW Z WYKORZYSTANIEM LAST-MODIFIED (HEAD)
        target_urls = set()
        
        # Dodanie plików ramkowych w głównym katalogu, jeśli włączone
        if CHECK_FRAME_INDEX_FILES:
            for fname in ["index_n.htm", "index_o.htm", "index_s.htm"]:
                target_urls.add(BASE_URL + fname)
                if CHECK_ALT_BASE_PLAN_PATH:
                    target_urls.add(ALT_BASE_URL + fname)

        for branch in active_links:
            if "uczniowieth" in branch:
                continue 
            target_urls.add(BASE_URL + branch)
            target_urls.add(BASE_URL + branch + "oddzialy.htm")
            if CHECK_ALT_BASE_PLAN_PATH:
                target_urls.add(ALT_BASE_URL + branch)
                target_urls.add(ALT_BASE_URL + branch + "oddzialy.htm")

            if CHECK_FRAME_INDEX_FILES:
                for fname in ["index_n.htm", "index_o.htm", "index_s.htm"]:
                    target_urls.add(BASE_URL + branch + fname)
                    if CHECK_ALT_BASE_PLAN_PATH:
                        target_urls.add(ALT_BASE_URL + branch + fname)

            is_special_branch = any(k in branch for k in ["nauczyciele", "/n/", "uczniowie", "uczen", "/u/", "sale", "/s/"])
            max_slides = 54 if is_special_branch else 22
            for i in range(max_slides):
                target_urls.add(BASE_URL + branch + f"img{i}.png")
                target_urls.add(BASE_URL + branch + f"img{i}.html")
                if CHECK_ALT_BASE_PLAN_PATH:
                    target_urls.add(ALT_BASE_URL + branch + f"img{i}.png")
                    target_urls.add(ALT_BASE_URL + branch + f"img{i}.html")

        target_urls = list(target_urls)
        total_links = len(target_urls)
        print(f"\n[*] [Krok 2 - Zasoby] Liczba linków do przetestowania: {total_links}")

        tasks = [throttled_head(client, url) for url in target_urls]
        completed_links = 0
        step2_deep_start = datetime.now()

        for future in asyncio.as_completed(tasks):
            url, response = await future
            completed_links += 1
            
            if completed_links % 200 == 0 or completed_links == total_links:
                elapsed = (datetime.now() - step2_deep_start).total_seconds()
                if elapsed > 0 and completed_links > 0:
                    rate = completed_links / elapsed
                    eta_sec = int((total_links - completed_links) / rate) if rate > 0 else 0
                    percent = (completed_links / total_links) * 100
                    print(f"[*] [Krok 2 - Zasoby] Postęp: {completed_links}/{total_links} ({percent:.1f}%) | ETA: {format_eta(eta_sec)} | Wątki: {adaptive_limiter.concurrency}")

            if response and response.status_code == 200:
                if url.endswith((".htm", ".html")) and is_soft_404(response.text if hasattr(response, 'text') else ""):
                    continue

                last_mod = response.headers.get("Last-Modified", "")
                previous_data = await get_state_from_db(url)
                
                if previous_data and previous_data.get("last_modified") == last_mod:
                    if url.endswith(".png"):
                        save_success_url(url)
                    continue

                _, full_resp = await throttled_get(client, url)
                if full_resp and full_resp.status_code == 200:
                    raw_bytes = full_resp.content
                    file_hash = hashlib.md5(raw_bytes).hexdigest()
                    
                    await save_state_to_db(url, {
                        "hash": file_hash,
                        "size": len(raw_bytes),
                        "last_modified": last_mod,
                        "last_checked": current_time
                    })

                    if url.endswith((".htm", ".html", "oddzialy.htm")):
                        save_success_url(url)

                    if url.endswith(".png"):
                        save_png_locally(url, raw_bytes)

                    if previous_data:
                        log_change(f"ALARM: Zmiana wykryta w zasobie: {url}")
                        changes_detected = True
                    else:
                        if url.endswith(".png") or url.endswith("oddzialy.htm"):
                            log_change(f"NOWOŚĆ: Nowy zasób -> {url}")
                            save_success_url(url)

        step2_deep_duration = (datetime.now() - step2_deep_start).total_seconds()
        s2_deep_speed = (total_links / step2_deep_duration) if step2_deep_duration > 0 else 0
        print(f"[*] [Krok 2 - Zasoby] Zakończony w {step2_deep_duration:.2f}s | Średnio: {s2_deep_speed:.2f} adresów/s")

        # KROK 3: DOGRYWKA TIMEOUTÓW
        if timeout_urls_temp:
            print(f"\n[*] Krok 3: Dogrywka dla {len(timeout_urls_temp)} linków z timeoutem...")
            s3_start = datetime.now()
            for t_url in timeout_urls_temp:
                _, t_resp = await throttled_get(client, t_url, timeout=15.0)
                if t_resp and t_resp.status_code == 200 and not is_soft_404(t_resp.text):
                    h = hashlib.md5(t_resp.content).hexdigest()
                    await save_state_to_db(t_url, {"hash": h, "size": len(t_resp.content), "last_checked": current_time})
                    save_success_url(t_url)
                    print(f"\n[+] [ZNALEZIONO 200 OK (Dogrywka)] {t_url}")
            s3_duration = (datetime.now() - s3_start).total_seconds()
            print(f"[*] [Krok 3] Zakończony w {s3_duration:.2f}s")

    total_duration = (datetime.now() - start_time).total_seconds()
    print(f"\n[*] Cykl zakończony. Całkowity czas: {total_duration:.2f} sekund.")

    # Komunikat końcowy w przypadku ręcznego skanowania
    if IS_MANUAL_RUN and WEBHOOK_STATUS:
        status_msg = "✅ **Skanowanie planów zakończone!**"
        if changes_detected:
            status_msg += " Wykryto nowe plany lub zmiany! Zobacz kanał <#plany-lekcji>."
        else:
            status_msg += " Brak nowych planów ani zmian."
        await send_discord_msg(WEBHOOK_STATUS, status_msg)

    return changes_detected

async def main():
    if DAEMON_MODE:
        while True:
            try:
                await run_scan_cycle()
            except Exception as e:
                print(f"[!] Błąd: {e}")
            await asyncio.sleep(CHECK_INTERVAL_SECONDS)
    else:
        try:
            await run_scan_cycle()
        except Exception as e:
            print(f"[!] Błąd: {e}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[*] Zatrzymano.")