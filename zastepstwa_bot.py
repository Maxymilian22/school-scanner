import os
import re
import httpx
import hashlib
import asyncio
import aiosqlite
import random
import subprocess
from datetime import datetime, timezone, timedelta

# --- KONFIGURACJA ---
DAEMON_MODE = True  # Tryb pętli na czas działania GitHub Actions (max 6h)
TZ_POLAND = timezone(timedelta(hours=2))  # UTC+2 (czas polski)

URL_ZASTEPSTWA = "https://broniewski.edu.pl/index.php/zmiany-w-planie"
BASE_URL = "https://broniewski.edu.pl"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_FILE = os.path.join(BASE_DIR, "zastepstwa_state.db")

WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL_2")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

def get_target_date_text() -> str:
    """
    Wyznacza datę docelową zastępstw w języku polskim:
    - Piątek po 12:00, Sobota, Niedziela -> Data najbliższego Poniedziałku
    - Po godz. 12:00 (Pn-Czw) -> Data jutrzejsza
    - Przed godz. 12:00 (Pn-Pt) -> Data dzisiejsza
    """
    now = datetime.now(TZ_POLAND)
    weekday = now.weekday()  # 0=Pon, 1=Wt, ..., 4=Pt, 5=Sob, 6=Niedz
    hour = now.hour

    # Okno weekendowe: od Piątku 12:00 do końca Niedzieli
    if (weekday == 4 and hour >= 12) or weekday >= 5:
        days_until_monday = (7 - weekday) % 7
        if days_until_monday == 0:
            days_until_monday = 7  # Gdyby wywołać w poniedziałek, ale ten warunek wyklucza
        target_date = now + timedelta(days=days_until_monday)
    # Dni powszednie po 12:00 -> jutro
    elif hour >= 12:
        target_date = now + timedelta(days=1)
    # Przed 12:00 -> dzisiaj
    else:
        target_date = now

    days_pol = ["poniedziałek", "wtorek", "środa", "czwartek", "piątek", "sobota", "niedziela"]
    day_name = days_pol[target_date.weekday()]
    formatted_date = target_date.strftime("%d.%m.%Y")

    return f"{day_name}, {formatted_date}"

def commit_db_to_github():
    """Wysyła zaktualizowany plik bazy danych z powrotem do repozytorium GitHub z autoryzacją."""
    token = os.getenv("GITHUB_TOKEN")
    repo_url = os.getenv("GITHUB_REPOSITORY")
    
    if not token or not repo_url:
        print("[!] Brak tokenu GITHUB_TOKEN lub nazwy repozytorium w środowisku.")
        return

    try:
        subprocess.run(["git", "config", "user.name", "github-actions[bot]"], check=True)
        subprocess.run(["git", "config", "user.email", "github-actions[bot]@users.noreply.github.com"], check=True)
        
        auth_url = f"https://x-access-token:{token}@github.com/{repo_url}.git"
        subprocess.run(["git", "remote", "set-url", "origin", auth_url], check=True)

        subprocess.run(["git", "add", "zastepstwa_state.db"], check=True)
        
        result = subprocess.run(["git", "commit", "-m", "Auto-update stanu bazy danych [skip ci]"], capture_output=True, text=True)
        if "nothing to commit" not in result.stdout:
            subprocess.run(["git", "push"], check=True)
            print("[+] Zapisano i wysłano stan bazy danych do repozytorium GitHub.")
        else:
            print("[*] Brak nowych zmian w bazie danych do wysłania.")
    except Exception as e:
        print(f"[!] Błąd zapisywania bazy do GitHub: {e}")

async def init_db():
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS state (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                primary_image TEXT,
                image_hash TEXT
            )
        """)
        await db.commit()

async def get_saved_state():
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute("SELECT primary_image, image_hash FROM state WHERE id = 1") as cursor:
            row = await cursor.fetchone()
            if row:
                return {"primary_image": row[0], "image_hash": row[1]}
    return None

async def save_state(primary_image, image_hash):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("""
            INSERT OR REPLACE INTO state (id, primary_image, image_hash)
            VALUES (1, ?, ?)
        """, (primary_image, image_hash))
        await db.commit()
    commit_db_to_github()

async def send_discord_alert(image_urls, images_bytes_list, date_text):
    if not WEBHOOK_URL:
        print("[!] Brak skonfigurowanego DISCORD_WEBHOOK_URL.")
        return

    content = f"📢 **NOWE ZASTĘPSTWA!** ({date_text})\n"
    
    files = {}
    for idx, (url, img_bytes) in enumerate(zip(image_urls, images_bytes_list)):
        filename = os.path.basename(url)
        files[f"file{idx}"] = (filename, img_bytes, "image/png")

    async with httpx.AsyncClient() as client:
        try:
            if files:
                await client.post(WEBHOOK_URL, data={"content": content}, files=files, timeout=30.0)
            else:
                await client.post(WEBHOOK_URL, json={"content": content}, timeout=10.0)
            print("[+] Pomyślnie wysłano powiadomienie na Discorda.")
        except Exception as e:
            print(f"[!] Błąd wysyłania na Discorda: {e}")

async def check_zastepstwa():
    await init_db()

    async with httpx.AsyncClient(http2=True, headers=HEADERS, follow_redirects=True) as client:
        try:
            response = await client.get(URL_ZASTEPSTWA, timeout=10.0)
            if response.status_code != 200:
                print(f"[!] Nie udało się pobrać strony (Kod: {response.status_code})")
                return
        except Exception as e:
            print(f"[!] Błąd połączenia ze stroną: {e}")
            return

        html = response.text

        # Wyznaczenie daty z wykorzystaniem logiki zegara w Pythonie
        date_text = get_target_date_text()

        image_matches = re.findall(r'src=["\'](/images/[a-zA-Z0-9_-]+\.png)["\']', html)
        
        if not image_matches:
            print("[*] Nie znaleziono obrazków zastępstw w kodzie HTML.")
            return

        full_image_urls = [BASE_URL + img if img.startswith("/") else BASE_URL + "/" + img for img in image_matches]
        primary_image_url = full_image_urls[0]

        # Pobieranie obrazków z próbami ponowienia (Retry x3)
        images_bytes_list = []
        for img_url in full_image_urls:
            success = False
            for attempt in range(3):
                try:
                    img_res = await client.get(img_url, timeout=10.0)
                    if img_res.status_code == 200:
                        images_bytes_list.append(img_res.content)
                        success = True
                        break
                except Exception:
                    pass
                await asyncio.sleep(1)
            
            if not success:
                print(f"[!] Ostrzeżenie: Nie udało się pobrać obrazka po 3 próbach: {img_url}")

        if not images_bytes_list:
            print("[!] Nie udało się pobrać żadnej grafiki zastępstw.")
            return

        primary_image_bytes = images_bytes_list[0]
        current_hash = hashlib.md5(primary_image_bytes).hexdigest()

        saved_state = await get_saved_state()

        if saved_state is None:
            print(f"[*] Pierwsze uruchomienie. Wykryto obrazek: {primary_image_url}")
            await save_state(primary_image_url, current_hash)
            await send_discord_alert(full_image_urls, images_bytes_list, date_text)
        elif saved_state["primary_image"] != primary_image_url or saved_state["image_hash"] != current_hash:
            print(f"[!] ZMIANA ZASTĘPSTW! Nowy plik: {primary_image_url}")
            await save_state(primary_image_url, current_hash)
            await send_discord_alert(full_image_urls, images_bytes_list, date_text)
        else:
            print("[*] Brak zmian w zastępstwach.")

async def main():
    if DAEMON_MODE:
        print("[*] Uruchamianie w trybie DAEMON na GitHub Actions...")
        while True:
            sleep_time = 300
            try:
                now_pl = datetime.now(TZ_POLAND)
                current_hour = now_pl.hour
                weekday = now_pl.weekday()
                
                is_night = 1 <= current_hour < 4
                is_weekend = weekday >= 5
                is_slow_window = 10 <= current_hour < 12

                if is_night:
                    print(f"[*] [{now_pl.strftime('%H:%M')}] Nocna cisza – usypianie na 1 godzinę.")
                    sleep_time = 3600
                else:
                    await check_zastepstwa()
                    
                    if is_weekend:
                        sleep_time = random.randint(1200, 2400)  # 20-40 min
                        print(f"[*] [{now_pl.strftime('%H:%M')}] Weekend. Kolejny skan za {sleep_time // 60} min.")
                    elif is_slow_window:
                        sleep_time = random.randint(840, 960)    # 14-16 min
                        print(f"[*] [{now_pl.strftime('%H:%M')}] Okno 10-12. Kolejny skan za {sleep_time // 60} min.")
                    else:
                        sleep_time = random.randint(240, 360)    # 4-6 min
                        print(f"[*] [{now_pl.strftime('%H:%M')}] Standardowy skan. Kolejny skan za {sleep_time // 60} min.")

            except Exception as e:
                print(f"[!] Wystąpił błąd w pętli: {e}")
                sleep_time = 300

            await asyncio.sleep(sleep_time)
    else:
        print("[*] Uruchamianie w trybie POJEDYNCZYM...")
        await check_zastepstwa()

if __name__ == "__main__":
    asyncio.run(main())
