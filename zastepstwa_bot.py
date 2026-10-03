import os
import re
import httpx
import hashlib
import asyncio
import aiosqlite

URL_ZASTEPSTWA = "https://broniewski.edu.pl/index.php/zmiany-w-planie"
BASE_URL = "https://broniewski.edu.pl"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_FILE = os.path.join(BASE_DIR, "zastepstwa_state.db")

WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL_2")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

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

async def send_discord_alert(image_urls, primary_image_bytes, date_text):
    if not WEBHOOK_URL:
        print("[!] Brak skonfigurowanego DISCORD_WEBHOOK_URL.")
        return

    content = f"📢 **NOWE ZASTĘPSTWA!** ({date_text})\n"
    for idx, url in enumerate(image_urls, start=1):
        content += f"🖼️ Grafika {idx}: {url}\n"

    files = {}
    if primary_image_bytes:
        filename = os.path.basename(image_urls[0])
        files = {"file": (filename, primary_image_bytes, "image/png")}

    async with httpx.AsyncClient() as client:
        try:
            if files:
                await client.post(WEBHOOK_URL, data={"content": content}, files=files, timeout=15.0)
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

        # 1. Wyciągamy nagłówek daty (np. ZASTĘPSTWA W DNIU 05.10.2026)
        date_match = re.search(r'<h2>(.*?)</h2>', html, re.IGNORECASE)
        date_text = date_match.group(1).strip() if date_match else "Brak daty"

        # 2. Wyciągamy wszystkie obrazy PNG z katalogu /images/
        image_matches = re.findall(r'src=["\'](/images/[a-zA-Z0-9_-]+\.png)["\']', html)
        
        if not image_matches:
            print("[*] Nie znaleziono obrazków zastępstw w kodzie HTML.")
            return

        # Budujemy pełne adresy URL
        full_image_urls = [BASE_URL + img if img.startswith("/") else BASE_URL + "/" + img for img in image_matches]
        primary_image_url = full_image_urls[0]

        # 3. Pobieramy pierwszy (najważniejszy) obrazek i liczymy jego hash
        img_response = await client.get(primary_image_url, timeout=10.0)
        if img_response.status_code != 200:
            print("[!] Nie udało się pobrać głównej grafiki zastępstw.")
            return

        primary_image_bytes = img_response.content
        current_hash = hashlib.md5(primary_image_bytes).hexdigest()

        # 4. Porównujemy stan z bazą danych
        saved_state = await get_saved_state()

        if saved_state is None:
            # Pierwsze uruchomienie
            print(f"[*] Pierwsze uruchomienie. Wykryto obrazek: {primary_image_url}")
            await save_state(primary_image_url, current_hash)
            await send_discord_alert(full_image_urls, primary_image_bytes, date_text)
        elif saved_state["primary_image"] != primary_image_url or saved_state["image_hash"] != current_hash:
            # Zmiana nazwy pliku LUB zmiana zawartości grafiki!
            print(f"[!] ZMIANA ZASTĘPSTW! Nowy plik: {primary_image_url}")
            await save_state(primary_image_url, current_hash)
            await send_discord_alert(full_image_urls, primary_image_bytes, date_text)
        else:
            print("[*] Brak zmian w zastępstwach.")

if __name__ == "__main__":
    asyncio.run(check_zastepstwa())