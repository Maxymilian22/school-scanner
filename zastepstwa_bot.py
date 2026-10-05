import os
import re
import httpx
import hashlib
import asyncio
import aiosqlite

# --- KONFIGURACJA TRYBU DAEMON ---
DAEMON_MODE = True  # Ustaw na True, jeśli chcesz uruchomić w ciągłej pętli
COOLDOWN_SECONDS = 300  # Czas oczekiwania w sekundach (5 minut)

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

async def send_discord_alert(image_urls, images_bytes_list, date_text):
    if not WEBHOOK_URL:
        print("[!] Brak skonfigurowanego DISCORD_WEBHOOK_URL.")
        return

    content = f"📢 **NOWE ZASTĘPSTWA!** ({date_text})\n"
    
    # Pakujemy wszystkie pobrane pliki graficzne jako załączniki do webhooka
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
            print("[+] Pomyślnie wysłano powiadomienie na Discorda z pełnymi obrazkami.")
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

        # 1. Wyciągamy nagłówek daty
        date_match = re.search(r'<h2>(.*?)</h2>', html, re.IGNORECASE)
        date_text = date_match.group(1).strip() if date_match else "Brak daty"

        # 2. Wyciągamy wszystkie obrazy PNG z katalogu /images/
        image_matches = re.findall(r'src=["\'](/images/[a-zA-Z0-9_-]+\.png)["\']', html)
        
        if not image_matches:
            print("[*] Nie znaleziono obrazków zastępstw w kodzie HTML.")
            return

        full_image_urls = [BASE_URL + img if img.startswith("/") else BASE_URL + "/" + img for img in image_matches]
        primary_image_url = full_image_urls[0]

        # 3. Pobieramy WSZYSTKIE grafik do pamięci (żeby wysłać je jako fizyczne pliki)
        images_bytes_list = []
        for img_url in full_image_urls:
            try:
                img_res = await client.get(img_url, timeout=10.0)
                if img_res.status_code == 200:
                    images_bytes_list.append(img_res.content)
            except Exception as e:
                print(f"[!] Błąd pobierania obrazka {img_url}: {e}")

        if not images_bytes_list:
            print("[!] Nie udało się pobrać żadnej grafiki zastępstw.")
            return

        # Obliczamy hash pierwszej (głównej) grafiki do weryfikacji zmian
        primary_image_bytes = images_bytes_list[0]
        current_hash = hashlib.md5(primary_image_bytes).hexdigest()

        # 4. Porównujemy stan z bazą danych
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
        print(f"[*] Uruchamianie w trybie DAEMON (cooldown: {COOLDOWN_SECONDS}s)...")
        while True:
            try:
                await check_zastepstwa()
            except Exception as e:
                print(f"[!] Wystąpił nieoczekiwany błąd w pętli: {e}")
            await asyncio.sleep(COOLDOWN_SECONDS)
    else:
        print("[*] Uruchamianie w trybie POJEDYNCZYM (GitHub Actions)...")
        await check_zastepstwa()

if __name__ == "__main__":
    asyncio.run(main())
