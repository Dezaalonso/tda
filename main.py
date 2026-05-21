from fastapi import FastAPI, UploadFile, File, Depends, HTTPException, Header
from fastapi.staticfiles import StaticFiles
import requests
import time
import random
from psycopg2.pool import SimpleConnectionPool
import re
from ddgs import DDGS
from rapidfuzz import fuzz
from fastapi.middleware.cors import CORSMiddleware
from io import BytesIO
from PIL import Image
import os
import shutil
import threading
from datetime import datetime
from contextlib import asynccontextmanager
from pydantic import BaseModel
from typing import Optional
from dotenv import load_dotenv
load_dotenv()


# ⚙️ CONFIG
BASE_URL = "https://golosinasdacom.sistemerp.com"
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "port": os.getenv("DB_PORT")
}
ERP_LOGIN = os.getenv("ERP_LOGIN", "armando@dacom.com")
ERP_PASSWORD = os.getenv("ERP_PASSWORD", "004795322")
ADMIN_API_KEY = os.getenv("ADMIN_API_KEY", "changeme123")

# Image validation settings
MAX_IMAGE_SIZE_MB = 2
MAX_IMAGE_WIDTH = 800
MAX_IMAGE_HEIGHT = 800
ALLOWED_IMAGE_TYPES = ['JPEG', 'PNG', 'JPG', 'WEBP']
ALLOWED_CONTENT_TYPES = ["image/jpeg", "image/png", "image/webp", "image/jpg"]

# Pull settings
BATCH_SIZE = 30
REST_MINUTES = 2
MAX_DAILY_FETCHES = 300

# Upload directory
IMAGES_DIR = "product_images_upload"
os.makedirs(IMAGES_DIR, exist_ok=True)

# 🏪 Store sites (fallback only)
STORE_SITES = [
    "site:plazavea.com.pe",
    "site:tottus.com.pe",
    "site:wong.pe",
    "site:mercadolibre.com.pe",
    "site:vivanda.com.pe",
    "site:makro.com.pe",
    "site:linio.com.pe",
    "site:falabella.com.pe",
]

# 🚫 Bad sources
BAD_SOURCES = [
    "mysourcedepot", "autozone", "hardware", "tools",
    "vehicle", "motor", "engine", "belt", "mechanical",
    "dental", "clinic", "teeth", "oatmeal", "workout",
    "fitness", "gym", "gettyimages", "shutterstock",
    "eporner", "xvideos", "pornhub", "xhamster",
    "redtube", "youporn", "tube8", "spankbang", "xnxx",
]

BAD_TITLE_WORDS = [
    "porn", "sex", "xxx", "nude", "naked", "cosplay sex",
    "wallpaper", "meme", "tattoo", "automotive", "car belt",
    "hentai", "escort", "erotic",
]

# 🚫 IMAGE BLOCKLIST DOMAINS
BLOCKLIST_DOMAINS = [
    "feriaemprendedora.com",
    "lagranbodega.com.pe",
    "tiendaperuonline.com",
    "minimarketmajaz.com",
    "i.btcdn.co",
    "distribuidoralacruz.com",
    "farmacenter.com.pe",
    "droxicolprd.vtexassets.com",
    "pages.am-usercontent.com",
    "candylandperu.com",
    "www.officedepot.com.mx",
    "lolliesnstuff.com.au",
    "a-static.mlcdn.com.br",
    "oriundo.pe",
    "shutterstock.com",
    "istockphoto.com",
    "gettyimages.com",
    "dreamstime.com",
    "unsplash.com",
    "pexels.com",
    "alamy.com",
    "home.ripley.com.pe",
    "eporner.com",
    "xvideos.com",
    "pornhub.com",
    "xhamster.com",
    "redtube.com",
    "youporn.com",
    "tube8.com",
    "spankbang.com",
    "xnxx.com",
]

BLOCKLIST_PATTERNS = [
    "logo", "icon", "avatar", "favicon",
    "placeholder", "banner", "thumbnail",
]

# 🔥 CATEGORIES
CATEGORIES = {
    "Drinks": ["coca", "pepsi", "fanta", "jugo", "agua", "bebida", "oriundo", "inca", "sprite", "dr. pepper", "starbucks", "crush", "frumas", "frappuccino"],
    "Snacks": ["lays", "snax", "chicharron", "cheetos", "tortees", "cuates", "mexi", "prezlet", "snyder", "alfajores arequipeños", "cereal", "pringles", "act", "combos", "nachos", "atomico"],
    "Salsas": ["spitze"],
    "Conservas": ["florida"],
    "Colageno": ["colageno"],
    "Chocolates": ["hershey", "kisses", "snickers", "reeses", "milky way", "twix", "iberica", "m&m", "pirucream", "kinder", "sublime", "triangulo", "princesa", "ferrero", "toblerone", "monfer", "trento"],
    "Galletas": ["casino", "san jorge", "club social", "margarita", "chomp", "marquesitas", "tentacion", "glacitas", "soda", "field", "ritz", "integrakers", "costa", "animalitos", "black out", "coco nut", "municion", "fruta mixta", "rellenita", "galleta", "fibra", "nutri deli", "almoahada", "chaplin", "picaras", "chips ahoy", "morochas", "crackelet", "nik", "wafer", "cua cua", "oreo", "marquesita"],
    "Golosinas": ["trolli", "trululu", "crismelo", "mentos", "skittles", "nerds", "starburst", "tic tac", "chicle", "bubb", "globo", "caramelo", "big ben", "truffle", "gum", "candy", "pop"],
    "Alcohol": ["cerveza", "vino", "whisky", "ron", "pisco", "tres cruces", "heineken"],
    "Limpieza": ["tuinies", "gillete", "toallitas", "rexona", "desodorante", "axe", "dove", "suave", "noble", "elite", "servilleta", "papel", "jabon", "colgate", "pasta dental", "kolynos", "cepillo", "amaras", "h&s", "shampoo", "pantene", "ayudin", "nosotras", "gillette", "bahia", "floresta"],
    "Pilas": ["duracell"],
}

GENERIC_WORDS = {
    "de", "la", "el", "los", "las", "con", "sin", "x",
    "grs", "gr", "ml", "lt", "kg", "und", "unds", "pqte",
    "pote", "pack", "pqt", "unid", "onzas", "oz", "the",
    "and", "for", "with",
}

PLACEHOLDERS = {
    "Drinks": "https://placehold.co/400x400/blue/white?text=Bebida",
    "Snacks": "https://placehold.co/400x400/orange/white?text=Snack",
    "Chocolates": "https://placehold.co/400x400/brown/white?text=Chocolate",
    "Galletas": "https://placehold.co/400x400/gold/white?text=Galleta",
    "Golosinas": "https://placehold.co/400x400/pink/white?text=Golosina",
    "Limpieza": "https://placehold.co/400x400/green/white?text=Limpieza",
    "Alcohol": "https://placehold.co/400x400/purple/white?text=Alcohol",
    "Other": "https://placehold.co/400x400/gray/white?text=Producto"
}

# 🔒 Thread-safe cache
cache_lock = threading.Lock()
cache = {"data": None, "timestamp": 0, "is_loading": False}

# 🛑 Shutdown signal
shutdown_event = threading.Event()

# Daily fetch counter
daily_fetch_count = 0
daily_fetch_date = datetime.now().date()
fetch_count_lock = threading.Lock()

# Batch counter
current_batch_count = 0
batch_count_lock = threading.Lock()


# ─────────────────────────────────────────────
# 🗄️ DATABASE
# ─────────────────────────────────────────────

connection_pool = SimpleConnectionPool(
    1, 20,
    host=DB_CONFIG["host"],
    database=DB_CONFIG["database"],
    user=DB_CONFIG["user"],
    password=DB_CONFIG["password"],
    port=DB_CONFIG["port"]
)

def get_db_connection():
    return connection_pool.getconn()

def release_db_connection(conn):
    connection_pool.putconn(conn)

def init_db():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS products (
            name TEXT PRIMARY KEY,
            list_price REAL,
            qty_available REAL,
            category TEXT,
            custom_group TEXT,
            last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS product_images (
            name TEXT PRIMARY KEY,
            image TEXT,
            image_size REAL,
            width INTEGER,
            height INTEGER,
            last_fetch TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            fetch_attempts INTEGER DEFAULT 0,
            is_placeholder BOOLEAN DEFAULT FALSE,
            is_manual BOOLEAN DEFAULT FALSE
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS offers (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL,
            description TEXT,
            product_name TEXT,
            discount_percent REAL,
            active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            expires_at TIMESTAMP
        )
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_product_images_last_fetch
        ON product_images(last_fetch)
    """)

    # Add columns if they don't exist (for existing DBs)
    try:
        cur.execute("ALTER TABLE products ADD COLUMN IF NOT EXISTS custom_group TEXT")
        cur.execute("ALTER TABLE product_images ADD COLUMN IF NOT EXISTS is_manual BOOLEAN DEFAULT FALSE")
    except Exception:
        pass

    conn.commit()
    cur.close()
    release_db_connection(conn)
    print("✅ Database initialized")


# ─────────────────────────────────────────────
# 🔐 ADMIN AUTH
# ─────────────────────────────────────────────

def verify_admin(x_api_key: str = Header(...)):
    if x_api_key != ADMIN_API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")


# ─────────────────────────────────────────────
# 🖼️ IMAGE CACHE HELPERS
# ─────────────────────────────────────────────

def get_cached_image(name):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT image, is_placeholder FROM product_images WHERE name = %s", (name,))
    row = cur.fetchone()
    cur.close()
    release_db_connection(conn)
    return row[0] if row and not row[1] else None

def save_cached_image(name, image_url, image_size=None, width=None, height=None, is_placeholder=False, is_manual=False):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO product_images (name, image, image_size, width, height, last_fetch, fetch_attempts, is_placeholder, is_manual)
        VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP, 1, %s, %s)
        ON CONFLICT (name) DO UPDATE SET
            image = EXCLUDED.image,
            image_size = EXCLUDED.image_size,
            width = EXCLUDED.width,
            height = EXCLUDED.height,
            last_fetch = CURRENT_TIMESTAMP,
            fetch_attempts = product_images.fetch_attempts + 1,
            is_placeholder = EXCLUDED.is_placeholder,
            is_manual = EXCLUDED.is_manual
    """, (name, image_url, image_size, width, height, is_placeholder, is_manual))
    conn.commit()
    cur.close()
    release_db_connection(conn)

def update_fetch_attempt(name):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO product_images (name, fetch_attempts, last_fetch)
        VALUES (%s, 1, CURRENT_TIMESTAMP)
        ON CONFLICT (name) DO UPDATE SET
            fetch_attempts = product_images.fetch_attempts + 1,
            last_fetch = CURRENT_TIMESTAMP
    """, (name,))
    conn.commit()
    cur.close()
    release_db_connection(conn)

def get_missing_images_count():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(*)
        FROM products p
        LEFT JOIN product_images pi ON p.name = pi.name
        WHERE (pi.name IS NULL OR pi.is_placeholder = TRUE)
        AND (pi.is_manual IS NULL OR pi.is_manual = FALSE)
    """)
    count = cur.fetchone()[0]
    cur.close()
    release_db_connection(conn)
    return count

def update_live_cache_image(product_name, image_url):
    """Update image in live cache without full refresh"""
    with cache_lock:
        if cache["data"]:
            for cat, products in cache["data"].items():
                for p in products:
                    if p["name"] == product_name:
                        p["image"] = image_url
                        return


# ─────────────────────────────────────────────
# ⚙️ SETTINGS HELPERS
# ─────────────────────────────────────────────

def get_setting(key, default=None):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT value FROM settings WHERE key = %s", (key,))
    row = cur.fetchone()
    cur.close()
    release_db_connection(conn)
    return row[0] if row else default

def save_setting(key, value):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO settings (key, value, updated_at)
        VALUES (%s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (key) DO UPDATE SET
            value = EXCLUDED.value,
            updated_at = CURRENT_TIMESTAMP
    """, (key, value))
    conn.commit()
    cur.close()
    release_db_connection(conn)


# ─────────────────────────────────────────────
# 🔍 PRODUCT HELPERS
# ─────────────────────────────────────────────

def classify_product(name):
    name_lower = name.lower()
    for category, keywords in CATEGORIES.items():
        if any(word in name_lower for word in keywords):
            return category
    return "Other"

def get_brand_and_type(product_name):
    product_lower = product_name.lower()
    brand = None
    category = classify_product(product_name)

    for cat, keywords in CATEGORIES.items():
        for keyword in keywords:
            if keyword in product_lower:
                brand = keyword
                break
        if brand:
            break

    clean_name = re.sub(r'\b(x|pote|pqte|und|unds|pack|pqt)\b', '', product_lower)
    clean_name = re.sub(r'\s+', ' ', clean_name).strip()

    return brand, clean_name, category

def get_meaningful_words(product_name):
    words = product_name.lower().split()
    return [w for w in words if w not in GENERIC_WORDS and len(w) > 2]


# ─────────────────────────────────────────────
# 🖼️ IMAGE FETCHING
# ─────────────────────────────────────────────

def is_blocked_url(url):
    url_lower = url.lower()
    for domain in BLOCKLIST_DOMAINS:
        if domain in url_lower:
            return True
    for pattern in BLOCKLIST_PATTERNS:
        if pattern in url_lower:
            return True
    return False

def image_matches_product(img_url, product_name):
    url_lower = img_url.lower()
    bad_url_patterns = [
        "whale", "ballena", "wildlife", "nature", "animal",
        "landscape", "ocean", "sea-", "/sea/", "aerial",
        "forest", "mountain", "river", "sky", "cloud",
    ]
    for pattern in bad_url_patterns:
        if pattern in url_lower:
            return False
    return True

def is_bad_result(img_url, title, source):
    combined = f"{img_url} {title} {source}".lower()
    if any(bad in combined for bad in BAD_SOURCES):
        return True
    if any(bad in title.lower() for bad in BAD_TITLE_WORDS):
        return True
    return False

def validate_image_url(url):
    try:
        response = requests.get(url, timeout=15, stream=True)
        response.raise_for_status()

        content_length = response.headers.get('content-length')
        if content_length and int(content_length) / (1024 * 1024) > MAX_IMAGE_SIZE_MB:
            return False, None, None, None

        image_data = BytesIO()
        downloaded = 0
        for chunk in response.iter_content(chunk_size=8192):
            image_data.write(chunk)
            downloaded += len(chunk)
            if downloaded > MAX_IMAGE_SIZE_MB * 1024 * 1024:
                return False, None, None, None

        image_data.seek(0)
        img = Image.open(image_data)

        if (img.format or '').upper() not in ALLOWED_IMAGE_TYPES:
            return False, None, None, None

        width, height = img.size
        if width > MAX_IMAGE_WIDTH or height > MAX_IMAGE_HEIGHT:
            return False, None, None, None

        size_mb = downloaded / (1024 * 1024)
        print(f"   ✅ Valid image: {img.format} {width}x{height} ({size_mb:.2f}MB)")
        return True, size_mb, width, height

    except Exception as e:
        print(f"   ⚠️ Image validation error: {str(e)[:50]}")
        return False, None, None, None

def search_ddgs_images(query, product_name, meaningful_words, max_results=8):
    try:
        with DDGS() as ddgs:
            results = list(ddgs.images(
                query,
                max_results=max_results,
                safesearch="strict"
            ))
            for result in results:
                img_url = result.get('image', '')
                title = result.get('title', '').lower()
                source = result.get('source', '').lower()

                if not img_url or not img_url.startswith("http"):
                    continue
                if is_blocked_url(img_url):
                    continue
                if not image_matches_product(img_url, product_name):
                    continue
                if is_bad_result(img_url, title, source):
                    print(f"   🚫 Bad result: {source[:40]} | {title[:40]}")
                    continue

                print(f"   ✅ Match: {img_url[:70]}")
                return img_url
    except Exception as e:
        print(f"   ❌ DDGS error: {str(e)[:50]}")
    return None

def fetch_image(product_name):
    brand, clean_name, category = get_brand_and_type(product_name)
    meaningful_words = get_meaningful_words(product_name)

    simple_queries = []
    if brand:
        simple_queries.append(f"{clean_name} producto real")
        simple_queries.append(f"{clean_name} envase")
        simple_queries.append(f"{brand} {category}")
    else:
        simple_queries.append(f"{clean_name} producto")
        simple_queries.append(f"{clean_name} envase")

    print(f"\n🔍 Fetching image for: {product_name[:50]}")

    for query in simple_queries[:2]:
        if shutdown_event.is_set():
            break
        print(f"   🔎 Simple: {query[:60]}")
        result = search_ddgs_images(query, product_name, meaningful_words, max_results=8)
        if result:
            return result
        time.sleep(1)

    stores_to_try = random.sample(STORE_SITES, min(2, len(STORE_SITES)))
    for store in stores_to_try:
        if shutdown_event.is_set():
            break
        query = f"{clean_name} {store}"
        print(f"   🏪 Store: {query[:60]}")
        result = search_ddgs_images(query, product_name, meaningful_words, max_results=5)
        if result:
            return result
        time.sleep(0.8)

    if brand:
        query = f"{brand} {category} producto"
        print(f"   🔁 Last resort: {query[:60]}")
        result = search_ddgs_images(query, product_name, meaningful_words, max_results=10)
        if result:
            return result

    print(f"   ⚠️ No image found, using placeholder")
    return PLACEHOLDERS.get(category, PLACEHOLDERS["Other"])

def can_fetch_today():
    global daily_fetch_count, daily_fetch_date
    with fetch_count_lock:
        today = datetime.now().date()
        if today != daily_fetch_date:
            daily_fetch_date = today
            daily_fetch_count = 0
        return daily_fetch_count < MAX_DAILY_FETCHES

def increment_fetch_count():
    global daily_fetch_count, current_batch_count
    with fetch_count_lock:
        daily_fetch_count += 1
    with batch_count_lock:
        current_batch_count += 1


# ─────────────────────────────────────────────
# 🔄 ERP SYNC
# ─────────────────────────────────────────────

def sync_products_from_erp():
    session = requests.Session()
    auth_payload = {
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "db": "golosinasdacom_web",
            "login": ERP_LOGIN,
            "password": ERP_PASSWORD
        }
    }
    try:
        login_res = session.post(f"{BASE_URL}/web/session/authenticate", json=auth_payload)
        if "error" in login_res.json():
            print("❌ ERP login failed")
            return False
    except Exception as e:
        print(f"❌ ERP login error: {e}")
        return False

    all_records = []
    offset = 0
    limit = 80

    while True:
        try:
            payload = {
                "jsonrpc": "2.0",
                "method": "call",
                "params": {
                    "model": "product.product",
                    "domain": [["sale_ok", "=", True]],
                    "fields": ["name", "list_price", "qty_available"],
                    "limit": limit,
                    "offset": offset
                }
            }
            res = session.post(f"{BASE_URL}/web/dataset/search_read", json=payload).json()
            records = res.get("result", {}).get("records", [])
            if not records:
                break
            all_records.extend(records)
            offset += limit
        except Exception as e:
            print(f"❌ Error fetching products: {e}")
            break

    print(f"📦 Syncing {len(all_records)} products to local DB")
    conn = get_db_connection()
    cur = conn.cursor()
    for p in all_records:
        category = classify_product(p["name"])
        cur.execute("""
            INSERT INTO products (name, list_price, qty_available, category, last_seen)
            VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (name) DO UPDATE SET
                list_price = EXCLUDED.list_price,
                qty_available = EXCLUDED.qty_available,
                category = EXCLUDED.category,
                last_seen = CURRENT_TIMESTAMP
        """, (p["name"], p["list_price"], p["qty_available"], category))
    conn.commit()
    cur.close()
    release_db_connection(conn)
    print("✅ Product sync complete")
    return True


# ─────────────────────────────────────────────
# 📦 CACHE + ENRICHMENT
# ─────────────────────────────────────────────

def fetch_and_enrich():
    sync_products_from_erp()
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT p.name, p.list_price, p.qty_available,
               COALESCE(p.custom_group, p.category) as effective_category,
               p.category as original_category,
               pi.image, pi.is_placeholder
        FROM products p
        LEFT JOIN product_images pi ON p.name = pi.name
        ORDER BY p.name
    """)
    products = cur.fetchall()
    cur.close()
    release_db_connection(conn)

    grouped = {}
    for name, price, stock, effective_category, original_category, image, is_placeholder in products:
        cat = effective_category or classify_product(name)
        if cat not in grouped:
            grouped[cat] = []
        if is_placeholder or not image or image.startswith("https://placehold.co"):
            image = PLACEHOLDERS.get(original_category, PLACEHOLDERS["Other"])
        grouped[cat].append({
            "name": name,
            "price": price,
            "stock": stock,
            "image": image,
            "original_category": original_category
        })

    return grouped

def refresh_cache_task(background=True):
    with cache_lock:
        if cache["is_loading"]:
            return
        cache["is_loading"] = True
    try:
        print("🔄 Refreshing cache...")
        grouped = fetch_and_enrich()
        if grouped:
            with cache_lock:
                cache["data"] = grouped
                cache["timestamp"] = time.time()
            print(f"✅ Cache refreshed at {datetime.now()}")
    except Exception as e:
        print(f"❌ Cache refresh error: {e}")
    finally:
        with cache_lock:
            cache["is_loading"] = False


# ─────────────────────────────────────────────
# 🖼️ BACKGROUND IMAGE WORKER
# ─────────────────────────────────────────────

def process_batch(missing_products):
    global current_batch_count
    with batch_count_lock:
        current_batch_count = 0

    success_count = 0

    for product_name in missing_products:
        if shutdown_event.is_set():
            break
        if not can_fetch_today():
            print("⛔ Daily limit reached, stopping.")
            break

        with batch_count_lock:
            count = current_batch_count

        if count > 0 and count % BATCH_SIZE == 0:
            print(f"\n⏸️  Pulled {BATCH_SIZE} images — resting {REST_MINUTES} minutes...")
            shutdown_event.wait(timeout=REST_MINUTES * 60)
            if shutdown_event.is_set():
                break
            print("▶️  Resuming...")

        img_url = fetch_image(product_name)
        increment_fetch_count()

        is_placeholder_url = "placehold.co" in img_url or "placeholder" in img_url.lower()

        if not is_placeholder_url:
            is_valid, size_mb, width, height = validate_image_url(img_url)
            if is_valid:
                save_cached_image(product_name, img_url, size_mb, width, height, is_placeholder=False)
                print(f"   ✅ Saved: {product_name[:50]}")
                success_count += 1
                update_live_cache_image(product_name, img_url)
            else:
                save_cached_image(product_name, PLACEHOLDERS["Other"], 0, 0, 0, is_placeholder=True)
                update_fetch_attempt(product_name)
        else:
            save_cached_image(product_name, PLACEHOLDERS["Other"], 0, 0, 0, is_placeholder=True)
            update_fetch_attempt(product_name)

        time.sleep(2)

    return success_count

def get_all_missing_products():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT p.name
        FROM products p
        LEFT JOIN product_images pi ON p.name = pi.name
        WHERE (pi.name IS NULL OR pi.is_placeholder = TRUE)
        AND (pi.is_manual IS NULL OR pi.is_manual = FALSE)
        AND (pi.fetch_attempts IS NULL OR pi.fetch_attempts < 3)
        ORDER BY p.name
    """)
    products = [row[0] for row in cur.fetchall()]
    cur.close()
    release_db_connection(conn)
    return products

def continuous_image_worker():
    print("🖼️ Image worker started")
    while not shutdown_event.is_set():
        try:
            missing_products = get_all_missing_products()
            missing_count = len(missing_products)

            if missing_count == 0:
                print("✅ All images fetched. Sleeping for 1 hour.")
                shutdown_event.wait(timeout=3600)
                continue

            if not can_fetch_today():
                print("⛔ Daily limit reached. Sleeping until midnight...")
                now = datetime.now()
                seconds_until_midnight = (
                    ((24 - now.hour - 1) * 3600) +
                    ((60 - now.minute - 1) * 60) +
                    (60 - now.second)
                )
                shutdown_event.wait(timeout=seconds_until_midnight)
                continue

            print(f"\n🖼️ {missing_count} images missing — starting batch processing...")
            processed = process_batch(missing_products)

            if processed > 0:
                print(f"✅ Batch complete: {processed} new images saved")
                refresh_cache_task(background=False)

            shutdown_event.wait(timeout=10)

        except Exception as e:
            print(f"❌ Worker error: {e}")
            shutdown_event.wait(timeout=30)

    print("🛑 Image worker stopped")

def periodic_product_sync():
    print("🔄 Product sync worker started")
    while not shutdown_event.is_set():
        try:
            print("\n🔄 Periodic product sync...")
            sync_products_from_erp()
            refresh_cache_task(background=False)
        except Exception as e:
            print(f"❌ Sync error: {e}")
        shutdown_event.wait(timeout=3600)
    print("🛑 Sync worker stopped")


# ─────────────────────────────────────────────
# 🚀 APP LIFESPAN
# ─────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("🚀 Starting application...")
    init_db()
    sync_products_from_erp()
    refresh_cache_task(background=False)

    image_thread = threading.Thread(target=continuous_image_worker, daemon=True)
    image_thread.start()

    sync_thread = threading.Thread(target=periodic_product_sync, daemon=True)
    sync_thread.start()

    print("✅ All background services started")
    yield

    print("🛑 Shutting down background workers...")
    shutdown_event.set()
    image_thread.join(timeout=5)
    sync_thread.join(timeout=5)
    print("✅ Shutdown complete")

app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("ALLOWED_ORIGINS", "*").split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static/product-images", StaticFiles(directory=IMAGES_DIR), name="product-images")


# ─────────────────────────────────────────────
# 📡 PUBLIC ENDPOINTS
# ─────────────────────────────────────────────

@app.get("/products")
def get_products():
    with cache_lock:
        data = cache["data"]
        is_loading = cache["is_loading"]
    if data:
        return data
    if is_loading:
        return {"message": "Products are loading, please refresh in a moment"}
    refresh_cache_task(background=True)
    return {"message": "Products are being loaded, please refresh in a moment"}

@app.get("/products/status")
def get_status():
    missing_count = get_missing_images_count()
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM products")
    total_products = cur.fetchone()[0]
    cur.close()
    release_db_connection(conn)

    with cache_lock:
        cache_age = int(time.time() - cache["timestamp"]) if cache["timestamp"] else 0
        cache_loaded = cache["data"] is not None
        is_loading = cache["is_loading"]

    with fetch_count_lock:
        fetches_today = daily_fetch_count

    with batch_count_lock:
        batch_progress = current_batch_count

    return {
        "total_products": total_products,
        "missing_images": missing_count,
        "cache_age_seconds": cache_age,
        "cache_loaded": cache_loaded,
        "is_loading": is_loading,
        "fetches_today": fetches_today,
        "daily_fetch_limit": MAX_DAILY_FETCHES,
        "current_batch_progress": f"{batch_progress % BATCH_SIZE}/{BATCH_SIZE}",
        "rest_between_batches_minutes": REST_MINUTES,
    }

@app.get("/search")
def search(q: str):
    with cache_lock:
        data = cache["data"]
    if not data:
        return []

    results = []
    query = q.lower()
    for cat, products in data.items():
        for p in products:
            score = fuzz.partial_ratio(query, p["name"].lower())
            if score > 60:
                results.append({**p, "category": cat, "score": score})

    results.sort(key=lambda x: x["score"], reverse=True)
    return results[:20]

@app.get("/offers")
def get_offers():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, title, description, product_name, discount_percent, active, created_at, expires_at
        FROM offers
        WHERE active = TRUE
        AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)
        ORDER BY created_at DESC
    """)
    rows = cur.fetchall()
    cur.close()
    release_db_connection(conn)
    return [
        {
            "id": r[0], "title": r[1], "description": r[2],
            "product_name": r[3], "discount_percent": r[4],
            "active": r[5], "created_at": r[6], "expires_at": r[7]
        }
        for r in rows
    ]

@app.get("/contact")
def get_contact():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT key, value FROM settings WHERE key LIKE 'contact_%'")
    rows = cur.fetchall()
    cur.close()
    release_db_connection(conn)
    return {row[0].replace("contact_", ""): row[1] for row in rows}

@app.get("/groups")
def get_groups():
    """Get all available groups including custom ones"""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT COALESCE(custom_group, category) as group_name
        FROM products
        ORDER BY group_name
    """)
    rows = cur.fetchall()
    cur.close()
    release_db_connection(conn)
    return [row[0] for row in rows]


# ─────────────────────────────────────────────
# 🔐 ADMIN ENDPOINTS
# ─────────────────────────────────────────────

@app.get("/admin/products")
def admin_get_products(key: str = Depends(verify_admin)):
    """Get all products flat for admin table"""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT p.name, p.list_price, p.qty_available, p.category,
               p.custom_group, pi.image, pi.is_placeholder,
               pi.fetch_attempts, pi.is_manual, pi.last_fetch
        FROM products p
        LEFT JOIN product_images pi ON p.name = pi.name
        ORDER BY p.name
    """)
    rows = cur.fetchall()
    cur.close()
    release_db_connection(conn)
    return [
        {
            "name": r[0], "price": r[1], "stock": r[2],
            "category": r[3], "custom_group": r[4],
            "image": r[5], "is_placeholder": r[6],
            "fetch_attempts": r[7], "is_manual": r[8],
            "last_fetch": r[9]
        }
        for r in rows
    ]

@app.post("/admin/upload-image")
async def upload_product_image(
    name: str,  # query param instead of path param
    file: UploadFile = File(...),
    key: str = Depends(verify_admin)
):
    """Upload a local image for a product"""
    if file.content_type not in ALLOWED_CONTENT_TYPES:
        raise HTTPException(400, f"Only {ALLOWED_CONTENT_TYPES} allowed")

    # Read and validate image
    contents = await file.read()
    if len(contents) > MAX_IMAGE_SIZE_MB * 1024 * 1024:
        raise HTTPException(400, f"Image too large, max {MAX_IMAGE_SIZE_MB}MB")

    try:
        img = Image.open(BytesIO(contents))
        width, height = img.size
        if width > MAX_IMAGE_WIDTH or height > MAX_IMAGE_HEIGHT:
            raise HTTPException(400, f"Image too large, max {MAX_IMAGE_WIDTH}x{MAX_IMAGE_HEIGHT}px")
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(400, "Invalid image file")

    # Save to disk
    ext = file.filename.split(".")[-1].lower()
    safe_name = re.sub(r'[^a-z0-9]', '_', name.lower())
    filename = f"{safe_name}.{ext}"
    filepath = os.path.join(IMAGES_DIR, filename)

    with open(filepath, "wb") as f:
        f.write(contents)

    size_mb = len(contents) / (1024 * 1024) 

    # ✅ Save FULL URL, not relative path
    base_url = os.getenv("BASE_SERVER_URL", "http://localhost:8000")
    image_url = f"{base_url}/static/product-images/{filename}"

    save_cached_image(name, image_url, size_mb, width, height, is_placeholder=False, is_manual=True)
    update_live_cache_image(name, image_url)

    return {"message": "Image uploaded", "image_url": image_url}

@app.post("/admin/products/{name}/group")
def admin_set_group(name: str, body: dict, key: str = Depends(verify_admin)):
    """Move product to a custom group"""
    group = body.get("group", "").strip()
    if not group:
        raise HTTPException(400, "group is required")

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        "UPDATE products SET custom_group = %s WHERE name = %s",
        (group, name)
    )
    conn.commit()
    cur.close()
    release_db_connection(conn)

    # Refresh cache so frontend sees new group immediately
    thread = threading.Thread(target=refresh_cache_task, args=(False,), daemon=True)
    thread.start()

    return {"message": f"{name} moved to group '{group}'"}

@app.delete("/admin/products/{name}/group")
def admin_reset_group(name: str, key: str = Depends(verify_admin)):
    """Reset product back to its original category"""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        "UPDATE products SET custom_group = NULL WHERE name = %s",
        (name,)
    )
    conn.commit()
    cur.close()
    release_db_connection(conn)

    thread = threading.Thread(target=refresh_cache_task, args=(False,), daemon=True)
    thread.start()

    return {"message": f"{name} reset to original category"}

# ── Contact Info ───────────────────────────────

class ContactInfo(BaseModel):
    phone: Optional[str] = None
    whatsapp: Optional[str] = None
    email: Optional[str] = None
    address: Optional[str] = None
    instagram: Optional[str] = None
    facebook: Optional[str] = None
    hours: Optional[str] = None

@app.get("/admin/contact")
def admin_get_contact(key: str = Depends(verify_admin)):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT key, value FROM settings WHERE key LIKE 'contact_%'")
    rows = cur.fetchall()
    cur.close()
    release_db_connection(conn)
    return {row[0].replace("contact_", ""): row[1] for row in rows}

@app.post("/admin/contact")
def admin_save_contact(contact: ContactInfo, key: str = Depends(verify_admin)):
    """Save contact information"""
    fields = contact.dict(exclude_none=True)
    for field, value in fields.items():
        save_setting(f"contact_{field}", value)
    return {"message": "Contact info saved", "updated": list(fields.keys())}

# ── Offers ────────────────────────────────────

class OfferCreate(BaseModel):
    title: str
    description: Optional[str] = None
    product_name: Optional[str] = None
    discount_percent: Optional[float] = None
    expires_at: Optional[str] = None

@app.get("/admin/offers")
def admin_get_offers(key: str = Depends(verify_admin)):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, title, description, product_name,
               discount_percent, active, created_at, expires_at
        FROM offers ORDER BY created_at DESC
    """)
    rows = cur.fetchall()
    cur.close()
    release_db_connection(conn)
    return [
        {
            "id": r[0], "title": r[1], "description": r[2],
            "product_name": r[3], "discount_percent": r[4],
            "active": r[5], "created_at": r[6], "expires_at": r[7]
        }
        for r in rows
    ]

@app.post("/admin/offers")
def admin_create_offer(offer: OfferCreate, key: str = Depends(verify_admin)):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO offers (title, description, product_name, discount_percent, expires_at)
        VALUES (%s, %s, %s, %s, %s) RETURNING id
    """, (offer.title, offer.description, offer.product_name,
          offer.discount_percent, offer.expires_at))
    new_id = cur.fetchone()[0]
    conn.commit()
    cur.close()
    release_db_connection(conn)
    return {"message": "Offer created", "id": new_id}

@app.patch("/admin/offers/{offer_id}")
def admin_toggle_offer(offer_id: int, body: dict, key: str = Depends(verify_admin)):
    active = body.get("active")
    if active is None:
        raise HTTPException(400, "active field required")
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("UPDATE offers SET active = %s WHERE id = %s", (active, offer_id))
    conn.commit()
    cur.close()
    release_db_connection(conn)
    return {"message": f"Offer {'activated' if active else 'deactivated'}"}

@app.delete("/admin/offers/{offer_id}")
def admin_delete_offer(offer_id: int, key: str = Depends(verify_admin)):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("DELETE FROM offers WHERE id = %s", (offer_id,))
    conn.commit()
    cur.close()
    release_db_connection(conn)
    return {"message": "Offer deleted"}

# ── Misc Admin ────────────────────────────────

@app.post("/admin/refresh")
def admin_refresh(key: str = Depends(verify_admin)):
    thread = threading.Thread(target=refresh_cache_task, args=(False,), daemon=True)
    thread.start()
    return {"message": "Cache refresh initiated"}

@app.post("/admin/reset-placeholders")
def admin_reset_placeholders(key: str = Depends(verify_admin)):
    """Reset all auto-fetched placeholders so worker re-fetches them"""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        UPDATE product_images
        SET is_placeholder = TRUE, fetch_attempts = 0
        WHERE (is_placeholder = TRUE OR image LIKE '%placehold.co%')
        AND (is_manual IS NULL OR is_manual = FALSE)
    """)
    conn.commit()
    cur.close()
    release_db_connection(conn)
    return {"message": "Placeholders reset, worker will re-fetch"}

@app.get("/admin/status")
def admin_status(key: str = Depends(verify_admin)):
    missing_count = get_missing_images_count()
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM products")
    total = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM product_images WHERE is_manual = TRUE")
    manual = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM product_images WHERE is_placeholder = FALSE AND is_manual = FALSE")
    auto = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM offers WHERE active = TRUE")
    active_offers = cur.fetchone()[0]
    cur.close()
    release_db_connection(conn)

    with fetch_count_lock:
        fetches_today = daily_fetch_count

    return {
        "total_products": total,
        "missing_images": missing_count,
        "manual_images": manual,
        "auto_images": auto,
        "active_offers": active_offers,
        "fetches_today": fetches_today,
        "daily_fetch_limit": MAX_DAILY_FETCHES,
    }


# ─────────────────────────────────────────────
# PUBLIC UTILS
# ─────────────────────────────────────────────

@app.post("/refresh")
def refresh_cache():
    thread = threading.Thread(target=refresh_cache_task, args=(False,), daemon=True)
    thread.start()
    return {"message": "Cache refresh initiated"}

@app.post("/fetch-missing")
def fetch_missing():
    def manual_fetch():
        missing = get_all_missing_products()
        processed = process_batch(missing)
        refresh_cache_task(background=False)
        print(f"✅ Manual fetch done: {processed} images processed")
    thread = threading.Thread(target=manual_fetch, daemon=True)
    thread.start()
    return {"message": "Missing images fetch initiated"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
