from fastapi import FastAPI
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
import threading
from datetime import datetime
from contextlib import asynccontextmanager


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

# Image validation settings
MAX_IMAGE_SIZE_MB = 2
MAX_IMAGE_WIDTH = 800
MAX_IMAGE_HEIGHT = 800
ALLOWED_IMAGE_TYPES = ['JPEG', 'PNG', 'JPG', 'WEBP']

# Pull settings
BATCH_SIZE = 15
REST_MINUTES = 5
MAX_DAILY_FETCHES = 200

# 🏪 Trusted store sites for tier 1 search
STORE_SITES = [
    "site:plazavea.com.pe",
    "site:tottus.com.pe",
    "site:wong.pe",
    "site:mercadolibre.com.pe",
    "site:vivanda.com.pe",
    "site:makro.com.pe",
    "site:lacuracao.pe",
    "site:linio.com.pe",
    "site:ripley.com.pe",
    "site:falabella.com.pe",
]

# 🚫 Bad sources to reject
BAD_SOURCES = [
    "mysourcedepot", "autozone", "hardware", "tools",
    "vehicle", "motor", "engine", "belt", "mechanical",
    "dental", "clinic", "teeth", "oatmeal", "workout",
    "fitness", "gym", "gettyimages", "shutterstock",
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
            is_placeholder BOOLEAN DEFAULT FALSE
        )
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_product_images_last_fetch
        ON product_images(last_fetch)
    """)
    conn.commit()
    cur.close()
    release_db_connection(conn)
    print("✅ Database initialized")


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

def save_cached_image(name, image_url, image_size=None, width=None, height=None, is_placeholder=False):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO product_images (name, image, image_size, width, height, last_fetch, fetch_attempts, is_placeholder)
        VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP, 1, %s)
        ON CONFLICT (name) DO UPDATE SET
            image = EXCLUDED.image,
            image_size = EXCLUDED.image_size,
            width = EXCLUDED.width,
            height = EXCLUDED.height,
            last_fetch = CURRENT_TIMESTAMP,
            fetch_attempts = product_images.fetch_attempts + 1,
            is_placeholder = EXCLUDED.is_placeholder
    """, (name, image_url, image_size, width, height, is_placeholder))
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
        WHERE pi.name IS NULL OR pi.is_placeholder = TRUE
    """)
    count = cur.fetchone()[0]
    cur.close()
    release_db_connection(conn)
    return count


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
            print(f"   🚫 Blocked nature pattern in URL: {pattern}")
            return False
    return True

def is_bad_result(img_url, title, source):
    combined = f"{img_url} {title} {source}".lower()
    return any(bad in combined for bad in BAD_SOURCES)

def title_matches_product(title, meaningful_words):
    title_lower = title.lower()
    matches = sum(1 for w in meaningful_words if len(w) > 3 and w in title_lower)
    return matches >= 1

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

def search_ddgs_images(query, product_name, meaningful_words, max_results=10):
    """Run a single DDGS image search and return best matching URL"""
    try:
        with DDGS() as ddgs:
            results = list(ddgs.images(query, max_results=max_results))
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
                    print(f"   🚫 Bad source: {source[:40]}")
                    continue
                if not title_matches_product(title, meaningful_words):
                    print(f"   🚫 Title mismatch: {title[:40]}")
                    continue

                print(f"   ✅ Match: {img_url[:70]}")
                return img_url
    except Exception as e:
        print(f"   ❌ DDGS error: {str(e)[:50]}")
    return None

def fetch_image(product_name):
    brand, clean_name, category = get_brand_and_type(product_name)
    meaningful_words = get_meaningful_words(product_name)

    quoted_brand = f'"{brand}"' if brand else ""
    descriptors = " ".join(
        w for w in meaningful_words
        if brand and w != brand and len(w) > 3
        or not brand and len(w) > 3
    )[:40]

    print(f"\n🔍 Fetching image for: {product_name[:50]}")

    # ── TIER 1: Search within trusted store sites ──────────────────
    stores_to_try = random.sample(STORE_SITES, min(3, len(STORE_SITES)))
    for store in stores_to_try:
        if shutdown_event.is_set():
            break
        query = f"{quoted_brand} {descriptors} {store}".strip()
        print(f"   🏪 Store search: {query[:60]}")
        result = search_ddgs_images(query, product_name, meaningful_words, max_results=5)
        if result:
            return result
        time.sleep(0.8)

    # ── TIER 2: Broad web search with quoted brand ─────────────────
    broad_queries = []
    if brand:
        broad_queries.append(f'{quoted_brand} {descriptors} producto envase')
        broad_queries.append(f'{quoted_brand} {category} package')
    else:
        quoted_name = f'"{" ".join(meaningful_words[:3])}"'
        broad_queries.append(f'{quoted_name} producto envase')
        broad_queries.append(f'{quoted_name} package product')

    for query in broad_queries:
        if shutdown_event.is_set():
            break
        print(f"   🌐 Broad search: {query[:60]}")
        result = search_ddgs_images(query, product_name, meaningful_words, max_results=8)
        if result:
            return result
        time.sleep(1)

    # ── TIER 3: Last resort — brand + category only ────────────────
    if brand:
        query = f'"{brand}" {category} producto'
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
        SELECT p.name, p.list_price, p.qty_available, p.category,
               COALESCE(pi.image, 'https://placehold.co/400x400/gray/white?text=Cargando...') as image,
               pi.is_placeholder
        FROM products p
        LEFT JOIN product_images pi ON p.name = pi.name
        ORDER BY p.name
    """)
    products = cur.fetchall()
    cur.close()
    release_db_connection(conn)

    grouped = {}
    for name, price, stock, category, image, is_placeholder in products:
        cat = category or classify_product(name)
        if cat not in grouped:
            grouped[cat] = []
        if is_placeholder or not image or image.startswith("https://placehold.co"):
            image = PLACEHOLDERS.get(cat, PLACEHOLDERS["Other"])
        grouped[cat].append({"name": name, "price": price, "stock": stock, "image": image})

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
            print("⛔ Daily limit reached mid-batch, stopping.")
            break

        with batch_count_lock:
            count = current_batch_count

        if count > 0 and count % BATCH_SIZE == 0:
            print(f"\n⏸️  Pulled {BATCH_SIZE} images — resting {REST_MINUTES} minutes...")
            shutdown_event.wait(timeout=REST_MINUTES * 60)
            if shutdown_event.is_set():
                break
            print("▶️  Resuming image fetching...")

        img_url = fetch_image(product_name)
        increment_fetch_count()

        is_placeholder_url = "placehold.co" in img_url or "placeholder" in img_url.lower()

        if not is_placeholder_url:
            is_valid, size_mb, width, height = validate_image_url(img_url)
            if is_valid:
                save_cached_image(product_name, img_url, size_mb, width, height, is_placeholder=False)
                print(f"   ✅ Saved: {product_name[:50]}")
                success_count += 1
                with cache_lock:
                    if cache["data"]:
                        for cat, products in cache["data"].items():
                            for product in products:
                                if product["name"] == product_name:
                                    product["image"] = img_url
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


# ─────────────────────────────────────────────
# 📡 ENDPOINTS
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

@app.post("/refresh")
def refresh_cache():
    thread = threading.Thread(target=refresh_cache_task, args=(False,), daemon=True)
    thread.start()
    return {"message": "Cache refresh initiated"}

@app.post("/fetch-missing")
def fetch_missing():
    def manual_fetch():
        print("🔍 Manual fetch triggered")
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
