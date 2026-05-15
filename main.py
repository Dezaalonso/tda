from fastapi import FastAPI
import requests
import time
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import SimpleConnectionPool
import re
from ddgs import DDGS
from rapidfuzz import fuzz
from fastapi.middleware.cors import CORSMiddleware
from io import BytesIO
from PIL import Image
import os


app = FastAPI()

BASE_URL = "https://golosinasdacom.sistemerp.com"
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "port": os.getenv("DB_PORT")
}

connection_pool = SimpleConnectionPool(
    1,
    20,
    host=DB_CONFIG["host"],
    database=DB_CONFIG["database"],
    user=DB_CONFIG["user"],
    password=DB_CONFIG["password"],
    port=DB_CONFIG["port"]
)

# Image validation settings
MAX_IMAGE_SIZE_MB = 2
MAX_IMAGE_WIDTH = 800
MAX_IMAGE_HEIGHT = 800
ALLOWED_IMAGE_TYPES = ['JPEG', 'PNG', 'JPG', 'WEBP']

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 🚫 IMAGE BLOCKLIST - Sites that serve bad/irrelevant images
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
]

BLOCKLIST_PATTERNS = [
    "logo",
    "icon",
    "avatar",
    "favicon",
    "placeholder",
]

def is_blocked_url(url):
    """Check if a URL should be blocked"""
    url_lower = url.lower()
    
    # Check domain blocklist
    for domain in BLOCKLIST_DOMAINS:
        if domain in url_lower:
            print(f"   🚫 Blocked by domain: {domain}")
            return True
    
    # Check pattern blocklist  
    for pattern in BLOCKLIST_PATTERNS:
        if pattern in url_lower:
            print(f"   🚫 Blocked by pattern: {pattern}")
            return True
    
    return False

# 🗄️ DATABASE HELPERS
def get_db_connection():
    return connection_pool.getconn()

def release_db_connection(conn):
    connection_pool.putconn(conn)

def init_db():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS product_images (
            name TEXT PRIMARY KEY,
            image TEXT,
            image_size REAL,
            width INTEGER,
            height INTEGER
        )
    """)

    conn.commit()
    cur.close()
    release_db_connection(conn)

init_db()

def get_cached_image(name):
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute(
        "SELECT image FROM product_images WHERE name = %s",
        (name,)
    )

    row = cur.fetchone()

    cur.close()
    release_db_connection(conn)

    return row[0] if row else None



def save_cached_image(name, image_url, image_size=None, width=None, height=None):
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO product_images (name, image, image_size, width, height)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (name)
        DO UPDATE SET
            image = EXCLUDED.image,
            image_size = EXCLUDED.image_size,
            width = EXCLUDED.width,
            height = EXCLUDED.height
    """, (name, image_url, image_size, width, height))

    conn.commit()

    cur.close()
    release_db_connection(conn)

def validate_image_url(url):
    """Validate image URL by checking size and dimensions using PIL"""
    try:
        # Download image with stream to check size
        response = requests.get(url, timeout=15, stream=True)
        response.raise_for_status()
        
        # Check content length from headers first
        content_length = response.headers.get('content-length')
        if content_length:
            size_mb = int(content_length) / (1024 * 1024)
            if size_mb > MAX_IMAGE_SIZE_MB:
                print(f"   ⚠️ Image too large: {size_mb:.2f}MB > {MAX_IMAGE_SIZE_MB}MB")
                return False, None, None, None
        
        # Read image data in chunks to avoid memory issues
        image_data = BytesIO()
        downloaded = 0
        for chunk in response.iter_content(chunk_size=8192):
            image_data.write(chunk)
            downloaded += len(chunk)
            if downloaded > MAX_IMAGE_SIZE_MB * 1024 * 1024:
                print(f"   ⚠️ Image download exceeded size limit")
                return False, None, None, None
        
        image_data.seek(0)
        
        # Use PIL to check image type and dimensions
        try:
            img = Image.open(image_data)
            
            # Check image format
            img_format = img.format if img.format else 'Unknown'
            if img_format.upper() not in ALLOWED_IMAGE_TYPES:
                print(f"   ⚠️ Invalid image type: {img_format}")
                return False, None, None, None
            
            width, height = img.size
            
            # Check dimensions
            if width > MAX_IMAGE_WIDTH or height > MAX_IMAGE_HEIGHT:
                print(f"   ⚠️ Image dimensions too large: {width}x{height} > {MAX_IMAGE_WIDTH}x{MAX_IMAGE_HEIGHT}")
                return False, None, None, None
            
            size_mb = downloaded / (1024 * 1024)
            print(f"   ✅ Valid image: {img_format} {width}x{height} ({size_mb:.2f}MB)")
            return True, size_mb, width, height
            
        except Exception as e:
            print(f"   ⚠️ PIL error: {str(e)[:50]}")
            return False, None, None, None
            
    except Exception as e:
        print(f"   ⚠️ Image validation error: {str(e)[:50]}")
        return False, None, None, None

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

def classify_product(name):
    name = name.lower()
    for category, keywords in CATEGORIES.items():
        if any(word in name for word in keywords):
            return category
    return "Other"

def get_brand_and_type(product_name):
    product_lower = product_name.lower()
    brand = None
    category = classify_product(product_name)
    
    # Detect brand
    for cat, keywords in CATEGORIES.items():
        for keyword in keywords:
            if keyword in product_lower:
                brand = keyword
                break
        if brand:
            break
    
    clean_name = product_name.lower()

    # Remove useless words ONLY
    clean_name = re.sub(r'\b(x|pote|pqte|und|unds|pack|pqt)\b', '', clean_name)

    # Remove extra spaces
    clean_name = re.sub(r'\s+', ' ', clean_name).strip()

    return brand, clean_name, category

# 🖼️ IMAGE FETCHING WITH BLOCKLIST
def fetch_image(product_name):
    """Fetch product image with blocklist filtering"""
    brand, clean_name, category = get_brand_and_type(product_name)
    
    search_queries = []

    if brand:
        search_queries.append(f"{clean_name} producto real")
        search_queries.append(f"{clean_name} envase")
        search_queries.append(f"{brand} {category}")
    else:
        search_queries.append(f"{clean_name} producto")
    
    print(f"🔍 Buscando: {product_name[:50]}...")

    for query in search_queries[:2]:
        try:
            with DDGS() as ddgs:
                results = list(ddgs.images(query, max_results=5))
                
                for result in results:
                    img_url = result.get('image')
                    if not img_url:
                        continue

                    # Check blocklist first (fast, no network)
                    if is_blocked_url(img_url):
                        continue

                    # Basic URL validation
                    if not img_url.startswith("http"):
                        continue

                    print(f"   ✅ Image found: {img_url[:80]}...")
                    return img_url

        except Exception as e:
            print(f"   ❌ Error: {str(e)[:50]}")
            continue

        time.sleep(0.5)

    # Fallback placeholder
    placeholders = {
        "Drinks": "https://placehold.co/400x400/blue/white?text=Bebida",
        "Snacks": "https://placehold.co/400x400/orange/white?text=Snack",
        "Chocolates": "https://placehold.co/400x400/brown/white?text=Chocolate",
        "Galletas": "https://placehold.co/400x400/gold/white?text=Galleta",
        "Golosinas": "https://placehold.co/400x400/pink/white?text=Golosina",
        "Limpieza": "https://placehold.co/400x400/green/white?text=Limpieza",
        "Alcohol": "https://placehold.co/400x400/purple/white?text=Alcohol",
        "Other": "https://placehold.co/400x400/gray/white?text=Producto"
    }

    print(f"   ⚠️ Using placeholder")
    return placeholders.get(category, placeholders["Other"])

# Cache for products
cache = {"data": None, "timestamp": 0, "is_loading": False}

def fetch_and_enrich():
    """Fetch products from ERP"""
    session = requests.Session()
    
    # Login logic
    auth_payload = {
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "db": "golosinasdacom_web",
            "login": "armando@dacom.com",
            "password": "004795322"
        }
    }
    
    try:
        login_res = session.post(f"{BASE_URL}/web/session/authenticate", json=auth_payload)
        if "error" in login_res.json():
            print("❌ Login failed")
            return None, []
    except Exception as e:
        print(f"❌ Login error: {e}")
        return None, []

    # Fetch all products
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

    print(f"📦 Total products fetched: {len(all_records)}")

    # Process products
    grouped = {}
    products_to_fetch = []
    
    for p in all_records:
        cat = classify_product(p["name"])
        if cat not in grouped:
            grouped[cat] = []
        
        # Check cache first
        img = get_cached_image(p["name"])
        
        if not img:
            products_to_fetch.append(p)
            img = "https://placehold.co/400x400/gray/white?text=Cargando..."

        grouped[cat].append({
            "name": p["name"],
            "price": p["list_price"],
            "stock": p["qty_available"],
            "image": img
        })
    
    return grouped, products_to_fetch

def fetch_missing_images(products_to_fetch):
    """Background task to fetch missing images"""
    MAX_NEW = 10
    new_images_count = 0
    
    for p in products_to_fetch[:MAX_NEW]:
        print(f"📸 Background fetch: {p['name'][:50]}...")
        img = fetch_image(p["name"])
        if img and "Cargando" not in img and "placeholder" not in img:
            save_cached_image(p["name"], img)
            new_images_count += 1
        time.sleep(2)
    
    print(f"✅ Background fetch completed: {new_images_count} new images")
    
    # Update cache with new images
    if cache["data"]:
        for cat, products in cache["data"].items():
            for product in products:
                new_img = get_cached_image(product["name"])
                if new_img and new_img != product["image"]:
                    product["image"] = new_img

def refresh_cache_task():
    """Full cache refresh in background"""
    if cache["is_loading"]:
        print("⏳ Cache already loading, skipping...")
        return
    
    cache["is_loading"] = True
    try:
        grouped, products_to_fetch = fetch_and_enrich()
        if grouped:
            cache["data"] = grouped
            cache["timestamp"] = time.time()
            
            # Fetch missing images in background
            if products_to_fetch:
                import threading
                thread = threading.Thread(target=fetch_missing_images, args=(products_to_fetch,))
                thread.daemon = True
                thread.start()
    finally:
        cache["is_loading"] = False

# 📦 ENDPOINTS
@app.get("/products")
def get_products():
    if cache["data"]:
        return cache["data"]
    
    if not cache["is_loading"]:
        grouped, products_to_fetch = fetch_and_enrich()
        if grouped:
            cache["data"] = grouped
            cache["timestamp"] = time.time()
            
            if products_to_fetch:
                import threading
                thread = threading.Thread(target=fetch_missing_images, args=(products_to_fetch,))
                thread.daemon = True
                thread.start()
            
            return grouped
    
    return {"message": "Loading products, please refresh in a moment"}

@app.get("/search")
def search(q: str):
    if not cache["data"]:
        return []
    
    results = []
    query = q.lower()
    for cat, products in cache["data"].items():
        for p in products:
            score = fuzz.partial_ratio(query, p["name"].lower())
            if score > 60:
                results.append({**p, "category": cat, "score": score})
                
    results.sort(key=lambda x: x["score"], reverse=True)
    return results[:20]

@app.post("/refresh")
def refresh_cache():
    import threading
    thread = threading.Thread(target=refresh_cache_task)
    thread.daemon = True
    thread.start()
    return {"message": "Cache refresh initiated"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)