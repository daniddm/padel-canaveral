import csv
import os
import random
import re
import time
import unicodedata
from datetime import datetime
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from categories import categories as CATEGORY_LIST

SHOP_BASE_URL = "https://www.tiendapadelpoint.com"
PROXY_SERVICE = "https://api.codetabs.com/v1/proxy?quest="
REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    "Connection": "keep-alive",
}

SHOPIFY_HEADERS = [
    "Title", "URL handle", "Description", "Inventory policy", "Vendor",
    "Product category", "Type", "Tags", "Published on online store", "Status",
    "SKU", "Barcode", "Option1 name", "Option1 value", "Option2 name", "Option2 value",
    "Option3 name", "Option3 value", "Price", "Price / International",
    "Compare-at price", "Compare-at price / International", "Cost per item",
    "Charge tax", "Tax code", "Inventory tracker", "Inventory quantity",
    "Continue selling when out of stock", "Weight value (grams)", "Weight unit for display",
    "Requires shipping", "Fulfillment service", "Product image URL", "Image position",
    "Image alt text", "Variant image URL", "Gift card", "SEO title", "SEO description",
    "Google Shopping / Google product category", "Google Shopping / Gender",
    "Google Shopping / Age group", "Google Shopping / MPN", "Google Shopping / AdWords Grouping",
    "Google Shopping / AdWords labels", "Google Shopping / Condition",
    "Google Shopping / Custom product", "Google Shopping / Custom label 0",
    "Google Shopping / Custom label 1", "Google Shopping / Custom label 2",
    "Google Shopping / Custom label 3", "Google Shopping / Custom label 4",
]

# --- Utils para handles/slug/SKU ---
DUP_TWO_NUMS_TAIL = re.compile(r"-(\d+)-(\d+)$")
SINGLE_NUM_TAIL = re.compile(r"-(\d+)$")


def slugify(s: str) -> str:
    s = (s or "").strip()
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-")
    return re.sub(r"-{2,}", "-", s)


def canonical_handle(product_title: str, product_url_or_slug: str, max_dup_num: int = 9) -> str:
    """
    Normaliza el handle:
    - Si el slug termina en -A-B y el título termina en -A y B es pequeño (<=max_dup_num), deja -A.
    - Si el slug termina en -A-B y el título NO contiene -A-B y ambos son pequeños: corta el sufijo (-A-B).
    - Si el slug termina en -B y el título NO termina en -B y B es pequeño: corta el -B.
    - En caso contrario, conserva el slug.
    """
    title_slug = slugify(product_title or "")
    raw_slug = slugify((product_url_or_slug or "").rstrip("/").split("/")[-1])

    # Patrón -A-B (más específico)
    m2 = DUP_TWO_NUMS_TAIL.search(raw_slug)
    if m2:
        A, B = m2.groups()
        title_m2 = DUP_TWO_NUMS_TAIL.search(title_slug)
        if title_m2 and title_m2.groups() == (A, B):
            return raw_slug

        title_m1 = SINGLE_NUM_TAIL.search(title_slug)
        if title_m1 and title_m1.group(1) == A and int(B) <= max_dup_num:
            return raw_slug[:m2.start()] + f"-{A}"

        try:
            if int(A) <= max_dup_num and int(B) <= max_dup_num:
                return raw_slug[:m2.start()]
        except ValueError:
            pass
        return raw_slug

    # Patrón -B (una sola cifra al final)
    m1 = SINGLE_NUM_TAIL.search(raw_slug)
    if m1:
        B = m1.group(1)
        title_m1 = SINGLE_NUM_TAIL.search(title_slug)
        if title_m1 and title_m1.group(1) == B:
            return raw_slug
        try:
            if int(B) <= max_dup_num:
                return raw_slug[:m1.start()]
        except ValueError:
            pass
    return raw_slug


def build_handle(name: str, brand: str, ean: str, ref: str, fallback_slug: str) -> str:
    """
    Prioriza EAN o REF para hacer handles únicos cuando no hay EAN válido.
    """
    if ean and re.fullmatch(r"\d{8,14}", ean.strip()):
        return slugify(f"{name}-{ean}")
    if ref:
        return slugify(f"{name}-{ref}")
    if brand:
        return slugify(f"{brand}-{name}")
    return canonical_handle(name, fallback_slug)


def get_page(url, timeout=30, max_retries=3):
    """Obtiene una página usando el proxy service con reintentos."""
    for attempt in range(max_retries):
        try:
            proxy_url = PROXY_SERVICE + url
            response = requests.get(proxy_url, headers=REQUEST_HEADERS, timeout=timeout)
            if response.status_code == 200:
                response.encoding = response.apparent_encoding or 'utf-8'
                return response.text
            print(f"⚠️ Intento {attempt + 1}: Error {response.status_code} en {url}")
            if response.status_code == 429:
                wait_time = 2 ** attempt * 5
                print(f"⏳ Rate limit detectado. Esperando {wait_time}s...")
                time.sleep(wait_time)
        except Exception as exc:
            print(f"⚠️ Intento {attempt + 1}: Error al acceder a {url}: {exc}")
        if attempt < max_retries - 1:
            time.sleep(2 ** attempt)
    print(f"❌ No se pudo acceder a {url} después de {max_retries} intentos")
    return None


def collect_category_product_links(category_url, category_name):
    """Recolecta todas las URLs de productos de una categoría con paginación."""
    print(f"🏓 Scrapeando categoría: {category_name}")
    base_url = urljoin(SHOP_BASE_URL, category_url)
    product_urls = []
    seen = set()  # FIX: prevenir duplicados

    response = get_page(base_url)
    if not response:
        return []

    soup = BeautifulSoup(response, "html.parser")

    # Detectar paginación - FIX: más robusto
    paginator = soup.find("div", class_="pagination")
    total_pages = 1
    if paginator:
        nums = []
        for a in paginator.select("a"):
            m = re.search(r"[?&]page=(\d+)", a.get("href", ""))
            if m:
                nums.append(int(m.group(1)))
        total_pages = max(nums) if nums else 1
    
    print(f" 📄 Total páginas: {total_pages}")

    # Recorrer todas las páginas
    for page in range(1, total_pages + 1):
        page_url = f"{base_url}?page={page}"
        print(f" 📄 Página {page}/{total_pages}")
        html = get_page(page_url)
        if not html:
            continue

        soup = BeautifulSoup(html, "html.parser")
        main_products_div = soup.select_one("div.main-products")
        if main_products_div:
            products_div_image = main_products_div.select("div.image")
            print(f" 🔗 Productos en página: {len(products_div_image)}")
            for image in products_div_image:
                link = image.find("a")
                if link and link.get("href"):
                    href = link.get("href")
                    full_url = urljoin(SHOP_BASE_URL, href)
                    if full_url not in seen:  # FIX: evitar duplicados
                        seen.add(full_url)
                        product_urls.append(full_url)
        time.sleep(random.uniform(1, 2))

    print(f"📦 Total productos únicos: {len(product_urls)}")
    return product_urls


def extract_text(element):
    return element.get_text(" ", strip=True) if element else ""


def map_stock_quantity(label):
    text = label.strip().upper() if label else ""
    if "DISPONIBLE" in text or "EN STOCK" in text:  # FIX: añadido DISPONIBLE
        return "10"
    if "ÚLTIMAS UNIDADES" in text or "ULTIMAS UNIDADES" in text:
        return "5"
    return "0"


def scrape_product_details(product_url, category_name):
    """Scrapea los detalles de un producto."""
    print(f" 🔍 {product_url.split('/')[-1]}")
    html = get_page(product_url)
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")

    # Nombre
    name = extract_text(soup.find("h1", class_="heading-title"))

    # Marca
    brand = "NO BRAND"
    brand_link = soup.select_one("div.description > a")
    if brand_link:
        brand = brand_link.text.strip()
    if not brand or brand == "NO BRAND":
        manufacturer = soup.find("span", itemprop="manufacturer")
        if manufacturer:
            brand = extract_text(manufacturer)

    # Códigos
    model_spans = soup.select("span.p-model")
    codigo_producto = extract_text(model_spans[-1]) if model_spans else ""
    ean = extract_text(soup.find("span", class_="journal-ean"))

    # Stock
    stock_text = ""
    stock_container = soup.find("div", class_="round_count")
    if stock_container:
        sibling = stock_container.find_next_sibling()
        if sibling:
            stock_text = extract_text(sibling)
    stock_quantity = map_stock_quantity(stock_text)

    # Imagen
    url_img = ""
    product_info = soup.find("div", class_="product-info")
    if product_info:
        image_div = product_info.find("div", class_="image")
        if image_div:
            link_tag = image_div.find("a")
            if link_tag and link_tag.get("href"):
                raw_img = link_tag["href"]
                url_img = urljoin(SHOP_BASE_URL, raw_img)  # FIX: asegurar URL absoluta

    # Precios
    product_options = soup.find("div", class_="product-options")
    precio = ""
    precio_nuevo = ""
    precio_anterior = ""
    if product_options:
        precio_span = product_options.find("span", class_="product-price")
        if precio_span:
            precio = precio_span.text.strip()
        precio_new_span = product_options.find("span", class_="price-new")
        if precio_new_span:
            precio_nuevo = precio_new_span.text.strip()
        precio_old_span = product_options.find("span", class_="price-old")
        if precio_old_span:
            precio_anterior = precio_old_span.text.strip()

    price_value = precio_nuevo if precio_nuevo else precio
    compare_price = precio_anterior

    # Descripción
    descripcion_div = soup.find("div", {"id": "tab-description"})
    descripcion = extract_text(descripcion_div).replace("\n", " ").replace("\r", " ").strip()

    # Tags y handle inteligente
    tags = f"{name.split(' ')[0]} {brand}" if name else brand
    handle = build_handle(name, brand, ean, codigo_producto, product_url)

    # Base SKU compacto
    base_sku = (ean or codigo_producto or slugify(name))[:20]

    # Variantes
    variants = []
    cont = 1
    options_div = product_options.find("div", class_="options") if product_options else None
    options = options_div.find_all("option")[1:] if options_div else []

    if options and len(options) >= 1 and category_name != 'Palas de Padel':
        # Crear variantes
        for option in options:
            option_text = option.get_text(strip=True)
            if not option_text:
                continue
            
            # FIX: quita símbolos iniciales sin comer letras
            option_value = re.sub(r"^[^A-Za-z0-9]+", "", option_text)

            variant_key = re.sub(r'[^a-z0-9]', '', option_value.lower())[:8] or "default"
            sku = f"{base_sku}-{variant_key}"

            row = {
                "Title": name if cont == 1 else "",
                "URL handle": handle,
                "Description": descripcion if cont == 1 else "",
                "Inventory policy": "deny",
                "Vendor": brand,
                "Product category": f"{category_name} > {brand}" if cont == 1 else "",
                "Type": category_name if cont == 1 else "",
                "Tags": tags if cont == 1 else "",
                "Published on online store": "TRUE" if cont == 1 else "",
                "Status": "Active" if cont == 1 else "",
                "SKU": sku,
                "Barcode": ean or codigo_producto or "",
                "Option1 name": "Tallas-Peso" if cont == 1 else "",
                "Option1 value": option_value,
                "Option2 name": "",
                "Option2 value": "",
                "Option3 name": "",
                "Option3 value": "",
                "Price": price_value,
                "Price / International": price_value,
                "Compare-at price": compare_price,
                "Compare-at price / International": compare_price,
                "Cost per item": "",
                "Charge tax": "TRUE" if cont == 1 else "",
                "Tax code": "",
                "Inventory tracker": "shopify",
                "Inventory quantity": stock_quantity,
                "Continue selling when out of stock": "FALSE" if cont == 1 else "",
                "Weight value (grams)": "",
                "Weight unit for display": "g",
                "Requires shipping": "TRUE",
                "Fulfillment service": "manual",
                "Product image URL": url_img if cont == 1 else "",
                "Image position": "1" if cont == 1 else "",
                "Image alt text": name if cont == 1 else "",
                "Variant image URL": "",
                "Gift card": "FALSE" if cont == 1 else "",
                "SEO title": name if cont == 1 else "",
                "SEO description": descripcion if cont == 1 else "",
                "Google Shopping / Google product category": "",
                "Google Shopping / Gender": "",
                "Google Shopping / Age group": "",
                "Google Shopping / MPN": codigo_producto or "",
                "Google Shopping / AdWords Grouping": "",
                "Google Shopping / AdWords labels": "",
                "Google Shopping / Condition": "",
                "Google Shopping / Custom product": "",
                "Google Shopping / Custom label 0": "",
                "Google Shopping / Custom label 1": "",
                "Google Shopping / Custom label 2": "",
                "Google Shopping / Custom label 3": "",
                "Google Shopping / Custom label 4": "",
            }
            variants.append(row)
            cont += 1
    else:
        # Producto simple - solo si tiene stock
            row = {
                "Title": name,
                "URL handle": handle,
                "Description": descripcion,
                "Inventory policy": "deny",
                "Vendor": brand,
                "Product category": f"{category_name} > {brand}",
                "Type": category_name,
                "Tags": tags,
                "Published on online store": "TRUE",
                "Status": "Active",
                "SKU": base_sku,
                "Barcode": ean or codigo_producto or "",
                "Option1 name": "",
                "Option1 value": "",
                "Option2 name": "",
                "Option2 value": "",
                "Option3 name": "",
                "Option3 value": "",
                "Price": price_value,
                "Price / International": price_value,
                "Compare-at price": compare_price,
                "Compare-at price / International": compare_price,
                "Cost per item": "",
                "Charge tax": "TRUE",
                "Tax code": "",
                "Inventory tracker": "shopify",
                "Inventory quantity": stock_quantity,
                "Continue selling when out of stock": "FALSE",
                "Weight value (grams)": "",
                "Weight unit for display": "g",
                "Requires shipping": "TRUE",
                "Fulfillment service": "manual",
                "Product image URL": url_img,
                "Image position": "1",
                "Image alt text": name,
                "Variant image URL": "",
                "Gift card": "FALSE",
                "SEO title": name,
                "SEO description": descripcion,
                "Google Shopping / Google product category": "",
                "Google Shopping / Gender": "",
                "Google Shopping / Age group": "",
                "Google Shopping / MPN": codigo_producto or "",
                "Google Shopping / AdWords Grouping": "",
                "Google Shopping / AdWords labels": "",
                "Google Shopping / Condition": "",
                "Google Shopping / Custom product": "",
                "Google Shopping / Custom label 0": "",
                "Google Shopping / Custom label 1": "",
                "Google Shopping / Custom label 2": "",
                "Google Shopping / Custom label 3": "",
                "Google Shopping / Custom label 4": "",
            }
            variants.append(row)

    print(f" ✅ Marca: {brand} | Variantes: {len(variants)} | Stock: {stock_quantity}")
    return variants


def save_csv(products, category_name):
    if not products:
        print(f"⚠️ No hay productos para {category_name}")
        return

    fecha_hoy = datetime.now().date()
    output_dir = f"Extracción_{fecha_hoy}"
    os.makedirs(output_dir, exist_ok=True)

    filepath = os.path.join(output_dir, f"{category_name}.csv")
    print(f"💾 Guardando {len(products)} filas en {filepath}")

    with open(filepath, mode="w", newline="", encoding="utf-8-sig") as file:
        writer = csv.DictWriter(file, fieldnames=SHOPIFY_HEADERS)
        writer.writeheader()
        for product in products:
            writer.writerow(product)


def main():
    print("🚀 Iniciando scraping de tiendapadelpoint.com")
    print(f"📅 Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📊 Categorías a procesar: {len(CATEGORY_LIST)}\n")

    summary = {}
    for idx, entry in enumerate(CATEGORY_LIST, 1):
        category_url = entry["url"]
        category_name = entry["categoria"]

        print(f"\n{'=' * 70}")
        print(f"[{idx}/{len(CATEGORY_LIST)}] Procesando: {category_name}")
        print(f"{'=' * 70}")

        product_urls = collect_category_product_links(category_url, category_name)
        if not product_urls:
            print(f"⚠️ No se encontraron productos en {category_name}")
            summary[category_name] = 0
            continue

        category_rows = []
        for product_idx, product_url in enumerate(product_urls, 1):
            print(f" [{product_idx}/{len(product_urls)}] Procesando producto...")
            product_rows = scrape_product_details(product_url, category_name)
            if product_rows:
                category_rows.extend(product_rows)
            time.sleep(random.uniform(1.5, 3))

        save_csv(category_rows, category_name)
        summary[category_name] = len(category_rows)
        print(f"\n✅ {category_name}: {len(category_rows)} filas generadas")

        time.sleep(random.uniform(3, 5))

    print("\n" + "=" * 70)
    print("🎉 ¡Scraping completado!")
    print("=" * 70)
    print(f"📊 Total categorías procesadas: {len(summary)}")
    for category, count in summary.items():
        print(f" 📦 {category}: {count} filas")
    print("=" * 70)


if __name__ == "__main__":
    main()