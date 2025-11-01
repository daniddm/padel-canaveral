import csv
import os
import random
import time
from datetime import datetime
from urllib.parse import urljoin, urlsplit, urlunsplit

import requests
from bs4 import BeautifulSoup

from categories import categories as CATEGORY_LIST

SHOP_BASE_URL = "https://www.tiendapadelpoint.com"
PROXY_SERVICE = "https://api.codetabs.com/v1/proxy?quest="

REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    "Connection": "keep-alive",
}

SHOPIFY_HEADERS = [
    "Title",
    "URL handle",
    "Description",
    "Inventory policy",
    "Vendor",
    "Product category",
    "Type",
    "Tags",
    "Published on online store",
    "Status",
    "SKU",
    "Barcode",
    "Option1 name",
    "Option1 value",
    "Option2 name",
    "Option2 value",
    "Option3 name",
    "Option3 value",
    "Price",
    "Price / International",
    "Compare-at price",
    "Compare-at price / International",
    "Cost per item",
    "Charge tax",
    "Tax code",
    "Inventory tracker",
    "Inventory quantity",
    "Continue selling when out of stock",
    "Weight value (grams)",
    "Weight unit for display",
    "Requires shipping",
    "Fulfillment service",
    "Product image URL",
    "Image position",
    "Image alt text",
    "Variant image URL",
    "Gift card",
    "SEO title",
    "SEO description",
    "Google Shopping / Google product category",
    "Google Shopping / Gender",
    "Google Shopping / Age group",
    "Google Shopping / MPN",
    "Google Shopping / AdWords Grouping",
    "Google Shopping / AdWords labels",
    "Google Shopping / Condition",
    "Google Shopping / Custom product",
    "Google Shopping / Custom label 0",
    "Google Shopping / Custom label 1",
    "Google Shopping / Custom label 2",
    "Google Shopping / Custom label 3",
    "Google Shopping / Custom label 4",
]

FALLBACK_KEYWORDS = [
    "producto",
    "product",
    "pelot",
    "pala",
    "zapatilla",
    "ropa",
    "tenis",
    "pickle",
    "pack",
]

CATEGORY_URLS_SET = {urljoin(SHOP_BASE_URL, entry["url"]).rstrip("/") for entry in CATEGORY_LIST}


def get_page_with_proxy(url, timeout=30):
    """Fetch a page using the external proxy service."""
    proxy_url = PROXY_SERVICE + url
    try:
        response = requests.get(proxy_url, headers=REQUEST_HEADERS, timeout=timeout)
        if response.status_code == 200:
            content = response.content
            try:
                return content.decode("utf-8")
            except UnicodeDecodeError:
                fallback_encoding = response.apparent_encoding or "latin-1"
                print(
                    f"âš ï¸ No se pudo decodificar como UTF-8 {url}, usando {fallback_encoding}."
                )
                return content.decode(fallback_encoding, errors="replace")
        print(f"âŒ Error {response.status_code} al acceder a {url}")
    except Exception as exc:
        print(f"âŒ ExcepciÃ³n al acceder a {url}: {exc}")
    return None


def normalize_url(url, *, keep_query: bool = False) -> str:
    absolute = urljoin(SHOP_BASE_URL, url)
    parts = urlsplit(absolute)
    path = parts.path.rstrip("/")
    query = parts.query if keep_query else ""
    normalized = urlunsplit((parts.scheme, parts.netloc, path, query, ""))
    return normalized


def collect_category_product_links(category_url, category_name):
    """Collect all product URLs for a category, following pagination."""
    print(f"ðŸ“ Scrapeando categorÃ­a: {category_name}")
    base_url = normalize_url(category_url)
    next_page_url = base_url
    visited_pages = set()
    seen_products = set()
    product_urls = []
    page_number = 1

    candidate_selectors = [
        "li.product a.woocommerce-LoopProduct-link",
        "li.product a",
        "article.product a",
        "div.product-item a",
        ".product-grid-item a",
    ]

    while next_page_url and next_page_url not in visited_pages:
        print(f"  ðŸ“„ PÃ¡gina {page_number}: {next_page_url}")
        visited_pages.add(next_page_url)

        html = get_page_with_proxy(next_page_url)
        if not html:
            print("  âš ï¸ No se pudo obtener la pÃ¡gina, se detiene la categorÃ­a.")
            break

        soup = BeautifulSoup(html, "html.parser")

        if page_number == 1:
            category_slug = category_name.lower().replace(" ", "-")
            mega_menu_items = soup.find_all("div", class_="mega-menu-item")
            menu_products = 0
            for item in mega_menu_items:
                for link in item.find_all("a", href=True):
                    href = link.get("href")
                    if not href:
                        continue
                    candidate_url = normalize_url(
                        urljoin(next_page_url, href)
                    )
                    if not candidate_url.startswith(SHOP_BASE_URL):
                        continue
                    if candidate_url in CATEGORY_URLS_SET:
                        continue
                    if candidate_url == base_url:
                        continue
                    lowered_candidate = candidate_url.lower()
                    if (
                        category_slug in lowered_candidate
                        or any(word in lowered_candidate for word in FALLBACK_KEYWORDS)
                    ) and candidate_url not in seen_products:
                        seen_products.add(candidate_url)
                        product_urls.append(candidate_url)
                        menu_products += 1
            if menu_products:
                print(f"  ðŸ”— Productos desde mega menÃº: {menu_products}")

        # Intentar encontrar enlaces mediante selectores especÃ­ficos
        candidates = []
        for selector in candidate_selectors:
            candidates.extend(soup.select(selector))

        used_fallback = False
        if not candidates:
            candidates = soup.find_all("a", href=True)
            used_fallback = True

        page_products = 0
        for link in candidates:
            href = link.get("href")
            if not href:
                continue

            full_url = normalize_url(urljoin(next_page_url, href))

            if not full_url.startswith(SHOP_BASE_URL):
                continue

            if full_url in CATEGORY_URLS_SET:
                continue

            if full_url == base_url:
                continue

            if used_fallback:
                lowered_href = full_url.lower()
                if not any(word in lowered_href for word in FALLBACK_KEYWORDS):
                    continue

            if full_url in seen_products:
                continue

            seen_products.add(full_url)
            product_urls.append(full_url)
            page_products += 1

        print(f"  ðŸ”— Productos nuevos en la pÃ¡gina: {page_products}")

        next_page_candidate = None
        link_rel_next = soup.find("link", rel="next")
        if link_rel_next and link_rel_next.get("href"):
            next_page_candidate = normalize_url(
                urljoin(next_page_url, link_rel_next["href"]), keep_query=True
            )
        else:
            next_link = soup.select_one("a.next, a.next.page-numbers, li.next a")
            if next_link and next_link.get("href"):
                next_page_candidate = normalize_url(
                    urljoin(next_page_url, next_link["href"]), keep_query=True
                )

        if next_page_candidate and next_page_candidate not in visited_pages:
            next_page_url = next_page_candidate
            page_number += 1
            time.sleep(random.uniform(1.5, 2.5))
        else:
            next_page_url = None

    print(f"ðŸ“¦ Total productos encontrados en {category_name}: {len(product_urls)}")
    return product_urls


def extract_text(element):
    return element.get_text(" ", strip=True) if element else ""


def map_stock_quantity(label):
    text = label.strip().upper() if label else ""
    if "EN STOCK" in text:
        return "10"
    if "ÃšLTIMAS UNIDADES" in text or "ULTIMAS UNIDADES" in text:
        return "5"
    if text:
        return "0"
    return "0"


def build_shopify_row(
    *,
    cont,
    name,
    handle,
    description,
    brand,
    category_name,
    tags,
    price,
    compare_price,
    stock_quantity,
    url_img,
    codigo_producto,
    ean,
    option_name,
    option_value,
):
    return {
        "Title": name if cont == 1 else "",
        "URL handle": handle,
        "Description": description if cont == 1 else "",
        "Inventory policy": "deny",
        "Vendor": brand,
        "Product category": f"{category_name} > {brand}" if cont == 1 else "",
        "Type": category_name if cont == 1 else "",
        "Tags": tags if cont == 1 else "",
        "Published on online store": "TRUE" if cont == 1 else "",
        "Status": "Active" if cont == 1 else "",
        "SKU": ean if cont == 1 else "",
        "Barcode": codigo_producto if cont == 1 else "",
        "Option1 name": option_name if cont == 1 else "",
        "Option1 value": option_value,
        "Option2 name": "",
        "Option2 value": "",
        "Option3 name": "",
        "Option3 value": "",
        "Price": price,
        "Price / International": price,
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
        "SEO description": description if cont == 1 else "",
        "Google Shopping / Google product category": "",
        "Google Shopping / Gender": "",
        "Google Shopping / Age group": "",
        "Google Shopping / MPN": "",
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


def scrape_product_details(product_url, category_name):
    """Scrape a product page and return Shopify-ready rows."""
    normalized_url = normalize_url(product_url)
    print(f"    ðŸ” Producto: {normalized_url}")

    html = get_page_with_proxy(normalized_url)
    if not html:
        print("    âš ï¸ No se pudo obtener la pÃ¡gina del producto.")
        return []

    soup = BeautifulSoup(html, "html.parser")

    name = extract_text(soup.find("h1", class_="heading-title"))
    product_info = soup.find("div", class_="product-info")
    url_img = ""
    if product_info:
        image_div = product_info.find("div", class_="image")
        if image_div:
            link_tag = image_div.find("a")
            if link_tag and link_tag.get("href"):
                url_img = urljoin(normalized_url, link_tag["href"])

    brand = extract_text(soup.select_one("div.description > a")) or "NO BRAND"
    model_spans = soup.select("span.p-model")
    codigo_producto = extract_text(model_spans[-1]) if model_spans else ""
    ean = extract_text(soup.find("span", class_="journal-ean"))

    stock_text = ""
    stock_container = soup.find("div", class_="round_count")
    if stock_container:
        sibling = stock_container.find_next_sibling()
        stock_text = extract_text(sibling)
    stock_quantity = map_stock_quantity(stock_text)

    product_options = soup.find("div", class_="product-options")
    precio = ""
    precio_nuevo = ""
    precio_anterior = ""
    if product_options:
        precio = extract_text(product_options.find("span", class_="product-price"))
        precio_nuevo = extract_text(product_options.find("span", class_="price-new"))
        precio_anterior = extract_text(product_options.find("span", class_="price-old"))

    price_value = precio_nuevo or precio
    compare_price = precio_anterior

    descripcion_bruta = extract_text(soup.find("div", {"id": "tab-description"}))
    descripcion_limpia = descripcion_bruta.replace("\n", " ").replace("\r", " ").strip()
    tags = f"{name.split(' ')[0]} {brand}" if name else brand

    handle = normalized_url.split("/")[-1]

    if stock_quantity == "0":
        print("    âš ï¸ Producto sin stock, se omite.")
        return []

    variants = []
    cont = 1
    options_wrapper = product_options.find("div", class_="options") if product_options else None
    options = options_wrapper.find_all("option")[1:] if options_wrapper else []

    if options and category_name != "Palas de Padel":
        for option in options:
            option_text = option.get_text(strip=True)
            if not option_text:
                continue
            option_value = option_text[1:] if len(option_text) > 1 else option_text
            row = build_shopify_row(
                cont=cont,
                name=name,
                handle=handle,
                description=descripcion_limpia,
                brand=brand,
                category_name=category_name,
                tags=tags,
                price=price_value,
                compare_price=compare_price,
                stock_quantity=stock_quantity,
                url_img=url_img,
                codigo_producto=codigo_producto,
                ean=ean,
                option_name="Tallas-Peso",
                option_value=option_value,
            )
            variants.append(row)
            cont += 1

        if variants:
            return variants

    # Sin variantes o categorÃ­a especial
    row = build_shopify_row(
        cont=1,
        name=name,
        handle=handle,
        description=descripcion_limpia,
        brand=brand,
        category_name=category_name,
        tags=tags,
        price=price_value,
        compare_price=compare_price,
        stock_quantity=stock_quantity,
        url_img=url_img,
        codigo_producto=codigo_producto,
        ean=ean,
        option_name="",
        option_value="",
    )
    variants.append(row)

    return variants


def save_csv(products, category_name):
    if not products:
        print(f"âš ï¸ No hay productos para {category_name}")
        return

    fecha_hoy = datetime.now().date()
    output_dir = f"ExtracciÃ³n_{fecha_hoy}"
    os.makedirs(output_dir, exist_ok=True)

    filepath = os.path.join(output_dir, f"{category_name}.csv")
    print(f"ðŸ’¾ Guardando {len(products)} filas en {filepath}")

    with open(filepath, mode="w", newline="", encoding="utf-8-sig") as file:
        writer = csv.DictWriter(file, fieldnames=SHOPIFY_HEADERS)
        writer.writeheader()
        for product in products:
            writer.writerow(product)


def main():
    print("ðŸš€ Scraping final de tiendapadelpoint.com (modo Shopify)...")

    summary = {}
    for entry in CATEGORY_LIST:
        category_url = entry["url"]
        category_name = entry["categoria"]

        print(f"\n{'=' * 60}")
        product_urls = collect_category_product_links(category_url, category_name)

        if not product_urls:
            print("âš ï¸ No se encontraron productos en la categorÃ­a.")
            continue

        category_rows = []
        for idx, product_url in enumerate(product_urls, start=1):
            print(f"  ðŸ‘‰ Producto {idx}/{len(product_urls)}")
            product_rows = scrape_product_details(product_url, category_name)
            if product_rows:
                category_rows.extend(product_rows)
            time.sleep(random.uniform(2, 4))

        save_csv(category_rows, category_name)
        summary[category_name] = len(category_rows)

        print(f"  âœ… Filas Shopify generadas: {len(category_rows)}")
        time.sleep(random.uniform(4, 6))

    print("\nðŸŽ‰ Â¡Scraping completado!")
    print(f"ðŸ“Š Total categorÃ­as procesadas: {len(summary)}")
    for category, count in summary.items():
        print(f"  ðŸ“¦ {category}: {count} filas Shopify")


if __name__ == "__main__":
    main()