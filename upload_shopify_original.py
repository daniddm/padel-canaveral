"""
upload_shopify.py - Versión mínima con protección de borrador e ignore

Cambios aplicados:
1. Respeta productos en borrador (status=draft)
2. Respeta tag scraper:keep-draft
3. NUEVO: Respeta tags de ignore (scraper:ignore, no tocar)
4. NUEVO: Prune respeta productos ignorados
"""
import argparse
import csv
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, Any

import requests

SHOP_DOMAIN = os.getenv("SHOPIFY_DOMAIN", "").strip()
ACCESS_TOKEN = os.getenv("SHOPIFY_ADMIN_TOKEN", "").strip()
API_VERSION = os.getenv("SHOPIFY_API_VERSION", "2024-07").strip()
REQUEST_TIMEOUT = float(os.getenv("SHOPIFY_TIMEOUT", "30"))
RATE_LIMIT_DELAY = float(os.getenv("SHOPIFY_RATE_DELAY", "0.6"))
SCRAPER_TAG = os.getenv("SHOPIFY_SCRAPER_TAG", "padel-scraper-1").strip() or "padel-scraper-1"

# >>> Tags de control <<<
KEEP_DRAFT_TAG = os.getenv("SHOPIFY_KEEP_DRAFT_TAG", "scraper:keep-draft").strip()

# NUEVO: Acepta varios alias de ignore (incluye "no tocar")
IGNORE_TAGS = {
    t.strip().lower()
    for t in os.getenv("SHOPIFY_IGNORE_TAGS", "scraper:ignore,no tocar").split(",")
    if t.strip()
}

HEADERS = {
    "Content-Type": "application/json",
    "X-Shopify-Access-Token": ACCESS_TOKEN,
}

GRAPHQL_URL = f"https://{SHOP_DOMAIN}/admin/api/{API_VERSION}/graphql.json"

_existing_products_cache: Optional[Dict[str, int]] = None


class ShopifyUploaderError(Exception):
    pass


def log(message: str) -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now}] {message}")


# >>> NUEVO: helpers para tags <<<
def _split_tags(tags_raw: str) -> Set[str]:
    """Convierte string de tags CSV a set."""
    return {t.strip() for t in (tags_raw or "").split(",") if t and t.strip()}


def _has_ignore_tag(tags: Set[str]) -> bool:
    """Verifica si el producto tiene algún tag de ignore."""
    return any(t.lower() in IGNORE_TAGS for t in tags)


def clean_price(value: str) -> Optional[str]:
    if not value:
        return None
    cleaned = value.replace("€", "").replace("EUR", "").replace("euros", "")
    cleaned = cleaned.replace(" ", "")
    cleaned = cleaned.replace(",", ".")
    cleaned = re.sub(r"[^0-9.]", "", cleaned)
    if not cleaned:
        return None
    if cleaned.count(".") > 1:
        parts = cleaned.split(".")
        cleaned = "".join(parts[:-1]) + "." + parts[-1]
    return cleaned


def normalize_barcode(s: str) -> str:
    """Limpia el barcode: quita comillas simples de Excel, espacios, etc."""
    s = (s or "").strip()
    if s.startswith("'"):
        s = s[1:]
    s = s.replace(" ", "")
    return re.sub(r"[^0-9A-Za-z_-]", "", s)


def parse_inventory_quantity(value: str) -> int:
    if not value:
        return 0
    try:
        return int(re.sub(r"[^0-9]", "", value))
    except ValueError:
        return 0


def discover_latest_directory(base_path: Path) -> Path:
    candidates = sorted(
        [p for p in base_path.glob("Extracción_*") if p.is_dir()],
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not candidates:
        raise ShopifyUploaderError("No se encontró ninguna carpeta 'Extracción_*'.")
    return candidates[0]


def group_rows_by_handle(csv_path: Path) -> Dict[str, List[Dict[str, str]]]:
    groups: Dict[str, List[Dict[str, str]]] = {}
    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            handle = (row.get("URL handle") or "").strip()
            if not handle:
                log(f"⚠️ Fila sin 'URL handle' en {csv_path.name}, se omite.")
                continue
            groups.setdefault(handle, []).append(row)
    return groups


def build_product_payload(rows: List[Dict[str, str]]) -> Dict:
    base = rows[0]
    title = base.get("Title", "").strip()
    description = base.get("Description", "")
    vendor = base.get("Vendor", "")
    product_type = base.get("Type", "")
    tags = base.get("Tags", "")
    handle = base.get("URL handle", "")
    image_url = base.get("Product image URL", "").strip()
    option_name = base.get("Option1 name", "").strip() or "Title"

    options = [{"name": option_name}]
    variants = []

    for variant_row in rows:
        option_value = variant_row.get("Option1 value", "").strip() or "Default Title"
        price = clean_price(variant_row.get("Price", ""))
        compare = clean_price(variant_row.get("Compare-at price", ""))
        inventory_quantity = parse_inventory_quantity(variant_row.get("Inventory quantity", ""))

        variant = {
            "option1": option_value,
            "price": price or "0.00",
            "inventory_management": "shopify",
            "inventory_policy": "deny",
            "inventory_quantity": inventory_quantity,
            "requires_shipping": True,
        }

        if compare:
            variant["compare_at_price"] = compare

        sku = variant_row.get("SKU", "").strip()
        if sku:
            variant["sku"] = sku

        barcode = normalize_barcode(variant_row.get("Barcode", ""))
        if barcode:
            variant["barcode"] = barcode

        variants.append(variant)

    tag_list = [tag.strip() for tag in tags.split(",") if tag.strip()]
    if SCRAPER_TAG not in tag_list:
        tag_list.append(SCRAPER_TAG)

    product = {
        "title": title,
        "body_html": description,
        "vendor": vendor,
        "product_type": product_type,
        "tags": ", ".join(tag_list),
        "handle": handle,
        "status": "active" if (base.get("Status", "").strip().lower() == "active") else "draft",
        "options": options,
        "variants": variants,
    }

    if option_name == "Title":
        product["options"] = [{"name": "Title"}]

    if image_url:
        product["images"] = [{"src": image_url, "position": 1, "alt": base.get("Image alt text", "") or title}]

    return {"product": product}


def shopify_request(method: str, endpoint: str, *, params=None, json=None) -> requests.Response:
    url = f"https://{SHOP_DOMAIN}/admin/api/{API_VERSION}/{endpoint}"
    response = requests.request(
        method=method,
        url=url,
        headers=HEADERS,
        params=params,
        json=json,
        timeout=REQUEST_TIMEOUT,
    )
    if response.status_code >= 400:
        raise ShopifyUploaderError(
            f"Error {response.status_code} en {method} {endpoint}: {response.text}"
        )
    time.sleep(RATE_LIMIT_DELAY)
    return response


def shopify_graphql(query: str, variables: dict = None) -> dict:
    """Ejecuta una query GraphQL contra Shopify."""
    payload = {"query": query, "variables": variables or {}}
    resp = requests.post(
        GRAPHQL_URL,
        headers=HEADERS,
        json=payload,
        timeout=REQUEST_TIMEOUT,
    )

    if resp.status_code >= 400:
        preview = resp.text.strip()
        preview = preview[:400] + ("…" if len(preview) > 400 else "")
        raise ShopifyUploaderError(
            f"GraphQL error HTTP {resp.status_code}: {preview or '<cuerpo vacío>'}"
        )

    if not resp.text.strip():
        raise ShopifyUploaderError(
            f"GraphQL respuesta vacía (status {resp.status_code})"
        )

    try:
        data = resp.json()
    except requests.exceptions.JSONDecodeError as exc:
        preview = resp.text.strip()
        preview = preview[:400] + ("…" if len(preview) > 400 else "")
        raise ShopifyUploaderError(
            f"GraphQL respuesta no JSON (status {resp.status_code}): {preview or '<cuerpo vacío>'}"
        ) from exc

    if "errors" in data:
        raise ShopifyUploaderError(f"GraphQL error: {resp.status_code} {data}")

    if "data" not in data or data["data"] is None:
        raise ShopifyUploaderError(
            f"GraphQL respuesta sin campo 'data' (status {resp.status_code}): {data}"
        )
    time.sleep(RATE_LIMIT_DELAY)
    return data["data"]


def find_product_by_barcode_or_sku(barcode: str, sku: str):
    """Busca un producto por barcode o SKU. Devuelve {id, handle} o None."""
    q = f"barcode:{barcode}" if barcode else (f"sku:{sku}" if sku else None)
    if not q:
        return None

    data = shopify_graphql("""
      query ($q: String!) {
        productVariants(first: 1, query: $q) {
          edges {
            node {
              id
              product { id handle }
            }
          }
        }
      }
    """, {"q": q})

    edges = data.get("productVariants", {}).get("edges", [])
    if not edges:
        return None

    prod = edges[0]["node"]["product"]
    return {"id": int(prod["id"].split("/")[-1]), "handle": prod.get("handle")}


def create_redirect(old_handle: str, new_handle: str) -> None:
    """Crea un redirect 301 si el handle cambió."""
    if not old_handle or not new_handle or old_handle == new_handle:
        return
    try:
        shopify_request("POST", "redirects.json", json={
            "redirect": {"path": f"/products/{old_handle}", "target": f"/products/{new_handle}"}
        })
        log(f"🔁 Redirect 301 /products/{old_handle} → /products/{new_handle}")
    except ShopifyUploaderError as exc:
        log(f"⚠️ No se pudo crear redirect: {exc}")


def _fetch_products_by_status(status: Optional[str] = None) -> Dict[str, int]:
    params_base = {"limit": 250, "fields": "id,handle"}
    if status:
        params_base["status"] = status

    products: Dict[str, int] = {}
    last_id: Optional[int] = None

    while True:
        params = dict(params_base)
        if last_id:
            params["since_id"] = last_id

        response = shopify_request("GET", "products.json", params=params)
        data = response.json().get("products", [])
        if not data:
            break

        for product in data:
            handle = product.get("handle")
            product_id = product.get("id")
            if handle and product_id:
                products[handle] = product_id

        last_id = data[-1].get("id")
        if not last_id or len(data) < params_base["limit"]:
            break

    return products


def _load_existing_products_cache() -> Dict[str, int]:
    global _existing_products_cache
    if _existing_products_cache is not None:
        return _existing_products_cache

    products: Dict[str, int] = {}
    for status in (None, "draft", "archived"):
        status_products = _fetch_products_by_status(status=status)
        for handle, product_id in status_products.items():
            products.setdefault(handle, product_id)

    _existing_products_cache = products
    return _existing_products_cache


def _update_products_cache(handle: str, product_id: Optional[int]) -> None:
    if _existing_products_cache is None:
        return
    if product_id is None:
        _existing_products_cache.pop(handle, None)
    else:
        _existing_products_cache[handle] = product_id


def find_existing_product_id(handle: str) -> Optional[int]:
    if not handle:
        return None
    products = _load_existing_products_cache()
    return products.get(handle)


def delete_product(product_id: int) -> None:
    shopify_request("DELETE", f"products/{product_id}.json")
    log(f"🧹 Producto existente {product_id} eliminado para rehacerlo.")


def add_image_to_product(product_id: int, image_url: str, alt_text: str = "", *, max_retries: int = 3) -> bool:
    """
    Sube una imagen a un producto existente usando la API REST de imágenes.
    Hace reintentos básicos para mitigar errores intermitentes de Shopify.
    """
    if not product_id or not image_url:
        return False

    payload = {
        "image": {
            "src": image_url,
            "position": 1,
        }
    }
    if alt_text:
        payload["image"]["alt"] = alt_text

    endpoint = f"products/{product_id}/images.json"
    for attempt in range(1, max_retries + 1):
        try:
            shopify_request("POST", endpoint, json=payload)
            log(f"✅ Imagen añadida al producto {product_id}")
            return True
        except ShopifyUploaderError as exc:
            log(f"⚠️ Error al añadir imagen (intento {attempt}/{max_retries}): {exc}")
        except Exception as exc:  # pragma: no cover - defensivo
            log(f"❌ Excepción al subir imagen (intento {attempt}/{max_retries}): {exc}")

        if attempt < max_retries:
            time.sleep(2)

    log(f"❌ No se pudo subir la imagen del producto {product_id} tras {max_retries} intentos.")
    return False


def create_product(payload: Dict) -> Dict:
    # Shopify a veces ignora la imagen inicial si va en el mismo payload.
    # La quitamos y la añadimos manualmente tras la creación.
    product_block = payload.get("product") or {}
    images_data = product_block.pop("images", None)

    image_url = None
    alt_text = ""
    if images_data:
        first_image = images_data[0] or {}
        image_url = first_image.get("src") or None
        alt_text = first_image.get("alt") or product_block.get("title", "") or ""

    response = shopify_request("POST", "products.json", json=payload)
    product = response.json().get("product", {}) or {}

    if image_url and product.get("id"):
        added = add_image_to_product(product["id"], image_url, alt_text)
        if not added:
            log(f"⚠️ El producto {product.get('handle') or product['id']} quedó sin imagen.")

    return product


def fetch_product(product_id: int) -> dict:
    """Lee un producto completo desde Shopify."""
    resp = shopify_request("GET", f"products/{product_id}.json")
    return resp.json().get("product", {}) or {}


def process_csv(
    csv_path: Path, *, dry_run: bool = False, delete_existing: bool = True
) -> Tuple[Dict[str, int], Set[str]]:
    summary = {"created": 0, "skipped": 0, "deleted": 0}
    groups = group_rows_by_handle(csv_path)

    if not groups:
        log(f"⚠️ CSV {csv_path.name} sin filas válidas.")
        return summary, set()

    for handle, rows in groups.items():
        log(f"➡️ Procesando producto '{handle}' ({len(rows)} variantes)")

        # Construir payload desde CSV
        try:
            payload = build_product_payload(rows)
        except Exception as exc:
            log(f"❌ Error preparando el payload para '{handle}': {exc}")
            summary["skipped"] += 1
            continue

        if dry_run:
            log(f"🔎 Dry-run: no se sube '{handle}'.")
            summary["skipped"] += 1
            continue

        # Lookup por barcode/SKU primero
        base = rows[0]
        key_barcode = normalize_barcode(base.get("Barcode") or "")
        key_sku = (base.get("SKU") or "").strip()

        found = None
        for attempt in range(3):
            try:
                found = find_product_by_barcode_or_sku(key_barcode, key_sku)
                break
            except ShopifyUploaderError as exc:
                if attempt < 2:
                    log(f"⚠️ Lookup falló (intento {attempt + 1}/3): {exc}")
                    time.sleep(2)
                else:
                    log(f"⚠️ Lookup por EAN/SKU falló después de 3 intentos: {exc}")

        existing_id = None
        old_handle = None

        if found:
            existing_id = found["id"]
            old_handle = found.get("handle")

        if not existing_id:
            existing_id = find_existing_product_id(handle)

        # >>> Leer status y tags del producto existente <<<
        existing_status = None
        existing_tags: Set[str] = set()
        if existing_id:
            try:
                existing_product = fetch_product(existing_id)
                existing_status = (existing_product.get("status") or "").lower()
                existing_tags = _split_tags(existing_product.get("tags", ""))
            except ShopifyUploaderError as exc:
                log(f"⚠️ No se pudo leer el producto {existing_id}: {exc}")

        # >>> A) IGNORE: no tocar <<<
        if _has_ignore_tag(existing_tags):
            log(f"🚫 '{handle}' tiene tag de ignore (no tocar), omitido")
            summary["skipped"] += 1
            continue

        # >>> B) LOCK DE DRAFT: si el merchant lo tiene en borrador, se queda en borrador <<<
        if existing_status == "draft" or KEEP_DRAFT_TAG in existing_tags:
            payload["product"]["status"] = "draft"
            # Preservar el tag keep-draft para que se vea claro en el Admin
            new_tags = _split_tags(payload["product"].get("tags") or "")
            new_tags.add(KEEP_DRAFT_TAG)
            payload["product"]["tags"] = ", ".join(sorted(new_tags))
            log(f"📝 '{handle}' mantenido en borrador (status=draft o tag={KEEP_DRAFT_TAG})")

        # >>> Procesar producto <<<
        try:
            if existing_id and delete_existing:
                delete_product(existing_id)
                deleted_handle = old_handle or handle
                _update_products_cache(deleted_handle, None)
                summary["deleted"] += 1
            elif existing_id:
                log(f"ℹ️ Producto '{handle}' ya existe y 'delete_existing' es False. Se omite.")
                summary["skipped"] += 1
                continue

            created = create_product(payload)
            if created.get("id"):
                summary["created"] += 1
                _update_products_cache(handle, created["id"])
                log(f"✅ Producto '{handle}' creado (ID {created['id']}).")

                # Crear redirect 301 si el handle cambió
                if old_handle and old_handle != handle:
                    create_redirect(old_handle, handle)
            else:
                summary["skipped"] += 1
                log(f"⚠️ Shopify no devolvió ID para '{handle}'.")

        except ShopifyUploaderError as exc:
            summary["skipped"] += 1
            log(f"❌ Error subiendo '{handle}': {exc}")
        except Exception as exc:
            summary["skipped"] += 1
            log(f"❌ Error inesperado con '{handle}': {exc}")

    return summary, set(groups.keys())


def list_scraper_products() -> Dict[str, Dict[str, Any]]:
    """
    Return mapping handle -> {id, tags} for products tagged by the scraper.
    NUEVO: Devuelve también los tags para respetar ignore en prune.
    """
    products: Dict[str, Dict[str, Any]] = {}
    params_base = {"limit": 250, "fields": "id,handle,tags"}
    last_id: Optional[int] = None

    while True:
        params = dict(params_base)
        if last_id:
            params["since_id"] = last_id

        response = shopify_request("GET", "products.json", params=params)
        data = response.json().get("products", [])
        if not data:
            break

        for product in data:
            handle = product.get("handle")
            if not handle:
                continue

            tags = _split_tags(product.get("tags", ""))
            if SCRAPER_TAG in tags:
                products[handle] = {
                    "id": product.get("id"),
                    "tags": tags
                }

        last_id = data[-1].get("id")
        if not last_id or len(data) < params_base["limit"]:
            break

    return products


def prune_missing_scraper_products(
    *, current_handles: Set[str], dry_run: bool = False
) -> int:
    """
    Delete scraper-managed products that are not present in current handles.
    NUEVO: Respeta productos con tags de ignore (scraper:ignore, no tocar).
    """
    if dry_run:
        log("🔎 Dry-run activo: no se eliminan productos ausentes.")
        return 0

    products = list_scraper_products()
    to_delete = []

    for handle, meta in products.items():
        if handle in current_handles:
            continue

        # NUEVO: Respetar productos con tags de ignore
        tags = meta.get("tags", set())
        if _has_ignore_tag(tags) or KEEP_DRAFT_TAG in tags:
            log(f"🛡️ '{handle}' protegido por tags, no se elimina en prune")
            continue

        to_delete.append((handle, meta["id"]))

    if not to_delete:
        log("✅ No hay productos obsoletos para eliminar")
        return 0

    deleted = 0
    for handle, product_id in to_delete:
        try:
            delete_product(product_id)
            deleted += 1
            log(f"🧹 Producto '{handle}' eliminado por faltar en el scrape actual.")
        except ShopifyUploaderError as exc:
            log(f"⚠️ No se pudo eliminar '{handle}': {exc}")

    return deleted


def run(
    source_dir: Path,
    *,
    dry_run: bool = False,
    delete_existing: bool = True,
    disable_prune: bool = False,
) -> Dict[str, int]:
    if not SHOP_DOMAIN or not ACCESS_TOKEN:
        raise ShopifyUploaderError("Variables SHOPIFY_DOMAIN y SHOPIFY_ADMIN_TOKEN requeridas.")

    summary_total = {"created": 0, "skipped": 0, "deleted": 0, "pruned": 0}
    csv_files = sorted(source_dir.glob("*.csv"))

    if not csv_files:
        log(f"⚠️ No se encontraron CSV en {source_dir}")
        return summary_total

    current_handles: Set[str] = set()

    for csv_path in csv_files:
        log(f"📄 Subiendo CSV: {csv_path.name}")
        summary, handles = process_csv(
            csv_path, dry_run=dry_run, delete_existing=delete_existing
        )
        for key, value in summary.items():
            summary_total[key] += value
        current_handles.update(handles)

    if delete_existing and current_handles and not disable_prune:
        summary_total["pruned"] = prune_missing_scraper_products(
            current_handles=current_handles, dry_run=dry_run
        )

    return summary_total


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Subir productos Shopify con protección de borrador e ignore.",
        epilog="""
Tags de control disponibles:
  scraper:keep-draft    → Mantener siempre en borrador
  scraper:ignore        → No tocar este producto nunca
  no tocar              → Alias de scraper:ignore
  
Variables de entorno:
  SHOPIFY_DOMAIN              Tu dominio de Shopify
  SHOPIFY_ADMIN_TOKEN         Token de acceso admin
  SHOPIFY_KEEP_DRAFT_TAG      Tag para borrador (default: scraper:keep-draft)
  SHOPIFY_IGNORE_TAGS         Tags de ignore separados por comas (default: scraper:ignore,no tocar)
        """
    )
    parser.add_argument("--source-dir", type=Path, help="Directorio con los CSV a subir.")
    parser.add_argument("--dry-run", action="store_true", help="No realiza llamadas a Shopify, solo muestra pasos.")
    parser.add_argument(
        "--keep-existing",
        action="store_true",
        help="Si se especifica, no elimina productos existentes; simplemente omite los duplicados.",
    )
    parser.add_argument(
        "--disable-prune",
        action="store_true",
        help="No eliminar productos antiguos aunque falten en los CSV actuales.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    try:
        base_path = Path.cwd()
        source_dir = args.source_dir
        if source_dir is None:
            source_dir = discover_latest_directory(base_path)
        else:
            source_dir = source_dir.expanduser().resolve()

        log(f"📂 Directorio de origen: {source_dir}")
        log(f"🏷️  Tags de ignore configurados: {', '.join(IGNORE_TAGS)}")
        log(f"📝 Tag de borrador: {KEEP_DRAFT_TAG}")

        summary = run(
            source_dir,
            dry_run=args.dry_run,
            delete_existing=not args.keep_existing,
            disable_prune=args.disable_prune,
        )

        log(
            "📊 Resumen subida -> creados: {created}, borrados: {deleted}, omitidos: {skipped}, pruned: {pruned}".format(
                created=summary["created"],
                deleted=summary["deleted"],
                skipped=summary["skipped"],
                pruned=summary["pruned"],
            )
        )
        return 0

    except ShopifyUploaderError as exc:
        log(f"❌ {exc}")
        return 1
    except KeyboardInterrupt:
        log("⏹️ Proceso interrumpido por el usuario.")
        return 130


if __name__ == "__main__":
    sys.exit(main())
