"""
upload_shopify_hybrid.py - Versión híbrida con detección inteligente de cambios

ESTRATEGIA:
1. Si el producto NO existe → CREATE
2. Si cambió la estructura de variantes (tallas/colores) → DELETE + CREATE
3. Si solo cambió precio/stock/imagen → UPDATE selectivo
4. Si nada cambió → SKIP

VENTAJAS:
- Detecta cambios en variantes (tallas, colores añadidos/eliminados)
- Solo recrea cuando es necesario
- Actualiza precio/stock/imagen sin recrear
- Skip ultra-rápido si nada cambió
- Tiempo estimado: 2-3 horas (vs 20+ horas del script original)

MEJORAS v2:
- Auto-carga .env
- Flag --skip-images para procesar productos sin imágenes
- Reporte de imágenes fallidas
- Detección inteligente de imágenes duplicadas por filename
- Normalización de tallas (evita recreación por mayúsculas/minúsculas)
- Eliminación automática de placeholders al reemplazar

CONFIGURACIÓN OPTIMIZADA:
- TIMEOUT: 30s (suficiente para imágenes, no excesivo)
- DELAY: 0.75s (bajo límite rate, pero rápido)
- SLEEP IMAGEN: 1.5s (tiempo para que Shopify procese)
- REINTENTOS: 2.5s incrementales (2.5, 5, 7.5, 10, 12.5s)
"""
import argparse
import csv
import json
import os
import re
import sys
import time
from datetime import datetime
from http.client import RemoteDisconnected
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, Any, Type
from urllib.parse import urlparse

import requests
from requests.exceptions import ConnectionError, Timeout, RequestException

try:
    from urllib3.exceptions import ProtocolError
except Exception:
    ProtocolError = None

# Auto-cargar .env si existe
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    # Si python-dotenv no está instalado, intentar cargar manualmente
    env_path = Path(__file__).parent / ".env"
    if env_path.exists():
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, value = line.split("=", 1)
                    os.environ.setdefault(key.strip(), value.strip())

SHOP_DOMAIN = os.getenv("SHOPIFY_DOMAIN", "").strip()
ACCESS_TOKEN = os.getenv("SHOPIFY_ADMIN_TOKEN", "").strip()
API_VERSION = os.getenv("SHOPIFY_API_VERSION", "2024-07").strip()
REQUEST_TIMEOUT = float(os.getenv("SHOPIFY_TIMEOUT", "30"))  # ✅ Balance: 30s
RATE_LIMIT_DELAY = float(os.getenv("SHOPIFY_RATE_DELAY", "0.75"))  # ✅ Balance: 0.75s
SCRAPER_TAG = os.getenv("SHOPIFY_SCRAPER_TAG", "padel-scraper-1").strip() or "padel-scraper-1"
MAX_RETRIES = int(os.getenv("SHOPIFY_MAX_RETRIES", "3"))
SHOPIFY_LOCATION_NAME = os.getenv("SHOPIFY_LOCATION_NAME", "").strip()  # ✅ Location específico (opcional)

KEEP_DRAFT_TAG = os.getenv("SHOPIFY_KEEP_DRAFT_TAG", "scraper:keep-draft").strip()
IGNORE_TAGS = {
    t.strip().lower()
    for t in os.getenv("SHOPIFY_IGNORE_TAGS", "scraper:ignore,no tocar").split(",")
    if t.strip()
}

HEADERS = {
    "Content-Type": "application/json",
    "X-Shopify-Access-Token": ACCESS_TOKEN,
}

_existing_products_cache: Optional[Dict[str, int]] = None
_locations_cache: Optional[List[dict]] = None
_failed_images_report: List[Dict[str, str]] = []

_NETWORK_ERRORS: Tuple[Type[BaseException], ...]
_base_errors: Tuple[Type[BaseException], ...] = (
    ConnectionError,
    Timeout,
    RequestException,
    RemoteDisconnected,
)
if ProtocolError is not None:
    _NETWORK_ERRORS = _base_errors + (ProtocolError,)
else:
    _NETWORK_ERRORS = _base_errors


class ShopifyUploaderError(Exception):
    pass


def log(message: str) -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now}] {message}")
    sys.stdout.flush()


def _split_tags(tags_raw: str) -> Set[str]:
    return {t.strip() for t in (tags_raw or "").split(",") if t and t.strip()}


def _has_ignore_tag(tags: Set[str]) -> bool:
    return any(t.lower() in IGNORE_TAGS for t in tags)


def _is_description_empty(html: str) -> bool:
    """
    Verifica si una descripción HTML está realmente vacía.
    Considera vacío: None, "", solo espacios, solo <br>, <p></p>, etc.
    """
    if not html:
        return True
    
    # Quitar espacios
    text = html.strip()
    if not text:
        return True
    
    # Quitar tags HTML comunes que no aportan contenido
    # Eliminar: <br>, <br/>, <p></p>, <div></div>, &nbsp;, espacios
    cleaned = re.sub(r'<br\s*/?>', '', text, flags=re.IGNORECASE)
    cleaned = re.sub(r'<p>\s*</p>', '', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'<div>\s*</div>', '', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'&nbsp;', '', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\s+', '', cleaned)
    
    # Si después de limpiar queda algo, no está vacío
    return len(cleaned) == 0


def clean_price(value: str) -> Optional[str]:
    if not value:
        return None
    cleaned = value.replace("€", "").replace("EUR", "").replace("euros", "")
    cleaned = cleaned.replace(" ", "").replace(",", ".")
    cleaned = re.sub(r"[^0-9.]", "", cleaned)
    if not cleaned:
        return None
    if cleaned.count(".") > 1:
        parts = cleaned.split(".")
        cleaned = "".join(parts[:-1]) + "." + parts[-1]
    return cleaned


def _to_float(value: Any) -> float:
    """Convierte a float de forma segura; si falla, devuelve 0.0."""
    try:
        if value is None:
            return 0.0
        if isinstance(value, (int, float)):
            return float(value)
        s = str(value).strip()
        if not s:
            return 0.0
        return float(s)
    except Exception:
        return 0.0


def normalize_barcode(s: str) -> str:
    s = (s or "").strip()
    if s.startswith("'"):
        s = s[1:]
    s = s.replace(" ", "")
    return re.sub(r"[^0-9A-Za-z_-]", "", s)


def parse_inventory_quantity(value: str) -> int:
    if not value:
        return 0
    try:
        cleaned = re.sub(r"[^0-9-]", "", value)
        if cleaned == "" or cleaned == "-":
            return 0
        return max(0, int(cleaned))  # ✅ Clamp a 0 (evita negativos)
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
                continue
            groups.setdefault(handle, []).append(row)
    return groups


def shopify_request(method: str, endpoint: str, *, params=None, json=None) -> requests.Response:
    """Realiza peticiones a Shopify con reintentos automáticos."""
    url = f"https://{SHOP_DOMAIN}/admin/api/{API_VERSION}/{endpoint}"

    for attempt in range(MAX_RETRIES):
        try:
            response = requests.request(
                method=method,
                url=url,
                headers=HEADERS,
                params=params,
                json=json,
                timeout=REQUEST_TIMEOUT,
            )

            if response.status_code == 429:
                retry_after = float(response.headers.get("Retry-After", "5"))
                log(f"⏳ Rate limit alcanzado, esperando {retry_after}s...")
                time.sleep(retry_after)
                continue

            if response.status_code >= 500:
                if attempt < MAX_RETRIES - 1:
                    wait_time = (2 ** attempt) * 2
                    log(f"⚠️ Error {response.status_code} del servidor, reintentando en {wait_time}s...")
                    time.sleep(wait_time)
                    continue

            if response.status_code >= 400:
                raise ShopifyUploaderError(
                    f"Error {response.status_code} en {method} {endpoint}: {response.text[:500]}"
                )

            time.sleep(RATE_LIMIT_DELAY)
            return response

        except _NETWORK_ERRORS as e:
            if attempt < MAX_RETRIES - 1:
                wait_time = (2 ** attempt) * 2
                log(f"⚠️ Error de conexión (intento {attempt + 1}/{MAX_RETRIES}): {str(e)[:100]}")
                time.sleep(wait_time)
            else:
                raise ShopifyUploaderError(f"Conexión perdida después de {MAX_RETRIES} intentos: {e}")

    raise ShopifyUploaderError(f"No se pudo completar {method} {endpoint}")


def fetch_product(product_id: int) -> dict:
    """Lee un producto completo desde Shopify."""
    resp = shopify_request("GET", f"products/{product_id}.json")
    return resp.json().get("product", {}) or {}


def delete_product(product_id: int) -> None:
    """Elimina un producto de Shopify."""
    shopify_request("DELETE", f"products/{product_id}.json")
    log(f"  🗑️ Producto {product_id} eliminado")


def _load_existing_products_cache() -> Dict[str, int]:
    """Carga todos los productos existentes. Mapea handle -> product_id."""
    global _existing_products_cache
    if _existing_products_cache is not None:
        return _existing_products_cache

    log("📥 Cargando caché de productos existentes...")
    products: Dict[str, int] = {}
    params_base = {"limit": 250, "fields": "id,handle"}
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
                products.setdefault(handle, product_id)

        last_id = data[-1].get("id")
        if not last_id or len(data) < params_base["limit"]:
            break

    _existing_products_cache = products
    log(f"✅ Caché cargada: {len(products)} productos")
    return _existing_products_cache


def _update_products_cache(handle: str, product_id: Optional[int]) -> None:
    """Actualiza la caché de productos."""
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


def find_product_by_barcode_or_sku(barcode: str, sku: str) -> Optional[Dict[str, Any]]:
    """
    Busca un producto por barcode o SKU usando GraphQL.
    Retorna {id, handle} o None.
    """
    q = f"barcode:{barcode}" if barcode else (f"sku:{sku}" if sku else None)
    if not q:
        return None

    try:
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
        return {
            "id": int(prod["id"].split("/")[-1]),
            "handle": prod.get("handle")
        }
    except Exception as exc:
        log(f"⚠️ Error buscando por EAN/SKU: {exc}")
        return None


def shopify_graphql(query: str, variables: dict = None) -> dict:
    """Ejecuta una query GraphQL contra Shopify con reintentos."""
    GRAPHQL_URL = f"https://{SHOP_DOMAIN}/admin/api/{API_VERSION}/graphql.json"
    payload = {"query": query, "variables": variables or {}}
    
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.post(
                GRAPHQL_URL,
                headers=HEADERS,
                json=payload,
                timeout=REQUEST_TIMEOUT,
            )
            
            if resp.status_code == 429:
                retry_after = float(resp.headers.get('Retry-After', '5'))
                log(f"⏳ Rate limit GraphQL, esperando {retry_after}s...")
                time.sleep(retry_after)
                continue

            if resp.status_code >= 500:
                if attempt < MAX_RETRIES - 1:
                    wait_time = (2 ** attempt) * 2
                    log(f"⚠️ Error GraphQL {resp.status_code}, reintentando en {wait_time}s...")
                    time.sleep(wait_time)
                    continue

            if resp.status_code >= 400:
                preview = resp.text.strip()[:400]
                raise ShopifyUploaderError(
                    f"GraphQL error HTTP {resp.status_code}: {preview}"
                )

            if not resp.text.strip():
                raise ShopifyUploaderError(
                    f"GraphQL respuesta vacía (status {resp.status_code})"
                )

            try:
                data = resp.json()
            except (ValueError, json.JSONDecodeError) as exc:
                preview = resp.text.strip()[:400]
                raise ShopifyUploaderError(
                    f"GraphQL respuesta no JSON (status {resp.status_code}): {preview}"
                ) from exc

            if "errors" in data:
                raise ShopifyUploaderError(f"GraphQL error: {resp.status_code} {data}")

            if "data" not in data or data["data"] is None:
                raise ShopifyUploaderError(
                    f"GraphQL respuesta sin campo 'data' (status {resp.status_code}): {data}"
                )
            
            time.sleep(RATE_LIMIT_DELAY)
            return data["data"]
            
        except _NETWORK_ERRORS as e:
            if attempt < MAX_RETRIES - 1:
                wait_time = (2 ** attempt) * 2
                log(f"⚠️ Error GraphQL de conexión (intento {attempt + 1}/{MAX_RETRIES})")
                time.sleep(wait_time)
            else:
                raise ShopifyUploaderError(
                    f"GraphQL falló después de {MAX_RETRIES} intentos: {e}"
                )
    
    raise ShopifyUploaderError(f"GraphQL agotó {MAX_RETRIES} reintentos sin éxito")


def get_primary_location_id() -> Optional[int]:
    """
    Devuelve el location_id de Shopify, usando caché.
    
    Si SHOPIFY_LOCATION_NAME está definido, busca por nombre.
    Si no, devuelve el primero disponible.
    """
    global _locations_cache
    if _locations_cache is None:
        resp = shopify_request("GET", "locations.json")
        _locations_cache = resp.json().get("locations", []) or []

    if not _locations_cache:
        return None

    # ✅ Si hay nombre específico, buscar por nombre
    if SHOPIFY_LOCATION_NAME:
        for loc in _locations_cache:
            if (loc.get("name") or "").strip().lower() == SHOPIFY_LOCATION_NAME.lower():
                return loc.get("id")
        log(f"⚠️ Location '{SHOPIFY_LOCATION_NAME}' no encontrado, usando primero disponible")

    # Devolver el primero
    return _locations_cache[0].get("id")


def add_image_to_product(product_id: int, image_url: str, alt_text: str = "", *, handle: str = "", max_retries: int = 5) -> bool:
    """
    Sube una imagen a un producto con delays y reintentos optimizados.
    
    CONFIGURACIÓN OPTIMIZADA:
    - Delay inicial: 1.5s (suficiente para que Shopify procese el producto)
    - Reintentos: 2.5s incrementales (2.5, 5, 7.5, 10, 12.5s)
    - Total máximo: 37.5s si falla 5 veces
    """
    if not product_id or not image_url:
        return False

    # ✅ Dar tiempo a Shopify para que procese el producto recién creado
    time.sleep(1.5)

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
            if attempt > 1:
                wait_time = attempt * 2.5  # ✅ 2.5, 5, 7.5, 10, 12.5 segundos
                log(f"  ⏳ Esperando {wait_time}s antes de reintentar imagen...")
                time.sleep(wait_time)

            shopify_request("POST", endpoint, json=payload)
            log(f"  ✅ Imagen añadida al producto {product_id}")
            time.sleep(RATE_LIMIT_DELAY)
            return True

        except ShopifyUploaderError as exc:
            error_msg = str(exc)
            if "422" in error_msg or "Invalid image" in error_msg:
                log(f"  ❌ Imagen inválida o no accesible: {image_url}")
                _failed_images_report.append({
                    "handle": handle,
                    "image_url": image_url,
                    "reason": f"Invalid/422: {error_msg[:100]}"
                })
                return False
            log(f"  ⚠️ Error al añadir imagen (intento {attempt}/{max_retries}): {exc}")

        except Exception as exc:
            log(f"  ❌ Excepción inesperada (intento {attempt}/{max_retries}): {exc}")

    log(f"  ❌ No se pudo subir la imagen del producto {product_id} tras {max_retries} intentos.")
    _failed_images_report.append({
        "handle": handle,
        "image_url": image_url,
        "reason": f"Max retries ({max_retries}) exceeded"
    })
    return False


def compare_variants_structure(shopify_variants: List[dict], csv_variants: List[dict]) -> bool:
    """
    Detecta si cambió la ESTRUCTURA de variantes (tallas/colores añadidos o eliminados).
    Retorna True si hay diferencias estructurales que requieren recrear el producto.
    
    NOTA: SKU/Barcode NO se consideran estructurales, se actualizan en update_product_smart()
    MEJORA: Normaliza en lowercase para evitar recreación por "M" vs "m"
    """
    # 1. Comparar número de variantes
    if len(shopify_variants) != len(csv_variants):
        log("  🔍 Número de variantes cambió")
        return True

    # 2. Extraer option1 de ambos lados, normalizar a lowercase y ordenar
    shopify_options = sorted([(v.get("option1", "") or "").strip().lower() for v in shopify_variants])
    csv_options = sorted([(v.get("option1", "") or "").strip().lower() for v in csv_variants])

    # 3. Comparar si las opciones son idénticas
    if shopify_options != csv_options:
        log("  🔍 Variantes (tallas/colores) cambiaron")
        return True

    # ✅ SKU/Barcode NO causan recreación, se actualizan en update_product_smart()
    return False


def compare_prices(shopify_variants: List[dict], csv_variants: List[dict]) -> bool:
    """Compara precios entre Shopify y CSV. Retorna True si hay diferencias."""
    if len(shopify_variants) != len(csv_variants):
        return True

    # Ordenar por option1 en lowercase para emparejar correctamente
    shopify_sorted = sorted(shopify_variants, key=lambda v: (v.get("option1") or "").lower())
    csv_sorted = sorted(csv_variants, key=lambda v: (v.get("option1") or "").lower())

    for s_var, c_var in zip(shopify_sorted, csv_sorted):
        s_price = _to_float(s_var.get("price"))
        c_price = _to_float(c_var.get("price"))

        s_compare = _to_float(s_var.get("compare_at_price"))
        c_compare = _to_float(c_var.get("compare_at_price"))

        if abs(s_price - c_price) > 0.01 or abs(s_compare - c_compare) > 0.01:
            return True

    return False


def compare_inventory(shopify_variants: List[dict], csv_variants: List[dict]) -> bool:
    """Compara inventario entre Shopify y CSV. Retorna True si hay diferencias."""
    if len(shopify_variants) != len(csv_variants):
        return True

    # Ordenar por option1 en lowercase para emparejar correctamente
    shopify_sorted = sorted(shopify_variants, key=lambda v: (v.get("option1") or "").lower())
    csv_sorted = sorted(csv_variants, key=lambda v: (v.get("option1") or "").lower())

    for s_var, c_var in zip(shopify_sorted, csv_sorted):
        s_inv = int(s_var.get("inventory_quantity") or 0)
        c_inv = int(c_var.get("inventory_quantity") or 0)

        if s_inv != c_inv:
            return True

    return False


def update_product_smart(product_id: int, shopify_product: dict, csv_data: dict, *, skip_images: bool = False) -> Dict[str, bool]:
    """
    Actualiza solo los campos que cambiaron (sin recrear el producto).
    Solo se llama cuando NO cambió la estructura de variantes.
    MEJORA: Detección inteligente de imágenes duplicadas por filename
    MEJORA: Emparejamiento case-insensitive de variantes
    """
    updates = {"precio": False, "stock": False, "imagen": False, "otros": False}

    csv_variants = csv_data["variants"]
    shopify_variants = shopify_product.get("variants", [])

    # Helper para emparejar variantes por talla ignorando mayúsculas/minúsculas
    def _find_csv_variant(option1: str):
        """Encuentra variante CSV ignorando case (M vs m, XL vs xl)"""
        key = (option1 or "").strip().lower()
        return next(
            (v for v in csv_variants if (v.get("option1") or "").strip().lower() == key),
            None
        )

    # 1. ACTUALIZAR PRECIOS / COMPARE-AT / SKU / BARCODE (independiente)
    for s_var in shopify_variants:
        option1 = s_var.get("option1")
        csv_var = _find_csv_variant(option1)  # Usando helper case-insensitive
        if not csv_var:
            continue

        to_update: Dict[str, Any] = {"id": s_var["id"]}

        # Comparar precios
        if abs(_to_float(s_var.get("price")) - _to_float(csv_var.get("price"))) > 0.01:
            to_update["price"] = csv_var.get("price")
            updates["precio"] = True

        # Comparar compare_at_price
        s_comp = _to_float(s_var.get("compare_at_price"))
        c_comp = _to_float(csv_var.get("compare_at_price"))
        if abs(s_comp - c_comp) > 0.01:
            to_update["compare_at_price"] = csv_var.get("compare_at_price") or None
            updates["precio"] = True

        # Comparar SKU (independiente de precio)
        if (s_var.get("sku") or "") != (csv_var.get("sku") or ""):
            to_update["sku"] = csv_var.get("sku") or None
            updates["otros"] = True

        # Comparar Barcode (independiente de precio)
        if (s_var.get("barcode") or "") != (csv_var.get("barcode") or ""):
            to_update["barcode"] = csv_var.get("barcode") or None
            updates["otros"] = True

        # Solo hacer PUT si hay algo que cambiar (además del id)
        if len(to_update) > 1:
            shopify_request("PUT", f"variants/{s_var['id']}.json", json={"variant": to_update})

    if updates["precio"]:
        log("  💰 Precios actualizados")

    # 2. ACTUALIZAR INVENTARIO
    if compare_inventory(shopify_variants, csv_variants):
        location_id = get_primary_location_id()
        if not location_id:
            log("  ⚠️ No se pudo obtener location_id")
        else:
            for s_var in shopify_variants:
                option1 = s_var.get("option1")
                csv_var = _find_csv_variant(option1)  # Usando helper case-insensitive

                if csv_var:
                    inventory_item_id = s_var.get("inventory_item_id")
                    if inventory_item_id:
                        shopify_request("POST", "inventory_levels/set.json", json={
                            "location_id": location_id,
                            "inventory_item_id": inventory_item_id,
                            "available": csv_var["inventory_quantity"],
                        })

            updates["stock"] = True
            log("  📦 Stock actualizado")

    # 3. IMÁGENES: añadir solo si no existe ya la misma imagen (MEJORA v2)
    if not skip_images:
        csv_image_url = (csv_data.get("image_url") or "").strip()
        shopify_images = shopify_product.get("images", []) or []

        def _norm(u: str) -> str:
            """Normaliza URL: sin query string, lowercase."""
            return (u or "").split("?")[0].strip().lower()
        
        def _filename(u: str) -> str:
            """Extrae el nombre del archivo de una URL."""
            try:
                return os.path.basename(urlparse(u).path).lower()
            except Exception:
                return _norm(u).rsplit("/", 1)[-1]

        def _is_placeholder(url: str) -> bool:
            u = _norm(url)
            return any(x in u for x in ("no-image", "placeholder", "sin-imagen", "default"))

        if csv_image_url:
            csv_name = _filename(csv_image_url)
            
            # Dedupe: misma URL normalizada O mismo filename ya en la galería
            already_present = any(
                _norm(csv_image_url) == _norm(img.get("src")) or 
                csv_name == _filename(img.get("src"))
                for img in shopify_images
            )
            
            if already_present:
                log("  🖼️ Imagen del CSV ya existe; no se sube")
            elif not shopify_images:
                if add_image_to_product(
                    product_id, 
                    csv_image_url, 
                    csv_data.get("title", ""),
                    handle=shopify_product.get("handle", "")
                ):
                    updates["imagen"] = True
                    log("  🖼️ Imagen añadida (producto sin imagen)")
            else:
                main_image = next((img for img in shopify_images if img.get("position") == 1), shopify_images[0])
                if _is_placeholder(main_image.get("src", "")):
                    if add_image_to_product(
                        product_id, 
                        csv_image_url, 
                        csv_data.get("title", ""),
                        handle=shopify_product.get("handle", "")
                    ):
                        updates["imagen"] = True
                        log("  🖼️ Placeholder reemplazado por imagen real")
                        # Eliminar el placeholder para dejar solo la buena
                        try:
                            if main_image.get("id"):
                                shopify_request("DELETE", f"products/{product_id}/images/{main_image['id']}.json")
                                log("  🧽 Placeholder eliminado")
                        except ShopifyUploaderError as exc:
                            log(f"  ⚠️ No se pudo eliminar placeholder: {exc}")
                else:
                    log("  🖼️ Imagen existente detectada; no se reemplaza")

    # 4. ACTUALIZAR OTROS CAMPOS (título, descripción)
    product_update: Dict[str, Any] = {"id": product_id}
    changed = False

    # Título: actualízalo si cambia
    if (shopify_product.get("title") or "") != csv_data["title"]:
        product_update["title"] = csv_data["title"]
        changed = True

    # Descripción: SOLO rellenar si en Shopify está realmente vacía y el CSV trae algo
    existing_body = shopify_product.get("body_html", "")
    csv_body = csv_data.get("body_html", "")
    if _is_description_empty(existing_body) and not _is_description_empty(csv_body):
        product_update["body_html"] = csv_body
        changed = True

    if changed:
        shopify_request("PUT", f"products/{product_id}.json", json={"product": product_update})
        updates["otros"] = True
        log("  📝 Título/descripción actualizados")

    return updates


def build_product_data_from_csv(rows: List[Dict[str, str]]) -> dict:
    """Construye estructura de datos desde CSV."""
    base = rows[0]
    title = base.get("Title", "").strip()
    description = base.get("Description", "")
    image_url = base.get("Product image URL", "").strip()
    option_name = base.get("Option1 name", "").strip() or "Title"

    variants = []
    for variant_row in rows:
        option_value = variant_row.get("Option1 value", "").strip() or "Default Title"
        price = clean_price(variant_row.get("Price", ""))
        compare = clean_price(variant_row.get("Compare-at price", ""))
        inventory_quantity = parse_inventory_quantity(variant_row.get("Inventory quantity", ""))

        variant: Dict[str, Any] = {
            "option1": option_value,
            "price": price or "0.00",
            "inventory_quantity": inventory_quantity,
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

    return {
        "title": title,
        "body_html": description,
        "image_url": image_url,
        "option_name": option_name,
        "variants": variants,
    }


def create_product_from_csv(rows: List[Dict[str, str]], *, skip_images: bool = False) -> dict:
    """Crea un nuevo producto en Shopify."""
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
    variants: List[Dict[str, Any]] = []

    for variant_row in rows:
        option_value = variant_row.get("Option1 value", "").strip() or "Default Title"
        price = clean_price(variant_row.get("Price", ""))
        compare = clean_price(variant_row.get("Compare-at price", ""))
        inventory_quantity = parse_inventory_quantity(variant_row.get("Inventory quantity", ""))

        variant: Dict[str, Any] = {
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

    tag_list = [t.strip() for t in tags.split(",") if t and t.strip()]
    # Usar set para eliminar duplicados y luego sorted para orden consistente
    tag_list = sorted(set(tag_list + [SCRAPER_TAG]))

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

    response = shopify_request("POST", "products.json", json={"product": product})
    created_product = response.json().get("product", {}) or {}

    # Añadir imagen (solo si no está en modo skip-images)
    if not skip_images and image_url and created_product.get("id"):
        added = add_image_to_product(
            created_product["id"],
            image_url,
            title,
            handle=created_product.get("handle", "")
        )
        if not added:
            log(f"  ⚠️ Producto creado sin imagen")

    return created_product


def list_scraper_products() -> Dict[str, Dict[str, Any]]:
    """
    Devuelve mapeo handle -> {id, tags, status} para productos con SCRAPER_TAG.
    """
    products: Dict[str, Dict[str, Any]] = {}
    params_base = {"limit": 250, "fields": "id,handle,tags,status"}
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
                    "tags": tags,
                    "status": (product.get("status") or "").lower()
                }

        last_id = data[-1].get("id")
        if not last_id or len(data) < params_base["limit"]:
            break

    return products


def prune_missing_scraper_products(
    *,
    current_handles: Set[str],
    dry_run: bool = False
) -> int:
    """
    Elimina productos del scraper que ya no aparecen en los CSVs actuales.
    Respeta tags de protección (IGNORE_TAGS, KEEP_DRAFT_TAG).
    """
    if dry_run:
        log("🔎 Dry-run: no se eliminan productos obsoletos.")
        return 0

    log("🧹 Buscando productos obsoletos...")
    products = list_scraper_products()
    to_delete = []

    for handle, meta in products.items():
        if handle in current_handles:
            continue  # Producto actual, no borrar

        # ✅ No borrar productos archivados
        status = meta.get("status", "")
        if status == "archived":
            log(f"📦 '{handle}' archivado, no se elimina")
            continue

        # Respetar tags de protección
        tags = meta.get("tags", set())
        if _has_ignore_tag(tags) or KEEP_DRAFT_TAG in tags:
            log(f"🛡️ '{handle}' protegido, no se elimina")
            continue

        to_delete.append((handle, meta["id"]))

    if not to_delete:
        log("✅ No hay productos obsoletos")
        return 0

    deleted = 0
    for handle, product_id in to_delete:
        try:
            delete_product(product_id)
            _update_products_cache(handle, None)
            deleted += 1
            log(f"🧹 '{handle}' eliminado (obsoleto)")
        except ShopifyUploaderError as exc:
            log(f"⚠️ No se pudo eliminar '{handle}': {exc}")

    return deleted


def process_csv_hybrid(
    csv_path: Path,
    *,
    dry_run: bool = False,
    skip_images: bool = False,
) -> Tuple[Dict[str, int], Set[str]]:
    """
    Procesa CSV con estrategia híbrida:
    - Si variantes cambiaron → DELETE + CREATE
    - Si solo precio/stock/imagen → UPDATE selectivo
    - Si nada cambió → SKIP
    
    Retorna: (summary, set_of_handles_processed)
    """
    summary = {
        "created": 0,
        "updated": 0,
        "recreated": 0,  # DELETE + CREATE por cambio de variantes
        "skipped": 0,
        "updated_precio": 0,
        "updated_stock": 0,
        "updated_imagen": 0,
    }

    groups = group_rows_by_handle(csv_path)

    if not groups:
        log(f"⚠️ CSV {csv_path.name} sin filas válidas.")
        return summary, set()

    processed_handles: Set[str] = set()

    for original_handle, rows in groups.items():
        handle = original_handle  # se podrá ajustar si cambia por EAN/SKU
        log(f"➡️ Procesando '{handle}' ({len(rows)} variantes)")

        if dry_run:
            log(f"🔎 Dry-run: no se procesa '{handle}'.")
            summary["skipped"] += 1
            processed_handles.add(handle)
            continue

        try:
            csv_data = build_product_data_from_csv(rows)
        except Exception as exc:
            log(f"❌ Error preparando datos: {exc}")
            summary["skipped"] += 1
            processed_handles.add(handle)
            continue

        # Buscar producto existente: PRIMERO por EAN/SKU, luego por handle
        base = rows[0]
        key_barcode = normalize_barcode(base.get("Barcode") or "")
        key_sku = (base.get("SKU") or "").strip()

        found_by_ean = None
        if key_barcode or key_sku:
            try:
                found_by_ean = find_product_by_barcode_or_sku(key_barcode, key_sku)
            except Exception as exc:
                log(f"⚠️ Error buscando por EAN/SKU: {exc}")

        existing_id = None
        old_handle = None

        if found_by_ean:
            existing_id = found_by_ean["id"]
            old_handle = found_by_ean.get("handle")
            
            # Si el handle cambió, actualizarlo en Shopify
            if old_handle and old_handle != handle:
                log(f"  🔍 Encontrado por EAN (handle cambió: '{old_handle}' → '{handle}')")
                try:
                    # Actualizar handle en Shopify
                    shopify_request(
                        "PUT",
                        f"products/{existing_id}.json",
                        json={"product": {"id": existing_id, "handle": handle}},
                    )
                    log(f"  🔁 Handle actualizado en Shopify")
                    
                    # Crear redirect 301 para SEO
                    try:
                        shopify_request("POST", "redirects.json", json={
                            "redirect": {
                                "path": f"/products/{old_handle}",
                                "target": f"/products/{handle}"
                            }
                        })
                        log(f"  🔀 Redirect 301 creado: {old_handle} → {handle}")
                    except ShopifyUploaderError:
                        log(f"  ⚠️ No se pudo crear redirect (puede ya existir)")
                    
                    # Actualizar caché interna
                    _update_products_cache(old_handle, None)
                    _update_products_cache(handle, existing_id)
                    
                except ShopifyUploaderError as exc:
                    error_msg = str(exc)
                    # ✅ Detectar colisión de handle
                    if "422" in error_msg and ("has already been taken" in error_msg or "already exists" in error_msg):
                        log(f"  ⚠️ Handle '{handle}' ya existe, manteniendo '{old_handle}'")
                    else:
                        log(f"  ⚠️ No se pudo actualizar handle: {exc}")
                    # Usar el handle viejo en current_handles para evitar que prune lo borre
                    handle = old_handle
        
        # Si no se encontró por EAN, buscar por handle
        if not existing_id:
            existing_id = find_existing_product_id(handle)

        # CASO 1: NO EXISTE → CREAR
        if not existing_id:
            try:
                created = create_product_from_csv(rows, skip_images=skip_images)
                if created.get("id"):
                    summary["created"] += 1
                    _update_products_cache(handle, created["id"])
                    log(f"✅ Producto creado (ID {created['id']})")
                else:
                    summary["skipped"] += 1
                    log(f"⚠️ No se pudo crear")
            except ShopifyUploaderError as exc:
                summary["skipped"] += 1
                log(f"❌ Error creando: {exc}")
            processed_handles.add(handle)
            continue

        # CASO 2 y 3: EXISTE → Verificar cambios
        try:
            shopify_product = fetch_product(existing_id)
            existing_desc = shopify_product.get("body_html", "")

            # Verificar tags de protección
            existing_tags = _split_tags(shopify_product.get("tags", ""))
            if _has_ignore_tag(existing_tags):
                log(f"🚫 Tag de ignore, omitido")
                summary["skipped"] += 1
                processed_handles.add(handle)
                continue

            # ✅ Saltar productos archivados
            existing_status = (shopify_product.get("status") or "").lower()
            if existing_status == "archived":
                log("📦 Archivado: no se toca")
                summary["skipped"] += 1
                processed_handles.add(handle)
                continue

            # ✅ Si está en draft o tiene KEEP_DRAFT_TAG, asegurar que tiene el tag visible
            if existing_status == "draft" or KEEP_DRAFT_TAG in existing_tags:
                if KEEP_DRAFT_TAG not in existing_tags:
                    try:
                        new_tags = ", ".join(sorted(existing_tags | {KEEP_DRAFT_TAG}))
                        shopify_request("PUT", f"products/{existing_id}.json", json={
                            "product": {"id": existing_id, "tags": new_tags}
                        })
                        log(f"  🏷️ Tag '{KEEP_DRAFT_TAG}' añadido (producto en draft)")
                    except ShopifyUploaderError as exc:
                        log(f"  ⚠️ No se pudo añadir tag: {exc}")

            # CASO 2: Cambió estructura de variantes → DELETE + CREATE
            if compare_variants_structure(shopify_product.get("variants", []), csv_data["variants"]):
                log("  🔄 Variantes cambiaron, recreando producto...")
                
                # PROTECCIÓN DE BORRADOR: si está en draft o tiene KEEP_DRAFT_TAG, mantenerlo
                force_draft = existing_status == "draft" or KEEP_DRAFT_TAG in existing_tags
                
                delete_product(existing_id)
                _update_products_cache(handle, None)
                
                # Elegir rows para crear (con o sin draft)
                if force_draft:
                    log("  📝 Manteniendo en borrador (protegido)")
                    # Crear copia de rows para modificar status/tags sin alterar el original
                    rows_draft = [row.copy() for row in rows]
                    rows_draft[0]["Status"] = "draft"
                    
                    # Normalizar tags y añadir KEEP_DRAFT_TAG sin duplicar
                    current_tags_str = rows_draft[0].get("Tags", "")
                    current_tags_list = [t.strip() for t in current_tags_str.split(",") if t and t.strip()]
                    if KEEP_DRAFT_TAG not in current_tags_list:
                        current_tags_list.append(KEEP_DRAFT_TAG)
                    rows_draft[0]["Tags"] = ", ".join(current_tags_list)
                    
                    rows_to_use = rows_draft
                else:
                    rows_to_use = rows
                
                # 👇 Preservar descripción existente si tiene contenido real
                if existing_desc and not _is_description_empty(existing_desc):
                    rows_to_use[0]["Description"] = existing_desc
                    log("  📝 Descripción preservada del producto existente")
                
                created = create_product_from_csv(rows_to_use, skip_images=skip_images)
                
                if created.get("id"):
                    summary["recreated"] += 1
                    _update_products_cache(handle, created["id"])
                    log(f"✅ Producto recreado (ID {created['id']})")
                else:
                    summary["skipped"] += 1

                processed_handles.add(handle)
                continue

            # CASO 3: Solo cambió precio/stock/imagen → UPDATE
            updates = update_product_smart(existing_id, shopify_product, csv_data, skip_images=skip_images)

            if any(updates.values()):
                summary["updated"] += 1
                if updates["precio"]:
                    summary["updated_precio"] += 1
                if updates["stock"]:
                    summary["updated_stock"] += 1
                if updates["imagen"]:
                    summary["updated_imagen"] += 1
                log(f"✅ Producto actualizado (ID {existing_id})")
            else:
                summary["skipped"] += 1
                log(f"⏭️ Sin cambios, skip")

        except ShopifyUploaderError as exc:
            summary["skipped"] += 1
            log(f"❌ Error procesando: {exc}")

        processed_handles.add(handle)

    return summary, processed_handles


def run(
    source_dir: Path,
    *,
    dry_run: bool = False,
    disable_prune: bool = False,
    skip_images: bool = False,
) -> Dict[str, int]:
    if not SHOP_DOMAIN or not ACCESS_TOKEN:
        raise ShopifyUploaderError("Variables SHOPIFY_DOMAIN y SHOPIFY_ADMIN_TOKEN requeridas.")

    summary_total = {
        "created": 0,
        "updated": 0,
        "recreated": 0,
        "skipped": 0,
        "pruned": 0,
        "updated_precio": 0,
        "updated_stock": 0,
        "updated_imagen": 0,
    }

    csv_files = sorted(source_dir.glob("*.csv"))

    if not csv_files:
        log(f"⚠️ No se encontraron CSV en {source_dir}")
        return summary_total

    current_handles: Set[str] = set()

    for csv_path in csv_files:
        log(f"📄 Procesando CSV: {csv_path.name}")
        summary, handles = process_csv_hybrid(csv_path, dry_run=dry_run, skip_images=skip_images)

        for key, value in summary.items():
            summary_total[key] += value
        
        current_handles.update(handles)

    # Prune: eliminar productos obsoletos
    if not disable_prune and current_handles:
        pruned = prune_missing_scraper_products(
            current_handles=current_handles,
            dry_run=dry_run
        )
        summary_total["pruned"] = pruned

    # Generar reporte de imágenes fallidas
    if _failed_images_report:
        report_path = source_dir / "failed_images_report.csv"
        try:
            with report_path.open("w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=["handle", "image_url", "reason"])
                writer.writeheader()
                writer.writerows(_failed_images_report)
            log(f"\n⚠️  Se generó un reporte de imágenes fallidas: {report_path.name}")
            log(f"    Total fallos: {len(_failed_images_report)}")
        except Exception as e:
            log(f"❌ Error escribiendo reporte de imágenes: {e}")

    return summary_total


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Subir productos Shopify con estrategia híbrida inteligente",
        epilog="""
ESTRATEGIA HÍBRIDA v2:
- Variantes cambiaron → DELETE + CREATE (seguro)
- Solo precio/stock/imagen → UPDATE (rápido)
- Nada cambió → SKIP (ultra-rápido)
- Prune: elimina productos obsoletos que ya no están en CSV

MEJORAS v2:
- Detección inteligente de imágenes duplicadas por filename
- Normalización de tallas (evita recreación por "M" vs "m")
- Eliminación automática de placeholders al reemplazar

CONFIGURACIÓN OPTIMIZADA (balance velocidad/estabilidad):
- Timeout: 30s (suficiente para imágenes pesadas)
- Delay: 0.75s (bajo límite rate, pero rápido)
- Sleep imagen: 1.5s (tiempo para que Shopify procese)
- Reintentos: 2.5s incrementales (2.5, 5, 7.5, 10, 12.5s)
- Tiempo estimado: ~2-3 horas para 8,000 productos

MODO --skip-images:
Primera pasada: crea/actualiza productos SIN imágenes (rápido)
Segunda pasada: sin flag, solo actualiza imágenes que cambiaron

Variables de entorno (.env):
  SHOPIFY_DOMAIN              Tu dominio
  SHOPIFY_ADMIN_TOKEN         Token admin
  SHOPIFY_RATE_DELAY          Delay (default: 0.75s)
  SHOPIFY_MAX_RETRIES         Reintentos (default: 3)
  SHOPIFY_TIMEOUT             Timeout (default: 30s)
  SHOPIFY_LOCATION_NAME       Nombre exacto de la ubicación (opcional)
  SHOPIFY_KEEP_DRAFT_TAG      Tag para mantener draft (default: scraper:keep-draft)
  SHOPIFY_IGNORE_TAGS         Tags de protección (default: scraper:ignore,no tocar)
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--source-dir", type=Path, help="Directorio con CSV")
    parser.add_argument("--dry-run", action="store_true", help="Simular sin cambios")
    parser.add_argument("--disable-prune", action="store_true", help="No eliminar productos obsoletos")
    parser.add_argument("--skip-images", action="store_true", help="No procesar imágenes (útil para primera pasada rápida)")
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

        log(f"📂 Directorio: {source_dir}")
        log(f"⚡ Modo: HÍBRIDO v2 (UPDATE inteligente + recreación si cambian variantes)")
        log(f"⚙️ Config optimizada: Timeout={REQUEST_TIMEOUT}s | Delay={RATE_LIMIT_DELAY}s | Reintentos={MAX_RETRIES}")
        log(f"🖼️ Imágenes: {'❌ DESACTIVADAS (--skip-images)' if args.skip_images else '✅ ACTIVADAS'}")
        log(f"🧹 Prune: {'Deshabilitado' if args.disable_prune else 'Habilitado'}")

        summary = run(source_dir, dry_run=args.dry_run, disable_prune=args.disable_prune, skip_images=args.skip_images)

        log("\n📊 Resumen:")
        log(f"  ✅ Creados: {summary['created']}")
        log(f"  🔄 Recreados (variantes cambiaron): {summary['recreated']}")
        log(f"  ♻️ Actualizados (sin recrear): {summary['updated']}")
        log(f"    💰 Precios: {summary['updated_precio']}")
        log(f"    📦 Stock: {summary['updated_stock']}")
        log(f"    🖼️ Imágenes: {summary['updated_imagen']}")
        log(f"  ⏭️ Sin cambios: {summary['skipped']}")
        log(f"  🧹 Eliminados (obsoletos): {summary['pruned']}")

        if args.skip_images:
            log("\n💡 Ejecuta sin --skip-images para procesar imágenes en una segunda pasada")

        return 0

    except ShopifyUploaderError as exc:
        log(f"❌ {exc}")
        return 1
    except KeyboardInterrupt:
        log("⏹️ Interrumpido.")
        return 130


if __name__ == "__main__":
    sys.exit(main())