import argparse
import re
import os
import sys
from typing import Any, Dict, List, Optional

import requests


SHOP_DOMAIN = os.getenv("SHOPIFY_DOMAIN", "").strip()
ACCESS_TOKEN = os.getenv("SHOPIFY_ADMIN_TOKEN", "").strip()
API_VERSION = os.getenv("SHOPIFY_API_VERSION", "2024-07").strip()

GRAPHQL_URL = f"https://{SHOP_DOMAIN}/admin/api/{API_VERSION}/graphql.json"
HEADERS = {
    "Content-Type": "application/json",
    "X-Shopify-Access-Token": ACCESS_TOKEN,
}


class ShopifyError(Exception):
    pass


def shopify_graphql(query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    if not SHOP_DOMAIN or not ACCESS_TOKEN:
        raise ShopifyError("Faltan SHOPIFY_DOMAIN o SHOPIFY_ADMIN_TOKEN en el entorno.")
    resp = requests.post(
        GRAPHQL_URL,
        headers=HEADERS,
        json={"query": query, "variables": variables or {}},
        timeout=float(os.getenv("SHOPIFY_TIMEOUT", "30")),
    )
    data = resp.json()
    if resp.status_code >= 400 or "errors" in data:
        raise ShopifyError(f"GraphQL error: {resp.status_code} {data}")
    return data["data"]


def search_products(q: str, first: int = 5) -> List[Dict[str, Any]]:
    query = (
        """
        query ($q: String!, $first: Int!) {
          products(first: $first, query: $q) {
            edges {
              node {
                id
                title
                handle
                tags
                status
              }
            }
          }
        }
        """
    )
    data = shopify_graphql(query, {"q": q, "first": first})
    edges = data.get("products", {}).get("edges", [])
    return [e["node"] for e in edges]


def find_by_handle(handle: str) -> Optional[Dict[str, Any]]:
    results = search_products(f"handle:{handle}", first=1)
    return results[0] if results else None


def find_by_title(title: str, first: int = 5) -> Optional[Dict[str, Any]]:
    # BÃºsqueda exacta por tÃ­tulo, con fallback a primer resultado
    results = search_products(f'title:"{title}"', first=first)
    for p in results:
        if (p.get("title") or "").strip().lower() == title.strip().lower():
            return p
    return results[0] if results else None


def search_products_partial(term: str, first: int = 10) -> List[Dict[str, Any]]:
    """BÃºsqueda parcial por tÃ­tulo: combina tokens con AND (title:tok1 title:tok2)."""
    tokens = [t.strip() for t in re.split(r"\s+", term or "") if t.strip()]
    q = " ".join(f"title:{t}" for t in tokens) if tokens else ""
    if not q:
        return []
    return search_products(q, first=first)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Consulta las tags de un producto de Shopify por tÃ­tulo o handle."
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--title", help="TÃ­tulo exacto del producto")
    group.add_argument("--handle", help="Handle del producto (slug)")
    group.add_argument("--search", help="BÃºsqueda parcial por tÃ­tulo (contiene)")
    parser.add_argument("--limit", type=int, default=10, help="LÃ­mite de resultados para --search")
    parser.add_argument("--json", action="store_true", help="Salida en JSON")
    args = parser.parse_args()

    try:
        if args.handle:
            product = find_by_handle(args.handle)
        elif args.title:
            product = find_by_title(args.title)
        else:
            # --search (parcial)
            results = search_products_partial(args.search, first=args.limit)
            if not results:
                print("âŒ Sin coincidencias.")
                return 2

            if args.json:
                import json

                out = [
                    {
                        "title": p.get("title"),
                        "handle": p.get("handle"),
                        "status": p.get("status"),
                        "tags": p.get("tags") or [],
                    }
                    for p in results
                ]
                print(json.dumps(out, ensure_ascii=False, indent=2))
            else:
                print(f"ðŸ”Ž Coincidencias: {len(results)}")
                for idx, p in enumerate(results, 1):
                    print(f"\n[{idx}] TÃ­tulo: {p.get('title')}")
                    print(f"    Handle: {p.get('handle')}")
                    print(f"    Estado: {p.get('status')}")
                    tags = p.get("tags") or []
                    print("    Tags:")
                    if isinstance(tags, list):
                        for t in tags:
                            print(f"     - {t}")
                    else:
                        print(f"     - {tags}")
            return 0

        if not product:
            print("âŒ Producto no encontrado.")
            return 2

        title = product.get("title")
        handle = product.get("handle")
        status = product.get("status")
        tags = product.get("tags") or []

        if args.json:
            import json

            print(
                json.dumps(
                    {
                        "title": title,
                        "handle": handle,
                        "status": status,
                        "tags": tags,
                    },
                    ensure_ascii=False,
                    indent=2,
                )
            )
        else:
            print(f"TÃ­tulo: {title}")
            print(f"Handle: {handle}")
            print(f"Estado: {status}")
            print("Tags:")
            if isinstance(tags, list):
                for t in tags:
                    print(f" - {t}")
            else:
                print(f" - {tags}")

        return 0

    except ShopifyError as exc:
        print(f"âŒ {exc}")
        return 1


if __name__ == "__main__":
    sys.exit(main())