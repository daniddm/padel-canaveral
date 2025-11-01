from upload_shopify import shopify_request, SCRAPER_TAG
from collections import defaultdict

products = []
last_id = None

print(f"Obteniendo productos con tag '{SCRAPER_TAG}'...")
while True:
    params = {"limit": 250, "fields": "id,handle,title,tags"}
    if last_id:
        params["since_id"] = last_id
    
    response = shopify_request("GET", "products.json", params=params)
    data = response.json().get("products", [])
    
    if not data:
        break
    
    # Filtrar solo con tag padel-scraper
    for p in data:
        tags = {tag.strip() for tag in (p.get("tags") or "").split(",")}
        if SCRAPER_TAG in tags:
            products.append(p)
    
    last_id = data[-1].get("id")
    print(f"  Procesados: {len(products)}...", end="\r")
    
    if len(data) < 250:
        break

print(f"\nTotal productos con tag: {len(products)}")

# Agrupar por handle
by_handle = defaultdict(list)
for p in products:
    by_handle[p["handle"]].append({
        "id": p["id"],
        "title": p.get("title", "")
    })

# Encontrar DUPLICADOS REALES (mismo handle, IDs DIFERENTES)
real_duplicates = {}
for handle, prods in by_handle.items():
    unique_ids = set(p["id"] for p in prods)
    if len(unique_ids) > 1:  # IDs diferentes = duplicado real
        real_duplicates[handle] = prods

print(f"Duplicados REALES con IDs diferentes: {len(real_duplicates)}\n")

if real_duplicates:
    print("PRODUCTOS DUPLICADOS REALES:\n")
    for handle, prods in sorted(real_duplicates.items()):
        print(f"{prods[0]['title']}")
        print(f"  Handle: {handle}")
        for p in prods:
            print(f"  - ID: {p['id']}")
        print()
else:
    print("No hay duplicados reales. El problema puede ser visual/cache.")