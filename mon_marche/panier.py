# This script drives the cart of a mon-marche.fr account: read it, add products
# to it, change quantities, empty it.
#
# The cart lives server side, attached to the session cookie set by
# `scrap.login`. It is created lazily: as long as no delivery slot has been
# chosen, every cart endpoint answers 404 "Le panier est introuvable"
# (`E_08_0005`). Choosing a slot with PATCH /api/cart/delivery2 creates it, and
# products are then added with PATCH /api/cart/product.
#
# Nothing here pays or places an order: the checkout endpoints
# (`/api/cart/createPaymentIntent`, `/api/cart/initialOrder`) are deliberately
# left out. Writes still need `--execute`, the dry run is the default.
import argparse
import json
from typing import Any, Optional

import requests

from scrap import (
    format_price,
    format_timestamp,
    get_addresses,
    get_json,
    login,
    product_price,
    request_json,
    search,
)

# Error code returned by every cart endpoint while no cart exists yet.
NO_CART_CODE = "E_08_0005"


def get_cart(session: requests.Session) -> Optional[dict]:
    """Get the current cart.

    Args:
        session (requests.Session): Session returned by `scrap.login`.

    Returns:
        dict, optional: The cart, or None when no cart exists yet.
    """
    cart = request_json(session, "GET", "/api/cart", allow_status=(404,))
    if isinstance(cart, dict) and cart.get("code") == NO_CART_CODE:
        return None
    return cart


def get_delivery_zones(session: requests.Session, address: dict) -> list[dict]:
    """Get the delivery zones and their slots for an address.

    Args:
        session (requests.Session): Session returned by `scrap.login`.
        address (dict): An address returned by `scrap.get_addresses`.

    Returns:
        list: One dict per zone, each with its `deliverySlots`.
    """
    components = address.get("addressComponents", {})
    body = {
        "postalCode": components.get("postalCode"),
        "countryCode": components.get("countryCode", "FR"),
        "location": address.get("location"),
    }
    zones = request_json(session, "POST", "/api/addresses/deliverySlots2", body=body)
    return zones.get("deliveryZones", [])


def available_slots(zones: list[dict]) -> list[dict]:
    """Keep the slots that can still be booked.

    Args:
        zones (list): Zones returned by `get_delivery_zones`.

    Returns:
        list: The bookable slots, in chronological order.
    """
    slots = [slot for zone in zones for slot in zone.get("deliverySlots", [])]
    slots = [
        slot
        for slot in slots
        if not slot.get("isExpired") and not slot.get("isFull") and not slot.get("isExcluded")
    ]
    return sorted(slots, key=lambda slot: slot.get("from", 0))


def set_delivery(session: requests.Session, address: dict, slot: dict) -> dict:
    """Attach a delivery address and slot to the cart, creating it if needed.

    Args:
        session (requests.Session): Session returned by `scrap.login`.
        address (dict): An address returned by `scrap.get_addresses`.
        slot (dict): A slot returned by `available_slots`.

    Returns:
        dict: The cart as returned by the endpoint.
    """
    body = {
        "delivery": {
            "note": address.get("note", ""),
            "address": {
                "formattedAddress": address.get("formattedAddress"),
                "location": address.get("location"),
                "addressComponents": address.get("addressComponents"),
            },
        },
        "timeSlot": slot,
    }
    return request_json(session, "PATCH", "/api/cart/delivery2", body=body)


def ensure_cart(
    session: requests.Session, slot_id: Optional[str] = None
) -> tuple[dict, Optional[dict]]:
    """Get the cart, creating it on the first address and slot if it is missing.

    Args:
        session (requests.Session): Session returned by `scrap.login`.
        slot_id (str, optional): Id of the slot to book, the first one if None.

    Returns:
        tuple: The cart and the slot that was booked, None if the cart existed.
    """
    cart = get_cart(session)
    if cart is not None:
        return cart, None

    addresses = get_addresses(session)
    if not addresses:
        raise Exception("No delivery address on the account, add one on the website first")
    address = addresses[0]
    slots = available_slots(get_delivery_zones(session, address))
    if not slots:
        raise Exception("No delivery slot available for this address")
    slot = next((s for s in slots if s.get("id") == slot_id), None) if slot_id else slots[0]
    if slot is None:
        raise Exception(f"Unknown delivery slot {slot_id}")
    return set_delivery(session, address, slot), slot


def set_product_quantity(session: requests.Session, product_id: str, quantity: int) -> dict:
    """Set the quantity of a product in the cart.

    A quantity of 0 removes the product. The id is the canonical id, i.e. the
    part of a catalog id before the "$".

    Args:
        session (requests.Session): Session returned by `scrap.login`.
        product_id (str): Canonical id of the product.
        quantity (int): Number of items wanted in the cart.

    Returns:
        dict: The cart as returned by the endpoint.
    """
    body = {"product": {"id": canonical_id(product_id), "quantity": quantity}}
    return request_json(session, "PATCH", "/api/cart/product", body=body)


def set_products_quantities(session: requests.Session, products: list[dict]) -> dict:
    """Set the quantities of several products in one call.

    Args:
        session (requests.Session): Session returned by `scrap.login`.
        products (list): Dicts with an `id` and a `quantity`.

    Returns:
        dict: The cart as returned by the endpoint.
    """
    body = {
        "products": [
            {"id": canonical_id(product["id"]), "quantity": product["quantity"]}
            for product in products
        ]
    }
    return request_json(session, "PATCH", "/api/cart/products", body=body)


def empty_cart(session: requests.Session) -> Any:
    """Empty the cart.

    Args:
        session (requests.Session): Session returned by `scrap.login`.

    Returns:
        Any: The response of the endpoint.
    """
    return request_json(session, "DELETE", "/api/cart")


def canonical_id(product_id: str) -> str:
    """Reduce a catalog id to the id the cart expects.

    Catalog ids look like "QH9QWo2sF$rDyRbLxRPFibN8zcdLJN6" ; the cart wants the
    part before the "$", which is also the `canonicalId` field.

    Args:
        product_id (str): A catalog id or a canonical id.

    Returns:
        str: The canonical id.
    """
    return product_id.split("$")[0]


def resolve_sku(session: requests.Session, sku: str, hint: str = "") -> dict:
    """Find a catalog product from its sku.

    The catalog search does not index the sku, so the product is looked for in
    the account history first, then in the results of a search on `hint`.

    Args:
        session (requests.Session): Session returned by `scrap.login`.
        sku (str): The sku, e.g. "EP0568".
        hint (str): Search term naming the product, used when the account has
            never bought it.

    Returns:
        dict: The product carrying that sku.
    """
    categories = get_json(session, "/api/account/products").get("categories", [])
    for category in categories:
        for product in category.get("products", []):
            if product.get("sku") == sku:
                return product
    # The search is fussy about long phrases: "banane" finds the product that
    # "main de banane bio" misses, so narrow the hint down word by word.
    words = hint.split()
    attempts = [hint] + [" ".join(words[:count]) for count in range(len(words) - 1, 0, -1)]
    for attempt in dict.fromkeys(attempt for attempt in attempts if attempt):
        for product in search(session, attempt, "PRODUCT", limit=20).get("items", []):
            if product.get("sku") == sku:
                return product
    raise Exception(
        f"Sku {sku} not found; it is not in the history, pass a term naming it as well"
    )


def resolve_product(session: requests.Session, term: str) -> dict:
    """Find a catalog product from a search term.

    The catalog search does not index the SKU, so the term has to be a name.

    Args:
        session (requests.Session): Session returned by `scrap.login`.
        term (str): Search term, e.g. "citron jaune".

    Returns:
        dict: The first available product matching the term.
    """
    items = search(session, term, "PRODUCT", limit=10).get("items", [])
    if not items:
        raise Exception(f"No product found for « {term} »")
    available = [
        item
        for item in items
        if (item.get("availableQuantity") or 0) >= (item.get("packSize") or 1)
    ]
    if not available:
        raise Exception(f"No product in stock for « {term} »")
    return available[0]


def line_quantity(product: dict) -> int:
    """Read the quantity of a cart line.

    Args:
        product (dict): A product of `cart["products"]`.

    Returns:
        int: The number of items of that line.
    """
    return product.get("quotation", {}).get("count", 0)


def line_label(product: dict) -> str:
    """Say how much of a product a cart line holds.

    A line sold per weight carries a count of 1 whatever its weight, so showing
    the count would read "1 pièces" for half a kilo of onions. Its real size is
    in `quotation.weight`.

    Args:
        product (dict): A product of `cart["products"]`.

    Returns:
        str: The quantity with its unit, e.g. "0,5 kg" or "2 pièces".
    """
    quotation = product.get("quotation", {})
    definition = product.get("quotation2", {}).get("count", {}).get("itemDefinition", {})
    if definition.get("type") == "arbitraryQuantity" and quotation.get("weight"):
        unit = (definition.get("weight") or {}).get("unit", "kg")
        return f"{quotation['weight']:g} {unit}".replace(".", ",")
    count = quotation.get("count", 0)
    granularity = product.get("granularity", {})
    unit = granularity.get("singular" if count <= 1 else "plural", "")
    return f"{count} {unit}".strip()


def line_total(product: dict) -> Optional[int]:
    """Read the total price of a cart line, in cents.

    Args:
        product (dict): A product of `cart["products"]`.

    Returns:
        int, optional: The VAT included total of the line.
    """
    return product.get("quotation2", {}).get("totals", {}).get("net")


def print_cart(cart: Optional[dict]) -> None:
    """Print the content of the cart.

    Args:
        cart (dict, optional): The cart returned by `get_cart`.
    """
    if cart is None:
        print("Panier vide (aucun panier ouvert, il faut choisir un créneau de livraison)")
        return
    products = cart.get("products") or []
    print(f"Panier {cart.get('id', '')} : {len(products)} lignes")
    for product in products:
        print(
            f"  {line_label(product)} | {product.get('name')} "
            f"({product.get('sku')}) | {format_price(line_total(product))}"
        )

    quotation = cart.get("price", {}).get("quotation", {})
    if quotation:
        print(f"  produits      : {format_price(quotation.get('net'))}")
        print(f"  livraison     : {format_price(quotation.get('shipping'))}")
        print(f"  préparation   : {format_price(quotation.get('preparationFee'))}")
        if quotation.get("discount"):
            print(f"  remise        : -{format_price(quotation.get('discount'))}")
        print(f"  préautorisé   : {format_price(quotation.get('preauthorization'))}")
    if cart.get("minOrderAmountReached") is False:
        print("  ⚠ minimum de commande non atteint")

    slot = (cart.get("delivery") or {}).get("timeSlot") or {}
    if slot:
        print(
            f"  livré le      : {format_timestamp(slot.get('from'))} → "
            f"{format_timestamp(slot.get('to'))}"
        )


def print_slots(slots: list[dict], limit: int = 15) -> None:
    """Print the bookable delivery slots.

    Args:
        slots (list): Slots returned by `available_slots`.
        limit (int): Maximum number of slots to print.
    """
    print(f"Créneaux disponibles ({len(slots)})")
    for slot in slots[:limit]:
        print(
            f"  {slot.get('id')} | {format_timestamp(slot.get('from'))} → "
            f"{format_timestamp(slot.get('to'))} | "
            f"frais {format_price(slot.get('activeDeliveryPrice'))}"
        )


def main() -> None:
    """Drive the cart from the command line."""
    parser = argparse.ArgumentParser(description="Drive the cart of a mon-marche.fr account")
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument(
        "--execute",
        action="store_true",
        help="really write to the cart (dry run by default)",
    )
    common.add_argument("--json", help="also dump the raw payload to this file")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("voir", parents=[common], help="show the cart")
    subparsers.add_parser("creneaux", parents=[common], help="list the bookable delivery slots")

    ajouter_parser = subparsers.add_parser(
        "ajouter", parents=[common], help="add a product to the cart (sets its quantity)"
    )
    ajouter_parser.add_argument("terme", nargs="+", help="product name, or --id")
    ajouter_parser.add_argument("--id", help="canonical id, skips the catalog search")
    ajouter_parser.add_argument("--sku", help="sku, looked up in the history then in the terms")
    ajouter_parser.add_argument("--quantite", type=int, default=1, help="number of items")
    ajouter_parser.add_argument("--creneau", help="delivery slot id, if the cart must be created")

    retirer_parser = subparsers.add_parser(
        "retirer", parents=[common], help="remove a product from the cart"
    )
    retirer_parser.add_argument("terme", nargs="+", help="product name, or --id")
    retirer_parser.add_argument("--id", help="canonical id, skips the catalog search")
    retirer_parser.add_argument("--sku", help="sku, looked up in the history then in the terms")

    creneau_parser = subparsers.add_parser(
        "creneau", parents=[common], help="book another delivery slot"
    )
    creneau_parser.add_argument("slot_id", help="slot id, from `panier.py creneaux`")

    subparsers.add_parser("vider", parents=[common], help="empty the cart")

    args = parser.parse_args()
    session, _ = login()
    payload: Any = None

    if args.command == "voir":
        payload = get_cart(session)
        print_cart(payload)
    elif args.command == "creneaux":
        addresses = get_addresses(session)
        if not addresses:
            raise Exception("No delivery address on the account")
        payload = available_slots(get_delivery_zones(session, addresses[0]))
        print(f"Adresse : {addresses[0].get('formattedAddress')}")
        print_slots(payload)
    elif args.command in ("ajouter", "retirer"):
        quantity = args.quantite if args.command == "ajouter" else 0
        if getattr(args, "sku", None):
            product = resolve_sku(session, args.sku, " ".join(args.terme))
            product_id = canonical_id(product["id"])
            label = f"{product['name']} ({product['sku']}, {product_price(product)})"
        elif args.id:
            product_id = canonical_id(args.id)
            # Look the id up so the line printed names the product, not the id.
            found = next(
                (
                    product
                    for product in search(session, " ".join(args.terme), "PRODUCT", limit=20).get(
                        "items", []
                    )
                    if canonical_id(product["id"]) == product_id
                ),
                None,
            )
            label = (
                f"{found['name']} ({found['sku']}, {product_price(found)})"
                if found
                else args.id
            )
        else:
            product = resolve_product(session, " ".join(args.terme))
            product_id = canonical_id(product["id"])
            label = f"{product['name']} ({product['sku']}, {product_price(product)})"
        action = f"Quantité fixée à {quantity}" if quantity else "Retrait"
        print(f"{action} : {label} [{product_id}]")
        if not args.execute:
            print("Dry run, rien n'a été écrit. Relancer avec --execute pour appliquer.")
            return
        cart, booked = ensure_cart(session, getattr(args, "creneau", None))
        if booked is not None:
            print(
                f"Panier créé sur le créneau {booked.get('id')} "
                f"({format_timestamp(booked.get('from'))})"
            )
        payload = set_product_quantity(session, product_id, quantity)
        print_cart(payload)
    elif args.command == "creneau":
        addresses = get_addresses(session)
        if not addresses:
            raise Exception("No delivery address on the account")
        slots = available_slots(get_delivery_zones(session, addresses[0]))
        slot = next((s for s in slots if s.get("id") == args.slot_id), None)
        if slot is None:
            raise Exception(f"Slot {args.slot_id} is unknown or not bookable")
        print(
            f"Créneau : {format_timestamp(slot.get('from'))} → "
            f"{format_timestamp(slot.get('to'))} "
            f"(frais {format_price(slot.get('activeDeliveryPrice'))})"
        )
        if not args.execute:
            print("Dry run, le créneau n'a pas été changé. Relancer avec --execute.")
            return
        payload = set_delivery(session, addresses[0], slot)
        print_cart(payload)
    elif args.command == "vider":
        if not args.execute:
            print("Dry run : le panier serait vidé. Relancer avec --execute pour appliquer.")
            return
        payload = empty_cart(session)
        print("Panier vidé.")

    if args.json and payload is not None:
        with open(args.json, "w", encoding="utf-8") as file:
            json.dump(payload, file, ensure_ascii=False, indent=2)
        print(f"\nPayload brut écrit dans {args.json}")


if __name__ == "__main__":
    main()
