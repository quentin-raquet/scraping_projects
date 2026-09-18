# This script logs into https://www.mon-marche.fr/ with a customer account and
# reads the account data (profile, addresses, orders, favourites, loyalty) and
# the catalog.
#
# The website is a Next.js app that talks to its own JSON API on the same origin
# (`https://www.mon-marche.fr/api/...`, a Keplr e-commerce backend). Signing in
# on `/api/auth/signin` with the email and the password sets a `session` cookie,
# and every account endpoint is then readable with that cookie.
#
# The script is READ ONLY: it never writes to the cart and never places an
# order.
import argparse
import datetime
import json
import os
import time
from typing import Any, Optional

import requests

BASE_URL = "https://www.mon-marche.fr"
# The API rejects the default python-requests user agent.
USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/140.0 Safari/537.36"
)
# Environment variables holding the credentials of the account.
EMAIL_ENV = "EMAIL_MON_MARCHE"
PASSWORD_ENV = "PASSWORD_MON_MARCHE"
# Allowed values of the `type` parameter of the search endpoint.
SEARCH_TYPES = ["PRODUCT", "RECIPE"]
# The API answers an occasional 503 from its upstream, retry those.
RETRY_STATUS = (502, 503, 504)
RETRY_COUNT = 3
# Price units used by the API, translated for display.
UNITS = {"count": "pièce", "kg": "kg", "l": "L"}


def get_credentials(
    email: Optional[str] = None, password: Optional[str] = None
) -> tuple[str, str]:
    """Read the account credentials from the environment.

    Args:
        email (str, optional): Email, read from `EMAIL_MON_MARCHE` if None.
        password (str, optional): Password, read from `PASSWORD_MON_MARCHE` if None.

    Returns:
        tuple: The email and the password.
    """
    email = email or os.environ.get(EMAIL_ENV)
    password = password or os.environ.get(PASSWORD_ENV)
    if not email or not password:
        raise Exception(
            f"Missing credentials, set {EMAIL_ENV} and {PASSWORD_ENV} in the environment"
        )
    return email, password


def login(email: Optional[str] = None, password: Optional[str] = None) -> tuple:
    """Sign in and return an authenticated session.

    Args:
        email (str, optional): Email, read from the environment if None.
        password (str, optional): Password, read from the environment if None.

    Returns:
        tuple: The `requests.Session` carrying the `session` cookie and the
            profile returned by the sign in endpoint.
    """
    email, password = get_credentials(email, password)
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": USER_AGENT,
            "Accept": "application/json",
            "Origin": BASE_URL,
            "Referer": f"{BASE_URL}/connexion",
        }
    )
    res = session.post(
        f"{BASE_URL}/api/auth/signin",
        json={"email": email, "password": password},
        timeout=30,
    )
    if res.status_code != 200:
        message = res.json().get("message", res.text[:200])
        raise Exception(f"Login failed ({res.status_code}) : {message}")
    if "session" not in session.cookies:
        raise Exception("Login succeeded but no session cookie was set")
    return session, res.json()


def request_json(
    session: requests.Session,
    method: str,
    path: str,
    params: Optional[dict] = None,
    body: Optional[dict] = None,
    allow_status: tuple = (),
) -> Any:
    """Call a JSON endpoint of the site with the authenticated session.

    Args:
        session (requests.Session): Session returned by `login`.
        method (str): HTTP method, e.g. "GET" or "PATCH".
        path (str): Endpoint path, e.g. "/api/orders/past".
        params (dict, optional): Query string parameters.
        body (dict, optional): JSON body, for the writing methods.
        allow_status (tuple): Status codes to return instead of raising.

    Returns:
        Any: The decoded JSON body.
    """
    for attempt in range(RETRY_COUNT):
        res = session.request(
            method,
            f"{BASE_URL}{path}",
            params=params,
            json=body,
            headers={"Referer": f"{BASE_URL}/"},
            timeout=30,
        )
        if res.status_code not in RETRY_STATUS or attempt == RETRY_COUNT - 1:
            break
        time.sleep(2**attempt)
    if res.status_code != 200 and res.status_code not in allow_status:
        try:
            message = res.json().get("message", res.text[:200])
        except ValueError:
            message = res.text[:200]
        raise Exception(
            f"Bad request status code {res.status_code}, {method} {path} ({message})"
        )
    return res.json()


def get_json(session: requests.Session, path: str, params: Optional[dict] = None) -> Any:
    """Call a JSON endpoint of the site in GET.

    Args:
        session (requests.Session): Session returned by `login`.
        path (str): Endpoint path, e.g. "/api/orders/past".
        params (dict, optional): Query string parameters.

    Returns:
        Any: The decoded JSON body.
    """
    return request_json(session, "GET", path, params=params)


def get_addresses(session: requests.Session) -> list[dict]:
    """Get the delivery addresses of the account.

    Args:
        session (requests.Session): Session returned by `login`.

    Returns:
        list: One dict per address, with its contact and delivery note.
    """
    return get_json(session, "/api/account/addresses").get("items", [])


def get_orders(session: requests.Session, past: bool = True) -> list[dict]:
    """Get the orders of the account.

    Args:
        session (requests.Session): Session returned by `login`.
        past (bool): True for the delivered orders, False for the ongoing ones.

    Returns:
        list: One dict per order, with its products, delivery slot and total.
    """
    path = "/api/orders/past" if past else "/api/orders/current"
    return get_json(session, path).get("items", [])


def get_top_products(session: requests.Session) -> list[dict]:
    """Get the products the account orders the most.

    Args:
        session (requests.Session): Session returned by `login`.

    Returns:
        list: One dict per product, in the catalog format.
    """
    return get_json(session, "/api/account/top-products").get("items", [])


def get_bookmarks(session: requests.Session) -> dict:
    """Get the bookmark lists of the account.

    Args:
        session (requests.Session): Session returned by `login`.

    Returns:
        dict: The lists, keyed by id, and the bookmarked products by sku.
    """
    return get_json(session, "/api/account/bookmarks")


def get_coupons(session: requests.Session) -> list[dict]:
    """Get the coupons available on the account.

    Args:
        session (requests.Session): Session returned by `login`.

    Returns:
        list: One dict per coupon, with its code and its conditions.
    """
    return get_json(session, "/api/account/coupons").get("items", [])


def get_loyalty_points(session: requests.Session) -> int:
    """Get the loyalty points balance of the account.

    Args:
        session (requests.Session): Session returned by `login`.

    Returns:
        int: The number of points.
    """
    return get_json(session, "/api/loyalty/user").get("totalPoints", 0)


def search(
    session: requests.Session,
    text: str,
    search_type: str = "PRODUCT",
    limit: int = 20,
) -> dict:
    """Search the catalog.

    Args:
        session (requests.Session): Session returned by `login`.
        text (str): Search terms.
        search_type (str): "PRODUCT" or "RECIPE".
        limit (int): Maximum number of results.

    Returns:
        dict: The `count` of matches and the `items` found.
    """
    if search_type not in SEARCH_TYPES:
        raise Exception(f"Unknown search type {search_type}, expected one of {SEARCH_TYPES}")
    return get_json(
        session,
        "/api/search2",
        params={"text": text, "type": search_type, "limit": limit},
    )


def format_price(cents: Optional[int]) -> str:
    """Format a price returned by the API.

    The API gives every amount in cents, VAT included in the `net` fields.

    Args:
        cents (int, optional): The amount in cents.

    Returns:
        str: The amount in euros, e.g. "245,63 €".
    """
    if cents is None:
        return "-"
    return f"{cents / 100:.2f} €".replace(".", ",")


def format_timestamp(milliseconds: Optional[int]) -> str:
    """Format a millisecond timestamp returned by the API.

    Args:
        milliseconds (int, optional): The timestamp in milliseconds.

    Returns:
        str: The local date and time, e.g. "2026-08-04 18:30".
    """
    if not milliseconds:
        return "-"
    return datetime.datetime.fromtimestamp(milliseconds / 1000).strftime("%Y-%m-%d %H:%M")


def product_price(product: dict) -> str:
    """Format the shelf price of a catalog product.

    Args:
        product (dict): A product returned by the catalog or the account endpoints.

    Returns:
        str: The price with its unit, e.g. "3,99 € / kg".
    """
    prices = product.get("pricing", {}).get("sellPrices", {})
    main = next(
        (price for price in prices.values() if price.get("main")),
        next(iter(prices.values()), {}),
    )
    unit = main.get("unit", "")
    if not unit:
        return format_price(main.get("net"))
    return f"{format_price(main.get('net'))} / {UNITS.get(unit, unit)}"


def print_account(profile: dict, session: requests.Session) -> None:
    """Print a summary of the account.

    Args:
        profile (dict): Profile returned by `login`.
        session (requests.Session): Session returned by `login`.
    """
    print(f"{profile.get('firstName', '')} {profile.get('lastName', '')} <{profile.get('email')}>")
    print(f"  compte      : {profile.get('accountType')} (id {profile.get('id')})")
    print(f"  créé le     : {format_timestamp(profile.get('createdAt'))}")
    print(f"  fidélité    : {get_loyalty_points(session)} points")
    for address in get_addresses(session):
        print(f"  adresse     : {address.get('formattedAddress')}")
    coupons = get_coupons(session)
    for coupon in coupons:
        print(f"  coupon      : {coupon.get('name')} (code {coupon.get('code')})")
    if not coupons:
        print("  coupon      : aucun")


def print_orders(orders: list[dict], title: str) -> None:
    """Print a list of orders.

    Args:
        orders (list): Orders returned by `get_orders`.
        title (str): Section title.
    """
    print(f"\n{title} ({len(orders)})")
    for order in orders:
        slot = order.get("delivery", {}).get("timeSlot", {})
        print(
            f"  {order.get('id')} | {order.get('state')} | "
            f"{format_price(order.get('totalPrice'))} | "
            f"{len(order.get('products', []))} produits | "
            f"livraison {format_timestamp(slot.get('from'))}"
        )


def main() -> None:
    """Read the mon-marche.fr account from the command line."""
    parser = argparse.ArgumentParser(description="Read a mon-marche.fr account (read only)")
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--json", help="also dump the raw payload to this file")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser(
        "compte", parents=[common], help="profile, addresses, loyalty points and coupons"
    )
    subparsers.add_parser("commandes", parents=[common], help="past and ongoing orders")

    favoris_parser = subparsers.add_parser(
        "favoris", parents=[common], help="most ordered products"
    )
    favoris_parser.add_argument("--limit", type=int, default=20, help="number of products")

    recherche_parser = subparsers.add_parser(
        "recherche", parents=[common], help="search the catalog"
    )
    recherche_parser.add_argument("text", help="search terms")
    recherche_parser.add_argument("--type", default="PRODUCT", choices=SEARCH_TYPES)
    recherche_parser.add_argument("--limit", type=int, default=10, help="number of results")

    args = parser.parse_args()

    session, profile = login()
    payload: Any = None

    if args.command == "compte":
        print_account(profile, session)
        payload = profile
    elif args.command == "commandes":
        past, current = get_orders(session, past=True), get_orders(session, past=False)
        print_orders(current, "Commandes en cours")
        print_orders(past, "Commandes passées")
        payload = {"current": current, "past": past}
    elif args.command == "favoris":
        products = get_top_products(session)[: args.limit]
        print(f"Produits les plus commandés ({len(products)})")
        for product in products:
            print(f"  {product.get('sku')} | {product.get('name')} | {product_price(product)}")
        payload = products
    elif args.command == "recherche":
        results = search(session, args.text, args.type, args.limit)
        print(f"{results.get('count', 0)} résultats pour « {args.text} »")
        for item in results.get("items", []):
            if args.type == "PRODUCT":
                print(f"  {item.get('sku')} | {item.get('name')} | {product_price(item)}")
            else:
                print(f"  {item.get('slug')} | {item.get('name')}")
        payload = results

    if args.json:
        with open(args.json, "w", encoding="utf-8") as file:
            json.dump(payload, file, ensure_ascii=False, indent=2)
        print(f"\nPayload brut écrit dans {args.json}")


if __name__ == "__main__":
    main()
