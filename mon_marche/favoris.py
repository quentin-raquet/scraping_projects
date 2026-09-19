# This script handles the bookmark lists of a mon-marche.fr account.
#
# A list is a named set of products living under `/api/account/bookmarks-lists`,
# shown on the site at `/client/favoris/liste/<id>`. That page is the simplest
# way to hand somebody a set of products on the site itself, with the real
# pictures, prices and add to cart buttons, instead of a list of links.
#
# The page is private to the account: opening it needs to be logged in as its
# owner, otherwise the site redirects to `/?login=true&redirect=…`.
import argparse
from typing import Optional

import requests

from scrap import login, product_price, request_json, search

# Page of the site showing a bookmark list.
LIST_URL = "https://www.mon-marche.fr/client/favoris/liste/{list_id}"


def list_url(list_id: str) -> str:
    """Build the URL of the page showing a bookmark list.

    Args:
        list_id (str): Id of the list.

    Returns:
        str: The URL of the list on the site.
    """
    return LIST_URL.format(list_id=list_id)


def find_by_sku(session: requests.Session, sku: str) -> Optional[dict]:
    """Find a catalog product from its sku.

    The catalog search does not index the sku and the API exposes no lookup by
    sku, so this only finds the products already bought by the account. For
    anything else, search by name and pass the product itself to `add_to_list`.

    Args:
        session (requests.Session): Session returned by `scrap.login`.
        sku (str): The sku, e.g. "NA0223".

    Returns:
        dict, optional: The product, None when it is not in the history.
    """
    categories = request_json(session, "GET", "/api/account/products").get("categories", [])
    for category in categories:
        for product in category.get("products", []):
            if product.get("sku") == sku:
                return product
    return None


def get_lists(session: requests.Session) -> list[dict]:
    """Get the bookmark lists of the account.

    Args:
        session (requests.Session): Session returned by `scrap.login`.

    Returns:
        list: One dict per list, with its `id` and its `name`.
    """
    return request_json(session, "GET", "/api/account/bookmarks-lists")


def get_list(session: requests.Session, list_id: str) -> dict:
    """Read one bookmark list and its products.

    Args:
        session (requests.Session): Session returned by `scrap.login`.
        list_id (str): Id of the list.

    Returns:
        dict: The list, its products under `bookmarks.items`.
    """
    return request_json(session, "GET", f"/api/account/bookmarks-lists/{list_id}")


def create_list(session: requests.Session, name: str) -> dict:
    """Create a bookmark list.

    Args:
        session (requests.Session): Session returned by `scrap.login`.
        name (str): Name of the list.

    Returns:
        dict: The created list, with its `id`.
    """
    return request_json(session, "POST", "/api/account/bookmarks-lists", body={"name": name})


def rename_list(session: requests.Session, list_id: str, name: str) -> dict:
    """Rename a bookmark list.

    Args:
        session (requests.Session): Session returned by `scrap.login`.
        list_id (str): Id of the list.
        name (str): New name.

    Returns:
        dict: The updated list.
    """
    return request_json(
        session, "PATCH", f"/api/account/bookmarks-lists/{list_id}", body={"name": name}
    )


def add_to_list(session: requests.Session, list_id: str, products: list[dict]) -> dict:
    """Add products to a bookmark list.

    Args:
        session (requests.Session): Session returned by `scrap.login`.
        list_id (str): Id of the list.
        products (list): Catalog products, each carrying a `canonicalId`.

    Returns:
        dict: The updated list.
    """
    body = {
        "bookmarks": [
            {"type": "PRODUCT", "articleId": product["canonicalId"]} for product in products
        ]
    }
    return request_json(
        session, "POST", f"/api/account/bookmarks-lists/{list_id}/bookmarks", body=body
    )


def delete_list(session: requests.Session, list_id: str) -> None:
    """Delete a bookmark list.

    Args:
        session (requests.Session): Session returned by `scrap.login`.
        list_id (str): Id of the list.
    """
    request_json(session, "DELETE", f"/api/account/bookmarks-lists/{list_id}", allow_status=(204,))


def publish_products(
    session: requests.Session, name: str, products: list[dict], list_id: Optional[str] = None
) -> str:
    """Put a set of products on a bookmark list and return its URL.

    Args:
        session (requests.Session): Session returned by `scrap.login`.
        name (str): Name of the list to create, ignored when `list_id` is given.
        products (list): Catalog products to put on the list.
        list_id (str, optional): Existing list to fill instead of a new one.

    Returns:
        str: The URL of the list on the site.
    """
    if list_id is None:
        list_id = create_list(session, name)["id"]
    add_to_list(session, list_id, products)
    return list_url(list_id)


def main() -> None:
    """Handle the bookmark lists from the command line."""
    parser = argparse.ArgumentParser(description="Handle mon-marche.fr bookmark lists")
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--execute", action="store_true", help="really write (dry run by default)")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("lister", help="show the lists of the account")

    voir_parser = subparsers.add_parser("voir", help="show the products of a list")
    voir_parser.add_argument("list_id")

    creer_parser = subparsers.add_parser(
        "creer", parents=[common], help="create a list from search terms"
    )
    creer_parser.add_argument("nom", help="name of the list")
    creer_parser.add_argument("--termes", nargs="+", default=[], help="search terms to add")
    creer_parser.add_argument("--skus", nargs="+", default=[], help="exact skus to add")

    supprimer_parser = subparsers.add_parser(
        "supprimer", parents=[common], help="delete a list"
    )
    supprimer_parser.add_argument("list_id")

    args = parser.parse_args()
    session, _ = login()

    if args.command == "lister":
        for bookmark_list in get_lists(session):
            print(
                f"{bookmark_list['id']} | {bookmark_list['name']} | "
                f"{list_url(bookmark_list['id'])}"
            )
    elif args.command == "voir":
        bookmark_list = get_list(session, args.list_id)
        items = bookmark_list.get("bookmarks", {}).get("items", [])
        print(f"{bookmark_list['name']} ({len(items)} produits)")
        for item in items:
            print(f"  {item.get('sku')} | {item.get('name')}")
        print(list_url(args.list_id))
    elif args.command == "creer":
        if not args.termes and not args.skus:
            parser.error("give --termes or --skus")
        products = []
        for term in args.termes:
            found = search(session, term, "PRODUCT", limit=1).get("items", [])
            if not found:
                print(f"  introuvable : {term}")
                continue
            products.append(found[0])
        for sku in args.skus:
            found = find_by_sku(session, sku)
            if found is None:
                print(f"  introuvable : {sku}")
                continue
            products.append(found)
        for product in products:
            print(f"  {product['sku']} | {product['name']} | {product_price(product)}")
        if not args.execute:
            print(f"\nDry run : la liste « {args.nom} » n'a pas été créée.")
            return
        print(f"\n{publish_products(session, args.nom, products)}")
    elif args.command == "supprimer":
        bookmark_list = get_list(session, args.list_id)
        print(f"Suppression de « {bookmark_list['name']} »")
        if not args.execute:
            print("Dry run, la liste n'a pas été supprimée.")
            return
        delete_list(session, args.list_id)
        print("Liste supprimée.")


if __name__ == "__main__":
    main()
