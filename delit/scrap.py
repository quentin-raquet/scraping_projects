# This script scrapes the DELIT. traiteur website (https://www.delit.co/)
# to get the menu of the day.
#
# The website is a single page app whose data lives in Cloud Firestore: the
# menu is stored in the document `config/produits` (field `liste`) and the
# ordering status in `config/horaire`. Both are readable through the public
# Firestore REST API with the web API key served by the homepage, so there is
# no HTML parsing to do apart from reading that key.
import argparse
import json
import re
from typing import Any, Optional

import pandas as pd
import requests

SITE_URL = "https://www.delit.co/"
FIRESTORE_URL = "https://firestore.googleapis.com/v1/projects/{project}/databases/(default)/documents/{path}"
# Order used by the website to display the categories.
CATEGORIES_ORDER = [
    "Nos formules",
    "Entrées",
    "Féculents",
    "Légumes",
    "Viandes",
    "Poissons",
    "Plats composés",
    "Desserts",
    "Boissons",
]
# `unite` values: "100g" is a legacy key, the price it carries is a price per kg.
UNITE_POIDS = "100g"


def get_firebase_config(site_url: str = SITE_URL) -> dict:
    """Extract the Firebase configuration inlined in the homepage.

    Args:
        site_url (str): URL of the DELIT. homepage.

    Returns:
        dict: The firebaseConfig object, with at least `apiKey` and `projectId`.
    """
    res = requests.get(site_url, timeout=30)
    if res.status_code != 200:
        raise Exception(f"Bad request status code {res.status_code}, url : {site_url}")
    match = re.search(r"const firebaseConfig\s*=\s*\{(.*?)\}\s*;", res.text, re.DOTALL)
    if match is None:
        raise Exception("firebaseConfig not found, the website structure has changed")
    config = dict(re.findall(r'(\w+)\s*:\s*"([^"]*)"', match.group(1)))
    if "apiKey" not in config or "projectId" not in config:
        raise Exception(f"Incomplete firebaseConfig : {config}")
    return config


def parse_value(value: dict) -> Any:
    """Convert a Firestore typed value into a plain Python value.

    Args:
        value (dict): A Firestore value, e.g. {"integerValue": "15"}.

    Returns:
        Any: The corresponding Python value.
    """
    (type_name, content), = value.items()
    if type_name == "integerValue":
        return int(content)
    if type_name == "doubleValue":
        return float(content)
    if type_name == "nullValue":
        return None
    if type_name == "mapValue":
        return parse_fields(content.get("fields", {}))
    if type_name == "arrayValue":
        return [parse_value(item) for item in content.get("values", [])]
    return content


def parse_fields(fields: dict) -> dict:
    """Convert the `fields` object of a Firestore document into a plain dict.

    Args:
        fields (dict): The `fields` object of a Firestore document.

    Returns:
        dict: The document as a plain Python dict.
    """
    return {key: parse_value(value) for key, value in fields.items()}


def get_document(project: str, api_key: str, path: str) -> dict:
    """Read a single document through the Firestore REST API.

    Args:
        project (str): The Firebase project id.
        api_key (str): The Firebase web API key.
        path (str): Document path, e.g. "config/produits".

    Returns:
        dict: The document content as a plain Python dict.
    """
    url = FIRESTORE_URL.format(project=project, path=path)
    res = requests.get(url, params={"key": api_key}, timeout=30)
    if res.status_code != 200:
        raise Exception(f"Bad request status code {res.status_code}, path : {path}")
    return parse_fields(res.json().get("fields", {}))


def get_menu(config: Optional[dict] = None, actifs_only: bool = True) -> dict:
    """Get the menu of the day.

    Args:
        config (dict, optional): Firebase config, read from the website if None.
        actifs_only (bool): Keep only the products available today.

    Returns:
        dict: The menu with its products, service info and last update date.
    """
    config = config or get_firebase_config()
    project, api_key = config["projectId"], config["apiKey"]
    produits_doc = get_document(project, api_key, "config/produits")
    horaire_doc = get_document(project, api_key, "config/horaire")

    produits = produits_doc.get("liste", [])
    if actifs_only:
        produits = [produit for produit in produits if produit.get("actif")]
    produits = sorted(
        produits,
        key=lambda produit: (
            CATEGORIES_ORDER.index(produit["categorie"])
            if produit.get("categorie") in CATEGORIES_ORDER
            else len(CATEGORIES_ORDER),
            produit.get("ordre", 999),
            produit.get("nom", ""),
        ),
    )
    return {
        "produits": produits,
        "ouvert": horaire_doc.get("ouvert"),
        "horaire_limite": horaire_doc.get("horaire"),
        "menu_updated_at": produits_doc.get("updatedAt"),
        "horaire_updated_at": horaire_doc.get("updatedAt"),
    }


def prix_portion(produit: dict, quantite: float) -> float:
    """Compute the price of a given quantity of a product.

    The website prices weighted products per kg (`unite` == "100g", quantity in
    grams, 10 g steps) and the other ones per piece or per portion.

    Args:
        produit (dict): A product of the menu.
        quantite (float): Quantity, in grams for weighted products.

    Returns:
        float: The price in euros, rounded to the cent.
    """
    if produit.get("unite") == UNITE_POIDS:
        return round(produit["prix"] * quantite / 1000, 2)
    return round(produit["prix"] * quantite, 2)


def to_dataframe(produits: list[dict]) -> pd.DataFrame:
    """Build a tabular view of the menu.

    Args:
        produits (list): List of products of the menu.

    Returns:
        pd.DataFrame: One row per product, with the unit and the price basis.
    """
    df = pd.DataFrame(produits)
    df["au_poids"] = df["unite"] == UNITE_POIDS
    df["prix_kg"] = df["prix"].where(df["au_poids"])
    df["prix_piece"] = df["prix"].where(~df["au_poids"])
    df["prix_100g"] = (df["prix"] / 10).where(df["au_poids"])
    columns = [
        "id",
        "categorie",
        "nom",
        "description",
        "unite",
        "au_poids",
        "prix_kg",
        "prix_100g",
        "prix_piece",
        "actif",
    ]
    return df[[column for column in columns if column in df.columns]]


def main() -> None:
    """Scrape the menu of the day and save it as JSON and CSV."""
    parser = argparse.ArgumentParser(description="Scrape the DELIT. menu of the day")
    parser.add_argument("--all", action="store_true", help="include unavailable products")
    parser.add_argument("--json", default="menu_du_jour.json", help="output JSON file")
    parser.add_argument("--csv", default="menu_du_jour.csv", help="output CSV file")
    args = parser.parse_args()

    menu = get_menu(actifs_only=not args.all)
    with open(args.json, "w", encoding="utf-8") as file:
        json.dump(menu, file, ensure_ascii=False, indent=2)
    df = to_dataframe(menu["produits"])
    df.to_csv(args.csv, index=False)

    etat = "ouvert" if menu["ouvert"] else "fermé"
    print(f"Menu du {menu['menu_updated_at']} — service {etat} (commandes jusqu'à {menu['horaire_limite']})")
    print(f"{len(menu['produits'])} produits enregistrés dans {args.json} et {args.csv}")
    for categorie, group in df.groupby("categorie", sort=False):
        print(f"\n## {categorie}")
        for _, produit in group.iterrows():
            prix = f"{produit['prix_kg']:.2f} €/kg" if produit["au_poids"] else f"{produit['prix_piece']:.2f} €/{produit['unite']}"
            print(f"  - {produit['nom']} : {prix}")


if __name__ == "__main__":
    main()
