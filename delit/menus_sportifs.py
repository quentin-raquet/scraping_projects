# This script builds three healthy "sportif" menus out of the DELIT. menu of
# the day, and checks that each one fits into a 16 € budget.
#
# Quantities are expressed in grams for the products sold by weight (the
# website uses 10 g steps) and in pieces for the others.
import argparse
from typing import Optional

from scrap import UNITE_POIDS, get_menu, prix_portion

BUDGET_MAX = 16.0
# Each menu is a list of (product name, quantity) pairs.
MENUS = {
    "Menu 1 — Poulet rôti & riz (récupération post-entraînement)": [
        ("Cuisse de Poulet désossée et rôtie", 150),
        ("Riz Thaï", 200),
        ("Brocolis à la vapeur", 150),
        ("Fromage blanc nature", 1),
    ],
    "Menu 2 — Saumon & grenailles (oméga-3, veille de compétition)": [
        ("Pavé de Saumon grillé", 120),
        ("Pommes de terre Grenailles rôties", 180),
        ("Courgettes grillées", 120),
        ("Sauce Poisson au Citron & Basilic", 1),
        ("Fromage blanc coulis fruits rouges", 1),
    ],
    "Menu 3 — Végétarien protéiné (quinoa, lentilles & oeufs)": [
        ("Salade de Quinoa", 150),
        ("Salade de lentilles corail", 150),
        ("Oeuf dur", 2),
        ("Ratatouille", 110),
        ("Fromage blanc coulis de mangue", 1),
    ],
}


def index_produits(produits: list[dict]) -> dict:
    """Index the products of the menu by name.

    Args:
        produits (list): List of products of the menu.

    Returns:
        dict: Products indexed by name.
    """
    return {produit["nom"]: produit for produit in produits}


def chiffrer_menu(lignes: list[tuple[str, float]], produits: dict) -> tuple[list[dict], float]:
    """Price a menu against the products available today.

    Args:
        lignes (list): List of (product name, quantity) pairs.
        produits (dict): Products of the menu of the day, indexed by name.

    Returns:
        tuple: The detailed lines and the total price in euros.
    """
    detail = []
    for nom, quantite in lignes:
        produit = produits.get(nom)
        if produit is None:
            detail.append({"nom": nom, "quantite": quantite, "prix": None, "disponible": False})
            continue
        au_poids = produit.get("unite") == UNITE_POIDS
        detail.append(
            {
                "nom": nom,
                "quantite": f"{quantite:g} g" if au_poids else f"{quantite:g} × {produit['unite']}",
                "prix": prix_portion(produit, quantite),
                "disponible": True,
            }
        )
    total = round(sum(ligne["prix"] for ligne in detail if ligne["prix"] is not None), 2)
    return detail, total


def main(budget: Optional[float] = None) -> None:
    """Print the suggested menus priced with the menu of the day."""
    parser = argparse.ArgumentParser(description="Suggest healthy sport menus from the DELIT. menu")
    parser.add_argument("--budget", type=float, default=BUDGET_MAX, help="maximum budget per menu, in euros")
    args = parser.parse_args()
    budget = budget or args.budget

    menu = get_menu()
    produits = index_produits(menu["produits"])
    print(f"Menu du jour mis à jour le {menu['menu_updated_at']} ({len(produits)} produits disponibles)\n")

    for titre, lignes in MENUS.items():
        detail, total = chiffrer_menu(lignes, produits)
        print(titre)
        for ligne in detail:
            if not ligne["disponible"]:
                print(f"  - {ligne['nom']} : INDISPONIBLE aujourd'hui")
                continue
            print(f"  - {ligne['nom']} ({ligne['quantite']}) : {ligne['prix']:.2f} €")
        statut = "OK" if total <= budget else "HORS BUDGET"
        print(f"  Total : {total:.2f} € / {budget:.2f} € — {statut}\n")


if __name__ == "__main__":
    main()
