# This script turns a shopping list into a cart on mon-marche.fr.
#
# For each term of the list it searches the catalog and ranks the candidates
# with the preferences of the account:
#   1. a product already bought weighs the most, even more if it is one of the
#      top products of the account;
#   2. then the organic products (`labels` carrying "BIO");
#   3. then the French ones (`origin` mentioning France);
#   4. then the quality labels (AOP, IGP, Label Rouge, HVE).
#
# When the first two candidates end up too close, the term is left undecided
# and written to an HTML page showing the pictures, so the choice can be made
# by eye. Nothing is written to the cart without `--execute`.
import argparse
import html
import json
import re
import unicodedata
from typing import Optional

import requests

from panier import canonical_id, ensure_cart, print_cart, set_products_quantities
from scrap import get_json, login, product_price, search

# A product is organic when one of its labels mentions BIO ("Label BIO AB",
# "Label BIO UE").
BIO_PATTERN = re.compile(r"\bbio\b", re.IGNORECASE)
# `origin` is "France" but also "élaboré en France", "élaborées en France"…
FRENCH_PATTERN = re.compile(r"france", re.IGNORECASE)
# Other labels worth a small bonus.
QUALITY_PATTERN = re.compile(r"\b(AOP|IGP|Label Rouge|HVE)\b", re.IGNORECASE)
# Score gap below which two candidates are considered too close to decide.
AMBIGUITY_GAP = 15
# Words too short or too common to tell two products apart.
STOP_WORDS = {"de", "du", "des", "la", "le", "les", "au", "aux", "en", "et", "a"}


def load_preferences(session: requests.Session) -> dict:
    """Read the products already bought by the account.

    Args:
        session (requests.Session): Session returned by `scrap.login`.

    Returns:
        dict: `bought` maps a sku to its product, `top` maps a sku to its rank
            in the most ordered products (0 is the most ordered).
    """
    categories = get_json(session, "/api/account/products").get("categories", [])
    bought = {
        product["sku"]: product
        for category in categories
        for product in category.get("products", [])
        if product.get("sku")
    }
    top = get_json(session, "/api/account/top-products").get("items", [])
    return {
        "bought": bought,
        "top": {product["sku"]: rank for rank, product in enumerate(top) if product.get("sku")},
    }


def is_bio(product: dict) -> bool:
    """Tell whether a product carries an organic label.

    Args:
        product (dict): A catalog product.

    Returns:
        bool: True when one of its labels mentions BIO.
    """
    return any(BIO_PATTERN.search(label.get("label", "")) for label in product.get("labels") or [])


def is_french(product: dict) -> bool:
    """Tell whether a product comes from or is made in France.

    Args:
        product (dict): A catalog product.

    Returns:
        bool: True when its origin mentions France.
    """
    return bool(FRENCH_PATTERN.search(product.get("origin") or ""))


def quality_labels(product: dict) -> list[str]:
    """List the quality labels of a product.

    Args:
        product (dict): A catalog product.

    Returns:
        list: The labels matching AOP, IGP, Label Rouge or HVE.
    """
    labels = [label.get("label", "") for label in product.get("labels") or []]
    return [label for label in labels if QUALITY_PATTERN.search(label)]


def normalize(text: str) -> str:
    """Lower a text and strip its accents, to compare words loosely.

    Args:
        text (str): The text to normalize.

    Returns:
        str: The normalized text.
    """
    stripped = unicodedata.normalize("NFD", text or "")
    return "".join(char for char in stripped if unicodedata.category(char) != "Mn").lower()


def term_words(term: str) -> list[str]:
    """Split a search term into the words worth matching.

    Args:
        term (str): Search term, e.g. "tomate cerise".

    Returns:
        list: The normalized words, without the stop words.
    """
    words = re.findall(r"\w+", normalize(term))
    return [word for word in words if word not in STOP_WORDS and len(word) > 2]


def matched_words(product: dict, words: list[str]) -> list[str]:
    """List the words of a term found in the name of a product.

    The catalog search is fuzzy enough to return a cottage cheese for
    "tomate cerise", so the name is checked again here.

    Args:
        product (dict): A catalog product.
        words (list): Words returned by `term_words`.

    Returns:
        list: The words found in the name or the category of the product.
    """
    haystack = normalize(f"{product.get('name', '')} {product.get('pimCategoryName', '')}")
    return [word for word in words if word[:-1] in haystack or word in haystack]


def is_available(product: dict) -> bool:
    """Tell whether a product can be added to the cart.

    Args:
        product (dict): A catalog product.

    Returns:
        bool: True when enough stock is left for one item.
    """
    return (product.get("availableQuantity") or 0) >= (product.get("packSize") or 1)


def score_product(
    product: dict, preferences: dict, position: int, words: Optional[list[str]] = None
) -> tuple[int, list[str]]:
    """Score a candidate against the preferences of the account.

    Args:
        product (dict): A catalog product.
        preferences (dict): The preferences returned by `load_preferences`.
        position (int): Rank of the product in the search results.
        words (list, optional): Words of the term, to reward a closer name.

    Returns:
        tuple: The score and the reasons that built it.
    """
    score, reasons = 0, []
    sku = product.get("sku")

    # A product matching every word of the term is a closer answer.
    if words:
        score += 8 * len(matched_words(product, words))

    if sku in preferences["bought"]:
        score += 60
        reasons.append("déjà commandé")
    if sku in preferences["top"]:
        rank = preferences["top"][sku]
        score += max(30 - rank, 5)
        reasons.append(f"top produit #{rank + 1}")
    if is_bio(product):
        score += 25
        reasons.append("bio")
    if is_french(product):
        score += 20
        reasons.append(f"origine {product.get('origin')}")
    for label in quality_labels(product):
        score += 10
        reasons.append(label)

    # The catalog already sorts by relevance, keep a light preference for it.
    score -= 2 * position
    return score, reasons


def rank_candidates(
    session: requests.Session, term: str, preferences: dict, limit: int = 12
) -> list[dict]:
    """Search a term and rank the available products it returns.

    Args:
        session (requests.Session): Session returned by `scrap.login`.
        term (str): Search term, e.g. "tomate cerise".
        preferences (dict): The preferences returned by `load_preferences`.
        limit (int): Number of catalog results to consider.

    Returns:
        list: The candidates, best first, each with its `score` and `reasons`.
    """
    items = search(session, term, "PRODUCT", limit=limit).get("items", [])
    words = term_words(term)
    candidates = []
    for position, product in enumerate(items):
        if not is_available(product):
            continue
        # Drop what the fuzzy search returned but has nothing to do with the term.
        if words and not matched_words(product, words):
            continue
        score, reasons = score_product(product, preferences, position, words)
        candidates.append({**product, "score": score, "reasons": reasons})
    return sorted(candidates, key=lambda candidate: -candidate["score"])


def is_ambiguous(candidates: list[dict]) -> bool:
    """Tell whether the choice between the candidates should be left to a human.

    The choice is ambiguous when the two best candidates are too close, or when
    the best one carries no signal at all: no purchase history, not organic and
    not French.

    Args:
        candidates (list): Candidates returned by `rank_candidates`.

    Returns:
        bool: True when the term needs a manual choice.
    """
    if not candidates:
        return False
    if len(candidates) == 1:
        return False
    best, second = candidates[0], candidates[1]
    if not best["reasons"]:
        return True
    return best["score"] - second["score"] < AMBIGUITY_GAP


def build_selection(
    session: requests.Session, wanted: list[dict], preferences: dict
) -> dict:
    """Turn a shopping list into picked products and open questions.

    Args:
        session (requests.Session): Session returned by `scrap.login`.
        wanted (list): Dicts with a `terme` and an optional `quantite`.
        preferences (dict): The preferences returned by `load_preferences`.

    Returns:
        dict: `retenus` holds the decided lines, `a_choisir` the ambiguous ones,
            `introuvables` the terms with no result.
    """
    selection: dict = {"retenus": [], "a_choisir": [], "introuvables": []}
    for item in wanted:
        term, quantity = item["terme"], item.get("quantite", 1)
        candidates = rank_candidates(session, term, preferences)
        if not candidates:
            selection["introuvables"].append(term)
        elif is_ambiguous(candidates):
            selection["a_choisir"].append(
                {"terme": term, "quantite": quantity, "candidats": candidates[:6]}
            )
        else:
            best = candidates[0]
            selection["retenus"].append(
                {
                    "terme": term,
                    "quantite": quantity,
                    "id": canonical_id(best["id"]),
                    "sku": best["sku"],
                    "nom": best["name"],
                    "prix": product_price(best),
                    "prix_article": best.get("itemPrice"),
                    "raisons": best["reasons"],
                }
            )
    return selection


def product_image(product: dict) -> str:
    """Get the picture URL of a product.

    Args:
        product (dict): A catalog product.

    Returns:
        str: The URL of its first image, empty when it has none.
    """
    images = product.get("images") or []
    return images[0].get("url", "") if images else ""


def render_choices(selection: dict, path: str) -> None:
    """Write the ambiguous terms to an HTML page showing the pictures.

    Args:
        selection (dict): The selection returned by `build_selection`.
        path (str): Path of the HTML file to write.
    """
    blocks = []
    for question in selection["a_choisir"]:
        cards = []
        for candidate in question["candidats"]:
            badges = "".join(
                f'<span class="badge">{html.escape(reason)}</span>'
                for reason in candidate["reasons"]
            )
            cards.append(
                f"""<figure class="card">
    <img src="{html.escape(product_image(candidate))}" alt="{html.escape(candidate['name'])}" loading="lazy">
    <figcaption>
      <strong>{html.escape(candidate['name'])}</strong>
      <span class="price">{html.escape(product_price(candidate))}</span>
      <span class="sku">{html.escape(candidate['sku'])}</span>
      <div class="badges">{badges}</div>
    </figcaption>
  </figure>"""
            )
        blocks.append(
            f"""<section>
  <h2>{html.escape(question['terme'])}<span class="qty">× {question['quantite']}</span></h2>
  <div class="grid">{''.join(cards)}</div>
</section>"""
        )

    page = f"""<!DOCTYPE html>
<html lang="fr"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Choix produits</title>
<style>
  :root {{ color-scheme: light dark; --bg:#fbfaf7; --fg:#1a1a1a; --card:#fff; --line:#e6e2da; --accent:#003301; }}
  @media (prefers-color-scheme: dark) {{
    :root {{ --bg:#14150f; --fg:#f2f0ea; --card:#1e1f18; --line:#32332a; --accent:#a8c9a0; }}
  }}
  body {{ margin:0; padding:24px 16px; background:var(--bg); color:var(--fg);
         font:16px/1.5 ui-sans-serif, system-ui, -apple-system, sans-serif; }}
  h1 {{ font-size:1.4rem; margin:0 0 4px; }}
  .lead {{ color:#8a8578; margin:0 0 28px; font-size:.9rem; }}
  h2 {{ font-size:1.05rem; margin:28px 0 12px; padding-bottom:6px; border-bottom:1px solid var(--line); }}
  .qty {{ color:#8a8578; font-weight:400; margin-left:8px; font-size:.85rem; }}
  .grid {{ display:grid; grid-template-columns:repeat(auto-fill, minmax(150px, 1fr)); gap:14px; }}
  .card {{ margin:0; background:var(--card); border:1px solid var(--line); border-radius:10px;
           overflow:hidden; display:flex; flex-direction:column; }}
  .card img {{ width:100%; aspect-ratio:1; object-fit:cover; background:#efece5; }}
  figcaption {{ padding:10px; display:flex; flex-direction:column; gap:4px; font-size:.82rem; }}
  .price {{ color:var(--accent); font-weight:600; }}
  .sku {{ color:#8a8578; font-size:.75rem; font-family:ui-monospace, monospace; }}
  .badges {{ display:flex; flex-wrap:wrap; gap:4px; margin-top:4px; }}
  .badge {{ background:var(--accent); color:var(--bg); border-radius:99px;
            padding:2px 8px; font-size:.68rem; }}
</style></head>
<body>
<h1>Produits à choisir</h1>
<p class="lead">Un doute subsiste sur ces {len(selection['a_choisir'])} lignes : dis-moi le SKU retenu pour chacune.</p>
{''.join(blocks)}
</body></html>"""
    with open(path, "w", encoding="utf-8") as file:
        file.write(page)


def print_selection(selection: dict) -> None:
    """Print the selection on the console.

    Args:
        selection (dict): The selection returned by `build_selection`.
    """
    total = sum(
        (line.get("prix_article") or 0) * line["quantite"] for line in selection["retenus"]
    )
    print(f"Retenus ({len(selection['retenus'])}) :")
    for line in selection["retenus"]:
        reasons = ", ".join(line["raisons"]) or "meilleur résultat"
        print(
            f"  {line['quantite']} x {line['nom']} ({line['sku']}, {line['prix']}) "
            f"— {reasons}   [{line['terme']}]"
        )
    print(f"  sous-total indicatif : {total / 100:.2f} €".replace(".", ","))

    if selection["a_choisir"]:
        print(f"\nÀ choisir ({len(selection['a_choisir'])}) :")
        for question in selection["a_choisir"]:
            print(f"  {question['terme']} :")
            for candidate in question["candidats"]:
                reasons = ", ".join(candidate["reasons"]) or "-"
                print(
                    f"    {candidate['sku']} | {candidate['name']} | "
                    f"{product_price(candidate)} | {reasons}"
                )
    if selection["introuvables"]:
        print(f"\nIntrouvables : {', '.join(selection['introuvables'])}")


def apply_selection(
    session: requests.Session, selection: dict, slot_id: Optional[str] = None
) -> dict:
    """Push the decided lines of a selection into the cart.

    Args:
        session (requests.Session): Session returned by `scrap.login`.
        selection (dict): The selection returned by `build_selection`.
        slot_id (str, optional): Delivery slot to book if the cart must be created.

    Returns:
        dict: The cart after the update.
    """
    products = [
        {"id": line["id"], "quantity": line["quantite"]} for line in selection["retenus"]
    ]
    if not products:
        raise Exception("Nothing to add, the selection holds no decided line")
    _, booked = ensure_cart(session, slot_id)
    if booked is not None:
        print(f"Panier créé sur le créneau {booked.get('id')}")
    return set_products_quantities(session, products)


def read_list(path: Optional[str], terms: list[str]) -> list[dict]:
    """Build the shopping list from a JSON file or from command line terms.

    Args:
        path (str, optional): JSON file holding a list of terms or of
            `{"terme": ..., "quantite": ...}` dicts.
        terms (list): Terms given on the command line, "terme:quantite" allowed.

    Returns:
        list: Dicts with a `terme` and a `quantite`.
    """
    if path:
        with open(path, encoding="utf-8") as file:
            raw = json.load(file)
        return [item if isinstance(item, dict) else {"terme": item} for item in raw]
    wanted = []
    for term in terms:
        name, _, quantity = term.rpartition(":")
        if name and quantity.isdigit():
            wanted.append({"terme": name, "quantite": int(quantity)})
        else:
            wanted.append({"terme": term, "quantite": 1})
    return wanted


def main() -> None:
    """Build a cart from a shopping list."""
    parser = argparse.ArgumentParser(description="Fill a mon-marche.fr cart from a list")
    parser.add_argument("termes", nargs="*", help='search terms, "terme:quantite" allowed')
    parser.add_argument("--liste", help="JSON file holding the shopping list")
    parser.add_argument("--selection", help="JSON file of a selection to reuse")
    parser.add_argument("--out", help="write the ambiguous terms to this HTML file")
    parser.add_argument("--json", help="write the selection to this JSON file")
    parser.add_argument("--creneau", help="delivery slot id, if the cart must be created")
    parser.add_argument("--execute", action="store_true", help="really fill the cart")
    args = parser.parse_args()

    session, _ = login()

    if args.selection:
        with open(args.selection, encoding="utf-8") as file:
            selection = json.load(file)
    else:
        wanted = read_list(args.liste, args.termes)
        if not wanted:
            parser.error("give at least one term, or a list with --liste")
        selection = build_selection(session, wanted, load_preferences(session))

    print_selection(selection)

    if args.out and selection["a_choisir"]:
        render_choices(selection, args.out)
        print(f"\nPage de choix écrite dans {args.out}")
    if args.json:
        with open(args.json, "w", encoding="utf-8") as file:
            json.dump(selection, file, ensure_ascii=False, indent=2)
        print(f"Sélection écrite dans {args.json}")

    if not args.execute:
        print("\nDry run, le panier n'a pas été touché. Relancer avec --execute.")
        return
    print()
    print_cart(apply_selection(session, selection, args.creneau))


if __name__ == "__main__":
    main()
