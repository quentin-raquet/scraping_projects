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

from favoris import publish_products
from panier import canonical_id, ensure_cart, print_cart, set_products_quantities
from scrap import format_price, login, product_price, request_json, search

# A product is organic when one of its labels mentions BIO ("Label BIO AB",
# "Label BIO UE").
BIO_PATTERN = re.compile(r"\bbio\b", re.IGNORECASE)
# `origin` is "France" but also "élaboré en France", "élaborées en France"…
FRENCH_PATTERN = re.compile(r"france", re.IGNORECASE)
# Other labels worth a small bonus.
QUALITY_PATTERN = re.compile(r"\b(AOP|IGP|Label Rouge|HVE)\b", re.IGNORECASE)
# Score gap below which two candidates are considered too close to decide.
AMBIGUITY_GAP = 15
# The first two letters of a sku name the department of the product:
# BC boucherie, CH charcuterie, MA marée, FR fromagerie, FL fruits et légumes,
# LS crèmerie, EP épicerie, TB traiteur, NA non alimentaire.
BUTCHER_PREFIXES = ("BC",)
# The catalog puts the brand of a product between double quotes in its name:
# 'L\'Essuie-tout "Renova"', 'Le Gel douche lait d\'amande douce XL "Le Petit
# Marseillais"'.
BRAND_PATTERN = re.compile(r'"([^"]+)"')
# Sweeteners counted as added sugar on an ingredient list.
ADDED_SUGAR_PATTERN = re.compile(
    r"\b(sucres?|dextrose|glucose|fructose|saccharose|maltodextrine|miel|"
    r"sirop (?:de|d')[a-zéè ]+)\b",
    re.IGNORECASE,
)
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
    categories = request_json(session, "GET", "/api/account/products").get("categories", [])
    bought = {
        product["sku"]: product
        for category in categories
        for product in category.get("products", [])
        if product.get("sku")
    }
    top = request_json(session, "GET", "/api/account/top-products").get("items", [])
    return {
        "bought": bought,
        "top": {product["sku"]: rank for rank, product in enumerate(top) if product.get("sku")},
        "brands": {
            brand
            for product in bought.values()
            for brand in BRAND_PATTERN.findall(product.get("name", ""))
        },
    }


def product_brand(product: dict) -> str:
    """Read the brand of a product from its name.

    Args:
        product (dict): A catalog product.

    Returns:
        str: The brand, empty when the name quotes none.
    """
    found = BRAND_PATTERN.findall(product.get("name", ""))
    return found[0] if found else ""


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
    # NFD splits the accents off but leaves the ligatures alone, and the catalog
    # writes "Les 12 Œufs BIO" where a search is typed "oeuf".
    folded = (text or "").replace("œ", "oe").replace("Œ", "OE")
    folded = folded.replace("æ", "ae").replace("Æ", "AE")
    stripped = unicodedata.normalize("NFD", folded)
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


def is_butcher(product: dict, prefixes: tuple = BUTCHER_PREFIXES) -> bool:
    """Tell whether a product belongs to the butcher department.

    Args:
        product (dict): A catalog product.
        prefixes (tuple): Sku prefixes bought at the butcher instead.

    Returns:
        bool: True when its sku starts with one of the prefixes.
    """
    return (product.get("sku") or "").upper().startswith(tuple(prefixes))


def goes_to_butcher(candidates: list[dict], prefixes: tuple = BUTCHER_PREFIXES) -> bool:
    """Tell whether a term should go to the butcher list rather than the cart.

    Args:
        candidates (list): Candidates returned by `rank_candidates`.
        prefixes (tuple): Sku prefixes bought at the butcher instead.

    Returns:
        bool: True when the term names meat.
    """
    top = candidates[:4]
    if not top:
        return False
    if is_butcher(top[0], prefixes):
        return True
    # A term like "saucisse" mixes departments. On a tie the butcher wins: not
    # ordering meat here is an explicit rule, a wrong routing is easy to spot.
    return 2 * sum(is_butcher(candidate, prefixes) for candidate in top) >= len(top)


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

    # Naming the product asked for outweighs a generic bio or French bonus:
    # "saucisse de toulouse" must not land on a Francfort because it is organic.
    if words:
        found = matched_words(product, words)
        score += 12 * len(found)
        if len(found) == len(words):
            score += 20
            reasons.append("nom exact")

    if sku in preferences["bought"]:
        score += 60
        reasons.append("déjà commandé")
    if sku in preferences["top"]:
        rank = preferences["top"][sku]
        score += max(30 - rank, 5)
        reasons.append(f"top produit #{rank + 1}")
    # "a similar product in the history" is not only the same sku: a brand
    # already bought is a preference too, and the catalog quotes the brand.
    brand = product_brand(product)
    if sku not in preferences["bought"] and brand and brand in preferences.get("brands", set()):
        score += 18
        reasons.append(f"marque déjà achetée ({brand})")
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

    def build(filter_on_name: bool) -> list[dict]:
        candidates = []
        for position, product in enumerate(items):
            if not is_available(product):
                continue
            # Drop what the fuzzy search returned but has nothing to do with the term.
            if filter_on_name and words and not matched_words(product, words):
                continue
            score, reasons = score_product(product, preferences, position, words)
            candidates.append({**product, "score": score, "reasons": reasons})
        return sorted(candidates, key=lambda candidate: -candidate["score"])

    # The name filter also drops the synonyms the catalog resolves on its own:
    # "sopalin" returns the Essuie-tout, whose name holds none of the word. When
    # it leaves nothing, the search engine knew better, so trust its results.
    return build(True) or build(False)


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
    session: requests.Session,
    wanted: list[dict],
    preferences: dict,
    butcher_prefixes: tuple = BUTCHER_PREFIXES,
) -> dict:
    """Turn a shopping list into picked products and open questions.

    Args:
        session (requests.Session): Session returned by `scrap.login`.
        wanted (list): Dicts with a `terme` and an optional `quantite`.
        preferences (dict): The preferences returned by `load_preferences`.
        butcher_prefixes (tuple): Sku prefixes to route to the butcher list.

    Returns:
        dict: `retenus` holds the decided lines, `a_choisir` the ambiguous ones,
            `boucher` the meat left out of the cart, `introuvables` the terms
            with no result.
    """
    selection: dict = {"retenus": [], "a_choisir": [], "boucher": [], "introuvables": []}
    for item in wanted:
        term, quantity = item["terme"], item.get("quantite", 1)
        candidates = rank_candidates(session, term, preferences)
        if not candidates:
            selection["introuvables"].append(term)
        elif item.get("boucher") or goes_to_butcher(candidates, butcher_prefixes):
            best = candidates[0]
            selection["boucher"].append(
                {
                    "terme": term,
                    "quantite": quantity,
                    "reference": best["name"],
                    "prix_indicatif": product_price(best),
                }
            )
        elif is_ambiguous(candidates):
            selection["a_choisir"].append(
                {"terme": term, "quantite": quantity, "candidats": candidates[:8]}
            )
        else:
            best = candidates[0]
            selection["retenus"].append(
                {
                    "terme": term,
                    "quantite": quantity,
                    "id": canonical_id(best["id"]),
                    "sku": best["sku"],
                    "slug": best["slug"],
                    "nom": best["name"],
                    "prix": product_price(best),
                    "prix_article": best.get("itemPrice"),
                    "raisons": best["reasons"],
                }
            )
    return selection


def unit_price(product: dict) -> str:
    """Format the price per litre or per kilo of a product.

    The API gives it in `weightPrice.unitPrice`, which is what makes a 25 cl
    bottle comparable to a 3 l can.

    Args:
        product (dict): A catalog product.

    Returns:
        str: The price per unit, empty when the API gives none.
    """
    weight = product.get("weightPrice") or {}
    if not weight.get("unitPrice") or not weight.get("unit"):
        return ""
    return f"{format_price(weight['unitPrice'])} / {weight['unit']}"


def content_size(product: dict) -> str:
    """Format the size of one item of a product.

    Args:
        product (dict): A catalog product.

    Returns:
        str: The content, e.g. "0.75 l", empty when the API gives none.
    """
    weight = (product.get("itemDefinition") or {}).get("weight") or {}
    if not weight.get("value"):
        return ""
    return f"{weight['value']:g} {weight.get('unit', '')}".strip()


def packaging(product: dict) -> str:
    """Format how a product is packed.

    For the products sold by the pack the count is not in `packSize` but in
    `itemDefinition.terminologyOverride`, e.g. "12 rouleaux", "Pack de 3".

    Args:
        product (dict): A catalog product.

    Returns:
        str: The packaging, empty when the API gives none.
    """
    override = (product.get("itemDefinition") or {}).get("terminologyOverride") or ""
    return "" if override in ("Bouteille", "Bidon") else override


def quantity_meaning(product: dict) -> str:
    """Say what one unit of quantity buys for a product.

    `itemDefinition.type` decides how the site reads a quantity:
      - `piece`: one item, e.g. a bunch of spring onions;
      - `pieceWeight`: one item of a known weight, e.g. a 500 g net;
      - `arbitraryQuantity`: a multiple of a reference weight, so a quantity of
        2 on onions sold per 500 g buys a kilo, not two onions.

    Args:
        product (dict): A catalog product.

    Returns:
        str: What a quantity of 1 buys.
    """
    definition = product.get("itemDefinition") or {}
    weight = definition.get("weight") or {}
    label = definition.get("terminologyOverride") or (definition.get("terminology") or {}).get(
        "singular", "pièce"
    )
    size = f"{weight['value']:g} {weight.get('unit', '')}".strip() if weight.get("value") else ""
    if definition.get("type") == "arbitraryQuantity" and size:
        return f"1 = {size}"
    # A terminologyOverride is often already a count ("6 rouleaux"), so an
    # article prefix would read "1 = 1 6 rouleaux".
    article = "" if label[:1].isdigit() else "1 "
    if definition.get("type") == "pieceWeight" and size:
        return f"1 = {article}{label} de {size}"
    return f"1 = {article}{label}"


def get_details(session: requests.Session, product: dict) -> dict:
    """Read the detail page of a product.

    The search results carry no ingredient list; the detail endpoint does, under
    the `liste-ingredient` attribute.

    Args:
        session (requests.Session): Session returned by `scrap.login`.
        product (dict): A catalog product, carrying a `slug`.

    Returns:
        dict: The detailed product, empty when the slug resolves to nothing.
    """
    slug = product.get("slug")
    if not slug:
        return {}
    details = request_json(
        session, "GET", f"/api/articleDetailBySlug/{slug}", allow_status=(404,)
    )
    return details if isinstance(details, dict) and "attributes" in details else {}


def ingredient_list(session: requests.Session, product: dict) -> str:
    """Read the ingredient list of a product.

    Args:
        session (requests.Session): Session returned by `scrap.login`.
        product (dict): A catalog product, carrying a `slug`.

    Returns:
        str: The ingredients, empty when the product declares none.
    """
    details = get_details(session, product)
    raw = next(
        (
            attribute.get("value", "")
            for attribute in details.get("attributes") or []
            if attribute.get("key") == "liste-ingredient"
        ),
        "",
    )
    return html.unescape(raw)


def added_sugars(session: requests.Session, product: dict) -> tuple[str, list[str]]:
    """Say whether a product declares added sugar.

    Many products declare no ingredient list at all. Reading that silence as
    "no sugar" would let a sweetened product through a dietary constraint, so
    it is reported apart.

    Args:
        session (requests.Session): Session returned by `scrap.login`.
        product (dict): A catalog product, carrying a `slug`.

    Returns:
        tuple: The status, one of "avec", "sans" or "inconnu", and the
            sweeteners found.
    """
    ingredients = ingredient_list(session, product)
    if not ingredients.strip():
        return "inconnu", []
    found = sorted({match.strip().lower() for match in ADDED_SUGAR_PATTERN.findall(ingredients)})
    return ("avec", found) if found else ("sans", [])


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
            details = " · ".join(
                part
                for part in (quantity_meaning(candidate), unit_price(candidate))
                if part
            )
            cards.append(
                f"""<figure class="card">
    <img src="{html.escape(product_image(candidate))}" alt="{html.escape(candidate['name'])}" loading="lazy">
    <figcaption>
      <strong>{html.escape(candidate['name'])}</strong>
      <span class="price">{html.escape(product_price(candidate))}</span>
      <span class="unit">{html.escape(details)}</span>
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
  .unit {{ color:#8a8578; font-size:.75rem; }}
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


def render_butcher_list(selection: dict, path: str) -> None:
    """Write the meat lines to a markdown list to take to the butcher.

    Args:
        selection (dict): The selection returned by `build_selection`.
        path (str): Path of the markdown file to write.
    """
    lines = ["# Liste boucher", ""]
    for line in selection.get("boucher", []):
        lines.append(f"- [ ] **{line['terme']}** × {line['quantite']}")
        lines.append(
            f"      _équivalent Mon Marché : {line['reference']} — {line['prix_indicatif']}_"
        )
    lines.append("")
    with open(path, "w", encoding="utf-8") as file:
        file.write("\n".join(lines))


def filter_added_sugar(
    session: requests.Session, selection: dict, preferences: dict
) -> None:
    """Drop the candidates whose ingredient list declares a sweetener, in place.

    A term whose every candidate is sweetened keeps them, flagged
    `sans_sucre_impossible`: the constraint failing has to be visible rather
    than silently emptying the line.

    Args:
        session (requests.Session): Session returned by `scrap.login`.
        selection (dict): The selection returned by `build_selection`.
        preferences (dict): The preferences, to re-rank a rejected term.
    """
    still_decided, now_open = [], []
    for line in selection["retenus"]:
        status, sugars = added_sugars(session, line)
        line["sucre"], line["sucres"] = status, sugars
        (now_open if status != "sans" else still_decided).append(line)
    selection["retenus"] = still_decided

    for line in now_open:
        # The line had no candidate list, being decided; rebuild it to offer
        # the alternatives rather than a bare rejection.
        others = [
            candidate
            for candidate in rank_candidates(session, line["terme"], preferences)
            if candidate["sku"] != line["sku"]
        ][:8]
        reason = ", ".join(line["sucres"]) if line["sucres"] else "composition non déclarée"
        selection["a_choisir"].append(
            {
                "terme": line["terme"],
                "quantite": line["quantite"],
                "candidats": others,
                "ecarte": f"{line['nom']} ({reason})",
            }
        )

    for question in selection["a_choisir"]:
        clean, unknown = [], []
        for candidate in question["candidats"]:
            status, sugars = added_sugars(session, candidate)
            candidate["sucre"], candidate["sucres"] = status, sugars
            if status == "sans":
                candidate["reasons"] = candidate.get("reasons", []) + ["sans sucre ajouté"]
                clean.append(candidate)
            elif status == "inconnu":
                candidate["reasons"] = candidate.get("reasons", []) + ["composition non déclarée"]
                unknown.append(candidate)
        # Confirmed first, unverifiable next, sweetened dropped.
        question["candidats"] = clean + unknown
        question["sans_sucre_impossible"] = not clean


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
            if question.get("ecarte"):
                print(f"    écarté, sucre ajouté : {question['ecarte']}")
            if question.get("sans_sucre_impossible"):
                print("    ⚠ aucun candidat ne déclare une composition sans sucre ajouté")
            for candidate in question["candidats"]:
                reasons = ", ".join(candidate["reasons"]) or "-"
                details = " · ".join(
                    part
                    for part in (
                        quantity_meaning(candidate),
                        unit_price(candidate),
                    )
                    if part
                )
                print(
                    f"    {candidate['sku']} | {candidate['name']} | "
                    f"{product_price(candidate)}"
                    f"{' | ' + details if details else ''} | {reasons}"
                )
    if selection.get("boucher"):
        print(f"\nPour le boucher ({len(selection['boucher'])}), hors panier :")
        for line in selection["boucher"]:
            print(
                f"  {line['quantite']} x {line['terme']} "
                f"(réf. Mon Marché : {line['reference']}, {line['prix_indicatif']})"
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
    parser.add_argument(
        "--favoris",
        nargs="?",
        const="À choisir",
        help="put the ambiguous candidates on a mon-marche.fr bookmark list and print its URL",
    )
    parser.add_argument("--json", help="write the selection to this JSON file")
    parser.add_argument("--creneau", help="delivery slot id, if the cart must be created")
    parser.add_argument(
        "--sans-sucre",
        action="store_true",
        help="drop the candidates whose ingredient list declares a sweetener",
    )
    parser.add_argument(
        "--boucher",
        help="write the meat lines, left out of the cart, to this markdown file",
    )
    parser.add_argument(
        "--rayons-boucher",
        default=",".join(BUTCHER_PREFIXES),
        help="sku prefixes bought at the butcher (default: BC, add CH for charcuterie)",
    )
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
        prefixes = tuple(
            prefix.strip().upper() for prefix in args.rayons_boucher.split(",") if prefix.strip()
        )
        selection = build_selection(session, wanted, load_preferences(session), prefixes)
        if args.sans_sucre:
            filter_added_sugar(session, selection, load_preferences(session))

    print_selection(selection)

    if args.boucher and selection.get("boucher"):
        render_butcher_list(selection, args.boucher)
        print(f"\nListe boucher écrite dans {args.boucher}")
    if args.out and selection["a_choisir"]:
        render_choices(selection, args.out)
        print(f"\nPage de choix écrite dans {args.out}")
    if args.favoris and selection["a_choisir"]:
        candidates = [
            candidate
            for question in selection["a_choisir"]
            for candidate in question["candidats"]
        ]
        if not args.execute:
            print(
                f"\nDry run : la liste « {args.favoris} » "
                f"({len(candidates)} produits) n'a pas été créée."
            )
        else:
            print(f"\n{publish_products(session, args.favoris, candidates)}")
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
