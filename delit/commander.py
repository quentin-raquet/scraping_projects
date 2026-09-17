# This script places an order on the DELIT. traiteur website (https://www.delit.co/).
#
# It reproduces what the browser does when the "VALIDER MA COMMANDE" button is
# pressed (function `passerCommande`) :
#   1. check that ordering is still open (config/horaire, cut-off time in Paris),
#   2. reserve an order number on the counter document (config/compteur),
#   3. write the order to the `commandes` collection (this is what shows up in
#      the caterer back-office, in real time),
#   4. send the order slip to the caterer and a confirmation to the customer
#      through EmailJS.
#
# Nothing is sent unless --execute is passed : the default is a dry run.
import argparse
import json
from datetime import datetime, timedelta
from typing import Any, Optional
from zoneinfo import ZoneInfo

import requests

from menus_sportifs import MENUS, index_produits
from scrap import CATEGORIES_ORDER, UNITE_POIDS, get_firebase_config, get_document, get_menu, parse_fields

PARIS = ZoneInfo("Europe/Paris")
FIRESTORE_DOC_URL = "https://firestore.googleapis.com/v1/projects/{project}/databases/(default)/documents/{path}"
EMAILJS_URL = "https://api.emailjs.com/api/v1.0/email/send"
# Recipient of the order slip, hardcoded in the website (const DESTINATAIRES).
DESTINATAIRES = ["linstempsdej@gmail.com"]
# Container choice, mandatory on the website. Name and price are the ones the
# site adds to the cart.
BARQUETTES = {
    "verre": ("J'ai ma barquette verre", 0),
    "achat-verre": ("Achat barquette verre", 9.00),
    "carton": ("Barquette carton", 0.25),
}
EXTRAS = {"sac": ("Sac en tissu", 0.25), "couverts": ("Couverts", 0.25)}

CLIENT = {
    "nom": "Quentin Raquet",
    "societe": "Mirakl",
    "tel": "0638649312",
    "email": "queraq86@gmail.com",
}
HEURE_RETRAIT = "13H30"
MESSAGE_TEMPLATE = "Bonjour, je passerai chercher ma commande à {heure_retrait}. Merci !"


def to_firestore_value(value: Any) -> dict:
    """Convert a Python value into a Firestore typed value.

    Whole numbers are written as integers, like the Firebase JS SDK does.

    Args:
        value (Any): A JSON-compatible Python value.

    Returns:
        dict: The Firestore typed value.
    """
    if value is None:
        return {"nullValue": None}
    if isinstance(value, bool):
        return {"booleanValue": value}
    if isinstance(value, int):
        return {"integerValue": str(value)}
    if isinstance(value, float):
        return {"integerValue": str(int(value))} if value.is_integer() else {"doubleValue": value}
    if isinstance(value, list):
        return {"arrayValue": {"values": [to_firestore_value(item) for item in value]}}
    if isinstance(value, dict):
        return {"mapValue": {"fields": {key: to_firestore_value(item) for key, item in value.items()}}}
    return {"stringValue": str(value)}


def jour_commande(now: Optional[datetime] = None) -> str:
    """Compute the order day key, as the website does.

    After 17:00 Paris time the website targets the next day.

    Args:
        now (datetime, optional): Reference time, current time if None.

    Returns:
        str: The day key, e.g. "2026-09-17".
    """
    now = (now or datetime.now(PARIS)).astimezone(PARIS)
    if now.hour >= 17:
        now = now + timedelta(days=1)
    return now.strftime("%Y-%m-%d")


def commandes_autorisees(horaire: dict, now: Optional[datetime] = None) -> tuple[bool, str]:
    """Check whether orders are currently accepted.

    Args:
        horaire (dict): The `config/horaire` document (`ouvert`, `horaire`).
        now (datetime, optional): Reference time, current time if None.

    Returns:
        tuple: (allowed, reason).
    """
    now = (now or datetime.now(PARIS)).astimezone(PARIS)
    if not horaire.get("ouvert"):
        return False, "le service est marqué fermé"
    limite = horaire.get("horaire", "12:00")
    h_limite, m_limite = (int(part) for part in limite.split(":"))
    if now.hour * 60 + now.minute >= h_limite * 60 + m_limite:
        return False, f"il est {now:%H:%M} à Paris, les commandes ferment à {limite}"
    return True, f"il est {now:%H:%M} à Paris, les commandes ferment à {limite}"


def euros(montant: float) -> str:
    """Format a price the way the website does, e.g. "5,99 €".

    Args:
        montant (float): The price in euros.

    Returns:
        str: The formatted price.
    """
    return f"{montant:.2f} €".replace(".", ",")


def prix_ligne(produit: dict, quantite: float) -> float:
    """Compute the price of a cart line, exactly as the website does.

    Unlike `scrap.prix_portion` the result is not rounded : the website stores
    the raw product and only rounds at display time, so the order total must be
    computed the same way to match the caterer back-office.

    Args:
        produit (dict): A product of the menu.
        quantite (float): Quantity, in grams for products sold by weight.

    Returns:
        float: The price in euros.
    """
    if produit.get("unite") == UNITE_POIDS:
        return produit["prix"] * quantite / 1000
    return produit["prix"] * quantite


def construire_items(
    lignes: list[tuple[str, float]],
    produits: dict,
    barquette: str,
    sac: bool = False,
    couverts: bool = False,
) -> list[dict]:
    """Build the cart items, in the format stored by the website.

    Args:
        lignes (list): List of (product name, quantity) pairs.
        produits (dict): Products of the menu of the day, indexed by name.
        barquette (str): Container choice, one of BARQUETTES.
        sac (bool): Add a cloth bag.
        couverts (bool): Add cutlery.

    Returns:
        list: The cart items.
    """
    if barquette not in BARQUETTES:
        raise Exception(f"Type de barquette inconnu : {barquette}, attendu {list(BARQUETTES)}")
    items = []
    for nom, quantite in lignes:
        produit = produits.get(nom)
        if produit is None:
            raise Exception(f"Produit indisponible aujourd'hui : {nom}")
        au_poids = produit.get("unite") == UNITE_POIDS
        items.append(
            {
                "prodId": produit["id"],
                "nom": produit["nom"],
                "categorie": produit["categorie"],
                "qte": quantite,
                "unite": produit["unite"],
                "label": f"{quantite:g} g" if au_poids else f"{quantite:g} x {produit['unite']}",
                "prix": prix_ligne(produit, quantite),
            }
        )
    options = [BARQUETTES[barquette]]
    if sac:
        options.append(EXTRAS["sac"])
    if couverts:
        options.append(EXTRAS["couverts"])
    for nom, prix in options:
        items.append({"prodId": 0, "nom": nom, "qte": 1, "unite": "pièce", "label": "1 x pièce", "prix": prix})
    return items


def construire_commande(items: list[dict], num: int, client: dict, message: str, barquette: str) -> dict:
    """Build the order document stored in the `commandes` collection.

    Args:
        items (list): The cart items.
        num (int): The order number of the day.
        client (dict): Customer details (nom, societe, tel, email).
        message (str): Free message sent to the caterer.
        barquette (str): Container choice.

    Returns:
        dict: The order document.
    """
    now = datetime.now(PARIS)
    return {
        "num": str(num).zfill(3),
        "date": now.strftime("%d/%m/%Y"),
        "heure": now.strftime("%H:%M"),
        "dateKey": jour_commande(now),
        "nom": client["nom"],
        "societe": client["societe"],
        "tel": client["tel"],
        "email": client["email"],
        "message": message,
        "items": items,
        "total": sum(item["prix"] for item in items),
        "barquette": barquette,
        "newsletter": False,
    }


def reserver_numero(project: str, api_key: str, essais: int = 5) -> int:
    """Reserve the next order number on the counter document.

    The website uses a Firestore transaction, we use the equivalent
    compare-and-set on the document update time, and retry on conflict.

    Args:
        project (str): The Firebase project id.
        api_key (str): The Firebase web API key.
        essais (int): Number of attempts before giving up.

    Returns:
        int: The reserved order number.
    """
    url = FIRESTORE_DOC_URL.format(project=project, path="config/compteur")
    jour = jour_commande()
    for _ in range(essais):
        res = requests.get(url, params={"key": api_key}, timeout=30)
        if res.status_code != 200:
            raise Exception(f"Bad request status code {res.status_code} on config/compteur")
        body = res.json()
        compteur = parse_fields(body.get("fields", {}))
        num = compteur["num"] + 1 if compteur.get("date") == jour else 1
        res = requests.patch(
            url,
            params={"key": api_key, "currentDocument.updateTime": body["updateTime"]},
            json={"fields": {"date": {"stringValue": jour}, "num": {"integerValue": str(num)}}},
            timeout=30,
        )
        if res.status_code == 200:
            return num
        if res.status_code not in (400, 409):
            raise Exception(f"Bad request status code {res.status_code} on config/compteur : {res.text[:200]}")
    raise Exception("Impossible de réserver un numéro de commande, trop de conflits")


def peek_numero(project: str, api_key: str) -> int:
    """Read the order number that would be reserved, without reserving it.

    Args:
        project (str): The Firebase project id.
        api_key (str): The Firebase web API key.

    Returns:
        int: The next order number.
    """
    compteur = get_document(project, api_key, "config/compteur")
    return compteur["num"] + 1 if compteur.get("date") == jour_commande() else 1


def enregistrer_commande(project: str, api_key: str, commande: dict) -> str:
    """Write the order to the `commandes` collection.

    Args:
        project (str): The Firebase project id.
        api_key (str): The Firebase web API key.
        commande (dict): The order document.

    Returns:
        str: The path of the created document.
    """
    path = f"commandes/{commande['dateKey']}_{commande['num']}"
    res = requests.patch(
        FIRESTORE_DOC_URL.format(project=project, path=path),
        params={"key": api_key, "currentDocument.exists": "false"},
        json={"fields": {key: to_firestore_value(value) for key, value in commande.items()}},
        timeout=30,
    )
    if res.status_code != 200:
        raise Exception(f"Bad request status code {res.status_code} on {path} : {res.text[:200]}")
    return path


def _lignes_html(commande: dict, padding: str, border: str) -> str:
    """Build the rows of the order table, grouped by category.

    Args:
        commande (dict): The order document.
        padding (str): CSS padding of the cells.
        border (str): CSS border of the cells.

    Returns:
        str: The HTML rows.
    """
    noms_options = [nom for nom, _ in list(BARQUETTES.values()) + list(EXTRAS.values())]
    plats = [item for item in commande["items"] if item["nom"] not in noms_options]
    extras = [item for item in commande["items"] if item["nom"] in [nom for nom, _ in EXTRAS.values()]]
    categories = list(dict.fromkeys(item.get("categorie", "Autres") for item in plats))
    categories = [cat for cat in CATEGORIES_ORDER if cat in categories] + [
        cat for cat in categories if cat not in CATEGORIES_ORDER
    ]
    entete = (
        '<tr><td colspan="3" style="padding:10px 10px;font-size:15px;font-weight:bold;color:#2D5016;'
        'background:#f0f4eb;border-bottom:2px solid #2D5016">{titre}</td></tr>'
    )
    lignes = ""
    for categorie in categories:
        lignes += entete.format(titre=categorie)
        for item in (item for item in plats if item.get("categorie", "Autres") == categorie):
            lignes += (
                f'<tr><td style="padding:{padding};border-bottom:{border}">{item["label"]}</td>'
                f'<td style="padding:{padding};border-bottom:{border}">{item["nom"]}</td>'
                f'<td style="padding:{padding};border-bottom:{border};text-align:right">'
                f'{euros(item["prix"])}</td></tr>'
            )
    if extras:
        lignes += entete.format(titre="Options")
        for item in extras:
            lignes += (
                f'<tr><td style="padding:{padding};border-bottom:{border}">1 x pièce</td>'
                f'<td style="padding:{padding};border-bottom:{border}">{item["nom"]}</td>'
                f'<td style="padding:{padding};border-bottom:{border};text-align:right">{euros(item["prix"])}</td></tr>'
            )
    return lignes


def generer_bon_html(commande: dict) -> str:
    """Build the order slip sent to the caterer.

    Args:
        commande (dict): The order document.

    Returns:
        str: The HTML order slip.
    """
    contenant = BARQUETTES[commande["barquette"]]
    prix_contenant = euros(contenant[1]) if contenant[1] > 0 else "Gratuit"
    return (
        '<table width="100%" style="border-bottom:2px solid #111;margin-bottom:16px;padding-bottom:12px"><tr>'
        '<td><span style="font-size:22px;font-weight:bold;letter-spacing:3px">DÉLIT.</span><br>'
        '<span style="font-size:11px;color:#555">Traiteur</span></td>'
        '<td style="text-align:right;font-size:11px;color:#555;line-height:1.6">45, rue de Chaillot<br>'
        "75116 Paris<br>01.73.71.27.70</td></tr></table>"
        '<div style="text-align:center;font-size:26px;font-weight:bold;letter-spacing:2px;padding:12px;'
        f'border:2px solid #111;margin-bottom:16px">n° commande : {commande["num"]}</div>'
        '<table width="100%" style="background:#f5f5f5;padding:10px;margin-bottom:14px"><tr>'
        f'<td><span style="font-size:11px;color:#666">Nom / Prénom</span><br><strong>{commande["nom"]}</strong></td>'
        f'<td><span style="font-size:11px;color:#666">Société</span><br><strong>{commande["societe"]}</strong></td>'
        '<td style="text-align:right"><span style="font-size:11px;color:#666">Téléphone</span><br>'
        f'<strong>{commande["tel"]}</strong></td>'
        '<td style="text-align:right"><span style="font-size:11px;color:#666">Date</span><br>'
        f'<strong>{commande["date"]} · {commande["heure"]}</strong></td></tr></table>'
        '<div style="border:2px solid #2D5016;background:#e8f0e0;padding:10px 14px;margin-bottom:16px;font-size:13px">'
        '<span style="font-weight:bold;text-transform:uppercase;letter-spacing:1px;color:#2D5016">Contenant : </span>'
        f"{contenant[0]} — {prix_contenant}</div>"
        '<table width="100%" style="border-collapse:collapse;margin-bottom:16px"><thead><tr>'
        '<th style="background:#111;color:white;padding:7px 10px;text-align:left;width:80px">Qté</th>'
        '<th style="background:#111;color:white;padding:7px 10px;text-align:left">Désignation</th>'
        '<th style="background:#111;color:white;padding:7px 10px;text-align:right;width:80px">Prix</th></tr></thead>'
        f'<tbody><tr><td colspan="3" style="padding:7px 10px;font-weight:bold;background:#f9f9f9">{commande["nom"]}'
        "</td></tr>" + _lignes_html(commande, "7px 10px", "1px solid #ddd") + "</tbody></table>"
        '<table width="100%" style="border-collapse:collapse;margin-bottom:14px">'
        '<tr style="border-top:2px solid #111">'
        '<td colspan="3" style="padding:8px 10px;text-align:right;font-weight:bold;font-size:15px">Total commande :'
        f'</td><td style="padding:8px 10px;font-weight:bold;font-size:15px;text-align:right">{euros(commande["total"])}'
        "</td></tr></table>"
        '<div style="font-weight:bold;font-size:12px;margin-bottom:4px">Message :</div>'
        f'<div style="border:1px solid #ddd;padding:10px;min-height:40px;font-size:13px;color:#555">'
        f'{commande["message"]}</div>'
        '<div style="margin-top:16px;padding-top:10px;border-top:1px solid #ddd;font-size:11px;color:#888;'
        'text-align:center">DÉLIT. Traiteur · 45 rue de Chaillot · 75116 Paris · 01.73.71.27.70</div>'
    )


def generer_confirmation_html(commande: dict) -> str:
    """Build the confirmation email sent to the customer.

    Args:
        commande (dict): The order document.

    Returns:
        str: The HTML confirmation.
    """
    contenant = BARQUETTES[commande["barquette"]]
    prix_contenant = euros(contenant[1]) if contenant[1] > 0 else "Gratuit"
    return (
        '<div style="font-family:Arial,sans-serif;font-size:14px;color:#111;max-width:580px;margin:0 auto;padding:20px">'
        '<div style="background:#2D5016;color:white;text-align:center;padding:24px;margin-bottom:24px">'
        '<div style="font-size:28px;font-weight:bold;letter-spacing:4px">DÉLIT.</div>'
        '<div style="font-size:12px;opacity:0.8;margin-top:4px">TRAITEUR · 45 RUE DE CHAILLOT · PARIS 16E</div></div>'
        f'<p>Bonjour <strong>{commande["nom"]}</strong>,</p>'
        '<p style="color:#2D5016;margin-top:8px">Votre commande a bien été enregistrée. Merci !</p>'
        '<div style="background:#f5f5f5;border-left:4px solid #2D5016;padding:16px;margin:20px 0;font-size:22px;'
        f'font-weight:bold;letter-spacing:2px;text-align:center">N° COMMANDE : {commande["num"]}</div>'
        '<p style="font-size:13px;color:#555">Présentez ce numéro lors du retrait de votre commande.</p>'
        '<div style="border:2px solid #2D5016;background:#e8f0e0;padding:10px 14px;margin:16px 0;font-size:13px">'
        '<span style="font-weight:bold;text-transform:uppercase;letter-spacing:1px;color:#2D5016">Contenant : </span>'
        f"{contenant[0]} — {prix_contenant}</div>"
        '<table width="100%" style="border-collapse:collapse;margin:20px 0"><thead><tr style="background:#2D5016;'
        'color:white"><th style="padding:8px 10px;text-align:left;font-size:12px">Qté</th>'
        '<th style="padding:8px 10px;text-align:left;font-size:12px">Produit</th>'
        '<th style="padding:8px 10px;text-align:right;font-size:12px">Prix</th></tr></thead><tbody>'
        + _lignes_html(commande, "6px 10px", "1px solid #eee")
        + "</tbody></table>"
        '<div style="text-align:right;font-size:16px;font-weight:bold;padding:10px;border-top:2px solid #111">'
        f'Total : {euros(commande["total"])}</div>'
        '<div style="margin-top:24px;padding:16px;background:#e8f0e0;border:1px solid #c5d9b0;font-size:13px">'
        "<strong>DÉLIT. Traiteur</strong><br>45, rue de Chaillot · 75116 Paris<br>"
        "01.73.71.27.70 · linstempsdej@gmail.com</div></div>"
    )


def construire_mails(commande: dict) -> list[dict]:
    """Build the EmailJS payloads sent when an order is placed.

    Args:
        commande (dict): The order document.

    Returns:
        list: One payload per email (caterer slip, customer confirmation).
    """
    sujet = f"Commande n°{commande['num']} du {commande['date']} — {commande['nom']} ({commande['societe']})"
    mails = [
        {"to_email": dest, "subject": sujet, "message_html": generer_bon_html(commande)} for dest in DESTINATAIRES
    ]
    mails.append(
        {
            "to_email": commande["email"],
            "subject": f"Confirmation commande n°{commande['num']} — DÉLIT. Traiteur",
            "message_html": generer_confirmation_html(commande),
        }
    )
    return mails


def envoyer_mails(emailjs: dict, mails: list[dict]) -> list[tuple[str, bool, str]]:
    """Send the order emails through the EmailJS REST API.

    Args:
        emailjs (dict): The `config/emailjs` document (pubkey, service, template).
        mails (list): The payloads built by `construire_mails`.

    Returns:
        list: One (recipient, sent, detail) tuple per email.
    """
    resultats = []
    for mail in mails:
        payload = {
            "service_id": emailjs["service"],
            "template_id": emailjs["template"],
            "user_id": emailjs["pubkey"],
            "template_params": mail,
        }
        try:
            res = requests.post(
                EMAILJS_URL,
                json=payload,
                headers={"Content-Type": "application/json", "Origin": "https://www.delit.co"},
                timeout=30,
            )
            resultats.append((mail["to_email"], res.status_code == 200, f"HTTP {res.status_code} {res.text[:120]}"))
        except requests.RequestException as error:
            resultats.append((mail["to_email"], False, str(error)))
    return resultats


def signaler_echec_mail(project: str, api_key: str, commande: dict) -> None:
    """Flag a failed notification in the caterer back-office.

    Mirrors the website fallback : the order stays visible in the back-office
    but the `config/alerte_mail` document warns that no email went out.

    Args:
        project (str): The Firebase project id.
        api_key (str): The Firebase web API key.
        commande (dict): The order document.
    """
    alerte = {
        "derniere_commande": commande["num"],
        "nom": commande["nom"],
        "date": commande["date"],
        "timestamp": datetime.now(PARIS).isoformat(),
    }
    requests.patch(
        FIRESTORE_DOC_URL.format(project=project, path="config/alerte_mail"),
        params={"key": api_key},
        json={"fields": {key: to_firestore_value(value) for key, value in alerte.items()}},
        timeout=30,
    )


def main() -> None:
    """Prepare an order, and send it only when --execute is passed."""
    parser = argparse.ArgumentParser(description="Passer une commande sur delit.co")
    parser.add_argument("--menu", type=int, default=1, help="numéro du menu sportif à commander")
    parser.add_argument("--barquette", default="carton", choices=list(BARQUETTES), help="type de barquette")
    parser.add_argument("--sac", action="store_true", help="ajouter un sac en tissu (+0,25 €)")
    parser.add_argument("--couverts", action="store_true", help="ajouter des couverts (+0,25 €)")
    parser.add_argument("--heure-retrait", default=HEURE_RETRAIT, help="heure de retrait annoncée au traiteur")
    parser.add_argument("--execute", action="store_true", help="envoyer réellement la commande")
    parser.add_argument("--bon", help="fichier où écrire l'aperçu HTML du bon de commande")
    args = parser.parse_args()

    config = get_firebase_config()
    project, api_key = config["projectId"], config["apiKey"]
    horaire = get_document(project, api_key, "config/horaire")
    ouvert, raison = commandes_autorisees(horaire)
    menu = get_menu(config=config)
    produits = index_produits(menu["produits"])

    titre = list(MENUS)[args.menu - 1]
    message = MESSAGE_TEMPLATE.format(heure_retrait=args.heure_retrait)
    items = construire_items(MENUS[titre], produits, args.barquette, args.sac, args.couverts)
    num = peek_numero(project, api_key)
    commande = construire_commande(items, num, CLIENT, message, args.barquette)

    print(f"{'COMMANDE RÉELLE' if args.execute else 'DRY RUN — aucune donnée envoyée'}\n")
    print(f"Service : {'ouvert' if ouvert else 'FERMÉ'} ({raison})")
    print(f"Menu    : {titre}")
    print(f"Client  : {commande['nom']} · {commande['societe']} · {commande['tel']} · {commande['email']}")
    print(f"Message : {commande['message']}")
    print(f"\nPanier (n° de commande {'réservé' if args.execute else 'prévu'} : {commande['num']}) :")
    for item in commande["items"]:
        print(f"  - {item['label']:>12}  {item['nom']:<45} {item['prix']:6.2f} €")
    print(f"  {'TOTAL':>60} {commande['total']:6.2f} €")
    print(f"\nDocument Firestore : commandes/{commande['dateKey']}_{commande['num']}")
    print(f"Mails : {', '.join(mail['to_email'] for mail in construire_mails(commande))}")

    if args.bon:
        with open(args.bon, "w", encoding="utf-8") as file:
            file.write(generer_bon_html(commande))
        print(f"Aperçu du bon de commande écrit dans {args.bon}")

    if not args.execute:
        print("\nPayload de la commande :")
        print(json.dumps(commande, ensure_ascii=False, indent=2))
        print("\nRelancer avec --execute pour envoyer la commande.")
        return

    if not ouvert:
        raise Exception(f"Commandes fermées : {raison}")
    num = reserver_numero(project, api_key)
    commande = construire_commande(items, num, CLIENT, message, args.barquette)
    path = enregistrer_commande(project, api_key, commande)
    print(f"\nCommande n°{commande['num']} enregistrée ({path})")

    emailjs = get_document(project, api_key, "config/emailjs")
    resultats = envoyer_mails(emailjs, construire_mails(commande))
    for destinataire, envoye, detail in resultats:
        print(f"  mail {destinataire} : {'envoyé' if envoye else 'ÉCHEC — ' + detail}")
    if not any(envoye for _, envoye, _ in resultats[: len(DESTINATAIRES)]):
        signaler_echec_mail(project, api_key, commande)
        print("  bon de commande non envoyé par mail : alerte back-office déposée (config/alerte_mail)")
        print("  la commande reste visible en temps réel dans l'admin du traiteur")


if __name__ == "__main__":
    main()
