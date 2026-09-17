#!/usr/bin/env python3
"""Outil de commande pour DÉLIT. Traiteur (https://www.delit.co/).

Le site est une single page app Firebase : le menu du jour et les commandes
vivent dans Cloud Firestore, lisibles et écrivables via l'API REST publique
avec la clé web servie par la page d'accueil.

Sous-commandes :
    menu        récupère le menu du jour (JSON, ou texte lisible avec --texte)
    devis       chiffre un panier et vérifie la disponibilité et le budget
    bon         génère le bon de commande HTML à faire valider par le client
    commander   passe la commande pour de vrai (exige --confirm)

Seule `commander --confirm` écrit quoi que ce soit. Tout le reste est en
lecture seule.

Dépendances : requests (stdlib pour le reste).
"""
import argparse
import json
import re
import sys
from datetime import datetime, timedelta
from typing import Any, Optional
from zoneinfo import ZoneInfo

import requests

SITE_URL = "https://www.delit.co/"
FIRESTORE_DOC_URL = "https://firestore.googleapis.com/v1/projects/{project}/databases/(default)/documents/{path}"
EMAILJS_URL = "https://api.emailjs.com/api/v1.0/email/send"
PARIS = ZoneInfo("Europe/Paris")
# Destinataire du bon de commande, codé en dur dans le site (const DESTINATAIRES).
DESTINATAIRES = ["linstempsdej@gmail.com"]
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
# `unite` = "100g" est une clé historique : le prix qu'elle porte est un prix au kg.
UNITE_POIDS = "100g"
# Choix du contenant, obligatoire sur le site. Nom et prix ajoutés au panier.
BARQUETTES = {
    "verre": ("J'ai ma barquette verre", 0),
    "achat-verre": ("Achat barquette verre", 9.00),
    "carton": ("Barquette carton", 0.25),
}
EXTRAS = {"sac": ("Sac en tissu", 0.25), "couverts": ("Couverts", 0.25)}
MESSAGE_TEMPLATE = "Bonjour, je passerai chercher ma commande à {heure_retrait}. Merci !"


# ============================================================
# FIRESTORE
# ============================================================
def get_firebase_config(site_url: str = SITE_URL) -> dict:
    """Extrait la configuration Firebase inlinée dans la page d'accueil.

    Args:
        site_url (str): URL de la page d'accueil de DÉLIT.

    Returns:
        dict: L'objet firebaseConfig, avec au moins `apiKey` et `projectId`.
    """
    res = requests.get(site_url, timeout=30)
    if res.status_code != 200:
        raise Exception(f"Bad request status code {res.status_code}, url : {site_url}")
    match = re.search(r"const firebaseConfig\s*=\s*\{(.*?)\}\s*;", res.text, re.DOTALL)
    if match is None:
        raise Exception("firebaseConfig introuvable, la structure du site a changé")
    config = dict(re.findall(r'(\w+)\s*:\s*"([^"]*)"', match.group(1)))
    if "apiKey" not in config or "projectId" not in config:
        raise Exception(f"firebaseConfig incomplet : {config}")
    return config


def parse_value(value: dict) -> Any:
    """Convertit une valeur typée Firestore en valeur Python.

    Args:
        value (dict): Une valeur Firestore, par ex. {"integerValue": "15"}.

    Returns:
        Any: La valeur Python correspondante.
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
    """Convertit l'objet `fields` d'un document Firestore en dict Python.

    Args:
        fields (dict): L'objet `fields` d'un document Firestore.

    Returns:
        dict: Le document en dict Python.
    """
    return {key: parse_value(value) for key, value in fields.items()}


def to_firestore_value(value: Any) -> dict:
    """Convertit une valeur Python en valeur typée Firestore.

    Les nombres entiers sont écrits en integerValue, comme le fait le SDK JS.

    Args:
        value (Any): Une valeur compatible JSON.

    Returns:
        dict: La valeur typée Firestore.
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


def get_document(project: str, api_key: str, path: str) -> dict:
    """Lit un document via l'API REST Firestore.

    Args:
        project (str): L'id du projet Firebase.
        api_key (str): La clé web Firebase.
        path (str): Chemin du document, par ex. "config/produits".

    Returns:
        dict: Le contenu du document en dict Python.
    """
    res = requests.get(FIRESTORE_DOC_URL.format(project=project, path=path), params={"key": api_key}, timeout=30)
    if res.status_code != 200:
        raise Exception(f"Bad request status code {res.status_code}, path : {path}")
    return parse_fields(res.json().get("fields", {}))


# ============================================================
# MENU DU JOUR
# ============================================================
def get_menu(config: Optional[dict] = None, actifs_only: bool = True) -> dict:
    """Récupère le menu du jour.

    Args:
        config (dict, optional): Config Firebase, lue sur le site si None.
        actifs_only (bool): Ne garder que les produits proposés aujourd'hui.

    Returns:
        dict: Le menu, l'état du service et la date de mise à jour.
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
    ouvert, raison = commandes_autorisees(horaire_doc)
    return {
        "produits": produits,
        "ouvert": ouvert,
        "raison": raison,
        "horaire_limite": horaire_doc.get("horaire"),
        "menu_updated_at": produits_doc.get("updatedAt"),
    }


def index_produits(produits: list[dict]) -> dict:
    """Indexe les produits du menu par nom.

    Args:
        produits (list): Liste des produits du menu.

    Returns:
        dict: Les produits indexés par nom.
    """
    return {produit["nom"]: produit for produit in produits}


def jour_commande(now: Optional[datetime] = None) -> str:
    """Calcule la clé du jour de commande, comme le site.

    Après 17:00 heure de Paris, le site vise le lendemain.

    Args:
        now (datetime, optional): Heure de référence, maintenant si None.

    Returns:
        str: La clé du jour, par ex. "2026-09-17".
    """
    now = (now or datetime.now(PARIS)).astimezone(PARIS)
    if now.hour >= 17:
        now = now + timedelta(days=1)
    return now.strftime("%Y-%m-%d")


def commandes_autorisees(horaire: dict, now: Optional[datetime] = None) -> tuple[bool, str]:
    """Indique si les commandes sont acceptées en ce moment.

    Args:
        horaire (dict): Le document `config/horaire` (`ouvert`, `horaire`).
        now (datetime, optional): Heure de référence, maintenant si None.

    Returns:
        tuple: (autorisé, raison).
    """
    now = (now or datetime.now(PARIS)).astimezone(PARIS)
    if not horaire.get("ouvert"):
        return False, "le service est marqué fermé"
    limite = horaire.get("horaire", "12:00")
    h_limite, m_limite = (int(part) for part in limite.split(":"))
    if now.hour * 60 + now.minute >= h_limite * 60 + m_limite:
        return False, f"il est {now:%H:%M} à Paris, les commandes ferment à {limite}"
    return True, f"il est {now:%H:%M} à Paris, les commandes ferment à {limite}"


# ============================================================
# PANIER
# ============================================================
def euros(montant: float) -> str:
    """Formate un prix comme le site, par ex. "5,99 €".

    Args:
        montant (float): Le prix en euros.

    Returns:
        str: Le prix formaté.
    """
    return f"{montant:.2f} €".replace(".", ",")


def prix_ligne(produit: dict, quantite: float) -> float:
    """Calcule le prix d'une ligne de panier, exactement comme le site.

    Le résultat n'est pas arrondi : le site stocke le produit brut et n'arrondit
    qu'à l'affichage, le total doit être calculé pareil pour correspondre au
    back-office du traiteur.

    Args:
        produit (dict): Un produit du menu.
        quantite (float): Quantité, en grammes pour les produits au poids.

    Returns:
        float: Le prix en euros.
    """
    if produit.get("unite") == UNITE_POIDS:
        return produit["prix"] * quantite / 1000
    return produit["prix"] * quantite


def construire_items(panier: dict, produits: dict) -> list[dict]:
    """Construit les lignes du panier, au format stocké par le site.

    Args:
        panier (dict): {"lignes": [{"nom", "qte"}], "barquette", "sac", "couverts"}.
        produits (dict): Les produits du menu du jour, indexés par nom.

    Returns:
        list: Les lignes du panier.
    """
    barquette = panier.get("barquette")
    if barquette not in BARQUETTES:
        raise Exception(f"Type de barquette inconnu : {barquette}, attendu {list(BARQUETTES)}")
    items = []
    for ligne in panier["lignes"]:
        produit = produits.get(ligne["nom"])
        if produit is None:
            raise Exception(f"Produit indisponible aujourd'hui : {ligne['nom']}")
        quantite = ligne["qte"]
        au_poids = produit.get("unite") == UNITE_POIDS
        if au_poids and quantite % 10:
            raise Exception(f"{produit['nom']} : la quantité se commande par pas de 10 g ({quantite} g demandés)")
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
    if panier.get("sac"):
        options.append(EXTRAS["sac"])
    if panier.get("couverts"):
        options.append(EXTRAS["couverts"])
    for nom, prix in options:
        items.append({"prodId": 0, "nom": nom, "qte": 1, "unite": "pièce", "label": "1 x pièce", "prix": prix})
    return items


def construire_commande(items: list[dict], num: int, client: dict, barquette: str) -> dict:
    """Construit le document de commande stocké dans la collection `commandes`.

    Args:
        items (list): Les lignes du panier.
        num (int): Le numéro de commande du jour.
        client (dict): Coordonnées (nom, societe, tel, email, heure_retrait, message).
        barquette (str): Le choix de contenant.

    Returns:
        dict: Le document de commande.
    """
    now = datetime.now(PARIS)
    message = client.get("message") or MESSAGE_TEMPLATE.format(heure_retrait=client.get("heure_retrait", ""))
    return {
        "num": str(num).zfill(3),
        "date": now.strftime("%d/%m/%Y"),
        "heure": now.strftime("%H:%M"),
        "dateKey": jour_commande(now),
        "nom": client["nom"],
        "societe": client.get("societe", ""),
        "tel": client["tel"],
        "email": client["email"],
        "message": message,
        "items": items,
        "total": sum(item["prix"] for item in items),
        "barquette": barquette,
        "newsletter": False,
    }


# ============================================================
# BON DE COMMANDE
# ============================================================
def _lignes_html(commande: dict, padding: str, border: str) -> str:
    """Construit les lignes du tableau de commande, groupées par catégorie.

    Args:
        commande (dict): Le document de commande.
        padding (str): Padding CSS des cellules.
        border (str): Bordure CSS des cellules.

    Returns:
        str: Les lignes HTML.
    """
    noms_extras = [nom for nom, _ in EXTRAS.values()]
    noms_options = [nom for nom, _ in BARQUETTES.values()] + noms_extras
    plats = [item for item in commande["items"] if item["nom"] not in noms_options]
    extras = [item for item in commande["items"] if item["nom"] in noms_extras]
    categories = list(dict.fromkeys(item.get("categorie", "Autres") for item in plats))
    categories = [cat for cat in CATEGORIES_ORDER if cat in categories] + [
        cat for cat in categories if cat not in CATEGORIES_ORDER
    ]
    entete = (
        '<tr><td colspan="3" style="padding:10px 10px;font-size:15px;font-weight:bold;color:#2D5016;'
        'background:#f0f4eb;border-bottom:2px solid #2D5016">{titre}</td></tr>'
    )
    cellule = f'style="padding:{padding};border-bottom:{border}"'
    lignes = ""
    for categorie in categories:
        lignes += entete.format(titre=categorie)
        for item in (item for item in plats if item.get("categorie", "Autres") == categorie):
            lignes += (
                f"<tr><td {cellule}>{item['label']}</td><td {cellule}>{item['nom']}</td>"
                f'<td style="padding:{padding};border-bottom:{border};text-align:right">{euros(item["prix"])}</td></tr>'
            )
    if extras:
        lignes += entete.format(titre="Options")
        for item in extras:
            lignes += (
                f"<tr><td {cellule}>1 x pièce</td><td {cellule}>{item['nom']}</td>"
                f'<td style="padding:{padding};border-bottom:{border};text-align:right">{euros(item["prix"])}</td></tr>'
            )
    return lignes


def generer_bon_html(commande: dict) -> str:
    """Construit le bon de commande envoyé au traiteur.

    Args:
        commande (dict): Le document de commande.

    Returns:
        str: Le bon de commande HTML.
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
        f'</td><td style="padding:8px 10px;font-weight:bold;font-size:15px;text-align:right">'
        f'{euros(commande["total"])}</td></tr></table>'
        '<div style="font-weight:bold;font-size:12px;margin-bottom:4px">Message :</div>'
        f'<div style="border:1px solid #ddd;padding:10px;min-height:40px;font-size:13px;color:#555">'
        f'{commande["message"]}</div>'
        '<div style="margin-top:16px;padding-top:10px;border-top:1px solid #ddd;font-size:11px;color:#888;'
        'text-align:center">DÉLIT. Traiteur · 45 rue de Chaillot · 75116 Paris · 01.73.71.27.70</div>'
    )


def generer_confirmation_html(commande: dict) -> str:
    """Construit le mail de confirmation envoyé au client.

    Args:
        commande (dict): Le document de commande.

    Returns:
        str: La confirmation HTML.
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


def page_bon_html(commande: dict, budget: Optional[float] = None) -> str:
    """Emballe le bon de commande dans une page autonome pour la validation.

    Args:
        commande (dict): Le document de commande.
        budget (float, optional): Budget annoncé, rappelé en bas de page.

    Returns:
        str: Une page HTML complète.
    """
    rappel = ""
    if budget is not None:
        etat = "dans le budget" if commande["total"] <= budget else "AU-DESSUS DU BUDGET"
        rappel = (
            f'<p style="font-family:Arial,sans-serif;font-size:13px;color:#555;max-width:580px;margin:8px auto">'
            f"Budget annoncé : {euros(budget)} — total {euros(commande['total'])}, {etat}.</p>"
        )
    return (
        '<!DOCTYPE html><html lang="fr"><head><meta charset="utf-8">'
        f'<title>Bon de commande DÉLIT. n°{commande["num"]}</title></head>'
        '<body style="background:#f7f2e8;padding:24px">'
        '<div style="max-width:580px;margin:0 auto;background:white;padding:24px;font-family:Arial,sans-serif">'
        + generer_bon_html(commande)
        + "</div>"
        + rappel
        + '<p style="font-family:Arial,sans-serif;font-size:12px;color:#888;text-align:center;margin-top:16px">'
        "Aperçu — la commande n'est pas encore envoyée.</p></body></html>"
    )


# ============================================================
# ENVOI DE LA COMMANDE
# ============================================================
def peek_numero(project: str, api_key: str) -> int:
    """Lit le numéro de commande qui serait réservé, sans le réserver.

    Args:
        project (str): L'id du projet Firebase.
        api_key (str): La clé web Firebase.

    Returns:
        int: Le prochain numéro de commande.
    """
    compteur = get_document(project, api_key, "config/compteur")
    return compteur["num"] + 1 if compteur.get("date") == jour_commande() else 1


def reserver_numero(project: str, api_key: str, essais: int = 5) -> int:
    """Réserve le prochain numéro de commande sur le document compteur.

    Le site utilise une transaction Firestore, on fait le compare-and-set
    équivalent sur l'`updateTime` du document, avec retry en cas de conflit.

    Args:
        project (str): L'id du projet Firebase.
        api_key (str): La clé web Firebase.
        essais (int): Nombre de tentatives avant abandon.

    Returns:
        int: Le numéro de commande réservé.
    """
    url = FIRESTORE_DOC_URL.format(project=project, path="config/compteur")
    jour = jour_commande()
    for _ in range(essais):
        res = requests.get(url, params={"key": api_key}, timeout=30)
        if res.status_code != 200:
            raise Exception(f"Bad request status code {res.status_code} sur config/compteur")
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
            raise Exception(f"Bad request status code {res.status_code} sur config/compteur : {res.text[:200]}")
    raise Exception("Impossible de réserver un numéro de commande, trop de conflits")


def enregistrer_commande(project: str, api_key: str, commande: dict) -> str:
    """Écrit la commande dans la collection `commandes`.

    Args:
        project (str): L'id du projet Firebase.
        api_key (str): La clé web Firebase.
        commande (dict): Le document de commande.

    Returns:
        str: Le chemin du document créé.
    """
    path = f"commandes/{commande['dateKey']}_{commande['num']}"
    res = requests.patch(
        FIRESTORE_DOC_URL.format(project=project, path=path),
        params={"key": api_key, "currentDocument.exists": "false"},
        json={"fields": {key: to_firestore_value(value) for key, value in commande.items()}},
        timeout=30,
    )
    if res.status_code != 200:
        raise Exception(f"Bad request status code {res.status_code} sur {path} : {res.text[:200]}")
    return path


def construire_mails(commande: dict) -> list[dict]:
    """Construit les mails envoyés au moment de la commande.

    Args:
        commande (dict): Le document de commande.

    Returns:
        list: Un payload par mail (bon traiteur, confirmation client).
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


def envoyer_mails(emailjs: dict, mails: list[dict]) -> list[dict]:
    """Envoie les mails de commande via l'API REST EmailJS.

    Args:
        emailjs (dict): Le document `config/emailjs` (pubkey, service, template).
        mails (list): Les payloads construits par `construire_mails`.

    Returns:
        list: Un résultat par mail.
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
            resultats.append(
                {"destinataire": mail["to_email"], "envoye": res.status_code == 200, "detail": f"HTTP {res.status_code} {res.text[:120]}"}
            )
        except requests.RequestException as error:
            resultats.append({"destinataire": mail["to_email"], "envoye": False, "detail": str(error)})
    return resultats


def signaler_echec_mail(project: str, api_key: str, commande: dict) -> None:
    """Dépose une alerte back-office si le bon n'est pas parti par mail.

    Reprend le filet de sécurité du site : la commande reste visible dans le
    back-office, le document `config/alerte_mail` signale l'absence de mail.

    Args:
        project (str): L'id du projet Firebase.
        api_key (str): La clé web Firebase.
        commande (dict): Le document de commande.
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


# ============================================================
# CLI
# ============================================================
def charger(chemin: str) -> dict:
    """Charge un fichier JSON.

    Args:
        chemin (str): Le chemin du fichier.

    Returns:
        dict: Le contenu du fichier.
    """
    with open(chemin, encoding="utf-8") as file:
        return json.load(file)


def menu_texte(menu: dict) -> str:
    """Formate le menu du jour de façon lisible.

    Args:
        menu (dict): Le menu retourné par `get_menu`.

    Returns:
        str: Le menu en texte.
    """
    etat = "ouvert" if menu["ouvert"] else "FERMÉ"
    lignes = [f"Menu du jour (maj {menu['menu_updated_at']}) — service {etat} : {menu['raison']}", ""]
    categorie = None
    for produit in menu["produits"]:
        if produit["categorie"] != categorie:
            categorie = produit["categorie"]
            lignes.append(f"## {categorie}")
        au_poids = produit["unite"] == UNITE_POIDS
        prix = f"{euros(produit['prix'])}/kg" if au_poids else f"{euros(produit['prix'])}/{produit['unite']}"
        description = f" — {produit['description']}" if produit.get("description") else ""
        lignes.append(f"  - {produit['nom']} : {prix}{description}")
    return "\n".join(lignes)


def cmd_menu(args: argparse.Namespace) -> None:
    """Récupère le menu du jour."""
    menu = get_menu(actifs_only=not args.all)
    sortie = menu_texte(menu) if args.texte else json.dumps(menu, ensure_ascii=False, indent=2)
    if args.out:
        with open(args.out, "w", encoding="utf-8") as file:
            file.write(sortie)
        print(f"Menu écrit dans {args.out} ({len(menu['produits'])} produits)")
    else:
        print(sortie)


def cmd_devis(args: argparse.Namespace) -> None:
    """Chiffre un panier et vérifie la disponibilité et le budget."""
    menu = get_menu()
    items = construire_items(charger(args.panier), index_produits(menu["produits"]))
    total = sum(item["prix"] for item in items)
    devis = {
        "ouvert": menu["ouvert"],
        "raison": menu["raison"],
        "lignes": [{"label": item["label"], "nom": item["nom"], "prix": round(item["prix"], 2)} for item in items],
        "total": round(total, 2),
    }
    if args.budget is not None:
        devis["budget"] = args.budget
        devis["dans_le_budget"] = total <= args.budget
    print(json.dumps(devis, ensure_ascii=False, indent=2))


def cmd_bon(args: argparse.Namespace) -> None:
    """Génère le bon de commande HTML à faire valider."""
    config = get_firebase_config()
    menu = get_menu(config=config)
    panier = charger(args.panier)
    items = construire_items(panier, index_produits(menu["produits"]))
    num = peek_numero(config["projectId"], config["apiKey"])
    commande = construire_commande(items, num, charger(args.client), panier["barquette"])
    with open(args.out, "w", encoding="utf-8") as file:
        file.write(page_bon_html(commande, args.budget))
    print(f"Bon de commande écrit dans {args.out}")
    print(f"Numéro prévu : {commande['num']} — total {euros(commande['total'])}")
    print(f"Service : {'ouvert' if menu['ouvert'] else 'FERMÉ'} ({menu['raison']})")


def cmd_commander(args: argparse.Namespace) -> None:
    """Passe la commande pour de vrai, après validation explicite du client."""
    if not args.confirm:
        raise SystemExit("Refus : --confirm est obligatoire, et n'est posé qu'après validation explicite du client.")
    config = get_firebase_config()
    project, api_key = config["projectId"], config["apiKey"]
    menu = get_menu(config=config)
    if not menu["ouvert"]:
        raise SystemExit(f"Commandes fermées : {menu['raison']}")
    panier = charger(args.panier)
    items = construire_items(panier, index_produits(menu["produits"]))
    num = reserver_numero(project, api_key)
    commande = construire_commande(items, num, charger(args.client), panier["barquette"])
    path = enregistrer_commande(project, api_key, commande)
    resultats = envoyer_mails(get_document(project, api_key, "config/emailjs"), construire_mails(commande))
    bon_traiteur = resultats[: len(DESTINATAIRES)]
    if not any(resultat["envoye"] for resultat in bon_traiteur):
        signaler_echec_mail(project, api_key, commande)
    print(
        json.dumps(
            {
                "num": commande["num"],
                "document": path,
                "total": round(commande["total"], 2),
                "mails": resultats,
                "alerte_deposee": not any(resultat["envoye"] for resultat in bon_traiteur),
            },
            ensure_ascii=False,
            indent=2,
        )
    )


def main() -> None:
    """Point d'entrée de la ligne de commande."""
    parser = argparse.ArgumentParser(description="Menu du jour et commande chez DÉLIT. Traiteur")
    sub = parser.add_subparsers(dest="commande", required=True)

    p_menu = sub.add_parser("menu", help="récupérer le menu du jour")
    p_menu.add_argument("--texte", action="store_true", help="sortie lisible plutôt que JSON")
    p_menu.add_argument("--all", action="store_true", help="inclure les produits indisponibles")
    p_menu.add_argument("--out", help="fichier de sortie")
    p_menu.set_defaults(func=cmd_menu)

    p_devis = sub.add_parser("devis", help="chiffrer un panier")
    p_devis.add_argument("--panier", required=True, help="fichier JSON du panier")
    p_devis.add_argument("--budget", type=float, help="budget annoncé par le client")
    p_devis.set_defaults(func=cmd_devis)

    p_bon = sub.add_parser("bon", help="générer le bon de commande HTML")
    p_bon.add_argument("--panier", required=True, help="fichier JSON du panier")
    p_bon.add_argument("--client", required=True, help="fichier JSON des coordonnées client")
    p_bon.add_argument("--out", required=True, help="fichier HTML de sortie")
    p_bon.add_argument("--budget", type=float, help="budget annoncé par le client")
    p_bon.set_defaults(func=cmd_bon)

    p_cmd = sub.add_parser("commander", help="passer la commande pour de vrai")
    p_cmd.add_argument("--panier", required=True, help="fichier JSON du panier")
    p_cmd.add_argument("--client", required=True, help="fichier JSON des coordonnées client")
    p_cmd.add_argument("--confirm", action="store_true", help="confirmation explicite du client")
    p_cmd.set_defaults(func=cmd_commander)

    args = parser.parse_args()
    try:
        args.func(args)
    except Exception as error:
        print(f"Erreur : {error}", file=sys.stderr)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
