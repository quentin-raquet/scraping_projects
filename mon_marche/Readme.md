# Mon Marché — accès au compte client

Client en lecture seule du compte client de [mon-marche.fr](https://www.mon-marche.fr/),
le site de courses en ligne (Keplr) livré à Paris.

## Comment le site s'authentifie

Le site est une app Next.js qui parle à sa propre API JSON, sur le même domaine
(`https://www.mon-marche.fr/api/...`). L'authentification est un simple cookie
de session :

```
POST /api/auth/signin
{"email": "...", "password": "..."}
→ 200 + Set-Cookie: session=...; HttpOnly; Secure; SameSite=Lax; Max-Age=5184000
```

- pas de CSRF token, pas de captcha, pas de 2FA sur ce parcours ;
- la réponse contient déjà le profil (nom, email, téléphone, adresse de
  facturation) ; le champ `accessToken` qu'elle renvoie vaut
  `dummy_and_unused_token_...`, tout passe bien par le cookie ;
- le cookie est valable 60 jours et suffit ensuite pour tous les endpoints du
  compte ;
- l'API refuse le User-Agent par défaut de `requests`, le script en envoie un de
  navigateur.

Les identifiants sont lus dans les variables d'environnement
`EMAIL_MON_MARCHE` et `PASSWORD_MON_MARCHE` — rien n'est stocké dans le dépôt.

## Endpoints utilisés

| Endpoint | Contenu |
| --- | --- |
| `POST /api/auth/signin` | connexion, profil, cookie de session |
| `GET /api/account/addresses` | adresses de livraison (contact, digicodes, GPS) |
| `GET /api/orders/past` | commandes livrées (produits, créneau, total) |
| `GET /api/orders/current` | commandes en cours |
| `GET /api/account/top-products` | produits les plus commandés |
| `GET /api/account/bookmarks` | listes de favoris |
| `GET /api/account/coupons` | coupons disponibles |
| `GET /api/loyalty/user` | solde de points de fidélité |
| `GET /api/search2?text=…&type=PRODUCT` | recherche catalogue |

Autres endpoints repérés dans le bundle JS mais non utilisés ici, car ils
écrivent : `/api/cart*` (panier), `/api/cart/createPaymentIntent`,
`/api/cart/initialOrder` (commande et paiement).

## Points d'attention

- **`type` de `/api/search2` est en majuscules** : `PRODUCT` ou `RECIPE`. Les
  valeurs `products` / `recipes` utilisées côté front comme noms d'onglets sont
  refusées par l'API (400).
- **Tous les montants sont en centimes** : `totalPrice: 24563` = 245,63 €,
  `pricing.sellPrices.perWeightUnit.net: 399` = 3,99 € / kg. Les champs `net`
  sont TTC, les champs `dutyFree` HT.
- L'unité `count` d'un prix veut dire « à la pièce », `kg` un prix au kilo. Le
  prix à afficher est celui dont `main` vaut `true`.
- Les dates (`createdAt`, créneaux de livraison `from` / `to` / `orderUntil`)
  sont des timestamps en millisecondes.
- `GET /api/cart` renvoie un 404 `E_08_0005` « Le panier est introuvable »
  quand le panier est vide : c'est nominal, pas une erreur d'auth.
- Les produits d'une commande passée ne portent que `id`, `name` et `image` :
  ni quantité ni prix ligne. Le détail d'une commande est à rechercher
  ailleurs si besoin.

## Usage

Le script n'a besoin que de `requests` :

```
pip install -r ../requirements.txt
```

```
python scrap.py compte                          # profil, adresse, fidélité, coupons
python scrap.py commandes                       # commandes en cours et passées
python scrap.py favoris --limit 10              # produits les plus commandés
python scrap.py recherche "tomate" --limit 5    # recherche catalogue
python scrap.py recherche "curry" --type RECIPE # recherche recettes
```

`--json fichier.json` ajoute le dump brut de la réponse à n'importe quelle
sous-commande.

## Périmètre

`scrap.py` est **strictement en lecture**. Il ne touche pas au panier et ne passe
aucune commande. Ajouter un parcours de commande (comme `delit/commander.py`)
impliquerait un vrai paiement : à traiter séparément, avec une validation
explicite avant l'envoi.
