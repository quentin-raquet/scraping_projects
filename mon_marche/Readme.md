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

Côté panier (`panier.py`) :

| Endpoint | Rôle |
| --- | --- |
| `POST /api/addresses/deliverySlots2` | créneaux de livraison pour une adresse |
| `PATCH /api/cart/delivery2` | choisit l'adresse et le créneau — **crée le panier** |
| `GET /api/cart` | contenu du panier |
| `PATCH /api/cart/product` | fixe la quantité d'un produit |
| `PATCH /api/cart/products` | fixe les quantités de plusieurs produits d'un coup |
| `DELETE /api/cart` | vide le panier |

Endpoints de paiement volontairement laissés de côté :
`PUT /api/cart/createPaymentIntent`, `PATCH /api/cart/initialOrder`,
`PATCH /api/cart/finalizePrepay`.

## Le panier

Le panier est créé **paresseusement** : tant qu'aucun créneau de livraison n'a
été choisi, tous les endpoints panier répondent 404 `E_08_0005`
« Le panier est introuvable ». La séquence est donc :

1. `POST /api/addresses/deliverySlots2` avec `{postalCode, countryCode, location}`
   de l'adresse → les zones et leurs créneaux ;
2. `PATCH /api/cart/delivery2` avec
   `{"delivery": {"note": …, "address": {"formattedAddress", "location", "addressComponents"}}, "timeSlot": <le créneau entier>}`
   → **crée le panier** et renvoie son contenu ;
3. `PATCH /api/cart/product` avec `{"product": {"id": <canonicalId>, "quantity": n}}`.

Points d'attention :

- **l'id attendu est le `canonicalId`**, c'est-à-dire la partie avant le `$` d'un
  id de catalogue (`QH9QWo2sF$rDyRbLxRPFibN8zcdLJN6` → `QH9QWo2sF`). Le front
  fait exactement ce `split("$")[0]` avant d'appeler l'API ;
- **la quantité est fixée, pas incrémentée** : envoyer `quantity: 3` sur une
  ligne à 1 donne 3, pas 4. `quantity: 0` retire la ligne ;
- la quantité est un nombre d'articles dans l'unité `granularity` du produit
  (pièces, bocaux…). Pour un produit vendu au poids, le prix suit le poids :
  2 citrons de 160 g à 3,99 € / kg = 1,28 € ;
- la recherche catalogue **n'indexe pas le SKU** : chercher `FL2846` ne renvoie
  rien, il faut chercher par nom puis lire le `canonicalId` ;
- `DELETE /api/cart` vide les produits mais **conserve le panier et son
  créneau** ;
- dans le panier, la quantité d'une ligne est dans `quotation.count` et son
  total dans `quotation2.totals.net` ; les totaux du panier sont dans
  `price.quotation` (`net`, `shipping`, `preparationFee`, `preauthorization`).

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

### Panier

```
python panier.py voir                                  # contenu du panier
python panier.py creneaux                              # créneaux de livraison
python panier.py ajouter citron jaune --quantite 3     # dry run
python panier.py ajouter citron jaune --quantite 3 --execute
python panier.py ajouter "pesto genovese" --id 3F3d7dC1T --execute
python panier.py retirer "pesto genovese" --execute
python panier.py vider --execute
```

- **le dry run est le mode par défaut**, rien n'est écrit sans `--execute` ;
- si aucun panier n'existe, `ajouter --execute` en crée un sur la première
  adresse du compte et le premier créneau libre ; `--creneau <id>` (pris dans
  `panier.py creneaux`) permet d'en choisir un autre ;
- `--id` court-circuite la recherche catalogue quand on connaît le
  `canonicalId`, utile si la recherche par nom est ambiguë.

## Périmètre

`scrap.py` est strictement en lecture. `panier.py` écrit dans le panier —
quantités, créneau de livraison — mais **ne paie rien et ne passe aucune
commande** : les endpoints de paiement ne sont pas appelés. Ajouter un parcours
de commande (comme `delit/commander.py`) impliquerait un vrai paiement : à
traiter séparément, avec une validation explicite avant l'envoi.
