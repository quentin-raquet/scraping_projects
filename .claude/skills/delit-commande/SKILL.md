---
name: delit-commande
description: Consulter le menu du jour de DÉLIT. Traiteur (delit.co), composer des menus selon un budget et des envies exprimées librement, et passer la commande après validation. À utiliser quand l'utilisateur veut voir le menu du jour de DÉLIT, des idées de déjeuner chez ce traiteur, ou commander son repas.
---

# Commander chez DÉLIT. Traiteur

Cette compétence pilote un parcours de commande complet sur https://www.delit.co/ :
menu du jour → budget et envies → 3 propositions → coordonnées → bon de commande
validé → commande réelle.

## Règle absolue

**Ne jamais lancer `commander --confirm` sans que l'utilisateur ait vu le bon de
commande et validé explicitement.** Une commande engage un vrai paiement chez un
vrai traiteur, écrit dans son back-office et déclenche deux e-mails. Il n'y a pas
d'annulation automatique : en cas d'erreur, il faut appeler le 01 73 71 27 70.

Toutes les autres sous-commandes (`menu`, `devis`, `bon`) sont en lecture seule et
peuvent être lancées librement.

## Le script

`scripts/delit.py` embarque tout le nécessaire (scraping, chiffrage, bon HTML,
envoi). Seule dépendance : `requests`.

```bash
python3 scripts/delit.py menu --texte              # menu du jour lisible
python3 scripts/delit.py menu --out menu.json      # menu du jour en JSON
python3 scripts/delit.py devis --panier p.json --budget 16
python3 scripts/delit.py bon --panier p.json --client c.json --out bon.html --budget 16
python3 scripts/delit.py commander --panier p.json --client c.json --confirm
```

Écrire les fichiers de travail (`panier.json`, `client.json`, `bon.html`) dans le
répertoire scratchpad de la session, pas dans le dépôt.

Format du panier :

```json
{"lignes": [{"nom": "Riz Thaï", "qte": 200}], "barquette": "carton", "sac": false, "couverts": false}
```

Format du client :

```json
{"nom": "Prénom Nom", "societe": "…", "tel": "…", "email": "…", "heure_retrait": "13H30"}
```

`nom`, `tel` et `email` sont obligatoires côté site. `message` est optionnel : s'il
est absent, le script génère « Bonjour, je passerai chercher ma commande à
{heure_retrait}. Merci ! ».

## Composer un menu

- Les produits « au poids » sont affichés en **€/kg** et se commandent **par pas de
  10 g**. Les autres sont à la pièce ou à la portion.
- Repères de portions : féculent 150–200 g, légume 100–150 g, viande 120–180 g,
  poisson 100–140 g, entrée/salade 120–150 g.
- Le choix de barquette est **obligatoire** : `verre` (l'utilisateur apporte la
  sienne, gratuit), `carton` (+0,25 €), `achat-verre` (+9 €). Dans le doute,
  proposer `carton`.
- Options facultatives : `sac` (+0,25 €), `couverts` (+0,25 €).
- **Ne jamais calculer les prix à la main** : passer par `devis`, qui chiffre au
  centime près comme le site et vérifie la disponibilité de chaque produit.
- Le budget doit couvrir le total, barquette et options comprises. La barquette
  carton fait souvent basculer un menu de quelques centimes au-dessus : ajuster de
  10 g plutôt que de dépasser.

## Parcours

### 1. Récupérer le menu du jour

Lancer `menu --texte`. Si le service est fermé (`ouvert: false` — après l'heure
limite, 12:00 en général, ou service marqué fermé), le dire tout de suite : le menu
reste consultable mais aucune commande n'est possible. Ne pas enchaîner sur le
parcours de commande dans ce cas, sauf demande explicite.

### 2. Demander budget et envies

Poser la question en texte libre, en une seule fois : budget maximum et envies du
jour (par ex. « sain et sportif », « couscous », « sans viande », « léger »,
« pas de poisson »). Ne pas proposer de liste fermée : les envies sont libres.

### 3. Proposer 3 menus

Composer **3 menus nettement différents** qui respectent le budget et les envies,
en s'appuyant sur le menu du jour réel. Chiffrer chacun avec `devis` avant de les
présenter, puis les afficher avec le détail des lignes et le total.

Si une envie ne peut pas être satisfaite (aucun couscous au menu, par exemple), le
dire franchement et proposer ce qui s'en rapproche le plus, sans faire passer un
substitut pour la demande initiale.

Demander ensuite lequel choisir avec `AskUserQuestion` (une option par menu), en
laissant la possibilité d'ajuster.

### 4. Recueillir les coordonnées

Présenter un formulaire clair, champ par champ, en une seule fois :

```
Nom et prénom  : …
Société        : …
Téléphone      : …
Email          : …
Heure de retrait : …
Message (optionnel) : …
```

Si `~/.delit/client.json` existe, pré-remplir avec son contenu et demander
simplement confirmation plutôt que de tout redemander. Demander aussi le type de
barquette et les options avec `AskUserQuestion` si ce n'est pas déjà tranché.

### 5. Faire valider le bon de commande

Générer le bon avec `bon`, puis le montrer à l'utilisateur avec `SendUserFile`
(`display: "render"`). Rappeler dans le message : le total, le numéro de commande
prévu, l'heure de retrait, et l'heure limite de commande restante.

Demander la validation avec `AskUserQuestion` : envoyer / modifier / annuler.

### 6. Commander

Uniquement après un « oui » explicite : lancer `commander --confirm`.

Puis rendre compte fidèlement de ce que retourne le script :

- le numéro de commande (à présenter au retrait) et le total ;
- le document Firestore créé ;
- l'état de chaque e-mail.

`alerte_deposee: true` signifie que le bon n'est pas parti par mail et qu'une
alerte back-office a été déposée : le dire, en précisant que la commande reste
visible en temps réel dans l'admin du traiteur.

**La collection `commandes` est en lecture interdite** (règles Firestore, seul le
back-office la lit). Une relecture du document renvoie 403 : c'est normal. Ne pas
présenter ce 403 comme un échec, et ne pas prétendre avoir relu la commande — la
confirmation, c'est le HTTP 200 de l'écriture, obtenue avec la précondition
`currentDocument.exists=false` qui garantit une vraie création.

Proposer enfin d'enregistrer les coordonnées dans `~/.delit/client.json` pour la
prochaine fois.

## Si le script casse

Le site peut changer. `references/architecture.md` documente les endpoints
Firestore, le modèle de données et la logique de prix pour réparer le script.
