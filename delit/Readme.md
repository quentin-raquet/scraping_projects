# DÉLIT. Traiteur — Menu du jour

Scraper du menu du jour de [delit.co](https://www.delit.co/), le site de commande
du traiteur DÉLIT. (Paris).

## Comment le menu est récupéré

Le site est une single page app : le HTML ne contient aucun produit, tout est
chargé depuis **Cloud Firestore** côté navigateur. Les informations nécessaires
sont donc :

| Information | Où la trouver |
| --- | --- |
| `apiKey` / `projectId` Firebase | bloc `const firebaseConfig = {...}` inliné dans la page d'accueil |
| Menu du jour | document Firestore `config/produits`, champ `liste` |
| État du service | document Firestore `config/horaire` (`ouvert`, `horaire` = heure limite de commande) |

Ces documents sont lisibles via l'API REST publique de Firestore, avec la clé web
servie par le site :

```
GET https://firestore.googleapis.com/v1/projects/<projectId>/databases/(default)/documents/config/produits?key=<apiKey>
```

`scrap.py` relit la clé sur la page d'accueil à chaque exécution, donc le script
continue de fonctionner si le projet Firebase change de clé.

## Modèle de données d'un produit

```json
{"id": 53, "nom": "Oeuf dur", "description": "", "categorie": "Entrées",
 "prix": 1.5, "unite": "pièce", "actif": true, "ordre": 1}
```

- `actif` : `true` = proposé aujourd'hui. Le menu du jour est la liste des
  produits actifs (`--all` permet de récupérer aussi le catalogue complet).
- `unite` :
  - `"100g"` — clé historique, **le prix est en € / kg** ; la commande se fait au
    poids par pas de 10 g, prix = `prix × grammes / 1000` ;
  - `"pièce"` / `"portion"` — prix = `prix × quantité`.
- `categorie` : `Nos formules`, `Entrées`, `Féculents`, `Légumes`, `Viandes`,
  `Poissons`, `Plats composés`, `Desserts`, `Boissons`.
- `updatedAt` du document `config/produits` donne la date de mise à jour du menu.

## Fichiers

- `scrap.py` : récupère le menu du jour et l'exporte en JSON et CSV.
- `menus_sportifs.py` : compose trois menus sains « sportif » à partir du menu du
  jour et vérifie qu'ils tiennent dans un budget donné (16 € par défaut).
- `commander.py` : passe une commande, en reproduisant ce que fait le navigateur
  quand on valide le panier.

## Usage

1. Installer les dépendances listées dans `requirements.txt` :
```
pip install -r ../requirements.txt
```
2. Récupérer le menu du jour :
```
python scrap.py
python scrap.py --all --json catalogue.json --csv catalogue.csv
```
3. Afficher les menus sportifs chiffrés sur le menu du jour :
```
python menus_sportifs.py
python menus_sportifs.py --budget 20
```

## Passer une commande

`commander.py` reproduit la fonction `passerCommande()` du site :

1. vérification de la fenêtre de commande (`config/horaire`, heure de Paris) ;
2. réservation d'un numéro de commande sur `config/compteur` — le site utilise
   une transaction Firestore, le script fait le compare-and-set équivalent sur
   l'`updateTime` du document, avec retry en cas de conflit ;
3. écriture de la commande dans `commandes/<dateKey>_<num>` — c'est ce document
   qui s'affiche en temps réel dans le back-office du traiteur ;
4. envoi du bon de commande au traiteur et de la confirmation au client via
   l'API REST EmailJS (`config/emailjs`).

Points d'attention :

- **le dry run est le mode par défaut**, rien n'est envoyé sans `--execute` ;
- le choix de barquette est obligatoire (`verre` gratuit, `carton` +0,25 €,
  `achat-verre` +9 €), comme sur le site ;
- les prix des lignes ne sont pas arrondis, exactement comme le site, qui
  n'arrondit qu'à l'affichage : le total doit correspondre au centime près à ce
  que voit le traiteur ;
- l'écriture de la commande utilise la précondition `currentDocument.exists=false`
  pour ne jamais écraser une commande existante ;
- si le mail au traiteur échoue, le script dépose l'alerte `config/alerte_mail`
  comme le fait le site ; la commande reste visible dans le back-office.

```
python commander.py --menu 1 --barquette carton              # dry run
python commander.py --menu 1 --barquette carton --bon bon.html
python commander.py --menu 1 --barquette carton --execute    # commande réelle
```

## Skill Claude

`.claude/skills/delit-commande` pilote le parcours de commande complet en
conversation (menu du jour → budget et envies → 3 propositions → coordonnées →
bon de commande validé → commande). La skill **embarque sa propre copie** du code
(`scripts/delit.py`, un seul fichier sans dépendance à ce dossier ni à pandas)
pour rester autonome : toute correction de la logique de scraping ou de commande
est à reporter des deux côtés.

