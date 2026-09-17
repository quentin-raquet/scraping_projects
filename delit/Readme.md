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
