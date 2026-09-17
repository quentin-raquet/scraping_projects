# Architecture du site delit.co

Référence technique pour réparer `scripts/delit.py` si le site évolue. Tout a été
relevé dans le source de la page d'accueil (une seule page, tout le JS est inline).

## Firebase

Le site est une single page app Firebase. Le HTML ne contient aucun produit : tout
est chargé depuis Cloud Firestore côté navigateur, via une clé web publique inlinée
dans la page :

```js
const firebaseConfig = { apiKey: "…", authDomain: "…", projectId: "…", … };
```

`get_firebase_config()` relit ce bloc à chaque exécution, donc une rotation de clé
ne casse rien.

Tout passe ensuite par l'API REST Firestore :

```
https://firestore.googleapis.com/v1/projects/<projectId>/databases/(default)/documents/<path>?key=<apiKey>
```

## Documents utilisés

| Chemin | Rôle |
| --- | --- |
| `config/produits` | menu, champ `liste` (tableau de produits) + `updatedAt` |
| `config/horaire` | `ouvert` (bool) et `horaire` (heure limite de commande, ex. "12:00") |
| `config/compteur` | `{date, num}` : numérotation des commandes du jour |
| `config/emailjs` | `pubkey`, `service`, `template` pour l'envoi des mails |
| `commandes/<dateKey>_<num>` | la commande elle-même — **écriture autorisée, lecture interdite** |
| `config/alerte_mail` | filet de sécurité si le bon n'est pas parti par mail |

Le lister d'une collection est refusé ; seuls les accès document par document
fonctionnent.

## Modèle d'un produit

```json
{"id": 53, "nom": "Oeuf dur", "description": "", "categorie": "Entrées",
 "prix": 1.5, "unite": "pièce", "actif": true, "ordre": 1}
```

- `actif: true` = proposé aujourd'hui. Le menu du jour, c'est ce filtre.
- `unite`:
  - `"100g"` — clé historique, **le prix est en € / kg**, commande par pas de 10 g,
    prix de ligne = `prix × grammes / 1000` ;
  - `"pièce"` / `"portion"` — prix de ligne = `prix × quantité`.
- Ordre d'affichage des catégories : `Nos formules`, `Entrées`, `Féculents`,
  `Légumes`, `Viandes`, `Poissons`, `Plats composés`, `Desserts`, `Boissons`.

Les prix de ligne ne sont **pas arrondis** dans le document de commande : le site
stocke le produit brut (`39.95 × 150 / 1000 = 5.9925`) et n'arrondit qu'à
l'affichage. Le total doit être calculé pareil pour correspondre au centime près à
ce que voit le traiteur.

## Parcours de commande du site (`passerCommande()`)

1. `commandesAutorisees()` : `ouvert` et heure de Paris avant `horaire`.
2. `nextNumCommande()` : transaction Firestore sur `config/compteur`, remise à 1
   quand la date change. Le script fait le compare-and-set équivalent sur
   l'`updateTime` du document, avec retry en cas de conflit.
3. Ajout au panier du contenant (obligatoire) puis des options :
   `J'ai ma barquette verre` 0 €, `Achat barquette verre` 9 €,
   `Barquette carton` 0,25 €, `Sac en tissu` 0,25 €, `Couverts` 0,25 €.
4. Écriture de `commandes/<dateKey>_<num>`. Le script ajoute la précondition
   `currentDocument.exists=false` pour ne jamais écraser une commande existante.
5. Envoi de deux mails via EmailJS (`POST https://api.emailjs.com/api/v1.0/email/send`) :
   le bon de commande au traiteur (`linstempsdej@gmail.com`, en dur dans le site)
   et la confirmation au client.
6. Si le mail au traiteur échoue, le site écrit `config/alerte_mail`. Le script
   fait de même.

`dateKey` est la date du jour à Paris, **basculée au lendemain après 17:00**
(fonction `getJourCommande()`).
