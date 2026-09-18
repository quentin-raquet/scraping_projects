# Scraping Projects

This workspace contains various projects related to web scraping.

## FFF Clubs Data Processing

Located in the `fff` directory, this project contains scripts for processing and cleaning data related to FFF clubs.

## Mairie Data Processing

Located in the `mairie` directory, this project contains scripts for processing and cleaning data related to french city hall.

## DÉLIT. Traiteur Menu

Located in the `delit` directory, this project contains scripts for scraping the daily menu of the DÉLIT. caterer and building sport-oriented menus out of it.

## Mon Marché

Located in the `mon_marche` directory, this project contains a client of a
mon-marche.fr customer account: account and catalog reading (`scrap.py`),
cart handling (`panier.py`) and a shopping list to cart resolver ranking the
catalog against the purchase history, organic and French labels, and routing
the meat to a separate butcher list (`courses.py`). Both writers are dry run by
default and never pay.

## Claude Skill

The `.claude/skills/delit-commande` skill drives a full ordering flow on delit.co: daily menu, budget and cravings, three menu proposals, contact form, order slip validation and real order. It embeds its own copy of the scraping and ordering code, so it works on its own.

