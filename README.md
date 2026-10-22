# Projet 3/7 — Pipeline Cloud (AWS S3 + Snowflake)

Pipeline Cloud construit sur les données **NYC TLC Yellow Taxi** : stockage dans un **data lake AWS S3**, ingestion sécurisée dans **Snowflake**, puis transformation **ELT avec dbt** sur **3 mois de données**.

Ce dépôt fait partie d'une **série pédagogique de 7 projets Data Engineering**. Chaque projet commence là où le précédent s'arrête.

## La série

| #   | Projet            | Statut     |
| --- | ----------------- | ---------- |
| 1/7 | Pipeline API      | ✅ [Voir le dépôt](https://github.com/ksilogacademy/data-engineer-01-pipeline-api) |
| 2/7 | Data Warehouse    | ✅ [Voir le dépôt](https://github.com/ksilogacademy/data-engineer-02-data-warehouse) |
| 3/7 | **Pipeline Cloud** | 👉 Tu es ici |
| 4/7 | Airflow           | À venir    |
| 5/7 | Big Data Spark    | À venir    |
| 6/7 | Streaming         | À venir    |
| 7/7 | End-to-End        | À venir    |

## Tu n'as pas fait les Projets 1 et 2 ?

Ce projet s'appuie sur **deux livrables précédents** :

- le **pipeline du Projet 1**, que tu relanceras pour produire **3 mois de CSV nettoyés** : [Projet 1/7 — Pipeline API](https://github.com/ksilogacademy/data-engineer-01-pipeline-api) ;
- le **Data Warehouse du Projet 2** (Snowflake + dbt), que tu feras évoluer vers le cloud : [Projet 2/7 — Data Warehouse](https://github.com/ksilogacademy/data-engineer-02-data-warehouse).

Commence par là si ce n'est pas déjà fait.

## Les consignes

Tout ce qu'il faut savoir pour réaliser le projet (objectif, prérequis, sources, directives, livrables, barème, date limite et soumission) se trouve dans [`docs/consignes.md`](docs/consignes.md).

**Date limite : samedi 31 octobre 2026, 23h59 (heure de Dakar).**

## Licence

[MIT](LICENSE)
