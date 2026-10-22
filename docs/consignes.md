# Projet 3 — Pipeline Cloud : Consignes de clôture : 31 octobre 2026 à 23h59

## Contexte et objectif

Le Projet 3 commence exactement là où le Projet 2 s'arrête. Ton Data Warehouse fonctionne, mais il a deux limites : tout vit **sur ta machine** (le CSV est chargé avec `PUT` depuis ton disque), et il ne traite qu'**un seul mois**. Une vraie plateforme de données ne marche pas comme ça : les fichiers arrivent dans un **stockage cloud**, le Data Warehouse vient les chercher de façon **sécurisée**, et le volume augmente sans que l'architecture s'effondre.

Dans ce projet, tu relances ton pipeline du Projet 1 sur **trois mois**, tu déposes les fichiers dans un **data lake AWS S3**, tu les ingères dans **Snowflake** sans jamais écrire de clé AWS en clair, puis tu fais évoluer ton projet **dbt** pour qu'il traite plusieurs mois sans tout recalculer.

L'objectif n'est pas de « mettre un fichier sur S3 ». C'est de savoir répondre à une question plus importante, et de pouvoir le prouver :

> **Comment passer d'un pipeline qui tourne sur ma machine à une architecture cloud qui ingère plusieurs mois de données de façon sécurisée, rejouable et à coût maîtrisé ?**

L'architecture attendue :

```text
Pipeline du Projet 1 (relancé pour 3 mois)
          │
          ▼
  CSV nettoyés : janvier, février, mars 2023
          │   conversion Parquet + upload (script)
          ▼
      AWS S3  (data lake : bucket privé, chemins par année/mois)
          │   Storage Integration + rôle IAM (aucune clé en dur)
          ▼
      SNOWFLAKE
          │
          ▼
      ┌─────────┐
      │ BRONZE  │  Données telles que reçues, avec leur fichier d'origine
      └────┬────┘
           │ dbt (modèles incrémentaux)
           ▼
      ┌─────────┐
      │ SILVER  │  Données préparées pour l'analyse
      └────┬────┘
           │ dbt
           ▼
      ┌─────────┐
      │  GOLD   │  Modèles analytiques, désormais multi-mois
      └─────────┘
```

- **Dataset** : le même que les Projets 1 et 2 — [NYC TLC Trip Record Data — Yellow Taxi (2023)](https://data.cityofnewyork.us/Transportation/2023-Yellow-Taxi-Trip-Data/4b4i-vvec)
- **Dictionnaire des champs** : [TLC Trip Record Data Dictionary (PDF)](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf)
- **Projets précédents** : [Projet 1 — Pipeline API](https://github.com/ksilogacademy/data-engineer-01-pipeline-api) · [Projet 2 — Data Warehouse](https://github.com/ksilogacademy/data-engineer-02-data-warehouse)

---

## 1. Prérequis

Avant de commencer, assure-toi d'avoir en main :

- **Les Projets 1 et 2 terminés** : le pipeline du Projet 1 fonctionnel (tu vas le relancer deux fois de plus) et un projet dbt/Snowflake du Projet 2 qui s'exécute.
- **Un compte AWS** avec les droits pour créer un bucket S3, une policy IAM et un rôle IAM. Vérifie les conditions du *free tier* en vigueur **avant** de commencer, et configure une alerte de budget dès le premier jour (voir « Maîtrise des coûts »).
- **Un compte Snowflake** fonctionnel, de préférence hébergé sur **AWS** et dans une **région proche de celle de ton bucket**. Vérifie que ton essai gratuit est encore actif : si ce n'est pas le cas, tu dois le savoir avant de te lancer, pas au milieu du projet.
- **L'AWS CLI** configurée sur ta machine, et **Python** avec `boto3` (ou l'AWS CLI seule), `pandas` et `pyarrow`.
- **dbt Core** et l'adaptateur `dbt-snowflake`, comme au Projet 2.
- **Des bases Git/GitHub** : tu vas versionner ton travail proprement, en excluant tout ce qui ne doit jamais être public.

Tu dois aussi comprendre, avant de commencer ou en cours de projet, les notions suivantes : stockage objet, bucket, préfixe, région, IAM (utilisateur, rôle, policy), principe du moindre privilège, relation d'approbation (*trust policy*), stage interne vs stage externe, Storage Integration, métadonnées de chargement, format colonnaire (Parquet), partitionnement, modèle incrémental, idempotence.

### Sources et documentation

Commence par la documentation officielle plutôt que par des tutoriels trouvés au hasard :

**AWS**
- **Amazon S3** — guide utilisateur : [docs.aws.amazon.com — S3 User Guide](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html)
- **Amazon S3** — bloquer l'accès public : [Block Public Access](https://docs.aws.amazon.com/AmazonS3/latest/userguide/access-control-block-public-access.html)
- **IAM** — introduction : [What is IAM?](https://docs.aws.amazon.com/IAM/latest/UserGuide/introduction.html)
- **IAM** — bonnes pratiques de sécurité : [Security best practices in IAM](https://docs.aws.amazon.com/IAM/latest/UserGuide/best-practices.html)
- **AWS CLI** — guide : [AWS CLI User Guide](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-welcome.html)
- **boto3** — documentation : [boto3 docs](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html)
- **AWS Budgets** — suivre ses coûts : [Managing your costs with AWS Budgets](https://docs.aws.amazon.com/cost-management/latest/userguide/budgets-managing-costs.html)

**Snowflake**
- **Snowflake** — chargement depuis Amazon S3 : [Bulk loading from Amazon S3](https://docs.snowflake.com/en/user-guide/data-load-s3)
- **Snowflake** — accès S3 par Storage Integration : [Configuring a Snowflake storage integration to access Amazon S3](https://docs.snowflake.com/en/user-guide/data-load-s3-config-storage-integration)
- **Snowflake** — commande : [CREATE STORAGE INTEGRATION](https://docs.snowflake.com/en/sql-reference/sql/create-storage-integration)
- **Snowflake** — formats de fichiers : [CREATE FILE FORMAT](https://docs.snowflake.com/en/sql-reference/sql/create-file-format)
- **Snowflake** — chargement dans une table : [COPY INTO](https://docs.snowflake.com/en/sql-reference/sql/copy-into-table)
- **Snowflake** — warehouses : [Warehouses overview](https://docs.snowflake.com/en/user-guide/warehouses-overview)
- **Snowflake** — limiter la consommation : [Resource monitors](https://docs.snowflake.com/en/user-guide/resource-monitors)
- **Snowflake** — cache des résultats : [Using persisted query results](https://docs.snowflake.com/en/user-guide/querying-persisted-results)

**Python et dbt**
- **pandas** — écriture Parquet : [DataFrame.to_parquet](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_parquet.html)
- **pyarrow** — lecture/écriture Parquet : [Reading and writing Parquet](https://arrow.apache.org/docs/python/parquet.html)
- **dbt** — modèles incrémentaux : [Incremental models](https://docs.getdbt.com/docs/build/incremental-models)
- **dbt** — tests, sources, macros : [Data Tests](https://docs.getdbt.com/docs/build/data-tests) · [Sources](https://docs.getdbt.com/docs/build/sources) · [Jinja and Macros](https://docs.getdbt.com/docs/build/jinja-macros)

Si l'un de ces points n'est pas clair pour toi, c'est le moment de combler la lacune — pas après avoir configuré la moitié de ton cloud.

---

## 2. Consigne

À partir du pipeline du Projet 1 et de ton Data Warehouse du Projet 2, construis une architecture cloud qui réalise, dans l'ordre, les étapes suivantes :

1. **Production des données** : relancer le pipeline du Projet 1 pour obtenir **trois mois consécutifs** de CSV nettoyés (janvier, février et mars 2023), sans que les mois s'écrasent les uns les autres.
2. **Stockage cloud** : déposer ces fichiers dans un **bucket AWS S3 privé**, organisé de façon lisible et partitionnée, après avoir converti les données en **Parquet** et **comparé** ce format au CSV avec des chiffres.
3. **Ingestion sécurisée** : charger S3 vers une table **Bronze** Snowflake à l'aide d'une **Storage Integration** et d'un **stage externe**, puis prouver que le chargement est complet **mois par mois**.
4. **Transformation (ELT)** : faire évoluer ton projet **dbt** pour traiter les trois mois, avec au moins un **modèle incrémental**, sans doublon quand on relance.
5. **Qualité, performance et coûts** : tester les règles propres au multi-mois, mesurer l'effet de la taille du warehouse sur un traitement réel, et démontrer que tu maîtrises ce que le cloud te facture.

L'ensemble doit être reproductible par un tiers à partir de ton README, sans intervention cachée.

### Le besoin analytique

L'équipe d'analystes du Projet 2 est contente, et en veut plus. Elle veut maintenant pouvoir répondre à des questions comme :

- **Évolution** : comment le nombre de trajets, le revenu total et le pourboire moyen évoluent-ils de janvier à mars ? Un mois se distingue-t-il ?
- **Comparabilité** : les trois mois sont-ils aussi propres les uns que les autres ? Le taux de lignes retirées par le Projet 1 est-il stable ?
- **Activité et localisation** : les lieux de départ et d'arrivée les plus fréquents, et les heures de pointe, changent-ils d'un mois à l'autre ?
- **Ingestion** : combien de fichiers et de lignes ont été chargés pour chaque mois, et peux-tu le prouver ?
- **Qualité** : les données restent-elles cohérentes entre elles après le passage par le cloud ? Qu'est-ce qui doit bloquer le pipeline, qu'est-ce qui peut simplement être documenté ?

**Ces questions ne veulent pas dire qu'il faut une table par question.** Comme au Projet 2, comprends d'abord le besoin, puis choisis ta modélisation.

---

## 3. Directives

### Les données

Le Projet 3 part des **CSV nettoyés produits par ton pipeline du Projet 1**, et uniquement d'eux. Tu ne dois pas :

- télécharger un autre dataset, ni un fichier déjà préparé trouvé sur Internet ;
- utiliser un autre mois que ceux demandés ;
- contourner le Projet 1 en extrayant les données par un autre moyen.

Les trois mois sont **janvier, février et mars 2023**. Janvier est le fichier que tu as déjà produit. Il te reste à relancer le Projet 1 pour février et mars, ce qui représente environ trois fois le volume du Projet 2. Prévois de la place sur ton disque et du temps pour l'extraction.

### Pièges à anticiper

Le passage de un à trois mois a l'air d'être un simple copier-coller. Il ne l'est pas. Avant d'écrire la moindre ligne, pose-toi ces questions et vérifie les réponses toi-même :

- Le pipeline du Projet 1 écrit-il ses fichiers de sortie (CSV, rapport qualité) sous un nom qui contient le mois ? Que se passe-t-il quand tu le relances sur un autre mois, et comment t'assures-tu de ne perdre aucun mois ?
- Le rapport qualité de chaque mois te donne un nombre de lignes finales. Comment l'utilises-tu pour **prouver** que ce qui arrive dans Snowflake est complet, mois par mois ?
- La clé de trajet que tu as construite au Projet 2 reste-t-elle unique sur trois mois ? Qu'est-ce qui pourrait créer une collision entre deux mois ?
- Un trajet qui commence le 31 janvier à 23h50 appartient-il à janvier ou à février ? Comment cette règle est-elle appliquée dans ton pipeline, et dans ton entrepôt ?
- Les trois mois ont-ils exactement les mêmes colonnes et les mêmes types ? Comment le vérifies-tu avant de charger, plutôt qu'après un échec ?
- Si tu relances le chargement, que se passe-t-il ? Et si tu corriges un fichier déjà chargé ?

### Comprendre avant de construire

Avant de créer quoi que ce soit dans AWS ou Snowflake, réponds **par écrit** à ces questions :

- Qu'est-ce qui change entre le Projet 2 et le Projet 3, et qu'est-ce qui reste valable ?
- Quelles parties de ton projet dbt dépendent d'un seul mois, et lesquelles fonctionnent déjà sur plusieurs mois ?
- Comment organises-tu ton bucket pour qu'un humain, et un outil, retrouve un mois précis sans lister tout le contenu ?
- Qui a besoin d'accéder à quoi (ta machine, Snowflake, dbt), avec quels droits minimaux ?
- Quelle quantité de données manipules-tu, en lignes et en octets, pour chaque format ?

Cette analyse fait partie de tes livrables.

### Architecture AWS : le data lake

Crée un **bucket S3 privé**. Les détails (nom, région, structure) sont libres, mais tu dois pouvoir justifier chacun de tes choix.

- **Accès public bloqué.** Les quatre options de *Block Public Access* sont activées. *Pourquoi un bucket de données ne doit-il jamais être public, même « juste pour tester » ?*
- **Organisation des chemins.** Les fichiers sont rangés dans une convention lisible et partitionnée, par exemple par année et par mois. *Qu'est-ce qu'un bon préfixe permet de faire, côté AWS et côté Snowflake ?*
- **Région.** *Pourquoi la région de ton bucket doit-elle être choisie en fonction de celle de ton compte Snowflake ?*
- **Chiffrement et versioning.** Tu dois savoir dire où en est le chiffrement de ton bucket et si tu as activé le versioning, et expliquer pourquoi.
- **Upload.** Le dépôt des fichiers sur S3 est fait par un **script ou une commande reproductible** (AWS CLI ou `boto3`), jamais à la main depuis la console. Relancer l'upload ne doit pas créer de doublons.

### Accès sécurisé : IAM et Storage Integration

Snowflake doit lire ton bucket **sans que tu ne lui donnes jamais une clé d'accès AWS**.

- Crée une **policy IAM** qui n'autorise que ce qui est nécessaire, et seulement sur ton bucket et ton préfixe.
- Crée un **rôle IAM** que Snowflake pourra assumer, puis configure la **relation d'approbation** avec les valeurs fournies par ta **Storage Integration**.
- Crée un **stage externe** qui pointe vers ton bucket **via la Storage Integration**.
- *Pourquoi une Storage Integration plutôt que des clés AWS écrites dans la définition du stage ?*
- *Pourquoi donner à Snowflake un droit de lecture seule plutôt que tous les droits sur le bucket ?*

Les fichiers de policy IAM (sans aucun secret) sont versionnés dans ton dépôt. Les clés d'accès AWS de ta propre machine, elles, n'y apparaissent **jamais**.

### Parquet ou CSV ?

Convertis tes données nettoyées en **Parquet** (avec `pandas` + `pyarrow`) et **compare** les deux formats sur au moins un mois : taille des fichiers, durée de chargement dans Snowflake, et lisibilité du schéma. Tu choisis ensuite le format que tu utilises pour alimenter Bronze, et tu **justifies** ce choix avec tes chiffres.

*Qu'est-ce qu'un format colonnaire change pour le stockage et pour la lecture ? Qu'est-ce que tu perds avec Parquet, par rapport au CSV ?*

### Ingestion dans Snowflake

Comme au Projet 2, crée l'environnement avec des **scripts SQL versionnés** (pas avec dbt). Il te faut au minimum : la **Storage Integration**, un **stage externe**, un **file format** adapté à ton format, et une **table Bronze** capable de recevoir plusieurs mois. Le flux est :

```text
S3 → stage externe (Storage Integration) → COPY INTO → table Bronze
```

- Conserve dans Bronze le **nom du fichier d'origine** et la **date de chargement** de chaque ligne, grâce aux métadonnées de Snowflake. *À quoi te sert cette information le jour où un mois pose problème ?*
- Vérifie le chargement **mois par mois** avec les commandes de contrôle de Snowflake : fichiers chargés, lignes chargées, erreurs, et comparaison avec le rapport qualité du Projet 1.
- *Quelle différence fais-tu entre le stage interne du Projet 2 et le stage externe de ce projet ? Qu'est-ce que tu y gagnes, et qu'est-ce que tu dois désormais sécuriser ?*

### Idempotence : relancer sans casser

Un pipeline cloud est relancé, par toi, par un collègue, ou par un orchestrateur (ce sera le Projet 4). Il doit donc se comporter correctement quand on le rejoue.

Teste et documente, avec des preuves :

- Que se passe-t-il quand tu relances le `COPY INTO` sur des fichiers déjà chargés ?
- Comment recharges-t-il **un seul mois** corrigé, sans dupliquer ni toucher aux deux autres ?
- Comment te protèges-tu d'un double chargement accidentel ?

*Pourquoi un pipeline « qui marche une fois » n'est-il pas un pipeline fiable ?*

### dbt : du mois unique au multi-mois

Reprends ton projet dbt du Projet 2 (il reste la base de ce projet) et fais-le évoluer :

- **Sources.** Les sources dbt pointent vers ta nouvelle table Bronze multi-mois.
- **Incrémental.** Au moins **un modèle incrémental** (typiquement ta table de faits), avec une clé unique. *Pourquoi ne pas simplement tout recalculer à chaque exécution ? Dans quels cas un full refresh reste-t-il nécessaire ?* Choisis ton critère d'incrémentalité (date du trajet, date de chargement, fichier d'origine) et **explique-le**.
- **Tests.** Ajoute des tests qui protègent les règles **propres au multi-mois** : pas de doublon de trajet entre mois, chaque mois reste dans ses bornes de dates, le nombre de lignes par mois correspond à ce que le Projet 1 a produit. Chaque test doit protéger une règle de qualité identifiable. Distingue une *anomalie à documenter* (avertissement) d'une *erreur bloquante*.
- **Macro.** Au moins **une macro utile** qui résout un vrai problème de répétition lié au multi-mois (pas une macro pour cocher la case).
- **Documentation et lineage.** Mets à jour la documentation dbt. Tu dois pouvoir remonter une métrique de Gold jusqu'au fichier S3 d'origine : `fichier S3 → Bronze → source() → staging → Gold → métrique`.

Les autres éléments du Projet 2 (seeds, snapshots, analyses) restent à ta discrétion. Si tu les conserves, vérifie qu'ils fonctionnent toujours sur trois mois ; si tu les retires, dis pourquoi.

### Traiter à plus grande échelle : mesurer, pas supposer

Le cloud te permet d'augmenter la puissance de calcul à la demande. Prouve que tu sais ce que ça change.

- Choisis **un traitement réel** de ton projet (par exemple ton `dbt build` complet, ou une agrégation lourde sur les trois mois).
- Exécute-le avec **deux tailles de warehouse différentes** (par exemple XS puis S), en désactivant le cache de résultats pour que la comparaison soit honnête.
- Relève, pour chaque taille, la **durée** et le **coût estimé en crédits**.

*Doubler la taille du warehouse divise-t-il le temps par deux ? Qu'est-ce que ça t'apprend sur le moment où il vaut la peine de payer plus cher ?*

### Reproductibilité et secrets

Un autre développeur doit pouvoir, en suivant ton README : configurer AWS (bucket, policy, rôle), produire les trois mois avec le Projet 1, convertir et déposer les fichiers, configurer Snowflake, charger Bronze, exécuter dbt, lancer les tests et générer la documentation. Documente les commandes nécessaires.

**Aucun secret ne doit être présent dans Git** : clé d'accès AWS (access key ID et secret access key), mot de passe Snowflake, token, clé privée, fichier `.env`, et donc ton `profiles.yml` s'il contient des identifiants. Utilise des variables d'environnement ou un profil AWS local, fournis un `.env.example` **sans valeurs réelles**, et fais en sorte que ton `.gitignore` empêche ces fichiers d'être versionnés. Une fuite dans l'historique Git reste une fuite même si le fichier est supprimé ensuite : et une clé AWS exposée peut être exploitée en quelques minutes.

Pense aussi à ce que tu montres dans tes captures d'écran : masque tes identifiants de compte AWS et Snowflake.

### Maîtrise des coûts

Le cloud facture ce que tu laisses allumé. Tu dois pouvoir expliquer les mesures que tu as prises :

- **AWS** : une **alerte de budget** configurée dès le début, un bucket qui ne contient que ce qui est nécessaire, et une section **« Nettoyage »** dans ton README qui explique comment supprimer les ressources AWS créées pour ce projet.
- **Snowflake** : un warehouse de taille adaptée avec **suspension automatique** courte, éventuellement un **resource monitor**, et aucune exécution inutile. *Quand ton warehouse consomme-t-il des crédits, et quand est-ce qu'il n'en consomme pas ?*

### Liberté d'architecture

L'arborescence proposée plus bas est un minimum, pas un carcan : elle peut évoluer si tu peux la justifier, à condition de rester **cohérente**, **documentée** dans ton `README.md` et **lisible à l'échelle du projet**.

---

## 4. Livrable

### Ce que tu dois rendre

Un dépôt Git propre contenant au minimum :

```text
.
├── README.md
├── .gitignore
├── .env.example
├── aws/                    # policies IAM et trust policy (JSON, sans secret)
├── scripts/                # conversion Parquet + upload vers S3
├── sql/
│   └── snowflake/          # integration, stage externe, file format, Bronze, contrôles
├── dbt_project.yml
├── models/
│   ├── staging/
│   ├── intermediate/
│   └── marts/
├── tests/
├── macros/
├── seeds/
├── snapshots/
├── analyses/
└── docs/
```

- **Les scripts** (dans `scripts/` et `aws/`) qui convertissent, déposent et sécurisent les données sur AWS.
- **Les scripts SQL Snowflake** (dans `sql/snowflake/`) qui créent tout ce qui est nécessaire au chargement depuis S3.
- **Le projet dbt complet**, exécutable à partir du dépôt, avec au moins un modèle incrémental.
- **Un `README.md`** qui explique : le contexte et l'objectif, l'architecture, les prérequis, l'installation, la configuration AWS, Snowflake et dbt, la production des trois mois, le chargement, l'exécution, les tests, la documentation, le nettoyage des ressources cloud, les choix importants (avec leur *pourquoi*) et les limites du projet. Il doit aussi répondre aux questions « à résoudre » posées dans ces consignes.
- **Une documentation d'architecture** (dans `docs/`) : un schéma du flux CSV → Parquet → S3 → Snowflake (Bronze → Silver → Gold) qui montre aussi **qui accède à quoi** (IAM, Storage Integration), et ton analyse du dataset.
- **Des preuves** (dans `docs/`) permettant de vérifier : le contenu du bucket (structure par mois), la comparaison CSV/Parquet, le chargement Snowflake et le nombre de lignes **par mois**, le test de relance (idempotence), les tests dbt, le lineage, la comparaison de deux tailles de warehouse, et l'alerte de budget AWS. Vérifie que tes captures ne montrent aucun identifiant sensible.

### Barème de validation — /100 points

Ton projet sera noté selon le barème ci-dessous. Chaque ligne est vérifiée soit automatiquement (script), soit par une lecture humaine ou assistée (pour juger la qualité d'une justification, pas juste sa présence).

**Règle éliminatoire — prioritaire sur tout le reste :** si une clé d'accès AWS, un mot de passe, un token, une clé privée ou tout autre secret AWS ou Snowflake apparaît à un seul endroit de l'historique Git (même dans un commit ancien, même supprimé depuis), **le score est automatiquement 0/100**, quel que soit le reste du projet.

**Seuil de réussite : 70/100.** En dessous, le projet est « à revoir » ; à partir de 70, il est « validé ».

#### Bloc 1 — Stockage cloud AWS et sécurité (15 pts) · *vérification automatique*

| Critère | Points |
| --- | --- |
| Bucket S3 privé (accès public bloqué), fichiers rangés par année/mois, les trois mois présents | 4 |
| Snowflake accède à S3 via une Storage Integration et un rôle IAM, avec une policy limitée au bucket concerné, sans aucune clé AWS dans le dépôt ni dans le stage | 7 |
| Maîtrise des coûts : alerte de budget AWS, suspension automatique du warehouse, procédure de nettoyage documentée | 4 |

#### Bloc 2 — Ingestion et traitement (25 pts) · *mixte*

| Critère | Points | Vérification |
| --- | --- | --- |
| Environnement Snowflake créé par scripts SQL versionnés (integration, stage externe, file format, Bronze) | 4 | Auto |
| Chargement par `COPY INTO` depuis S3, avec un nombre de lignes Bronze égal, **mois par mois**, à celui des fichiers sources | 8 | Auto |
| Conversion Parquet et comparaison chiffrée CSV vs Parquet, avec choix justifié | 5 | Humaine / assistée |
| Idempotence : relancer ne duplique rien, et la stratégie de rechargement d'un mois est testée et documentée | 8 | Mixte |

#### Bloc 3 — Modélisation dbt multi-mois et qualité (20 pts) · *mixte*

| Critère | Points | Vérification |
| --- | --- | --- |
| `dbt build` s'exécute sans erreur sur les trois mois, avec au moins un modèle incrémental et une clé unique | 8 | Auto |
| Tests dbt couvrant des règles propres au multi-mois (doublons entre mois, bornes de dates, cohérence des volumes) | 7 | Auto |
| Comparaison chiffrée de deux tailles de warehouse sur un traitement réel (durée, crédits) et interprétation | 5 | Humaine / assistée |

#### Bloc 4 — Documentation et présentation (40 pts) · *mixte*

| Critère | Points | Vérification |
| --- | --- | --- |
| `README.md` complet et structuré (objectif, architecture, installation, configuration AWS/Snowflake/dbt, exécution, tests, nettoyage, limites) | 10 | Auto (présence des sections) |
| Documentation d'architecture et dbt : schéma incluant IAM, lineage de S3 jusqu'à Gold, grain, clés, règles métier | 10 | Mixte |
| Les choix (organisation du bucket, Parquet ou CSV, moindre privilège, stratégie d'idempotence, modèle incrémental, taille du warehouse, maîtrise des coûts) sont expliqués et argumentés dans le README, pas seulement mentionnés | 15 | Humaine / assistée |
| Dépôt propre et preuves présentes (bucket, chargement, lignes par mois, tests, lineage, budget), sans secret ni donnée volumineuse | 5 | Auto |

**Exemple concret pour le critère « choix » :** lister n'est pas argumenter.

- ❌ *Listé* : « J'ai rangé les fichiers par année et par mois dans S3. »
- ✅ *Argumenté* : « J'ai rangé les fichiers sous `year=2023/month=01/` parce que le COPY INTO peut cibler un seul mois en pointant sur son préfixe : recharger février après une correction ne touche ni janvier ni mars, et on retrouve un mois sans lister tout le bucket. »

Même chose pour chaque policy, format, modèle, test ou taille de warehouse : le README doit dire *pourquoi* ils existent. Et si tu décides de ne pas utiliser une fonctionnalité, justifie pourquoi elle n'est pas pertinente.

---

## 5. Soumission

**📅 Date limite : samedi 31 octobre 2026, 23h59 (heure de Dakar).**

Une fois ton projet terminé et poussé sur GitHub (dépôt public, sinon je ne peux pas y accéder) :

- **Commente le post LinkedIn du projet** avec le lien direct vers ton dépôt GitHub.
- Vérifie avant de poster que le lien est bien public (teste-le en navigation privée) et qu'il pointe vers la racine du dépôt, pas vers un fichier ou une branche spécifique.
- Un seul commentaire par apprenant suffit ; si tu push des corrections après coup, un commentaire de mise à jour avec « edit » en préfixe évite la confusion sur quelle version évaluer.

---

## Checklist finale avant de considérer le projet terminé

- [ ] Les trois CSV (janvier, février, mars 2023) viennent bien du pipeline du Projet 1, et aucun mois n'a écrasé un autre.
- [ ] Le bucket S3 est privé, structuré par année/mois, et ne contient que les fichiers nécessaires.
- [ ] Snowflake lit S3 via une Storage Integration et un rôle IAM au moindre privilège : aucune clé AWS dans le stage, le code ou l'historique Git.
- [ ] Le nombre de lignes Bronze correspond, mois par mois, aux fichiers sources.
- [ ] Relancer le chargement ne crée aucun doublon, et le rechargement d'un seul mois est testé.
- [ ] La comparaison CSV vs Parquet est chiffrée et le choix du format est justifié.
- [ ] `dbt build` passe sur trois mois, avec au moins un modèle incrémental, une macro utile et des tests liés au multi-mois.
- [ ] La comparaison de deux tailles de warehouse est faite, avec durée et coût.
- [ ] Une alerte de budget AWS existe, le warehouse se suspend automatiquement, et le README explique comment nettoyer les ressources cloud.
- [ ] Aucun secret (clé AWS, mot de passe, token, `.env`, `profiles.yml` avec identifiants) n'est versionné ni présent dans l'historique Git.
- [ ] `README.md` est à jour, permet une prise en main autonome, et argumente les choix au lieu de les lister.