# TP E-Commerce : Pipeline de Données avec MinIO & MongoDB

## Description

Ce projet implémente un pipeline de données complet pour scraper un site e-commerce de démonstration et stocker les données dans une architecture hybride :
- **MongoDB** : Métadonnées des produits (prix, ratings, descriptions)
- **MinIO** : Images des produits et exports de données

**Site cible** : https://webscraper.io/test-sites/e-commerce/allinone

> Ce site est explicitement conçu pour l'apprentissage du scraping. Il est 100% légal de le scraper.

## Architecture

```
┌────────────────────┐
│   webscraper.io    │
│   (E-commerce)     │
└─────────┬──────────┘
          │ Scraping (Python)
          ▼
┌────────────────────┐
│      Pipeline      │
│   Extract → Transform → Load
└─────────┬──────────┘
          │
    ┌─────┴─────┐
    ▼           ▼
┌────────┐  ┌────────┐
│ MinIO  │  │MongoDB │
│        │  │        │
│ Images │  │Produits│
│ Exports│  │ Stats  │
└────────┘  └────────┘
```

### Justification de l'architecture hybride

| Stockage | Données | Justification |
|----------|---------|---------------|
| **MongoDB** | Métadonnées produits | Requêtes complexes, agrégations, recherche full-text |
| **MongoDB** | Historique prix | Time series, analyse temporelle |
| **MinIO** | Images produits | Données binaires volumineuses, accès par URL |
| **MinIO** | Exports CSV/JSON | Fichiers volumineux pour analytics |

## Démarrage rapide

### Prérequis

- Python 3.10+
- Docker et Docker Compose
- Git

### Installation

```bash
# 1. Cloner ou créer le projet
cd tp-ecommerce-solution

# 2. Créer l'environnement virtuel
python -m venv venv

# 3. Activer l'environnement
# Linux/Mac:
source venv/bin/activate
# Windows:
venv\Scripts\activate

# 4. Installer les dépendances
pip install -r requirements.txt

# 5. Démarrer l'infrastructure Docker
docker-compose up -d

# 6. Vérifier que tout fonctionne
docker-compose ps
```

### Vérification de l'infrastructure

- **MinIO Console** : http://localhost:9001
  - Login : `minioadmin`
  - Password : `minioadmin123`

- **Mongo Express** : http://localhost:8081
  - Base de données : `ecommerce_db`

## Utilisation

### Exécuter le pipeline

```bash
# Scraping basique (3 pages par catégorie)
python -m src.pipeline --pages 3

# Scraping avec export CSV
python -m src.pipeline --pages 5 --export-csv

# Scraper seulement les laptops
python -m src.pipeline --categories laptops --pages 10

# Scraping complet avec tous les exports
python -m src.pipeline --pages 5 --export-csv --export-json --analytics

# Mode sans images (plus rapide)
python -m src.pipeline --pages 10 --no-images
```

### Options disponibles

| Option | Description |
|--------|-------------|
| `--pages N` | Nombre de pages par catégorie (défaut: 3) |
| `--categories X Y` | Catégories spécifiques (laptops, tablets, touch) |
| `--no-images` | Ne pas télécharger les images |
| `--export-csv` | Exporter en CSV après scraping |
| `--export-json` | Exporter en JSON après scraping |
| `--export-parquet` | Exporter en Parquet après scraping |
| `--backup` | Créer un backup |
| `--analytics` | Afficher les analytics |
| `--quiet` | Mode silencieux |

### Utilisation en Python

```python
from src.pipeline import EcommercePipeline

# Créer le pipeline
pipeline = EcommercePipeline()

# Exécuter le scraping
stats = pipeline.run(
    categories=["laptops", "tablets"],
    max_pages=5,
    download_images=True
)

print(f"Produits scrapés: {stats['products_scraped']}")
print(f"Images stockées: {stats['images_stored']}")

# Exporter les données
pipeline.export_csv("data/products.csv")

# Afficher les analytics
pipeline.print_analytics()

# Fermer les connexions
pipeline.close()
```

## Structure du projet

```
tp-ecommerce-solution/
├── docker-compose.yml      # Infrastructure Docker
├── requirements.txt        # Dépendances Python
├── .env                    # Variables d'environnement
├── README.md              # Ce fichier
│
├── config/
│   ├── __init__.py
│   └── settings.py        # Configuration centralisée
│
├── src/
│   ├── __init__.py
│   ├── pipeline.py        # Pipeline principal
│   │
│   ├── storage/
│   │   ├── __init__.py
│   │   ├── minio_client.py    # Client MinIO
│   │   └── mongo_client.py    # Client MongoDB
│   │
│   └── scrapers/
│       ├── __init__.py
│       └── ecommerce_scraper.py  # Scraper
│
├── exercises/
│   ├── ex2_mongo_queries.py     # Solution exercice 2
│   └── ex3_minio_operations.py  # Solution exercice 3
│
├── tests/
│   └── (tests unitaires)
│
└── data/
    └── (exports locaux)
```

## Exercices

### Exercice 1 : Validation de l'infrastructure

```bash
# Exécuter un scraping minimal
python -m src.pipeline --pages 1

# Vérifier MongoDB : http://localhost:8081
# Vérifier MinIO : http://localhost:9001
```

**Questions** :
1. Combien de produits ont été scrapés ?
2. Quelle est la structure d'un document produit ?
3. Comment sont organisées les images dans MinIO ?

### Exercice 2 : Requêtes MongoDB

```bash
python exercises/ex2_mongo_queries.py
```

Requêtes implémentées :
- 2.1 : Laptops à moins de 500$
- 2.2 : Produit le plus cher par catégorie
- 2.3 : Prix moyen des produits bien notés
- 2.4 : Produits Samsung ou Apple
- 2.5 : Classement qualité/prix
- 2.6 : Distribution par tranche de prix
- 2.7 : Produits avec le même prix

### Exercice 3 : Opérations MinIO

```bash
python exercises/ex3_minio_operations.py
```

Opérations implémentées :
- 3.1 : Liste et taille totale des images
- 3.2 : Création de thumbnails
- 3.3 : URL présignée
- 3.4 : Rapport JSON des stats
- 3.5 : Backup des images

## Configuration

### Variables d'environnement (.env)

```env
# MinIO
MINIO_ENDPOINT=localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin123
MINIO_SECURE=false

# MongoDB
MONGO_HOST=localhost
MONGO_PORT=27017
MONGO_USER=admin
MONGO_PASSWORD=admin123
MONGO_DB=ecommerce_db
```

### Modification de la configuration

Éditez `config/settings.py` pour modifier :
- URLs et credentials
- Délai entre requêtes
- Nombre max de pages
- Noms des buckets

## Analytics disponibles

Le pipeline génère automatiquement :
- Statistiques globales (total, moyenne, min, max)
- Stats par catégorie et sous-catégorie
- Distribution des prix
- Classement qualité/prix
- Historique des scraping

```python
pipeline.print_analytics()
```

## Dépannage

### Erreur SSL avec MinIO

```
SSLError: WRONG_VERSION_NUMBER
```

**Solution** : Vérifier que `secure=False` dans `config/settings.py`

### MongoDB connexion refusée

```bash
# Vérifier que MongoDB est démarré
docker-compose ps

# Redémarrer si nécessaire
docker-compose restart mongodb
```

### Images non téléchargées

Vérifier la connexion internet et augmenter le timeout dans `config/settings.py`.

## Ressources

- [webscraper.io Test Sites](https://webscraper.io/test-sites)
- [BeautifulSoup Documentation](https://www.crummy.com/software/BeautifulSoup/bs4/doc/)
- [MinIO Python SDK](https://min.io/docs/minio/linux/developers/python/minio-py.html)
- [PyMongo Tutorial](https://pymongo.readthedocs.io/en/stable/tutorial.html)
- [MongoDB Aggregation](https://www.mongodb.com/docs/manual/aggregation/)

## Licence

Ce projet est créé à des fins éducatives dans le cadre du cursus Data & IA.

---

*Durée estimée du TP : 6-8 heures*
