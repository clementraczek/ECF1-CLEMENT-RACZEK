# ECF : Pipeline de Données Multi-Sources

## Titre Professionnel Data Engineer - RNCP35288
### Compétences évaluées : C1.1, C1.3, C1.4

---

## Contexte Professionnel

Vous êtes recruté(e) comme **Data Engineer** chez **DataPulse Analytics**, une startup spécialisée dans l'agrégation et l'analyse de données multi-sources.

Le directeur technique vous confie la mission suivante :

> *"Nous avons besoin d'une plateforme capable de collecter des données depuis plusieurs sources hétérogènes : sites web, APIs, fichiers partenaires. Ces données doivent être stockées, nettoyées et rendues disponibles pour nos analystes. Je compte sur vous pour proposer une architecture adaptée et l'implémenter."*

---

## Cahier des Charges

### Objectif

Concevoir et implémenter une **plateforme de collecte et d'analyse de données** répondant aux exigences suivantes :

### Exigences Fonctionnelles

| ID | Exigence | Priorité |
|----|----------|----------|
| EF1 | Collecter des données depuis **au moins 2 sites web** par scraping | Obligatoire |
| EF2 | Collecter des données depuis **au moins 1 API REST** | Obligatoire |
| EF3 | Importer des données depuis **1 fichier Excel** fourni | Obligatoire |
| EF4 | Stocker les données brutes de manière pérenne | Obligatoire |
| EF5 | Nettoyer et transformer les données collectées | Obligatoire |
| EF6 | Rendre les données interrogeables via SQL | Obligatoire |
| EF7 | Permettre des analyses croisées entre les sources | Souhaité |

### Exigences Non-Fonctionnelles

| ID | Exigence | Priorité |
|----|----------|----------|
| ENF1 | Respecter la légalité du scraping (sites autorisés uniquement) | Obligatoire |
| ENF2 | Respecter le RGPD pour les données personnelles | Obligatoire |
| ENF3 | Documenter l'architecture technique | Obligatoire |
| ENF4 | Conteneuriser l'infrastructure (Docker) | Obligatoire |
| ENF5 | Assurer la reproductibilité du pipeline | Souhaité |
| ENF6 | Gérer les erreurs et la reprise sur incident | Souhaité |

---

## Sources de Données Disponibles

### 1. Sites Web - Scraping Autorisé

Les sites suivants sont **explicitement conçus pour l'apprentissage du scraping** et affichent le message *"We love being scraped!"* :

| Site | URL | Contenu |
|------|-----|---------|
| **Books to Scrape** | https://books.toscrape.com | Librairie fictive : 1000 livres, prix, notes, catégories, images |
| **Quotes to Scrape** | https://quotes.toscrape.com | Citations : textes, auteurs, tags |
| **E-commerce Test** | https://webscraper.io/test-sites/e-commerce/allinone | Produits électroniques : laptops, phones, specs |

Vous devez utiliser **au minimum 2 de ces 3 sites**.

### 2. API Open Data

| API | URL | Documentation |
|-----|-----|---------------|
| **API Adresse** (Géocodage) | https://api-adresse.data.gouv.fr/search/ | https://adresse.data.gouv.fr/api-doc/adresse |

**Exemple d'appel :**
```bash
curl "https://api-adresse.data.gouv.fr/search/?q=20+avenue+de+Segur+Paris&limit=1"
```

**Réponse :**
```json
{
  "features": [{
    "geometry": {"coordinates": [2.308628, 48.850699]},
    "properties": {
      "label": "20 Avenue de Ségur 75007 Paris",
      "score": 0.95,
      "city": "Paris",
      "postcode": "75007"
    }
  }]
}
```

**Rate limit :** 50 requêtes/seconde

### 3. Fichier Partenaire (fourni)

Le fichier `partenaire_librairies.xlsx` contient une liste de 20 librairies partenaires :

| Colonne | Type | Exemple | Sensibilité RGPD |
|---------|------|---------|------------------|
| nom_librairie | string | "Librairie du Marais" | Public |
| adresse | string | "15 rue des Francs-Bourgeois" | Public |
| code_postal | string | "75004" | Public |
| ville | string | "Paris" | Public |
| contact_nom | string | "Marie Dubois" | **Donnée personnelle** |
| contact_email | string | "m.dubois@librairie.fr" | **Donnée personnelle** |
| contact_telephone | string | "0142789012" | **Donnée personnelle** |
| ca_annuel | float | 385000 | Confidentiel |
| date_partenariat | date | "2021-03-15" | Public |
| specialite | string | "Littérature" | Public |

⚠️ **Attention** : Ce fichier contient des données personnelles qui doivent être traitées conformément au RGPD.

---

## Travail Demandé

### Partie 1 : Conception de l'Architecture

**Compétence évaluée : C1.1**

Vous devez **concevoir et justifier** une architecture technique adaptée au besoin.

#### Livrable 1.1 : Dossier d'Architecture Technique (DAT)

Rédigez un document répondant aux questions suivantes :

**1. Choix d'architecture globale**
- Quelle architecture proposez-vous ? (Data Lake, Data Warehouse, Lakehouse, base NoSQL, autre ?)
- Pourquoi ce choix plutôt qu'une alternative ?
- Quels sont les avantages et inconvénients de votre choix ?

**2. Choix des technologies**
- Quelles technologies utilisez-vous pour le stockage des données brutes ? Justifiez.
- Quelles technologies utilisez-vous pour les données transformées ? Justifiez.
- Quelles technologies utilisez-vous pour l'interrogation SQL ? Justifiez.
- Comparez avec au moins une alternative pour chaque choix.

**3. Organisation des données**
- Comment organisez-vous les données dans votre architecture ?
- Proposez-vous des couches de transformation ? Lesquelles et pourquoi ?
- Quelle convention de nommage adoptez-vous ?

**4. Modélisation des données**
- Quel modèle de données proposez-vous pour la couche finale ?
- Fournissez un schéma (diagramme entité-relation ou autre)
- Justifiez vos choix de modélisation

**5. Conformité RGPD**
- Quelles données personnelles identifiez-vous dans les sources ?
- Quelles mesures de protection proposez-vous ?
- Comment gérez-vous le droit à l'effacement ?

#### Livrable 1.2 : Infrastructure Docker

Fournissez un fichier `docker-compose.yml` fonctionnel implémentant votre architecture.

---

### Partie 2 : Collecte des Données

**Compétence évaluée : C1.3**

#### Livrable 2.1 : Scrapers Web

Développez des scrapers pour **au moins 2 sites** parmi les 3 proposés.

**Exigences techniques :**
- Délai de politesse entre les requêtes (minimum 1 seconde)
- User-Agent identifiable
- Gestion des erreurs HTTP
- Pagination complète

**Données minimales à extraire :**

*Books to Scrape :*
- Titre, prix, note (1-5 étoiles), disponibilité, catégorie

*Quotes to Scrape :*
- Texte de la citation, auteur, tags

*E-commerce Test :*
- Nom du produit, prix, description, catégorie

#### Livrable 2.2 : Client API

Développez un client pour l'API Adresse permettant de géocoder les adresses des librairies partenaires.

**Exigences :**
- Respect du rate limit
- Gestion des adresses non trouvées

#### Livrable 2.3 : Import Fichier Excel

Développez un module d'import pour le fichier partenaire avec :
- Validation du format
- **Traitement des données personnelles conforme au RGPD** (anonymisation, pseudonymisation ou suppression)
- Stockage conforme à votre architecture

#### Livrable 2.4 : Documentation RGPD

Fournissez un document `RGPD_CONFORMITE.md` contenant :
- Inventaire des données personnelles collectées
- Base légale du traitement pour chaque donnée
- Mesures de protection mises en œuvre
- Procédure de suppression sur demande

---

### Partie 3 : Pipeline ETL

**Compétence évaluée : C1.4**

#### Livrable 3.1 : Transformations

Implémentez les transformations nécessaires pour passer des données brutes aux données exploitables.

**Exemples de transformations attendues :**
- Conversion des devises (£ → €, $ → €)
- Normalisation des formats (dates, textes)
- Nettoyage des valeurs aberrantes
- Déduplication
- Enrichissement (géocodage des adresses)

#### Livrable 3.2 : Chargement

Implémentez le chargement des données transformées vers votre solution d'interrogation SQL :
- Création du schéma
- Chargement des données
- Création des index nécessaires

#### Livrable 3.3 : Orchestration

Fournissez un script permettant d'exécuter l'ensemble du pipeline avec possibilité d'exécuter chaque étape séparément.

#### Livrable 3.4 : Requêtes Analytiques

Fournissez un fichier `analyses.sql` contenant **5 requêtes** démontrant la valeur de votre plateforme :

1. Une requête d'agrégation simple
2. Une requête avec jointure
3. Une requête avec fonction de fenêtrage (window function)
4. Une requête de classement (top N)
5. Une requête croisant au moins 2 sources de données

---

### Bonus

| Bonus | Points |
|-------|--------|
| Téléchargement des images | +2 |
| Logging structuré | +2 |
| Documentation développeur | +2 |


---

## Livrables Attendus

```
ecf-{nom}/
├── docker-compose.yml
├── requirements.txt
├── README.md
│
├── docs/
│   ├── DAT.md                    # Dossier Architecture Technique
│   └── RGPD_CONFORMITE.md        # Conformité RGPD
│
├── src/
│   └── # ... votre code organisé librement
│
├── sql/
│   └── analyses.sql
│
└── data/
    └── partenaire_librairies.xlsx  # Fichier fourni
```

---

## Fichier Fourni

Le fichier `partenaire_librairies.xlsx` contient 20 librairies partenaires avec les colonnes décrites ci-dessus.

---

**Bonne chance !**

