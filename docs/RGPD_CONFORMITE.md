# Documentation de Conformité RGPD

Ce document détaille les mesures techniques et organisationnelles mises en œuvre pour assurer la protection des données à caractère personnel (DCP) au sein du pipeline de données, conformément au cahier des charges.

## 1. Inventaire des données personnelles et sensibles

L'analyse du fichier `partenaire_librairies.xlsx` a permis d'identifier les colonnes nécessitant un traitement spécifique :

| Colonne | Type de donnée | Sensibilité | Mesure de protection |
| :--- | :--- | :--- | :--- |
| **contact_nom** | Donnée personnelle | Identifiant direct | **Suppression** |
| **contact_email** | Donnée personnelle | Identifiant direct | **Suppression**  |
| **contact_telephone** | Donnée personnelle | Identifiant direct | **Suppression**  |
| **ca_annuel** | Donnée stratégique | Confidentiel |  A ne pas diffuser | **Données confidentielles**

---


## 2. Procédure de "Droit à l'effacement"

Conformément au cahier des charges, la procédure de droit à l'effacement est la suivante :

1.  **Réception :** Le partenaire demande la suppression de ses données via son adresse email.
2.  **Hachage :** Le système calcule le `contact_id` (SHA-256 de l'email).
3.  **Suppression :** * Le script supprime l'entrée correspondante dans le fichier source `partenaire_librairies.xlsx`.
    * Les fichiers CSV dans le bucket `bronze` de **MinIO** contenant ce `contact_id` sont purgés.
    * La commande `DELETE FROM gold_librairies WHERE contact_id = '...'` est exécutée sur **PostgreSQL**.

