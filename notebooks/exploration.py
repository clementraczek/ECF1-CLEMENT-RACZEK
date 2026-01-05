"""
Notebook d'exploration des données e-commerce.

Ce script peut être converti en notebook Jupyter ou exécuté directement.

Usage:
    python notebooks/exploration.py
    
    # Ou en notebook:
    jupyter notebook notebooks/exploration.ipynb
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import pandas as pd
import matplotlib.pyplot as plt

# Configuration matplotlib
plt.style.use('seaborn-v0_8-whitegrid')
plt.rcParams['figure.figsize'] = (12, 6)
plt.rcParams['font.size'] = 10

from src.storage import MongoDBStorage


def main():
    """Exploration des données e-commerce."""
    
    print("="*60)
    print("EXPLORATION DES DONNÉES E-COMMERCE")
    print("="*60)
    
    # Connexion
    mongo = MongoDBStorage()
    
    # Charger les données
    print("\n📊 Chargement des données...")
    products = mongo.find_products(limit=1000)
    df = pd.DataFrame(products)
    
    if df.empty:
        print("⚠️  Aucune donnée trouvée. Exécutez d'abord le pipeline de scraping.")
        mongo.close()
        return
    
    # Nettoyer
    if '_id' in df.columns:
        df['_id'] = df['_id'].astype(str)
    
    print(f"   Produits chargés: {len(df)}")
    print(f"   Colonnes: {list(df.columns)}")
    
    # ============================================================
    # 1. APERÇU DES DONNÉES
    # ============================================================
    print("\n" + "="*60)
    print("1. APERÇU DES DONNÉES")
    print("="*60)
    
    print("\nPremières lignes:")
    print(df[['title', 'price', 'rating', 'category', 'subcategory']].head(10).to_string())
    
    print("\nStatistiques descriptives:")
    print(df[['price', 'rating', 'reviews_count']].describe().to_string())
    
    # ============================================================
    # 2. DISTRIBUTION DES PRIX
    # ============================================================
    print("\n" + "="*60)
    print("2. DISTRIBUTION DES PRIX")
    print("="*60)
    
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    
    # Histogramme
    axes[0].hist(df['price'], bins=30, edgecolor='black', alpha=0.7)
    axes[0].set_xlabel('Prix ($)')
    axes[0].set_ylabel('Nombre de produits')
    axes[0].set_title('Distribution des prix')
    axes[0].axvline(df['price'].mean(), color='red', linestyle='--', label=f'Moyenne: ${df["price"].mean():.2f}')
    axes[0].legend()
    
    # Box plot par catégorie
    if 'subcategory' in df.columns:
        categories = df.groupby('subcategory')['price'].apply(list).to_dict()
        axes[1].boxplot(categories.values(), labels=categories.keys())
        axes[1].set_ylabel('Prix ($)')
        axes[1].set_title('Prix par sous-catégorie')
        plt.setp(axes[1].xaxis.get_majorticklabels(), rotation=45)
    
    plt.tight_layout()
    plt.savefig('data/price_distribution.png', dpi=150)
    print("   Graphique sauvegardé: data/price_distribution.png")
    plt.show()
    
    # ============================================================
    # 3. RATINGS
    # ============================================================
    print("\n" + "="*60)
    print("3. DISTRIBUTION DES RATINGS")
    print("="*60)
    
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    
    # Distribution des ratings
    rating_counts = df['rating'].value_counts().sort_index()
    axes[0].bar(rating_counts.index, rating_counts.values, color='gold', edgecolor='black')
    axes[0].set_xlabel('Rating (étoiles)')
    axes[0].set_ylabel('Nombre de produits')
    axes[0].set_title('Distribution des ratings')
    axes[0].set_xticks([1, 2, 3, 4, 5])
    
    # Moyenne rating par catégorie
    if 'subcategory' in df.columns:
        avg_rating = df.groupby('subcategory')['rating'].mean().sort_values(ascending=True)
        axes[1].barh(avg_rating.index, avg_rating.values, color='skyblue', edgecolor='black')
        axes[1].set_xlabel('Rating moyen')
        axes[1].set_title('Rating moyen par sous-catégorie')
        for i, v in enumerate(avg_rating.values):
            axes[1].text(v + 0.05, i, f'{v:.2f}', va='center')
    
    plt.tight_layout()
    plt.savefig('data/ratings_distribution.png', dpi=150)
    print("   Graphique sauvegardé: data/ratings_distribution.png")
    plt.show()
    
    # ============================================================
    # 4. ANALYSE PAR CATÉGORIE
    # ============================================================
    print("\n" + "="*60)
    print("4. ANALYSE PAR CATÉGORIE")
    print("="*60)
    
    if 'subcategory' in df.columns:
        cat_stats = df.groupby('subcategory').agg({
            'price': ['count', 'mean', 'min', 'max'],
            'rating': 'mean'
        }).round(2)
        
        cat_stats.columns = ['Nombre', 'Prix moyen', 'Prix min', 'Prix max', 'Rating moyen']
        print("\nStatistiques par sous-catégorie:")
        print(cat_stats.to_string())
    
    # ============================================================
    # 5. TOP PRODUITS
    # ============================================================
    print("\n" + "="*60)
    print("5. TOP PRODUITS")
    print("="*60)
    
    # Plus chers
    print("\n🏆 Top 5 produits les plus chers:")
    top_expensive = df.nlargest(5, 'price')[['title', 'price', 'rating', 'subcategory']]
    for i, (_, row) in enumerate(top_expensive.iterrows(), 1):
        print(f"   {i}. {row['title'][:40]}... - ${row['price']} ({row['rating']}★)")
    
    # Meilleur rapport qualité/prix
    df['value_score'] = df['rating'] / (df['price'] / 100 + 0.01)
    print("\n💎 Top 5 meilleur rapport qualité/prix:")
    top_value = df.nlargest(5, 'value_score')[['title', 'price', 'rating', 'value_score']]
    for i, (_, row) in enumerate(top_value.iterrows(), 1):
        print(f"   {i}. {row['title'][:40]}... - ${row['price']} ({row['rating']}★)")
    
    # ============================================================
    # 6. CORRÉLATIONS
    # ============================================================
    print("\n" + "="*60)
    print("6. CORRÉLATIONS")
    print("="*60)
    
    numeric_cols = ['price', 'rating', 'reviews_count']
    existing_cols = [c for c in numeric_cols if c in df.columns]
    
    if len(existing_cols) >= 2:
        corr = df[existing_cols].corr()
        print("\nMatrice de corrélation:")
        print(corr.round(3).to_string())
        
        # Scatter plot prix vs rating
        plt.figure(figsize=(10, 6))
        plt.scatter(df['price'], df['rating'], alpha=0.5, c='blue')
        plt.xlabel('Prix ($)')
        plt.ylabel('Rating')
        plt.title('Prix vs Rating')
        plt.savefig('data/price_vs_rating.png', dpi=150)
        print("\n   Graphique sauvegardé: data/price_vs_rating.png")
        plt.show()
    
    # Fermer la connexion
    mongo.close()
    
    print("\n" + "="*60)
    print("FIN DE L'EXPLORATION")
    print("="*60)


if __name__ == "__main__":
    main()
