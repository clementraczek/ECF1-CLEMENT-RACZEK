import pandas as pd
from sqlalchemy import create_engine
import os

class AnalyticsReporter:
    def __init__(self):
        # Configuration de la connexion (Port 5433 pour ton Docker)
        self.url = "postgresql://dataeng:dataeng123@localhost:5433/analytics"
        self.engine = create_engine(self.url)
        self.output_file = "rapport_analytique_ecf.xlsx"

    def get_queries(self):
        """Définit les 5 requêtes du livrable 3.4"""
        return {
            "1_Aggregation": """
                SELECT rating, COUNT(*) as nb_livres, ROUND(AVG(price_gbp)::numeric, 2) as prix_moyen
                FROM fact_books GROUP BY rating ORDER BY rating DESC;
            """,
            "2_Jointure": """
                SELECT a.category, a.title as produit_1, b.title as produit_2, a.price
                FROM fact_products a
                JOIN fact_products b ON a.price = b.price AND a.category = b.category AND a.id < b.id
                LIMIT 20;
            """,
            "3_Window_Function": """
                SELECT category, title, price,
                RANK() OVER (PARTITION BY category ORDER BY price DESC) as rang_prix
                FROM fact_products
                WHERE price IS NOT NULL;
            """,
            "4_Top_N": """
                SELECT author, COUNT(*) as nb_citations
                FROM fact_quotes
                GROUP BY author
                ORDER BY nb_citations DESC
                LIMIT 5;
            """,
            "5_Cross_Source": """
                SELECT q.author, q.text as citation, b.title as livre_associe
                FROM fact_quotes q
                JOIN fact_books b ON b.title ILIKE '%' || q.tags || '%'
                WHERE q.tags <> ''
                LIMIT 15;
            """
        }

    def generate(self):
        print("🚀 Lancement de l'analyse décisionnelle...")
        queries = self.get_queries()
        
        # Utilisation de ExcelWriter pour gérer plusieurs onglets
        with pd.ExcelWriter(self.output_file, engine='openpyxl') as writer:
            for sheet_name, sql in queries.items():
                print(f"📊 Exécution de : {sheet_name}...")
                try:
                    df = pd.read_sql(sql, self.engine)
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
                except Exception as e:
                    print(f"❌ Erreur sur {sheet_name}: {e}")

        print(f"\n✅ Rapport terminé ! Disponible ici : {os.path.abspath(self.output_file)}")

if __name__ == "__main__":
    AnalyticsReporter().generate()