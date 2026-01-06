import pandas as pd
from sqlalchemy import create_engine
import os

class AnalyticsReporter:
    def __init__(self):
        self.url = "postgresql://dataeng:dataeng123@localhost:5433/analytics"
        self.engine = create_engine(self.url)
        self.output_dir = "sql"
        self.filename = "rapport_analytique_ecf.xlsx"
        self.output_path = os.path.join(self.output_dir, self.filename)
        
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

    def get_queries(self):
        return {
            "1_Aggregation": "SELECT rating, COUNT(*) as nb_livres FROM fact_books GROUP BY rating ORDER BY rating DESC;",
            "2_Condition": "SELECT title, price_gbp, availability FROM fact_books WHERE availability = 'In Stock' LIMIT 20;",
            "3_Window_Function": "SELECT category, title, price, RANK() OVER (PARTITION BY category ORDER BY price DESC) as rang_prix FROM fact_products;",
            "4_Top_N": "SELECT author, COUNT(*) as nb_citations FROM fact_quotes GROUP BY author ORDER BY nb_citations DESC LIMIT 5;"
        }

    def generate(self):
        queries = self.get_queries()
        results = {}
        
        # 1. Exécution et stockage en mémoire
        for sheet_name, sql in queries.items():
            print(f"Executing: {sheet_name}")
            try:
                df = pd.read_sql(sql, self.engine)
                if not df.empty:
                    results[sheet_name] = df
                else:
                    print(f"Status: {sheet_name} returned no data")
            except Exception as e:
                print(f"Error on {sheet_name}: {str(e)}")

        # 2. Condition d'écriture : seulement si results n'est pas vide
        if results:
            try:
                with pd.ExcelWriter(self.output_path, engine='openpyxl') as writer:
                    for sheet_name, df in results.items():
                        df.to_excel(writer, sheet_name=sheet_name, index=False)
                print(f"Status: Success | Exported: {self.output_path}")
            except Exception as e:
                print(f"Status: Failure | Write Error: {str(e)}")
        else:
            print("Status: Aborted | Message: No data found across all queries. File not created.")

if __name__ == "__main__":
    AnalyticsReporter().generate()