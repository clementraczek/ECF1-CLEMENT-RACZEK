import pandas as pd
from sqlalchemy import create_engine, text
import os

class TableExplorerReporter:
    def __init__(self):
        self.url = "postgresql://dataeng:dataeng123@localhost:5433/analytics"
        self.engine = create_engine(self.url)
        self.output_dir = "sql"
        self.filename = "rapport_complet_tables_ecf.xlsx"
        self.output_path = os.path.join(self.output_dir, self.filename)
        
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

    def get_all_tables(self):
        query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_type = 'BASE TABLE';
        """
        with self.engine.connect() as conn:
            result = conn.execute(text(query))
            return [row[0] for row in result]

    def generate(self):
        print("Exploration database: postgresql://analytics")
        tables = self.get_all_tables()
        
        if not tables:
            print("Status: No tables found")
            return

        print(f"Status: {len(tables)} tables identified")

        try:
            with pd.ExcelWriter(self.output_path, engine='openpyxl') as writer:
                for table_name in tables:
                    print(f"Processing: {table_name}")
                    try:
                        query = f'SELECT * FROM "{table_name}" LIMIT 100'
                        df = pd.read_sql(query, self.engine)
                        sheet_name = table_name[:31]
                        df.to_excel(writer, sheet_name=sheet_name, index=False)
                    except Exception as e:
                        print(f"Error processing {table_name}: {str(e)}")

            print(f"Export completed: {os.path.abspath(self.output_path)}")
            
        except Exception as e:
            print(f"Critical error: {str(e)}")

if __name__ == "__main__":
    TableExplorerReporter().generate()