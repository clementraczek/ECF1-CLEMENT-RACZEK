import os
import sys
import argparse
import structlog
from datetime import datetime
from typing import Dict, Any
from sqlalchemy import create_engine, text

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.storage.minio_client import MinioClient

logger = structlog.get_logger()

class EcommercePipeline:
    def __init__(self):
        self.storage = MinioClient()
        self.db_url = "postgresql://dataeng:dataeng123@localhost:5433/analytics"
        self.engine = create_engine(self.db_url)
        self.stats = {
            "steps_completed": [],
            "start_time": None,
            "errors": []
        }

    def _run_subprocess(self, command: str, desc: str) -> bool:
        logger.info("executing_command", task=desc)
        result = os.system(command)
        if result == 0:
            self.stats["steps_completed"].append(desc)
            return True
        else:
            self.stats["errors"].append(desc)
            logger.error("command_failed", task=desc)
            return False

    def run(self, run_ingestion: bool = True, run_cleaning: bool = True, run_gold: bool = True) -> Dict[str, Any]:
        self.stats["start_time"] = datetime.now().isoformat()

        if run_ingestion or run_gold:
            print("Status: Initializing system reset")
            self._run_subprocess("python src/storage/reset_minio.py", "Reset MinIO")
            self._run_subprocess("python src/storage/reset_database.py", "Reset Database")
        
        if run_ingestion:
            print("Status: Extraction phase")
            self._run_subprocess("scrapy runspider src/ingestion/books_scraper.py", "Extract Books")
            self._run_subprocess("scrapy runspider src/ingestion/quotes_scraper.py", "Extract Quotes")
            self._run_subprocess("python src/ingestion/ecommerce_scraper.py", "Extract Ecommerce")
            self._run_subprocess("python src/ingestion/get_librairies.py", "Extract Libraries")

        if run_cleaning:
            print("Status: Transformation phase")
            self._run_subprocess("python src/processing/clean_books.py", "Transform Books")
            self._run_subprocess("python src/processing/clean_quotes.py", "Transform Quotes")
            self._run_subprocess("python src/processing/clean_commerce.py", "Transform Ecommerce")
            self._run_subprocess("python src/processing/librairies_geo.py", "Transform Libraries")

        if run_gold:
            print("Status: Loading phase")
            self._run_subprocess("python src/processing/data_to_postgre.py", "Load Postgres Data")
            self._run_subprocess("python src/processing/librairies_to_postgre.py", "Load Postgres Libraries")
            self._run_subprocess("python sql/sql_view.py", "Create Analytics Views")
            self._run_subprocess("python sql/sql_test.py", "Execute Quality Tests")
            


        self.stats["end_time"] = datetime.now().isoformat()
        return self.stats

    def print_analytics(self):
            print("\nSUMMARY: DATABASE STATUS")
            print("-" * 30)
            try:
                with self.engine.connect() as conn:
                    books = conn.execute(text("SELECT COUNT(*) FROM fact_books")).scalar()
                    quotes = conn.execute(text("SELECT COUNT(*) FROM fact_quotes")).scalar()
                    ecommerce = conn.execute(text("SELECT COUNT(*) FROM fact_products")).scalar()
                    partners = conn.execute(text("SELECT COUNT(*) FROM dim_partners")).scalar()

                    print(f"Books records      : {books}")
                    print(f"Quotes records     : {quotes}")
                    print(f"Ecommerce records  : {ecommerce}")
                    print(f"Library records    : {partners}")
            except Exception as e:
                print(f"Error: Analytics retrieval failed - {str(e)}")

def main():
    parser = argparse.ArgumentParser(description="ETL Pipeline Orchestrator")
    parser.add_argument("--all", action="store_true")
    parser.add_argument("--ingest", action="store_true")
    parser.add_argument("--clean", action="store_true")
    parser.add_argument("--gold", action="store_true")
    parser.add_argument("--analytics", action="store_true")

    args = parser.parse_args()
    pipeline = EcommercePipeline()

    if args.all:
        stats = pipeline.run()
    else:
        stats = pipeline.run(
            run_ingestion=args.ingest,
            run_cleaning=args.clean,
            run_gold=args.gold
        )

    print(f"\nExecution finished. Errors: {len(stats['errors'])}")
    
    if args.analytics:
        pipeline.print_analytics()

if __name__ == "__main__":
    main()