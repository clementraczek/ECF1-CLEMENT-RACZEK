from sqlalchemy import create_engine, text
import structlog

logger = structlog.get_logger()

class DatabaseResetter:
    def __init__(self):
        self.db_url = "postgresql://dataeng:dataeng123@localhost:5433/analytics"
        self.engine = create_engine(self.db_url)

    def reset(self):
        logger.info("db_reset_started", database="analytics")
        try:
            with self.engine.connect() as conn:
                conn.execute(text("DROP SCHEMA public CASCADE;"))
                conn.execute(text("CREATE SCHEMA public;"))
                conn.execute(text("GRANT ALL ON SCHEMA public TO public;"))
                conn.commit()
            
            logger.info("db_reset_success")
            print("Status: Success | Database: public schema reset")
            return True
        except Exception as e:
            logger.error("db_reset_failed", error=str(e))
            print(f"Status: Failure | Message: {str(e)}")
            return False

if __name__ == "__main__":
    resetter = DatabaseResetter()
    resetter.reset()