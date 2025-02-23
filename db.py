import psycopg2
from psycopg2 import sql, pool
from psycopg2.extras import DictCursor
import logging
from typing import List, Tuple, Optional
from config import DB_CONFIG  # Import database configuration



# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database setup and maintenance class
class NewsDatabase:
    def __init__(self, db_config: dict, min_conn: int = 1, max_conn: int = 5):
        """
        Initialize the database connection pool using a configuration dictionary.

        :param db_config: A dictionary containing database connection parameters.
                         Expected keys: dbname, user, password, host, port.
        :param min_conn: Minimum number of connections in the pool.
        :param max_conn: Maximum number of connections in the pool.
        """
        self.db_config = db_config
        self.connection_pool = psycopg2.pool.SimpleConnectionPool(
            min_conn, max_conn,
            dbname=db_config.get("dbname"),
            user=db_config.get("user"),
            password=db_config.get("password"),
            host=db_config.get("host"),
            port=db_config.get("port")
        )
        self.init_db()

    def connect(self):
        """Get a connection from the connection pool."""
        return self.connection_pool.getconn()

    def release_connection(self, conn):
        """Release a connection back to the pool."""
        self.connection_pool.putconn(conn)

    def init_db(self):
        """Initialize the database schema."""
        conn = self.connect()
        try:
            with conn.cursor() as cursor:
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS news (
                        id SERIAL PRIMARY KEY,
                        title TEXT,
                        link TEXT UNIQUE,
                        summary TEXT,
                        published TIMESTAMP,
                        source TEXT
                    )
                ''')
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS sources (
                        id SERIAL PRIMARY KEY,
                        name TEXT,
                        url TEXT UNIQUE
                    )
                ''')
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS aggregated_news (
                        id SERIAL PRIMARY KEY,
                        title TEXT,
                        summary TEXT,
                        rank INTEGER
                    )
                ''')
                conn.commit()
                logger.info("Database schema initialized.")
        except Exception as e:
            logger.error(f"Error initializing database: {e}")
        finally:
            self.release_connection(conn)

    def insert_news(self, title: str, link: str, summary: str, published: str, source: str) -> bool:
        """Insert a news article into the database."""
        conn = self.connect()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO news (title, link, summary, published, source) VALUES (%s, %s, %s, %s, %s)",
                    (title, link, summary, published, source)
                )
                conn.commit()
                logger.info(f"Inserted news: {title}")
                return True
        except psycopg2.IntegrityError:
            logger.warning(f"Duplicate news article skipped: {link}")
            return False
        except Exception as e:
            logger.error(f"Error inserting news: {e}")
            return False
        finally:
            self.release_connection(conn)

    def insert_source(self, name: str, url: str) -> bool:
        """Insert a news source into the database."""
        conn = self.connect()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO sources (name, url) VALUES (%s, %s)",
                    (name, url)
                )
                conn.commit()
                logger.info(f"Inserted source: {name}")
                return True
        except psycopg2.IntegrityError:
            logger.warning(f"Duplicate source skipped: {url}")
            return False
        except Exception as e:
            logger.error(f"Error inserting source: {e}")
            return False
        finally:
            self.release_connection(conn)

    def get_sources(self) -> List[Tuple[str, str]]:
        """Fetch all news sources from the database."""
        conn = self.connect()
        try:
            with conn.cursor(cursor_factory=DictCursor) as cursor:
                cursor.execute("SELECT name, url FROM sources")
                sources = cursor.fetchall()
                return sources
        except Exception as e:
            logger.error(f"Error fetching sources: {e}")
            return []
        finally:
            self.release_connection(conn)

    def close(self):
        """Close all connections in the pool."""
        self.connection_pool.closeall()
        logger.info("Database connection pool closed.")


if __name__ == "__main__":
    # Import the database configuration from config.py
    from config import DB_CONFIG

    # Initialize the database using the configuration
    db = NewsDatabase(db_config=DB_CONFIG)
    print("PostgreSQL database initialized.")

    # Optionally, you can add some test operations here
    # For example, inserting a test source or news article
    db.insert_source("CBC", "https://www.cbc.ca/")
    """
     db.insert_news(
        title="Test News2",
        link="https://test.com/test-news",
        summary="This is a test news article.",
        published="2023-10-01 12:00:00",
        source="Test Source"
    )   
    
    """


    # Fetch and print sources to verify the database is working
    sources = db.get_sources()
    print("Sources in the database:", sources)

    # Close the database connection pool
    db.close()