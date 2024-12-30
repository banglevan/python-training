"""
Database utility module.
"""

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager
import logging
from typing import Generator
from src.utils.config import Config

logger = logging.getLogger(__name__)

class Database:
    """Database connection manager."""
    
    def __init__(self, config: Config):
        """Initialize database."""
        self.config = config
        self.engine = None
        self.Session = None
        self._connect()
    
    def _connect(self):
        """Establish database connection."""
        try:
            # Create connection URL
            db_config = self.config.sources['warehouse']
            url = (
                f"postgresql://{db_config['username']}:{db_config['password']}"
                f"@{db_config['host']}:{db_config['port']}"
                f"/{db_config['database']}"
            )
            
            # Create engine
            self.engine = create_engine(url)
            self.Session = sessionmaker(bind=self.engine)
            
            # Test connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            logger.info("Connected to database")
            
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
    
    @contextmanager
    def session(self) -> Generator:
        """Get database session."""
        session = self.Session()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
    
    def close(self):
        """Close database connection."""
        if self.engine:
            self.engine.dispose()
            logger.info("Closed database connection") 