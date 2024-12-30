"""
Database connection management module.
"""

import logging
from typing import Dict, Any, Optional
from contextlib import contextmanager
import psycopg2
from psycopg2.extras import RealDictCursor
import pymongo
from redis import Redis
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from .config import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

Base = declarative_base()

class DatabaseManager:
    """Database connection manager."""
    
    def __init__(self):
        """Initialize database connections."""
        self.pg_config = {
            'host': config.get('database.postgres.host'),
            'port': config.get('database.postgres.port'),
            'user': config.get('database.postgres.user'),
            'password': config.get('database.postgres.password'),
            'dbname': config.get('database.postgres.dbname')
        }
        
        self.mongo_config = {
            'host': config.get('database.mongodb.host'),
            'port': config.get('database.mongodb.port'),
            'username': config.get('database.mongodb.user'),
            'password': config.get('database.mongodb.password')
        }
        
        self.redis_config = {
            'host': config.get('database.redis.host'),
            'port': config.get('database.redis.port'),
            'db': config.get('database.redis.db', 0)
        }
        
        # Initialize connections
        self.init_connections()
    
    def init_connections(self):
        """Initialize database connections."""
        try:
            # PostgreSQL SQLAlchemy engine
            pg_url = (
                f"postgresql://{self.pg_config['user']}:{self.pg_config['password']}"
                f"@{self.pg_config['host']}:{self.pg_config['port']}"
                f"/{self.pg_config['dbname']}"
            )
            self.engine = create_engine(pg_url)
            self.SessionLocal = sessionmaker(
                autocommit=False,
                autoflush=False,
                bind=self.engine
            )
            
            # MongoDB connection
            self.mongo_client = pymongo.MongoClient(**self.mongo_config)
            self.mongodb = self.mongo_client[
                config.get('database.mongodb.dbname', 'analytics')
            ]
            
            # Redis connection
            self.redis = Redis(**self.redis_config)
            
            logger.info("Database connections initialized successfully")
            
        except Exception as e:
            logger.error(f"Database initialization failed: {e}")
            raise
    
    @contextmanager
    def get_pg_conn(self):
        """Get PostgreSQL connection."""
        conn = None
        try:
            conn = psycopg2.connect(
                **self.pg_config,
                cursor_factory=RealDictCursor
            )
            yield conn
        finally:
            if conn:
                conn.close()
    
    def get_db(self):
        """Get database session."""
        db = self.SessionLocal()
        try:
            yield db
        finally:
            db.close()
    
    def init_db(self):
        """Initialize database schema."""
        try:
            Base.metadata.create_all(bind=self.engine)
            logger.info("Database schema created successfully")
        except Exception as e:
            logger.error(f"Database schema creation failed: {e}")
            raise
    
    def close(self):
        """Close all connections."""
        try:
            self.mongo_client.close()
            self.redis.close()
            logger.info("Database connections closed successfully")
        except Exception as e:
            logger.error(f"Database cleanup failed: {e}")

# Global database manager instance
db_manager = DatabaseManager() 