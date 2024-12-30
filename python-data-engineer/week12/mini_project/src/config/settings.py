"""
Platform configuration settings.
"""

from typing import Dict, Any, Optional
import os
from pydantic import BaseSettings
from datetime import timedelta

class Settings(BaseSettings):
    """Platform settings."""
    
    # Application
    APP_NAME: str = "Data Platform"
    APP_VERSION: str = "1.0.0"
    DEBUG: bool = False
    
    # Security
    SECRET_KEY: str = os.getenv("SECRET_KEY", "your-secret-key")
    TOKEN_EXPIRY: timedelta = timedelta(hours=24)
    ENCRYPTION_KEY: Optional[str] = os.getenv("ENCRYPTION_KEY")
    
    # Database
    SPARK_MASTER: str = "local[*]"
    WAREHOUSE_PATH: str = "./warehouse"
    
    # Storage
    MINIO_ENDPOINT: str = "localhost:9000"
    MINIO_ACCESS_KEY: str = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    MINIO_SECRET_KEY: str = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    MINIO_SECURE: bool = False
    
    # Cache
    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379
    REDIS_DB: int = 0
    REDIS_PASSWORD: Optional[str] = None
    
    # Graph Database
    NEO4J_URI: str = "bolt://localhost:7687"
    NEO4J_USER: str = "neo4j"
    NEO4J_PASSWORD: str = os.getenv("NEO4J_PASSWORD", "password")
    
    # Feature Store
    FEATURE_STORE_TYPE: str = "redis"
    FEATURE_TTL: int = 3600  # 1 hour
    
    # Query Optimization
    MAX_QUERY_EXECUTION_TIME: int = 300  # seconds
    ENABLE_ADAPTIVE_EXECUTION: bool = True
    ENABLE_DYNAMIC_PARTITION_PRUNING: bool = True
    
    # Monitoring
    ENABLE_METRICS: bool = True
    METRICS_PORT: int = 9090
    LOG_LEVEL: str = "INFO"
    
    class Config:
        env_file = ".env"
        case_sensitive = True

settings = Settings() 