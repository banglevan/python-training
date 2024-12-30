"""
Platform initialization.
"""

from typing import Dict, Any, Optional
import logging
from pyspark.sql import SparkSession
from datetime import datetime

from src.config.settings import settings
from src.security.auth_manager import AuthManager
from src.security.access_control import AccessControl
from src.security.encryption_manager import EncryptionManager
from src.storage.delta_manager import DeltaManager
from src.storage.object_manager import ObjectManager
from src.storage.graph_manager import GraphManager
from src.storage.feature_store import FeatureStore

logging.basicConfig(level=settings.LOG_LEVEL)
logger = logging.getLogger(__name__)

class PlatformInitializer:
    """Platform initialization management."""
    
    def __init__(self):
        """Initialize platform components."""
        self.components = {}
        self.initialized = False
    
    def init_spark(self) -> None:
        """Initialize Spark session."""
        try:
            spark = (SparkSession.builder
                .appName(settings.APP_NAME)
                .master(settings.SPARK_MASTER)
                .config("spark.sql.warehouse.dir", settings.WAREHOUSE_PATH)
                .config("spark.sql.adaptive.enabled", settings.ENABLE_ADAPTIVE_EXECUTION)
                .config("spark.sql.optimizer.dynamicPartitionPruning.enabled",
                       settings.ENABLE_DYNAMIC_PARTITION_PRUNING)
                .getOrCreate())
            
            self.components['spark'] = spark
            logger.info("Initialized Spark session")
            
        except Exception as e:
            logger.error(f"Failed to initialize Spark: {e}")
            raise
    
    def init_security(self) -> None:
        """Initialize security components."""
        try:
            # Auth manager
            auth_manager = AuthManager(
                secret_key=settings.SECRET_KEY,
                token_expiry=settings.TOKEN_EXPIRY
            )
            
            # Access control
            access_control = AccessControl()
            
            # Encryption manager
            encryption_manager = EncryptionManager(
                master_key=settings.ENCRYPTION_KEY
            )
            
            self.components.update({
                'auth_manager': auth_manager,
                'access_control': access_control,
                'encryption_manager': encryption_manager
            })
            
            logger.info("Initialized security components")
            
        except Exception as e:
            logger.error(f"Failed to initialize security: {e}")
            raise
    
    def init_storage(self) -> None:
        """Initialize storage components."""
        try:
            # Delta manager
            delta_manager = DeltaManager(self.components['spark'])
            
            # Object manager
            object_manager = ObjectManager(
                endpoint=settings.MINIO_ENDPOINT,
                access_key=settings.MINIO_ACCESS_KEY,
                secret_key=settings.MINIO_SECRET_KEY,
                secure=settings.MINIO_SECURE
            )
            
            # Graph manager
            graph_manager = GraphManager(
                uri=settings.NEO4J_URI,
                username=settings.NEO4J_USER,
                password=settings.NEO4J_PASSWORD
            )
            
            # Feature store
            feature_store = FeatureStore(
                host=settings.REDIS_HOST,
                port=settings.REDIS_PORT,
                db=settings.REDIS_DB,
                password=settings.REDIS_PASSWORD
            )
            
            self.components.update({
                'delta_manager': delta_manager,
                'object_manager': object_manager,
                'graph_manager': graph_manager,
                'feature_store': feature_store
            })
            
            logger.info("Initialized storage components")
            
        except Exception as e:
            logger.error(f"Failed to initialize storage: {e}")
            raise
    
    def init_monitoring(self) -> None:
        """Initialize monitoring."""
        try:
            if settings.ENABLE_METRICS:
                # Create metrics tables
                self.components['spark'].sql("""
                    CREATE TABLE IF NOT EXISTS metrics (
                        timestamp TIMESTAMP,
                        metric_name STRING,
                        metric_value DOUBLE,
                        labels MAP<STRING, STRING>
                    ) USING DELTA
                    PARTITIONED BY (metric_name)
                """)
                
                # Create audit log table
                self.components['spark'].sql("""
                    CREATE TABLE IF NOT EXISTS audit_log (
                        timestamp TIMESTAMP,
                        user_id STRING,
                        action STRING,
                        resource_type STRING,
                        resource_id STRING,
                        status STRING,
                        details STRING
                    ) USING DELTA
                    PARTITIONED BY (date(timestamp))
                """)
                
                # Create alerts table
                self.components['spark'].sql("""
                    CREATE TABLE IF NOT EXISTS alerts (
                        timestamp TIMESTAMP,
                        severity STRING,
                        alert_type STRING,
                        message STRING,
                        details STRING
                    ) USING DELTA
                    PARTITIONED BY (severity)
                """)
                
                logger.info("Initialized monitoring tables")
                
        except Exception as e:
            logger.error(f"Failed to initialize monitoring: {e}")
            raise
    
    def init_platform(self) -> None:
        """Initialize all platform components."""
        try:
            if self.initialized:
                logger.warning("Platform already initialized")
                return
            
            # Initialize components
            self.init_spark()
            self.init_security()
            self.init_storage()
            self.init_monitoring()
            
            self.initialized = True
            logger.info(
                f"Platform initialized successfully at {datetime.now().isoformat()}"
            )
            
        except Exception as e:
            logger.error(f"Failed to initialize platform: {e}")
            raise
    
    def get_component(self, name: str) -> Any:
        """Get platform component."""
        if not self.initialized:
            raise ValueError("Platform not initialized")
            
        if name not in self.components:
            raise ValueError(f"Invalid component: {name}")
            
        return self.components[name]
    
    def shutdown(self) -> None:
        """Shutdown platform."""
        try:
            # Close connections
            if 'graph_manager' in self.components:
                self.components['graph_manager'].close()
            
            if 'spark' in self.components:
                self.components['spark'].stop()
            
            self.components.clear()
            self.initialized = False
            
            logger.info("Platform shutdown complete")
            
        except Exception as e:
            logger.error(f"Failed to shutdown platform: {e}")
            raise

# Create global instance
platform = PlatformInitializer() 