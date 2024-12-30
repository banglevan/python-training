"""
Data source connectors module.
"""

import logging
from typing import Dict, Any, List, Optional
from abc import ABC, abstractmethod
import pandas as pd
from confluent_kafka import Consumer, Producer
import json
from datetime import datetime, timedelta

from ..core.database import db_manager
from ..core.config import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataSource(ABC):
    """Abstract data source base class."""
    
    def __init__(self, source_config: Dict[str, Any]):
        """Initialize data source."""
        self.config = source_config
        self.name = source_config.get('name', 'unnamed_source')
    
    @abstractmethod
    def fetch_data(self, query: Dict[str, Any]) -> pd.DataFrame:
        """Fetch data from source."""
        pass
    
    @abstractmethod
    def get_schema(self) -> Dict[str, Any]:
        """Get data source schema."""
        pass
    
    @abstractmethod
    def validate_query(self, query: Dict[str, Any]) -> bool:
        """Validate query parameters."""
        pass

class PostgresSource(DataSource):
    """PostgreSQL data source."""
    
    def __init__(self, source_config: Dict[str, Any]):
        """Initialize PostgreSQL source."""
        super().__init__(source_config)
        self.table = source_config['table']
    
    def fetch_data(self, query: Dict[str, Any]) -> pd.DataFrame:
        """Fetch data from PostgreSQL."""
        try:
            with db_manager.get_pg_conn() as conn:
                # Build query
                sql = f"SELECT * FROM {self.table}"
                params = []
                
                # Add filters
                if 'filters' in query:
                    conditions = []
                    for field, value in query['filters'].items():
                        conditions.append(f"{field} = %s")
                        params.append(value)
                    if conditions:
                        sql += " WHERE " + " AND ".join(conditions)
                
                # Add time range
                if 'time_range' in query:
                    time_field = query['time_range'].get('field', 'created_at')
                    start_time = query['time_range'].get('start')
                    end_time = query['time_range'].get('end')
                    
                    if 'WHERE' not in sql:
                        sql += " WHERE"
                    else:
                        sql += " AND"
                    
                    sql += f" {time_field} BETWEEN %s AND %s"
                    params.extend([start_time, end_time])
                
                # Add grouping
                if 'group_by' in query:
                    sql += f" GROUP BY {', '.join(query['group_by'])}"
                
                # Execute query
                return pd.read_sql(sql, conn, params=params)
                
        except Exception as e:
            logger.error(f"PostgreSQL query failed: {e}")
            raise
    
    def get_schema(self) -> Dict[str, Any]:
        """Get PostgreSQL table schema."""
        try:
            with db_manager.get_pg_conn() as conn:
                sql = """
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = %s
                """
                df = pd.read_sql(sql, conn, params=[self.table])
                return dict(zip(df.column_name, df.data_type))
                
        except Exception as e:
            logger.error(f"Schema retrieval failed: {e}")
            raise
    
    def validate_query(self, query: Dict[str, Any]) -> bool:
        """Validate PostgreSQL query."""
        try:
            schema = self.get_schema()
            
            # Validate filters
            if 'filters' in query:
                for field in query['filters'].keys():
                    if field not in schema:
                        return False
            
            # Validate group by
            if 'group_by' in query:
                for field in query['group_by']:
                    if field not in schema:
                        return False
            
            return True
            
        except Exception:
            return False

class MongoSource(DataSource):
    """MongoDB data source."""
    
    def __init__(self, source_config: Dict[str, Any]):
        """Initialize MongoDB source."""
        super().__init__(source_config)
        self.collection = source_config['collection']
        self.db = db_manager.mongodb
    
    def fetch_data(self, query: Dict[str, Any]) -> pd.DataFrame:
        """Fetch data from MongoDB."""
        try:
            # Build query
            mongo_query = {}
            
            # Add filters
            if 'filters' in query:
                mongo_query.update(query['filters'])
            
            # Add time range
            if 'time_range' in query:
                time_field = query['time_range'].get('field', 'created_at')
                mongo_query[time_field] = {
                    '$gte': query['time_range']['start'],
                    '$lte': query['time_range']['end']
                }
            
            # Execute query
            cursor = self.db[self.collection].find(mongo_query)
            
            # Convert to DataFrame
            data = list(cursor)
            if not data:
                return pd.DataFrame()
            
            df = pd.DataFrame(data)
            
            # Apply grouping
            if 'group_by' in query:
                group_cols = query['group_by']
                agg_cols = [c for c in df.columns if c not in group_cols]
                df = df.groupby(group_cols)[agg_cols].agg('sum').reset_index()
            
            return df
            
        except Exception as e:
            logger.error(f"MongoDB query failed: {e}")
            raise
    
    def get_schema(self) -> Dict[str, Any]:
        """Get MongoDB collection schema."""
        try:
            # Sample document to infer schema
            sample = self.db[self.collection].find_one()
            if not sample:
                return {}
            
            def get_type(value: Any) -> str:
                if isinstance(value, (int, float)):
                    return 'number'
                elif isinstance(value, bool):
                    return 'boolean'
                elif isinstance(value, datetime):
                    return 'datetime'
                else:
                    return 'string'
            
            return {k: get_type(v) for k, v in sample.items()}
            
        except Exception as e:
            logger.error(f"Schema retrieval failed: {e}")
            raise
    
    def validate_query(self, query: Dict[str, Any]) -> bool:
        """Validate MongoDB query."""
        try:
            schema = self.get_schema()
            
            # Validate filters
            if 'filters' in query:
                for field in query['filters'].keys():
                    if field not in schema:
                        return False
            
            # Validate group by
            if 'group_by' in query:
                for field in query['group_by']:
                    if field not in schema:
                        return False
            
            return True
            
        except Exception:
            return False

class KafkaSource(DataSource):
    """Kafka data source."""
    
    def __init__(self, source_config: Dict[str, Any]):
        """Initialize Kafka source."""
        super().__init__(source_config)
        self.topic = source_config['topic']
        self.consumer_config = {
            'bootstrap.servers': config.get('kafka.bootstrap_servers'),
            'group.id': f"analytics_{self.name}",
            'auto.offset.reset': 'earliest'
        }
        self.producer_config = {
            'bootstrap.servers': config.get('kafka.bootstrap_servers')
        }
    
    def fetch_data(self, query: Dict[str, Any]) -> pd.DataFrame:
        """Fetch data from Kafka."""
        try:
            consumer = Consumer(self.consumer_config)
            consumer.subscribe([self.topic])
            
            messages = []
            start_time = datetime.now()
            timeout = query.get('timeout', 5)  # Default 5 seconds
            
            while (datetime.now() - start_time).seconds < timeout:
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    logger.error(f"Kafka consumer error: {msg.error()}")
                    continue
                
                try:
                    data = json.loads(msg.value().decode('utf-8'))
                    messages.append(data)
                except Exception as e:
                    logger.error(f"Message parsing failed: {e}")
            
            consumer.close()
            
            if not messages:
                return pd.DataFrame()
            
            return pd.DataFrame(messages)
            
        except Exception as e:
            logger.error(f"Kafka fetch failed: {e}")
            raise
    
    def get_schema(self) -> Dict[str, Any]:
        """Get Kafka topic schema."""
        try:
            # Sample message to infer schema
            df = self.fetch_data({'timeout': 1})
            if df.empty:
                return {}
            
            return dict(df.dtypes.apply(str))
            
        except Exception as e:
            logger.error(f"Schema retrieval failed: {e}")
            raise
    
    def validate_query(self, query: Dict[str, Any]) -> bool:
        """Validate Kafka query."""
        required_fields = ['timeout']
        return all(field in query for field in required_fields)

class SourceFactory:
    """Data source factory."""
    
    @staticmethod
    def create_source(source_config: Dict[str, Any]) -> DataSource:
        """Create data source instance."""
        source_type = source_config['type'].lower()
        
        if source_type == 'postgres':
            return PostgresSource(source_config)
        elif source_type == 'mongodb':
            return MongoSource(source_config)
        elif source_type == 'kafka':
            return KafkaSource(source_config)
        else:
            raise ValueError(f"Unsupported source type: {source_type}") 