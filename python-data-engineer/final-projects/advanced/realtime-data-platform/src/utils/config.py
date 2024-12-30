"""
Configuration management utility.
"""

import yaml
import os
from typing import Dict, Any
from dataclasses import dataclass

@dataclass
class KafkaConfig:
    """Kafka configuration."""
    bootstrap_servers: list
    topics: Dict[str, Dict[str, Any]]
    consumer_group: str

@dataclass
class SparkConfig:
    """Spark configuration."""
    master: str
    app_name: str
    packages: list
    properties: Dict[str, str]

@dataclass
class StorageConfig:
    """Storage configuration."""
    delta_path: str
    elasticsearch_host: str
    elasticsearch_port: int

class Config:
    """Configuration manager."""
    
    def __init__(self, config_path: str = "config"):
        """Initialize configuration."""
        self.config_path = config_path
        self.kafka = self._load_kafka_config()
        self.spark = self._load_spark_config()
        self.storage = self._load_storage_config()
    
    def _load_kafka_config(self) -> KafkaConfig:
        """Load Kafka configuration."""
        with open(f"{self.config_path}/kafka/kafka.yml") as f:
            config = yaml.safe_load(f)
        
        return KafkaConfig(
            bootstrap_servers=config['bootstrap_servers'],
            topics=config['topics'],
            consumer_group=config['consumer_group']
        )
    
    def _load_spark_config(self) -> SparkConfig:
        """Load Spark configuration."""
        with open(f"{self.config_path}/spark/spark.yml") as f:
            config = yaml.safe_load(f)
        
        return SparkConfig(
            master=config['master'],
            app_name=config['app_name'],
            packages=config['packages'],
            properties=config['properties']
        )
    
    def _load_storage_config(self) -> StorageConfig:
        """Load storage configuration."""
        with open(f"{self.config_path}/storage.yml") as f:
            config = yaml.safe_load(f)
        
        return StorageConfig(
            delta_path=config['delta_path'],
            elasticsearch_host=config['elasticsearch_host'],
            elasticsearch_port=config['elasticsearch_port']
        ) 