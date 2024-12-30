"""
Data source connectors package.

This package provides connectors for different data sources:
- Shopify: E-commerce platform
- WooCommerce: WordPress e-commerce
- ERP: Enterprise resource planning system
"""

from typing import Dict, Any, Optional, Type
import logging
from enum import Enum

from .base import DataConnector
from .shopify import ShopifyConnector
from .woocommerce import WooCommerceConnector
from .erp import ERPConnector

logger = logging.getLogger(__name__)

class ConnectorType(Enum):
    """Supported connector types."""
    SHOPIFY = 'shopify'
    WOOCOMMERCE = 'woocommerce'
    ERP = 'erp'

# Mapping of connector types to their implementations
CONNECTOR_REGISTRY: Dict[ConnectorType, Type[DataConnector]] = {
    ConnectorType.SHOPIFY: ShopifyConnector,
    ConnectorType.WOOCOMMERCE: WooCommerceConnector,
    ConnectorType.ERP: ERPConnector
}

def create_connector(
    connector_type: str,
    config: Dict[str, Any]
) -> Optional[DataConnector]:
    """
    Create a data source connector instance.

    Args:
        connector_type: Type of connector to create
        config: Connector configuration

    Returns:
        DataConnector instance or None if creation fails

    Example:
        >>> config = {
        ...     'name': 'my-shop',
        ...     'shop_url': 'shop.example.com',
        ...     'access_token': 'token123'
        ... }
        >>> connector = create_connector('shopify', config)
    """
    try:
        # Validate connector type
        try:
            conn_type = ConnectorType(connector_type.lower())
        except ValueError:
            logger.error(f"Unsupported connector type: {connector_type}")
            return None

        # Get connector class
        connector_class = CONNECTOR_REGISTRY.get(conn_type)
        if not connector_class:
            logger.error(f"No implementation found for: {connector_type}")
            return None

        # Create and initialize connector
        connector = connector_class(config)
        if not connector.initialize():
            logger.error(f"Failed to initialize {connector_type} connector")
            return None

        return connector

    except Exception as e:
        logger.error(f"Connector creation failed: {e}")
        return None

__all__ = [
    'DataConnector',
    'ShopifyConnector',
    'WooCommerceConnector',
    'ERPConnector',
    'ConnectorType',
    'create_connector'
] 