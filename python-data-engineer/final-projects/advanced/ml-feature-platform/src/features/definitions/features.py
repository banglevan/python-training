"""
Feature definitions and feature views.
"""

from datetime import timedelta
from feast import Feature, FeatureView, ValueType
from feast.data_source import FileSource, PushSource
from .entities import customer, product, store, order

# Customer Features
customer_features = FeatureView(
    name="customer_features",
    entities=[customer],
    ttl=timedelta(days=1),
    features=[
        Feature(name="total_orders", dtype=ValueType.INT64),
        Feature(name="total_amount", dtype=ValueType.FLOAT),
        Feature(name="avg_order_value", dtype=ValueType.FLOAT),
        Feature(name="days_since_last_order", dtype=ValueType.INT64),
        Feature(name="favorite_category", dtype=ValueType.STRING),
        Feature(name="order_frequency", dtype=ValueType.FLOAT)
    ],
    online=True,
    source=PushSource(
        name="customer_features_source",
        batch_source=FileSource(
            path="data/features/customer_features.parquet",
            timestamp_field="event_timestamp"
        )
    )
)

# Product Features
product_features = FeatureView(
    name="product_features",
    entities=[product],
    ttl=timedelta(days=1),
    features=[
        Feature(name="total_sales", dtype=ValueType.INT64),
        Feature(name="avg_price", dtype=ValueType.FLOAT),
        Feature(name="stock_level", dtype=ValueType.INT64),
        Feature(name="return_rate", dtype=ValueType.FLOAT),
        Feature(name="review_score", dtype=ValueType.FLOAT)
    ],
    online=True,
    source=PushSource(
        name="product_features_source",
        batch_source=FileSource(
            path="data/features/product_features.parquet",
            timestamp_field="event_timestamp"
        )
    )
)

# Store Features
store_features = FeatureView(
    name="store_features",
    entities=[store],
    ttl=timedelta(days=1),
    features=[
        Feature(name="daily_revenue", dtype=ValueType.FLOAT),
        Feature(name="customer_traffic", dtype=ValueType.INT64),
        Feature(name="inventory_turnover", dtype=ValueType.FLOAT),
        Feature(name="employee_count", dtype=ValueType.INT64)
    ],
    online=True,
    source=PushSource(
        name="store_features_source",
        batch_source=FileSource(
            path="data/features/store_features.parquet",
            timestamp_field="event_timestamp"
        )
    )
) 