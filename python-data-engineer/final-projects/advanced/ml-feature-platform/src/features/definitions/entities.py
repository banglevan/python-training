"""
Entity definitions for feature views.
"""

from feast import Entity, ValueType

# Customer entity
customer = Entity(
    name="customer",
    value_type=ValueType.INT64,
    description="Customer identifier",
    join_key="customer_id",
)

# Product entity
product = Entity(
    name="product",
    value_type=ValueType.STRING,
    description="Product identifier",
    join_key="product_id",
)

# Store entity
store = Entity(
    name="store",
    value_type=ValueType.INT64,
    description="Store identifier",
    join_key="store_id",
)

# Order entity
order = Entity(
    name="order",
    value_type=ValueType.STRING,
    description="Order identifier",
    join_key="order_id",
) 