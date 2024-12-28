"""
Unit tests cho E-commerce System
Tiêu chí đánh giá:
1. Product Models (30%): Physical, Digital, Subscription
2. Shopping Cart (35%): Cart operations
3. Order Processing (35%): Order workflow
"""

import pytest
from datetime import datetime, timedelta

from ecommerce.models import (
    Product,
    PhysicalProduct,
    DigitalProduct,
    SubscriptionProduct,
    ProductCategory,
    ProductStatus,
    OrderStatus,
    Discount,
    ShoppingCart,
    CartItem,
    Order,
    OrderItem
)
from ecommerce.cart import CartManager
from ecommerce.order import OrderProcessor

@pytest.fixture
def sample_products():
    """Fixture cho products."""
    laptop = PhysicalProduct(
        id=1,
        name="Laptop",
        price=1000,
        category=ProductCategory.ELECTRONICS,
        weight=2.5,
        dimensions=(35, 25, 2),
        description="High-end laptop"
    )
    
    ebook = DigitalProduct(
        id=2,
        name="Python Book",
        price=30,
        category=ProductCategory.BOOKS,
        file_size=10.5,
        format="PDF",
        description="Python programming book"
    )
    
    netflix = SubscriptionProduct(
        id=3,
        name="Netflix Premium",
        price=15,
        category=ProductCategory.OTHER,
        billing_period="monthly",
        features=["4K", "4 screens"],
        description="Netflix subscription"
    )
    
    return {
        "laptop": laptop,
        "ebook": ebook,
        "netflix": netflix
    }

@pytest.fixture
def sample_discount():
    """Fixture cho discount."""
    return Discount(
        code="SAVE20",
        percentage=20,
        min_amount=100,
        expiry_date=datetime.now() + timedelta(days=7)
    )

class TestProductModels:
    """
    Test product models (30%)
    Pass: Models hoạt động đúng ≥ 95%
    Fail: Models hoạt động đúng < 95%
    """
    
    def test_physical_product(self, sample_products):
        """Test PhysicalProduct."""
        laptop = sample_products["laptop"]
        
        # Properties
        assert laptop.id == 1
        assert laptop.name == "Laptop"
        assert laptop.price == 1000
        assert laptop.category == ProductCategory.ELECTRONICS
        assert laptop.weight == 2.5
        assert laptop.dimensions == (35, 25, 2)
        
        # Shipping calculation
        shipping = laptop.calculate_shipping()
        assert shipping == 25  # 10 * 2.5
        
        # Status
        assert laptop.status == ProductStatus.AVAILABLE
        
        # String representation
        assert "PhysicalProduct" in str(laptop)
        assert "Laptop" in str(laptop)

    def test_digital_product(self, sample_products):
        """Test DigitalProduct."""
        ebook = sample_products["ebook"]
        
        # Properties
        assert ebook.id == 2
        assert ebook.name == "Python Book"
        assert ebook.price == 30
        assert ebook.category == ProductCategory.BOOKS
        assert ebook.file_size == 10.5
        assert ebook.format == "PDF"
        
        # No shipping
        assert ebook.calculate_shipping() == 0
        
        # String representation
        assert "DigitalProduct" in str(ebook)
        assert "Python Book" in str(ebook)

    def test_subscription_product(self, sample_products):
        """Test SubscriptionProduct."""
        netflix = sample_products["netflix"]
        
        # Properties
        assert netflix.id == 3
        assert netflix.name == "Netflix Premium"
        assert netflix.price == 15
        assert netflix.category == ProductCategory.OTHER
        assert netflix.billing_period == "monthly"
        assert "4K" in netflix.features
        
        # No shipping
        assert netflix.calculate_shipping() == 0
        
        # String representation
        assert "SubscriptionProduct" in str(netflix)
        assert "Netflix Premium" in str(netflix)

class TestShoppingCart:
    """
    Test shopping cart (35%)
    Pass: Cart hoạt động đúng ≥ 95%
    Fail: Cart hoạt động đúng < 95%
    """
    
    def test_cart_operations(self, sample_products, sample_discount):
        """Test cart operations."""
        cart_manager = CartManager()
        customer_id = 1
        
        # Add items
        assert cart_manager.add_item(
            customer_id,
            sample_products["laptop"]
        )
        assert cart_manager.add_item(
            customer_id,
            sample_products["ebook"],
            2
        )
        
        # Get cart
        cart = cart_manager.get_cart(customer_id)
        assert len(cart.items) == 2
        
        # Update quantity
        assert cart_manager.update_quantity(
            customer_id,
            sample_products["laptop"].id,
            2
        )
        
        # Remove item
        assert cart_manager.remove_item(
            customer_id,
            sample_products["ebook"].id
        )
        assert len(cart.items) == 1
        
        # Apply discount
        assert cart_manager.apply_discount(
            customer_id,
            sample_discount
        )
        
        # Calculate totals
        assert cart.calculate_subtotal() == 2000  # 1000 * 2
        assert cart.calculate_shipping() == 50    # 25 * 2
        assert cart.calculate_discount() == 400   # 2000 * 0.2
        assert cart.calculate_total() == 1650     # 2000 + 50 - 400
        
        # Clear cart
        cart_manager.clear_cart(customer_id)
        assert len(cart.items) == 0

class TestOrderProcessing:
    """
    Test order processing (35%)
    Pass: Order workflow hoạt động đúng ≥ 95%
    Fail: Order workflow hoạt động đúng < 95%
    """
    
    def test_order_workflow(
        self,
        sample_products,
        sample_discount
    ):
        """Test order workflow."""
        cart_manager = CartManager()
        order_processor = OrderProcessor()
        customer_id = 1
        
        # Create cart
        cart_manager.add_item(
            customer_id,
            sample_products["laptop"]
        )
        cart_manager.add_item(
            customer_id,
            sample_products["ebook"],
            2
        )
        cart_manager.apply_discount(
            customer_id,
            sample_discount
        )
        
        # Create order
        cart = cart_manager.get_cart(customer_id)
        order = order_processor.create_order(
            customer_id,
            cart
        )
        assert order is not None
        assert order.status == OrderStatus.PENDING
        
        # Confirm order
        assert order_processor.confirm_order(order.id)
        assert order.status == OrderStatus.CONFIRMED
        
        # Ship order
        assert order_processor.ship_order(order.id)
        assert order.status == OrderStatus.SHIPPED
        
        # Deliver order
        assert order_processor.deliver_order(order.id)
        assert order.status == OrderStatus.DELIVERED
        
        # Get order history
        history = order_processor.get_order_history(customer_id)
        assert len(history) == 1
        order, action = history[0]
        assert action == "Delivered"
        
        # Try to cancel delivered order
        assert not order_processor.cancel_order(order.id)
        
        # Create another order and cancel it
        cart_manager.add_item(
            customer_id,
            sample_products["netflix"]
        )
        cart = cart_manager.get_cart(customer_id)
        order = order_processor.create_order(
            customer_id,
            cart
        )
        assert order_processor.cancel_order(order.id)
        assert order.status == OrderStatus.CANCELLED
        
        # Get customer orders
        pending_orders = order_processor.get_customer_orders(
            customer_id,
            OrderStatus.PENDING
        )
        assert len(pending_orders) == 0
        
        delivered_orders = order_processor.get_customer_orders(
            customer_id,
            OrderStatus.DELIVERED
        )
        assert len(delivered_orders) == 1
        
        cancelled_orders = order_processor.get_customer_orders(
            customer_id,
            OrderStatus.CANCELLED
        )
        assert len(cancelled_orders) == 1 