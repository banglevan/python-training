"""
E-commerce models
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import List, Optional, Dict

class ProductCategory(Enum):
    """Danh mục sản phẩm."""
    ELECTRONICS = "electronics"
    CLOTHING = "clothing"
    BOOKS = "books"
    FOOD = "food"
    OTHER = "other"

class ProductStatus(Enum):
    """Trạng thái sản phẩm."""
    AVAILABLE = "available"
    OUT_OF_STOCK = "out_of_stock"
    DISCONTINUED = "discontinued"

class OrderStatus(Enum):
    """Trạng thái đơn hàng."""
    PENDING = "pending"
    CONFIRMED = "confirmed"
    SHIPPED = "shipped"
    DELIVERED = "delivered"
    CANCELLED = "cancelled"

@dataclass
class Discount:
    """Giảm giá."""
    code: str
    percentage: float
    min_amount: float
    expiry_date: datetime

class Product(ABC):
    """Abstract base class cho sản phẩm."""
    
    def __init__(
        self,
        id: int,
        name: str,
        price: float,
        category: ProductCategory,
        description: Optional[str] = None
    ):
        """
        Khởi tạo sản phẩm.
        
        Args:
            id: ID sản phẩm
            name: Tên sản phẩm
            price: Giá
            category: Danh mục
            description: Mô tả
        """
        self.id = id
        self.name = name
        self.price = price
        self.category = category
        self.description = description
        self.status = ProductStatus.AVAILABLE
    
    @abstractmethod
    def calculate_shipping(self) -> float:
        """Tính phí vận chuyển."""
        pass
    
    def __str__(self) -> str:
        """String representation."""
        return (
            f"{self.__class__.__name__}("
            f"id={self.id}, "
            f"name={self.name}, "
            f"price={self.price}, "
            f"category={self.category.value})"
        )

class PhysicalProduct(Product):
    """Sản phẩm vật lý."""
    
    def __init__(
        self,
        id: int,
        name: str,
        price: float,
        category: ProductCategory,
        weight: float,
        dimensions: tuple,
        description: Optional[str] = None
    ):
        """
        Khởi tạo sản phẩm vật lý.
        
        Args:
            weight: Khối lượng (kg)
            dimensions: Kích thước (length, width, height) cm
        """
        super().__init__(id, name, price, category, description)
        self.weight = weight
        self.dimensions = dimensions
    
    def calculate_shipping(self) -> float:
        """Tính phí vận chuyển dựa trên khối lượng."""
        base_rate = 10  # Base shipping rate
        return base_rate * self.weight

class DigitalProduct(Product):
    """Sản phẩm số."""
    
    def __init__(
        self,
        id: int,
        name: str,
        price: float,
        category: ProductCategory,
        file_size: float,
        format: str,
        description: Optional[str] = None
    ):
        """
        Khởi tạo sản phẩm số.
        
        Args:
            file_size: Kích thước file (MB)
            format: Định dạng file
        """
        super().__init__(id, name, price, category, description)
        self.file_size = file_size
        self.format = format
    
    def calculate_shipping(self) -> float:
        """Không có phí vận chuyển."""
        return 0

class SubscriptionProduct(Product):
    """Sản phẩm subscription."""
    
    def __init__(
        self,
        id: int,
        name: str,
        price: float,
        category: ProductCategory,
        billing_period: str,
        features: List[str],
        description: Optional[str] = None
    ):
        """
        Khởi tạo subscription.
        
        Args:
            billing_period: Chu kỳ thanh toán
            features: Các tính năng
        """
        super().__init__(id, name, price, category, description)
        self.billing_period = billing_period
        self.features = features
    
    def calculate_shipping(self) -> float:
        """Không có phí vận chuyển."""
        return 0

@dataclass
class CartItem:
    """Item trong giỏ hàng."""
    product: Product
    quantity: int

class ShoppingCart:
    """Giỏ hàng."""
    
    def __init__(self):
        """Khởi tạo giỏ hàng."""
        self.items: List[CartItem] = []
        self.discount: Optional[Discount] = None
    
    def add_item(self, product: Product, quantity: int = 1):
        """Thêm sản phẩm vào giỏ."""
        # Check if product already in cart
        for item in self.items:
            if item.product.id == product.id:
                item.quantity += quantity
                return
        
        # Add new item
        self.items.append(CartItem(product, quantity))
    
    def remove_item(self, product_id: int):
        """Xóa sản phẩm khỏi giỏ."""
        self.items = [
            item for item in self.items
            if item.product.id != product_id
        ]
    
    def update_quantity(self, product_id: int, quantity: int):
        """Cập nhật số lượng."""
        for item in self.items:
            if item.product.id == product_id:
                item.quantity = quantity
                return
    
    def apply_discount(self, discount: Discount):
        """Áp dụng mã giảm giá."""
        if discount.expiry_date > datetime.now():
            self.discount = discount
    
    def calculate_subtotal(self) -> float:
        """Tính tổng tiền hàng."""
        return sum(
            item.product.price * item.quantity
            for item in self.items
        )
    
    def calculate_shipping(self) -> float:
        """Tính tổng phí vận chuyển."""
        return sum(
            item.product.calculate_shipping() * item.quantity
            for item in self.items
        )
    
    def calculate_discount(self) -> float:
        """Tính số tiền giảm giá."""
        if not self.discount:
            return 0
            
        subtotal = self.calculate_subtotal()
        if subtotal >= self.discount.min_amount:
            return subtotal * (self.discount.percentage / 100)
        return 0
    
    def calculate_total(self) -> float:
        """Tính tổng tiền."""
        subtotal = self.calculate_subtotal()
        shipping = self.calculate_shipping()
        discount = self.calculate_discount()
        return subtotal + shipping - discount
    
    def clear(self):
        """Xóa giỏ hàng."""
        self.items.clear()
        self.discount = None
    
    def __str__(self) -> str:
        """String representation."""
        return "\n".join(
            f"{item.product.name}: {item.quantity}"
            for item in self.items
        )

@dataclass
class OrderItem:
    """Item trong đơn hàng."""
    product_id: int
    product_name: str
    quantity: int
    unit_price: float

class Order:
    """Đơn hàng."""
    
    def __init__(
        self,
        id: int,
        customer_id: int,
        items: List[OrderItem]
    ):
        """
        Khởi tạo đơn hàng.
        
        Args:
            id: ID đơn hàng
            customer_id: ID khách hàng
            items: Các sản phẩm
        """
        self.id = id
        self.customer_id = customer_id
        self.items = items
        self.status = OrderStatus.PENDING
        self.created_at = datetime.now()
        self.updated_at = datetime.now()
        
        # Calculate totals
        self.subtotal = sum(
            item.quantity * item.unit_price
            for item in items
        )
        self.shipping = 0  # Will be calculated later
        self.discount = 0  # Will be calculated later
        self.total = self.subtotal
    
    def confirm(self):
        """Xác nhận đơn hàng."""
        self.status = OrderStatus.CONFIRMED
        self.updated_at = datetime.now()
    
    def ship(self):
        """Giao hàng."""
        self.status = OrderStatus.SHIPPED
        self.updated_at = datetime.now()
    
    def deliver(self):
        """Đã giao hàng."""
        self.status = OrderStatus.DELIVERED
        self.updated_at = datetime.now()
    
    def cancel(self):
        """Hủy đơn hàng."""
        self.status = OrderStatus.CANCELLED
        self.updated_at = datetime.now()
    
    def __str__(self) -> str:
        """String representation."""
        return (
            f"Order(id={self.id}, "
            f"status={self.status.value}, "
            f"total={self.total})"
        ) 