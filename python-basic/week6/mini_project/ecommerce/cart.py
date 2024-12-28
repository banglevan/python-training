"""
Shopping cart module
"""

from datetime import datetime
from typing import Dict, List, Optional

from .models import (
    Product,
    ShoppingCart,
    CartItem,
    Discount,
    ProductStatus
)

class CartManager:
    """Quản lý giỏ hàng."""
    
    def __init__(self):
        """Khởi tạo manager."""
        self.carts: Dict[int, ShoppingCart] = {}  # customer_id -> cart
    
    def get_cart(self, customer_id: int) -> ShoppingCart:
        """
        Lấy giỏ hàng của khách.
        Tạo mới nếu chưa có.
        """
        if customer_id not in self.carts:
            self.carts[customer_id] = ShoppingCart()
        return self.carts[customer_id]
    
    def add_item(
        self,
        customer_id: int,
        product: Product,
        quantity: int = 1
    ) -> bool:
        """
        Thêm sản phẩm vào giỏ.
        
        Args:
            customer_id: ID khách hàng
            product: Sản phẩm
            quantity: Số lượng
            
        Returns:
            True nếu thành công
        """
        if product.status != ProductStatus.AVAILABLE:
            return False
            
        if quantity <= 0:
            return False
            
        cart = self.get_cart(customer_id)
        cart.add_item(product, quantity)
        return True
    
    def remove_item(
        self,
        customer_id: int,
        product_id: int
    ) -> bool:
        """
        Xóa sản phẩm khỏi giỏ.
        
        Args:
            customer_id: ID khách hàng
            product_id: ID sản phẩm
            
        Returns:
            True nếu thành công
        """
        cart = self.get_cart(customer_id)
        
        # Check if product in cart
        for item in cart.items:
            if item.product.id == product_id:
                cart.remove_item(product_id)
                return True
        
        return False
    
    def update_quantity(
        self,
        customer_id: int,
        product_id: int,
        quantity: int
    ) -> bool:
        """
        Cập nhật số lượng.
        
        Args:
            customer_id: ID khách hàng
            product_id: ID sản phẩm
            quantity: Số lượng mới
            
        Returns:
            True nếu thành công
        """
        if quantity <= 0:
            return self.remove_item(customer_id, product_id)
            
        cart = self.get_cart(customer_id)
        
        # Check if product in cart
        for item in cart.items:
            if item.product.id == product_id:
                if item.product.status != ProductStatus.AVAILABLE:
                    return False
                cart.update_quantity(product_id, quantity)
                return True
        
        return False
    
    def apply_discount(
        self,
        customer_id: int,
        discount: Discount
    ) -> bool:
        """
        Áp dụng mã giảm giá.
        
        Args:
            customer_id: ID khách hàng
            discount: Mã giảm giá
            
        Returns:
            True nếu thành công
        """
        if discount.expiry_date <= datetime.now():
            return False
            
        cart = self.get_cart(customer_id)
        if cart.calculate_subtotal() < discount.min_amount:
            return False
            
        cart.apply_discount(discount)
        return True
    
    def clear_cart(self, customer_id: int):
        """Xóa giỏ hàng."""
        if customer_id in self.carts:
            self.carts[customer_id].clear()
    
    def remove_cart(self, customer_id: int):
        """Xóa giỏ hàng."""
        if customer_id in self.carts:
            del self.carts[customer_id] 