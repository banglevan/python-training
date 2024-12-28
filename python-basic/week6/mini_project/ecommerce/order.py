"""
Order processing module
"""

from datetime import datetime
from typing import Dict, List, Optional, Tuple

from .models import (
    Order,
    OrderItem,
    OrderStatus,
    Product,
    ShoppingCart
)

class OrderProcessor:
    """Xử lý đơn hàng."""
    
    def __init__(self):
        """Khởi tạo processor."""
        self.orders: Dict[int, Order] = {}  # order_id -> order
        self.customer_orders: Dict[int, List[int]] = {}  # customer_id -> [order_id]
        self._next_order_id = 1
    
    def create_order(
        self,
        customer_id: int,
        cart: ShoppingCart
    ) -> Optional[Order]:
        """
        Tạo đơn hàng từ giỏ hàng.
        
        Args:
            customer_id: ID khách hàng
            cart: Giỏ hàng
            
        Returns:
            Order nếu thành công, None nếu thất bại
        """
        if not cart.items:
            return None
            
        # Create order items
        items = []
        for cart_item in cart.items:
            if cart_item.product.status != "available":
                return None
                
            items.append(OrderItem(
                product_id=cart_item.product.id,
                product_name=cart_item.product.name,
                quantity=cart_item.quantity,
                unit_price=cart_item.product.price
            ))
        
        # Create order
        order = Order(
            id=self._next_order_id,
            customer_id=customer_id,
            items=items
        )
        
        # Add shipping and discount
        order.shipping = cart.calculate_shipping()
        order.discount = cart.calculate_discount()
        order.total = (
            order.subtotal +
            order.shipping -
            order.discount
        )
        
        # Save order
        self.orders[order.id] = order
        if customer_id not in self.customer_orders:
            self.customer_orders[customer_id] = []
        self.customer_orders[customer_id].append(order.id)
        
        # Increment order ID
        self._next_order_id += 1
        
        return order
    
    def get_order(
        self,
        order_id: int
    ) -> Optional[Order]:
        """Lấy thông tin đơn hàng."""
        return self.orders.get(order_id)
    
    def get_customer_orders(
        self,
        customer_id: int,
        status: Optional[OrderStatus] = None
    ) -> List[Order]:
        """
        Lấy đơn hàng của khách.
        
        Args:
            customer_id: ID khách hàng
            status: Lọc theo trạng thái
        """
        if customer_id not in self.customer_orders:
            return []
            
        orders = [
            self.orders[order_id]
            for order_id in self.customer_orders[customer_id]
        ]
        
        if status:
            orders = [
                order for order in orders
                if order.status == status
            ]
            
        return orders
    
    def confirm_order(self, order_id: int) -> bool:
        """Xác nhận đơn hàng."""
        order = self.get_order(order_id)
        if not order or order.status != OrderStatus.PENDING:
            return False
            
        order.confirm()
        return True
    
    def ship_order(self, order_id: int) -> bool:
        """Giao hàng."""
        order = self.get_order(order_id)
        if not order or order.status != OrderStatus.CONFIRMED:
            return False
            
        order.ship()
        return True
    
    def deliver_order(self, order_id: int) -> bool:
        """Đã giao hàng."""
        order = self.get_order(order_id)
        if not order or order.status != OrderStatus.SHIPPED:
            return False
            
        order.deliver()
        return True
    
    def cancel_order(self, order_id: int) -> bool:
        """Hủy đơn hàng."""
        order = self.get_order(order_id)
        if not order or order.status not in {
            OrderStatus.PENDING,
            OrderStatus.CONFIRMED
        }:
            return False
            
        order.cancel()
        return True
    
    def get_order_history(
        self,
        customer_id: int,
        from_date: Optional[datetime] = None,
        to_date: Optional[datetime] = None
    ) -> List[Tuple[Order, str]]:
        """
        Lấy lịch sử đơn hàng.
        
        Args:
            customer_id: ID khách hàng
            from_date: Từ ngày
            to_date: Đến ngày
            
        Returns:
            List các tuple (order, action)
        """
        if customer_id not in self.customer_orders:
            return []
            
        history = []
        for order_id in self.customer_orders[customer_id]:
            order = self.orders[order_id]
            
            # Filter by date
            if from_date and order.created_at < from_date:
                continue
            if to_date and order.created_at > to_date:
                continue
                
            # Add status changes
            if order.status == OrderStatus.PENDING:
                history.append((order, "Created"))
            elif order.status == OrderStatus.CONFIRMED:
                history.append((order, "Confirmed"))
            elif order.status == OrderStatus.SHIPPED:
                history.append((order, "Shipped"))
            elif order.status == OrderStatus.DELIVERED:
                history.append((order, "Delivered"))
            elif order.status == OrderStatus.CANCELLED:
                history.append((order, "Cancelled"))
                
        return sorted(
            history,
            key=lambda x: x[0].created_at
        ) 