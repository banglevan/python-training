"""
E-commerce Redis Manager
----------------------

PURPOSE:
Handles all real-time and session data:
1. Shopping Cart
   - Active carts
   - Item management
   - Price calculations

2. Session Management
   - User sessions
   - Authentication tokens
   - Rate limiting

3. Cache Layer
   - Product cache
   - Price cache
   - Stock levels
"""

from typing import Dict, List, Any, Optional
import redis
import json
import logging
from datetime import datetime, timedelta
from .base_manager import BaseManager

logger = logging.getLogger(__name__)

class EcommerceRedisManager(BaseManager):
    """Redis manager for e-commerce real-time data."""
    
    def _connect(self) -> None:
        """Initialize Redis connection."""
        try:
            self.redis = redis.Redis(
                host=self.config.get('host', 'localhost'),
                port=self.config.get('port', 6379),
                db=self.config.get('db', 0),
                decode_responses=True
            )
            self.redis.ping()
            
            # Set key prefixes
            self.cart_prefix = "cart:"
            self.session_prefix = "session:"
            self.stock_prefix = "stock:"
            self.price_prefix = "price:"
            
            logger.info("E-commerce Redis initialized")
            
        except Exception as e:
            logger.error(f"Redis initialization error: {e}")
            raise
    
    def manage_cart(
        self,
        customer_id: str,
        action: str,
        product_data: Dict = None
    ) -> Dict[str, Any]:
        """Manage shopping cart operations."""
        try:
            cart_key = f"{self.cart_prefix}{customer_id}"
            
            if action == "add":
                # Add/update item in cart
                product_id = product_data['product_id']
                quantity = product_data['quantity']
                
                # Check stock availability
                available = self.check_stock(product_id, quantity)
                if not available:
                    raise ValueError("Insufficient stock")
                
                # Update cart
                cart = self.get_cart(customer_id) or {}
                if product_id in cart:
                    cart[product_id]['quantity'] += quantity
                else:
                    cart[product_id] = product_data
                
                # Save updated cart
                self.redis.setex(
                    cart_key,
                    timedelta(hours=24),
                    json.dumps(cart)
                )
                
            elif action == "remove":
                # Remove item from cart
                product_id = product_data['product_id']
                cart = self.get_cart(customer_id)
                if cart and product_id in cart:
                    del cart[product_id]
                    if cart:
                        self.redis.setex(
                            cart_key,
                            timedelta(hours=24),
                            json.dumps(cart)
                        )
                    else:
                        self.redis.delete(cart_key)
                        
            elif action == "clear":
                # Clear entire cart
                self.redis.delete(cart_key)
            
            return self.get_cart(customer_id) or {}
            
        except Exception as e:
            logger.error(f"Cart operation error: {e}")
            raise
    
    def get_cart(
        self,
        customer_id: str
    ) -> Optional[Dict]:
        """Retrieve current cart contents."""
        try:
            cart_data = self.redis.get(
                f"{self.cart_prefix}{customer_id}"
            )
            return json.loads(cart_data) if cart_data else None
            
        except Exception as e:
            logger.error(f"Error retrieving cart: {e}")
            raise
    
    def manage_session(
        self,
        session_id: str,
        action: str,
        data: Dict = None
    ) -> bool:
        """Manage user sessions."""
        try:
            session_key = f"{self.session_prefix}{session_id}"
            
            if action == "create":
                # Create new session
                session_data = {
                    'created_at': datetime.utcnow().isoformat(),
                    'last_active': datetime.utcnow().isoformat(),
                    **data
                }
                return self.redis.setex(
                    session_key,
                    timedelta(hours=4),  # Session TTL
                    json.dumps(session_data)
                )
                
            elif action == "update":
                # Update existing session
                session_data = self.redis.get(session_key)
                if session_data:
                    current_data = json.loads(session_data)
                    current_data.update(data)
                    current_data['last_active'] = datetime.utcnow().isoformat()
                    return self.redis.setex(
                        session_key,
                        timedelta(hours=4),
                        json.dumps(current_data)
                    )
                return False
                
            elif action == "delete":
                # End session
                return bool(self.redis.delete(session_key))
                
            return False
            
        except Exception as e:
            logger.error(f"Session operation error: {e}")
            raise
    
    def update_stock(
        self,
        product_id: str,
        quantity: int,
        action: str = "decrease"
    ) -> int:
        """Update product stock levels."""
        try:
            stock_key = f"{self.stock_prefix}{product_id}"
            
            if action == "decrease":
                current_stock = int(
                    self.redis.get(stock_key) or 0
                )
                if current_stock >= quantity:
                    new_stock = self.redis.decrby(
                        stock_key,
                        quantity
                    )
                    return new_stock
                raise ValueError("Insufficient stock")
                
            elif action == "increase":
                return self.redis.incrby(stock_key, quantity)
                
            elif action == "set":
                return self.redis.set(stock_key, quantity)
            
            raise ValueError(f"Invalid stock action: {action}")
            
        except Exception as e:
            logger.error(f"Stock update error: {e}")
            raise
    
    def check_stock(
        self,
        product_id: str,
        quantity: int
    ) -> bool:
        """Check if product has sufficient stock."""
        try:
            current_stock = int(
                self.redis.get(
                    f"{self.stock_prefix}{product_id}"
                ) or 0
            )
            return current_stock >= quantity
            
        except Exception as e:
            logger.error(f"Stock check error: {e}")
            raise
    
    def _disconnect(self) -> None:
        """Close Redis connection."""
        if hasattr(self, 'redis'):
            self.redis.close()
    
    def health_check(self) -> Dict[str, Any]:
        """Check Redis connection health."""
        try:
            self.redis.ping()
            info = self.redis.info()
            return {
                'status': 'healthy',
                'timestamp': datetime.utcnow(),
                'metrics': {
                    'connected_clients': info['connected_clients'],
                    'used_memory_human': info['used_memory_human'],
                    'uptime_in_seconds': info['uptime_in_seconds']
                }
            }
        except Exception as e:
            return {
                'status': 'unhealthy',
                'error': str(e),
                'timestamp': datetime.utcnow()
            } 