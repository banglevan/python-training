"""
E-commerce Database
- Design schema
- Import sample data
- Create reports
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import pandas as pd
from typing import Dict, List, Any
import logging
from datetime import datetime, timedelta
import random
from faker import Faker

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EcommerceDB:
    """E-commerce database handler."""
    
    def __init__(
        self,
        dbname: str,
        user: str,
        password: str,
        host: str = 'localhost',
        port: int = 5432
    ):
        """Initialize database."""
        self.conn_params = {
            'dbname': dbname,
            'user': user,
            'password': password,
            'host': host,
            'port': port
        }
        self.conn = None
        self.cur = None
        self.faker = Faker()
    
    def connect(self):
        """Establish database connection."""
        try:
            self.conn = psycopg2.connect(**self.conn_params)
            self.cur = self.conn.cursor(
                cursor_factory=RealDictCursor
            )
            logger.info("Database connection established")
        except Exception as e:
            logger.error(f"Connection error: {e}")
            raise
    
    def disconnect(self):
        """Close database connection."""
        try:
            if self.cur:
                self.cur.close()
            if self.conn:
                self.conn.close()
            logger.info("Database connection closed")
        except Exception as e:
            logger.error(f"Disconnection error: {e}")
            raise
    
    def create_schema(self):
        """Create database schema."""
        try:
            # Create users table
            self.cur.execute("""
                CREATE TABLE users (
                    id SERIAL PRIMARY KEY,
                    email VARCHAR(255) UNIQUE NOT NULL,
                    name VARCHAR(100) NOT NULL,
                    address TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create products table
            self.cur.execute("""
                CREATE TABLE products (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    description TEXT,
                    price DECIMAL(10,2) NOT NULL,
                    stock INTEGER NOT NULL DEFAULT 0,
                    category VARCHAR(50),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create orders table
            self.cur.execute("""
                CREATE TABLE orders (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER REFERENCES users(id),
                    status VARCHAR(20) NOT NULL,
                    total_amount DECIMAL(10,2) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create order_items table
            self.cur.execute("""
                CREATE TABLE order_items (
                    id SERIAL PRIMARY KEY,
                    order_id INTEGER REFERENCES orders(id),
                    product_id INTEGER REFERENCES products(id),
                    quantity INTEGER NOT NULL,
                    price DECIMAL(10,2) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create indexes
            self.cur.execute("""
                CREATE INDEX idx_users_email ON users(email);
                CREATE INDEX idx_products_category ON products(category);
                CREATE INDEX idx_orders_user_id ON orders(user_id);
                CREATE INDEX idx_orders_created_at ON orders(created_at);
                CREATE INDEX idx_order_items_order_id ON order_items(order_id);
                CREATE INDEX idx_order_items_product_id ON order_items(product_id);
            """)
            
            self.conn.commit()
            logger.info("Schema created successfully")
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Schema creation error: {e}")
            raise
    
    def generate_sample_data(
        self,
        num_users: int = 1000,
        num_products: int = 100,
        num_orders: int = 5000
    ):
        """Generate and import sample data."""
        try:
            # Generate users
            for _ in range(num_users):
                self.cur.execute("""
                    INSERT INTO users (email, name, address)
                    VALUES (%s, %s, %s)
                """, (
                    self.faker.email(),
                    self.faker.name(),
                    self.faker.address()
                ))
            
            # Generate products
            categories = [
                'Electronics', 'Clothing', 'Books',
                'Home & Garden', 'Sports', 'Toys'
            ]
            
            for _ in range(num_products):
                self.cur.execute("""
                    INSERT INTO products (
                        name, description, price,
                        stock, category
                    )
                    VALUES (%s, %s, %s, %s, %s)
                """, (
                    self.faker.product_name(),
                    self.faker.text(),
                    random.uniform(10, 1000),
                    random.randint(0, 100),
                    random.choice(categories)
                ))
            
            # Generate orders and order items
            statuses = ['pending', 'completed', 'cancelled']
            
            for _ in range(num_orders):
                # Create order
                self.cur.execute("""
                    INSERT INTO orders (
                        user_id, status, total_amount
                    )
                    VALUES (%s, %s, %s)
                    RETURNING id
                """, (
                    random.randint(1, num_users),
                    random.choice(statuses),
                    0  # Will update after adding items
                ))
                
                order_id = self.cur.fetchone()['id']
                
                # Add order items
                total_amount = 0
                num_items = random.randint(1, 5)
                
                for _ in range(num_items):
                    product_id = random.randint(1, num_products)
                    quantity = random.randint(1, 3)
                    price = random.uniform(10, 1000)
                    
                    self.cur.execute("""
                        INSERT INTO order_items (
                            order_id, product_id,
                            quantity, price
                        )
                        VALUES (%s, %s, %s, %s)
                    """, (
                        order_id,
                        product_id,
                        quantity,
                        price
                    ))
                    
                    total_amount += quantity * price
                
                # Update order total
                self.cur.execute("""
                    UPDATE orders
                    SET total_amount = %s
                    WHERE id = %s
                """, (total_amount, order_id))
            
            self.conn.commit()
            logger.info("Sample data generated successfully")
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Data generation error: {e}")
            raise
    
    def generate_reports(self):
        """Generate business reports."""
        try:
            reports = {}
            
            # 1. Sales by category
            self.cur.execute("""
                SELECT 
                    p.category,
                    COUNT(DISTINCT o.id) as num_orders,
                    SUM(oi.quantity) as total_items,
                    SUM(oi.quantity * oi.price) as revenue
                FROM products p
                JOIN order_items oi ON p.id = oi.product_id
                JOIN orders o ON oi.order_id = o.id
                WHERE o.status = 'completed'
                GROUP BY p.category
                ORDER BY revenue DESC
            """)
            reports['sales_by_category'] = self.cur.fetchall()
            
            # 2. Top customers
            self.cur.execute("""
                SELECT 
                    u.name,
                    COUNT(o.id) as num_orders,
                    SUM(o.total_amount) as total_spent
                FROM users u
                JOIN orders o ON u.id = o.user_id
                WHERE o.status = 'completed'
                GROUP BY u.id, u.name
                ORDER BY total_spent DESC
                LIMIT 10
            """)
            reports['top_customers'] = self.cur.fetchall()
            
            # 3. Product performance
            self.cur.execute("""
                SELECT 
                    p.name,
                    p.category,
                    SUM(oi.quantity) as units_sold,
                    SUM(oi.quantity * oi.price) as revenue,
                    COUNT(DISTINCT o.user_id) as unique_customers
                FROM products p
                JOIN order_items oi ON p.id = oi.product_id
                JOIN orders o ON oi.order_id = o.id
                WHERE o.status = 'completed'
                GROUP BY p.id, p.name, p.category
                ORDER BY revenue DESC
                LIMIT 10
            """)
            reports['product_performance'] = self.cur.fetchall()
            
            # 4. Daily sales trend
            self.cur.execute("""
                SELECT 
                    DATE(o.created_at) as date,
                    COUNT(DISTINCT o.id) as num_orders,
                    SUM(o.total_amount) as revenue
                FROM orders o
                WHERE o.status = 'completed'
                GROUP BY DATE(o.created_at)
                ORDER BY date DESC
                LIMIT 30
            """)
            reports['daily_sales'] = self.cur.fetchall()
            
            # Export reports to CSV
            for name, data in reports.items():
                df = pd.DataFrame(data)
                df.to_csv(f"reports/{name}.csv", index=False)
                logger.info(f"Report exported: {name}.csv")
            
            return reports
            
        except Exception as e:
            logger.error(f"Report generation error: {e}")
            raise

def main():
    """Main function."""
    # Initialize database
    db = EcommerceDB(
        dbname='ecommerce',
        user='admin',
        password='admin123'
    )
    
    try:
        # Connect to database
        db.connect()
        
        # Create schema
        db.create_schema()
        
        # Generate sample data
        db.generate_sample_data(
            num_users=1000,
            num_products=100,
            num_orders=5000
        )
        
        # Generate reports
        reports = db.generate_reports()
        
        # Print sample insights
        logger.info("\nTop 5 Categories by Revenue:")
        for cat in reports['sales_by_category'][:5]:
            logger.info(
                f"{cat['category']}: "
                f"${cat['revenue']:,.2f}"
            )
        
        logger.info("\nTop 5 Customers:")
        for cust in reports['top_customers'][:5]:
            logger.info(
                f"{cust['name']}: "
                f"${cust['total_spent']:,.2f}"
            )
        
    finally:
        # Close connection
        db.disconnect()

if __name__ == '__main__':
    main() 