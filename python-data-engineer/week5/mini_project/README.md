# E-commerce Multi-Database System
A scalable e-commerce system utilizing multiple databases for optimal performance.

## Architecture Overview

### Database Roles
1. **MongoDB**
   - Product catalog
   - Customer profiles
   - Order management
   - Document-based data

2. **Redis**
   - Shopping carts
   - User sessions
   - Real-time inventory
   - Cache layer

3. **TimescaleDB**
   - Sales analytics
   - Inventory tracking
   - Performance metrics
   - Time-series data

4. **Neo4j**
   - Product recommendations
   - Purchase patterns
   - Customer behavior
   - Graph relationships

### System Components

```
├── db_managers/
│   ├── base_manager.py      # Abstract base class
│   ├── ecommerce_mongo.py   # MongoDB manager
│   ├── ecommerce_redis.py   # Redis manager
│   ├── ecommerce_timescale.py # TimescaleDB manager
│   └── ecommerce_neo4j.py   # Neo4j manager
├── config/
│   └── ecommerce_config.yaml # System configuration
├── tests/
│   └── test_ecommerce_system.py # Test suite
├── ecommerce_system.py      # Main system coordinator
└── README.md               # Documentation
```

## Setup Instructions

1. **Environment Setup**
   ```bash
   # Create virtual environment
   python -m venv venv
   source venv/bin/activate  # Linux/Mac
   venv\Scripts\activate     # Windows

   # Install dependencies
   pip install -r requirements.txt
   ```

2. **Database Setup**
   ```bash
   # MongoDB
   docker run -d -p 27017:27017 --name mongo mongodb

   # Redis
   docker run -d -p 6379:6379 --name redis redis

   # TimescaleDB
   docker run -d -p 5432:5432 --name timescale timescale/timescaledb

   # Neo4j
   docker run -d -p 7687:7687 -p 7474:7474 --name neo4j neo4j
   ```

3. **Configuration**
   - Copy `config/ecommerce_config.yaml.example` to `config/ecommerce_config.yaml`
   - Update database credentials and settings

## Usage Examples

1. **Initialize System**
   ```python
   from ecommerce_system import EcommerceSystem
   import yaml

   # Load configuration
   with open('config/ecommerce_config.yaml') as f:
       config = yaml.safe_load(f)

   # Create system instance
   system = EcommerceSystem(config)
   ```

2. **Process Order**
   ```python
   # Process customer order
   order = system.process_order(
       customer_id='customer123',
       items=[{
           'product_id': 'prod123',
           'quantity': 2,
           'price': 29.99
       }]
   )
   ```

3. **Get Product Details**
   ```python
   # Get product information with recommendations
   product = system.get_product_details(
       product_id='prod123',
       with_recommendations=True
   )
   ```

4. **Customer Insights**
   ```python
   # Get comprehensive customer data
   insights = system.get_customer_insights(
       customer_id='customer123'
   )
   ```

## Performance Considerations

1. **Data Distribution**
   - Use appropriate database for each data type
   - Maintain data consistency across databases
   - Implement proper synchronization

2. **Caching Strategy**
   - Cache frequently accessed products
   - Maintain session data in Redis
   - Update cache on data changes

3. **Monitoring**
   - Regular health checks
   - Performance metrics
   - Error tracking

## Testing

Run the test suite:
```bash
python -m pytest tests/
```

## Error Handling

The system implements comprehensive error handling:
1. Transaction rollback
2. Data consistency checks
3. Service health monitoring
4. Automatic retry mechanisms

## Maintenance

1. **Backup Strategy**
   - MongoDB: Daily dumps
   - TimescaleDB: Continuous archiving
   - Neo4j: Periodic snapshots

2. **Monitoring**
   - System health checks
   - Performance metrics
   - Error logs

3. **Scaling**
   - Horizontal scaling options
   - Load balancing considerations
   - Replication setup

## Contributing

1. Fork the repository
2. Create feature branch
3. Commit changes
4. Create pull request

## License

MIT License - See LICENSE file for details 