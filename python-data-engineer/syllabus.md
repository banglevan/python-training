# Python Data Engineer Course

## Tuần 1: Data Engineering Foundations
- Python cho Data Engineering
- ETL fundamentals 
- Data pipelines
- Data quality & validation

### Bài tập
1. **ETL Basic Pipeline**
   - Extract data từ CSV/JSON
   - Transform data đơn giản
   - Load vào SQLite
   - Path: `/week1/exercises/etl_basic.py`

2. **Data Validation**
   - Kiểm tra data types
   - Xử lý missing values
   - Data constraints
   - Path: `/week1/exercises/data_validation.py`

3. **Data Quality Check**
   - Metrics calculation
   - Data profiling
   - Quality reports
   - Path: `/week1/exercises/data_quality.py`

### Mini Project: Log Analysis Pipeline
- Parse log files
- Extract metrics
- Generate reports
- Path: `/week1/mini_project/log_analyzer.py`

## Tuần 2: Databases & SQL
- SQL fundamentals
- Database design
- Query optimization
- Database operations

### Bài tập
1. **SQL Operations**
   - CRUD operations
   - Joins & aggregations
   - Path: `/week2/exercises/sql_ops.py`

2. **Database Design**
   - Schema design
   - Normalization
   - Indexing
   - Path: `/week2/exercises/db_design.py`

3. **Query Performance**
   - Query plans
   - Optimization techniques
   - Benchmarking
   - Path: `/week2/exercises/query_perf.py`

### Mini Project: E-commerce Database
- Design schema
- Import sample data
- Create reports
- Path: `/week2/mini_project/ecommerce_db.py`

## Tuần 3: Big Data Processing
- Apache Spark basics
- Distributed computing
- Data partitioning
- Performance tuning

### Bài tập
1. **Spark Basics**
   - RDD operations
   - DataFrame API
   - Path: `/week3/exercises/spark_basics.py`

2. **Data Processing**
   - Transformations
   - Actions
   - Caching
   - Path: `/week3/exercises/data_processing.py`

3. **Performance Optimization**
   - Partitioning
   - Memory management
   - Job tuning
   - Path: `/week3/exercises/perf_optimization.py`

### Mini Project: Customer Analytics
- Process large datasets
- Customer segmentation
- Behavior analysis
- Path: `/week3/mini_project/customer_analytics.py`

## Tuần 4: Data Warehousing
- Data warehouse concepts
- Dimensional modeling
- ETL for DWH
- OLAP operations

### Bài tập
1. **Dimensional Modeling**
   - Fact tables
   - Dimension tables
   - Path: `/week4/exercises/dim_modeling.py`

2. **DWH Operations**
   - Slowly changing dimensions
   - Incremental loads
   - Path: `/week4/exercises/dwh_ops.py`

3. **OLAP Processing**
   - Cube operations
   - Drill-down/Roll-up
   - Path: `/week4/exercises/olap_proc.py`

### Mini Project: Sales DWH
- Design star schema
- Build ETL pipeline
- Create OLAP reports
- Path: `/week4/mini_project/sales_dwh.py`

## Tuần 5: Advanced Databases

### SQL Databases
- PostgreSQL deep dive
- MySQL optimization
- Oracle basics
- SQL Server features

#### Bài tập
1. **Advanced PostgreSQL**
   - Partitioning
   - Replication
   - VACUUM & maintenance
   - Path: `/week5/exercises/postgres_advanced.py`

2. **MySQL Performance**
   - Query optimization
   - Index strategies
   - Buffer pool tuning
   - Path: `/week5/exercises/mysql_perf.py`

### NoSQL Databases
- Document DB (MongoDB)
- Key-Value (Redis)
- Wide-column (Cassandra)
- Search Engine (Elasticsearch)

#### Bài tập
1. **MongoDB Operations**
   - Document CRUD
   - Aggregation pipeline
   - Indexing strategies
   - Path: `/week5/exercises/mongo_ops.py`

2. **Redis Caching**
   - Data structures
   - Caching patterns
   - Persistence options
   - Path: `/week5/exercises/redis_cache.py`

3. **Cassandra Modeling**
   - Data modeling
   - Partition strategies
   - Consistency levels
   - Path: `/week5/exercises/cassandra_model.py`

4. **Elasticsearch Search**
   - Index management
   - Query DSL
   - Aggregations
   - Path: `/week5/exercises/elastic_search.py`

### Time Series Databases
- InfluxDB
- TimescaleDB
- Prometheus
- OpenTSDB

#### Bài tập
1. **InfluxDB Metrics**
   - Data ingestion
   - Continuous queries
   - Retention policies
   - Path: `/week5/exercises/influx_metrics.py`

2. **TimescaleDB Analytics**
   - Hypertables
   - Continuous aggregates
   - Data lifecycle
   - Path: `/week5/exercises/timescale_analytics.py`

### Graph Databases
- Neo4j
- Amazon Neptune
- ArangoDB
- OrientDB

#### Bài tập
1. **Neo4j Graph**
   - Node & relationships
   - Cypher queries
   - Graph algorithms
   - Path: `/week5/exercises/neo4j_graph.py`

2. **Neptune Analytics**
   - RDF/Property graphs
   - SPARQL queries
   - Graph analytics
   - Path: `/week5/exercises/neptune_analytics.py`

### Mini Project: Multi-Database System
- Implement different database types
- Data synchronization
- Performance comparison
- Use case optimization
- Path: `/week5/mini_project/multi_db_system.py`

### Database Selection Guide
1. **SQL Databases**
   - Use cases:
     - Complex transactions
     - Structured data
     - ACID compliance
   - Popular options:
     - PostgreSQL: Advanced features, extensibility
     - MySQL: Simple, reliable, widely used
     - Oracle: Enterprise-grade, scalable
     - SQL Server: Microsoft ecosystem integration

2. **NoSQL Databases**
   - Document DB:
     - Use cases: Flexible schema, nested data
     - Options: MongoDB, CouchDB
   - Key-Value:
     - Use cases: Caching, session management
     - Options: Redis, Memcached
   - Wide-column:
     - Use cases: Large-scale data, high write throughput
     - Options: Cassandra, HBase
   - Search Engine:
     - Use cases: Full-text search, analytics
     - Options: Elasticsearch, Solr

3. **Time Series Databases**
   - Use cases:
     - IoT data
     - Monitoring metrics
     - Financial data
   - Options:
     - InfluxDB: Purpose-built, high performance
     - TimescaleDB: PostgreSQL extension
     - Prometheus: Monitoring focus
     - OpenTSDB: HBase-based scalability

4. **Graph Databases**
   - Use cases:
     - Social networks
     - Recommendation engines
     - Knowledge graphs
   - Options:
     - Neo4j: Mature, widely used
     - Neptune: AWS managed service
     - ArangoDB: Multi-model database
     - OrientDB: Graph + Document DB

## Final Project Options
1. **Data Pipeline System**
   - Multiple data sources
   - Complex transformations
   - Quality monitoring
   - Performance optimization

2. **Analytics Platform**
   - Data warehouse
   - ETL processes
   - Analysis capabilities
   - Reporting dashboard

3. **Real-time Processing**
   - Streaming data
   - Real-time analytics
   - Monitoring & alerts
   - Performance tuning

## Tuần 6: File Systems & Storage

### Traditional File Systems
- Local File Systems
  - EXT4
  - NTFS
  - XFS
- Network File Systems
  - NFS
  - SMB/CIFS
  - GlusterFS

#### Bài tập
1. **File System Operations**
   - File handling
   - Directory operations
   - Permissions management
   - Path: `/week6/exercises/fs_operations.py`

2. **NFS Client**
   - Mount operations
   - File sharing
   - Performance tuning
   - Path: `/week6/exercises/nfs_client.py`

### Distributed File Systems
- HDFS (Hadoop)
- Ceph
- LustreFS
- MooseFS

#### Bài tập
1. **HDFS Operations**
   - File management
   - Block operations
   - Replication control
   - Path: `/week6/exercises/hdfs_ops.py`

2. **Ceph Storage**
   - Object operations
   - Block device management
   - File system integration
   - Path: `/week6/exercises/ceph_storage.py`

### Object Storage
- Amazon S3
- MinIO
- OpenStack Swift
- Azure Blob Storage

#### Bài tập
1. **S3 Operations**
   - Bucket management
   - Object CRUD
   - Versioning
   - Lifecycle policies
   - Path: `/week6/exercises/s3_ops.py`

2. **MinIO Client**
   - Server setup
   - API operations
   - Security configuration
   - Path: `/week6/exercises/minio_client.py`

### Storage Features
- Data Compression
- Deduplication
- Encryption
- Versioning

#### Bài tập
1. **Storage Optimization**
   - Compression algorithms
   - Deduplication strategies
   - Performance analysis
   - Path: `/week6/exercises/storage_opt.py`

2. **Data Security**
   - Encryption methods
   - Access control
   - Audit logging
   - Path: `/week6/exercises/data_security.py`

### Mini Project: Hybrid Storage System
- Multiple storage integration
- Automatic tiering
- Data lifecycle management
- Performance monitoring
- Path: `/week6/mini_project/hybrid_storage.py`

### Storage Selection Guide
1. **Traditional File Systems**
   - Use cases:
     - Local application storage
     - Small-scale file sharing
     - Direct access requirements
   - Options:
     - EXT4: Linux standard
     - NTFS: Windows environments
     - XFS: Large-scale systems

2. **Network File Systems**
   - Use cases:
     - Shared storage access
     - Cross-platform file sharing
     - Enterprise storage
   - Options:
     - NFS: Unix/Linux environments
     - SMB: Windows integration
     - GlusterFS: Scale-out storage

3. **Distributed File Systems**
   - Use cases:
     - Big data storage
     - High availability
     - Scalable storage
   - Options:
     - HDFS: Hadoop ecosystem
     - Ceph: Multi-protocol storage
     - LustreFS: HPC workloads

4. **Object Storage**
   - Use cases:
     - Cloud-native applications
     - Backup and archive
     - Content distribution
   - Options:
     - Amazon S3: Cloud standard
     - MinIO: Self-hosted S3
     - Swift: OpenStack integration

### Storage Features Comparison
1. **Performance**
   - Sequential Access:
     - Traditional FS > DFS > Object
   - Random Access:
     - Traditional FS > Object > DFS
   - Latency:
     - Local FS > NFS > Object > DFS

2. **Scalability**
   - Capacity:
     - Object > DFS > NFS > Local
   - Nodes:
     - DFS > Object > NFS > Local
   - Management:
     - Object > DFS > NFS > Local

3. **Features**
   - Versioning:
     - Object Storage
     - Some DFS
   - Deduplication:
     - Enterprise Storage
     - Some Object Storage
   - Encryption:
     - All modern systems
   - Compression:
     - Available in most systems

## Tuần 7: Message Queue & Streaming Systems

### Traditional Message Queues
- RabbitMQ
- ActiveMQ
- IBM MQ
- ZeroMQ

#### Bài tập
1. **RabbitMQ Operations**
   - Queue management
   - Exchange types
   - Routing strategies
   - Dead letter queues
   - Path: `/week7/exercises/rabbitmq_ops.py`

2. **ActiveMQ Integration**
   - Topics & queues
   - Message persistence
   - Client integration
   - Path: `/week7/exercises/activemq_integration.py`

### Streaming Platforms
- Apache Kafka
- Apache Pulsar
- AWS Kinesis
- Google Pub/Sub

#### Bài tập
1. **Kafka Operations**
   - Topic management
   - Producer configurations
   - Consumer groups
   - Stream processing
   - Path: `/week7/exercises/kafka_ops.py`

2. **Pulsar Messaging**
   - Topics & subscriptions
   - Multi-tenancy
   - Geo-replication
   - Path: `/week7/exercises/pulsar_messaging.py`

### In-Memory Queue
- Redis Pub/Sub
- Redis Streams
- Memcached Queue

#### Bài tập
1. **Redis Queue**
   - Pub/Sub patterns
   - Stream operations
   - Consumer groups
   - Path: `/week7/exercises/redis_queue.py`

2. **Redis Streams**
   - Stream management
   - Consumer groups
   - Message acknowledgment
   - Path: `/week7/exercises/redis_streams.py`

### IoT Messaging
- MQTT
- AMQP
- CoAP
- Azure IoT Hub

#### Bài tập
1. **MQTT Broker**
   - Topic structure
   - QoS levels
   - Retained messages
   - Path: `/week7/exercises/mqtt_broker.py`

2. **IoT Integration**
   - Device management
   - Message routing
   - Security implementation
   - Path: `/week7/exercises/iot_integration.py`

### Mini Project: Real-time Data Pipeline
- Multiple queue integration
- Message transformation
- Error handling
- Monitoring system
- Path: `/week7/mini_project/realtime_pipeline.py`

### Message System Selection Guide
1. **Traditional Message Queues**
   - Use cases:
     - Enterprise integration
     - Reliable messaging
     - Complex routing
   - Options:
     - RabbitMQ: Feature-rich, mature
     - ActiveMQ: JMS compliance
     - IBM MQ: Enterprise grade
     - ZeroMQ: Lightweight, embedded

2. **Streaming Platforms**
   - Use cases:
     - Real-time analytics
     - Event sourcing
     - Log aggregation
   - Options:
     - Kafka: High throughput
     - Pulsar: Multi-tenancy
     - Kinesis: AWS integration
     - Pub/Sub: Google Cloud

3. **In-Memory Queues**
   - Use cases:
     - Fast message delivery
     - Temporary storage
     - Real-time features
   - Options:
     - Redis Pub/Sub: Simple, fast
     - Redis Streams: Persistent
     - Memcached: Pure cache

4. **IoT Messaging**
   - Use cases:
     - Device communication
     - Sensor data
     - Remote control
   - Options:
     - MQTT: Lightweight protocol
     - AMQP: Enterprise IoT
     - CoAP: Resource-constrained
     - IoT Hub: Cloud managed

### Feature Comparison
1. **Performance**
   - Throughput:
     - Kafka > Pulsar > RabbitMQ > MQTT
   - Latency:
     - Redis > ZeroMQ > RabbitMQ > Kafka
   - Scalability:
     - Kafka > Pulsar > RabbitMQ > MQTT

2. **Reliability**
   - Message Persistence:
     - Kafka > RabbitMQ > Redis Streams
   - Delivery Guarantees:
     - RabbitMQ > Kafka > MQTT
   - Fault Tolerance:
     - Kafka > Pulsar > RabbitMQ

3. **Features**
   - Routing Capabilities:
     - RabbitMQ > ActiveMQ > Kafka
   - Message Patterns:
     - RabbitMQ > Kafka > MQTT
   - Management Tools:
     - RabbitMQ > Kafka > MQTT

4. **Use Case Specific**
   - Real-time Analytics:
     - Kafka, Pulsar
   - IoT/Edge:
     - MQTT, CoAP
   - Enterprise Integration:
     - RabbitMQ, ActiveMQ
   - Cloud Native:
     - Kinesis, Pub/Sub

## Cấu trúc thư mục
```
python-data-engineer/
├── README.md
├── syllabus.md
├── week1/
│   ├── exercises/
│   │   ├── etl_basic.py
│   │   ├── data_validation.py
│   │   └── data_quality.py
│   └── mini_project/
│       └── log_analyzer.py
├── week2/
│   ├── exercises/
│   │   ├── sql_ops.py
│   │   ├── db_design.py
│   │   └── query_perf.py
│   └── mini_project/
│       └── ecommerce_db.py
├── week3/
│   ├── exercises/
│   │   ├── spark_basics.py
│   │   ├── data_processing.py
│   │   └── perf_optimization.py
│   └── mini_project/
│       └── customer_analytics.py
├── week4/
│   ├── exercises/
│   │   ├── dim_modeling.py
│   │   ├── dwh_ops.py
│   │   └── olap_proc.py
│   └── mini_project/
│       └── sales_dwh.py
├── week5/
│   ├── exercises/
│   │   ├── postgres_advanced.py
│   │   ├── mysql_perf.py
│   │   ├── mongo_ops.py
│   │   ├── redis_cache.py
│   │   ├── cassandra_model.py
│   │   ├── elastic_search.py
│   │   ├── influx_metrics.py
│   │   ├── timescale_analytics.py
│   │   ├── neo4j_graph.py
│   │   └── neptune_analytics.py
│   └── mini_project/
│       └── multi_db_system.py
├── week6/
│   ├── exercises/
│   │   ├── fs_operations.py
│   │   ├── nfs_client.py
│   │   ├── hdfs_ops.py
│   │   ├── ceph_storage.py
│   │   ├── s3_ops.py
│   │   ├── minio_client.py
│   │   ├── storage_opt.py
│   │   └── data_security.py
│   └── mini_project/
│       └── hybrid_storage.py
├── week7/
│   ├── exercises/
│   │   ├── rabbitmq_ops.py
│   │   ├── activemq_integration.py
│   │   ├── kafka_ops.py
│   │   ├── pulsar_messaging.py
│   │   ├── redis_queue.py
│   │   ├── redis_streams.py
│   │   ├── mqtt_broker.py
│   │   └── iot_integration.py
│   └── mini_project/
│       └── realtime_pipeline.py
└── final_project/
    ├── data_pipeline/
    ├── analytics_platform/
    └── realtime_processing/
``` 