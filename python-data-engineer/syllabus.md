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

### Storage Features **<-- here**
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
- Focus on a specified application
- Split to multiple files, that like a real system
- Multiple storage integration
- Automatic tiering
- Data lifecycle management
- Backing up, including backup pattern, schedule, rule, etc
- Performance monitoring
- Readme, requirements are needed
- Path: `/week6/mini_project`

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
- Focus on a specified application
- Add readme, requirements
- Describe database structure and interaction on readme
- Multiple queue integration
- Message transformation
- Error handling
- Monitoring system
- Path: `/week7/mini_project/`

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

## Tuần 8: Change Data Capture (CDC)

### CDC Fundamentals
- Log-based CDC
- Trigger-based CDC
- Polling-based CDC
- Hybrid approaches

#### Bài tập
1. **CDC Patterns**
   - Implementation strategies
   - Change detection
   - Data versioning
   - Path: `/week8/exercises/cdc_patterns.py`

2. **Change Tracking**
   - Version control
   - Audit logging
   - History management
   - Path: `/week8/exercises/change_tracking.py`

### Database CDC
- Debezium
- Oracle GoldenGate
- AWS DMS
- Maxwell's daemon

#### Bài tập
1. **Debezium Integration**
   - Connector setup
   - Event streaming
   - Schema evolution
   - Error handling
   - Path: `/week8/exercises/debezium_integration.py`

2. **DMS Operations**
   - Migration tasks
   - Replication
   - Monitoring
   - Path: `/week8/exercises/dms_operations.py`

### Streaming CDC
- Kafka Connect
- Apache Flink CDC
- Spark Structured Streaming
- Redis CDC

#### Bài tập
1. **Kafka CDC**
   - Connect setup
   - Transform configs
   - Sink management
   - Path: `/week8/exercises/kafka_cdc.py`

2. **Flink CDC**
   - Source connectors
   - Processing operators
   - Sink integration
   - Path: `/week8/exercises/flink_cdc.py`

### CDC Use Cases
- Data Replication
- Event Sourcing
- Audit Systems
- Cache Invalidation

#### Bài tập
1. **Data Sync**
   - Multi-source sync
   - Conflict resolution
   - Recovery strategies
   - Focus on an real product example
   - Desc the solution also
   - Path: `/week8/exercises/data_sync.py`

2. **Event Store**
   - Event capturing
   - State reconstruction
   - Event replay
   - Focus on an real product example
   - Desc the solution also
   - Path: `/week8/exercises/event_store.py`

### Mini Project: Real-time Data Sync System
- Focus on a real production sample
- Add readme, requirements
- Describe database structure and interactions on readme
- Split to modules
- Multiple source CDC
- Data transformation
- Conflict handling
- Monitoring & alerts
- Path: `/week8/mini_project/`

### CDC Tool Selection Guide
1. **Log-based CDC**
   - Use cases:
     - High-volume changes
     - Minimal source impact
     - Real-time replication
   - Options:
     - Debezium: Open source, mature
     - Maxwell: MySQL focused
     - GoldenGate: Enterprise grade

2. **Trigger-based CDC**
   - Use cases:
     - Complex change tracking
     - Custom business logic
     - Immediate notification
   - Options:
     - Database triggers
     - Application triggers
     - Hybrid solutions

3. **Polling-based CDC**
   - Use cases:
     - Simple requirements
     - Legacy systems
     - Batch processing
   - Options:
     - Timestamp-based
     - Version number
     - Checksum comparison

### Feature Comparison
1. **Performance Impact**
   - Source Database:
     - Log-based: Minimal
     - Trigger-based: High
     - Polling-based: Moderate
   
   - Network Traffic:
     - Log-based: Moderate
     - Trigger-based: High
     - Polling-based: High

2. **Reliability**
   - Data Consistency:
     - Log-based: High
     - Trigger-based: High
     - Polling-based: Moderate
   
   - Change Detection:
     - Log-based: Immediate
     - Trigger-based: Immediate
     - Polling-based: Delayed

3. **Implementation**
   - Complexity:
     - Log-based: High
     - Trigger-based: Moderate
     - Polling-based: Low
   
   - Maintenance:
     - Log-based: Moderate
     - Trigger-based: High
     - Polling-based: Low

### Best Practices
1. **Design Considerations**
   - Change data format
   - Order preservation
   - Error handling
   - Recovery procedures

2. **Performance Optimization**
   - Batch processing
   - Parallel processing
   - Resource management
   - Network optimization

3. **Monitoring & Management**
   - Lag monitoring
   - Error detection
   - Alert system
   - Performance metrics

4. **Security Aspects**
   - Data encryption
   - Access control
   - Audit logging
   - Compliance requirements

## Tuần 9: Data Visualization & BI Tools

### Open Source BI Tools
- Apache Superset
- Metabase
- Redash
- Grafana

#### Bài tập
1. **Superset Integration**
   - Dashboard creation
   - Chart types
   - SQL Lab usage
   - Security roles
   - Path: `/week9/exercises/superset_integration.py`

2. **Grafana Metrics**
   - Data sources
   - Panel creation
   - Alert rules
   - User management
   - Path: `/week9/exercises/grafana_metrics.py`

### Commercial BI Platforms
- Tableau
- Power BI
- Looker
- Sisense

#### Bài tập
1. **Tableau Connection**
   - Data preparation
   - Visual analytics
   - Dashboard design
   - Path: `/week9/exercises/tableau_connection.py`

2. **Power BI Integration**
   - Data modeling
   - DAX formulas
   - Report design
   - Path: `/week9/exercises/powerbi_integration.py`

### Real-time Visualization
- Apache ECharts
- D3.js
- Plotly
- Bokeh

#### Bài tập
1. **Real-time Charts**
   - Streaming data
   - Interactive features
   - Animation effects
   - Path: `/week9/exercises/realtime_charts.py`

2. **Custom Visualizations**
   - Chart components
   - Event handling
   - Data updates
   - Path: `/week9/exercises/custom_viz.py`

### Embedded Analytics
- Embedded Superset
- Tableau Embedded
- Power BI Embedded
- Custom Solutions

#### Bài tập
1. **Embed Integration**
   - Framework setup
   - Authentication
   - Customization
   - Path: `/week9/exercises/embed_integration.py`

2. **Custom Dashboard**
   - Layout design
   - Component integration
   - Interactivity
   - Path: `/week9/exercises/custom_dashboard.py`

### Mini Project: Analytics Platform
- Focus on a real production sample
- Add readme, requirements
- Describe database structure and interactions on readme
- Multiple data sources
- Custom visualizations
- Real-time updates
- User management
- Path: `/week9/mini_project/analytics_platform.py`

### Tool Selection Guide
1. **Open Source BI**
   - Use cases:
     - Self-hosted analytics
     - Custom integration
     - Community support
   - Options:
     - Superset: Modern, scalable
     - Metabase: User-friendly
     - Redash: Query focused
     - Grafana: Metrics & logs

2. **Commercial BI**
   - Use cases:
     - Enterprise analytics
     - Complex visualization
     - Managed service
   - Options:
     - Tableau: Rich features
     - Power BI: Microsoft integration
     - Looker: Modern cloud BI
     - Sisense: Embedded analytics

3. **Real-time Tools**
   - Use cases:
     - Live monitoring
     - Interactive dashboards
     - Custom visualization
   - Options:
     - ECharts: Feature-rich
     - D3.js: Flexible, powerful
     - Plotly: Interactive plots
     - Bokeh: Python native

### Feature Comparison
1. **Functionality**
   - Data Handling:
     - Commercial > Open Source > Real-time
   - Visualization Types:
     - Commercial > Real-time > Open Source
   - Customization:
     - Real-time > Open Source > Commercial

2. **Implementation**
   - Ease of Use:
     - Commercial > Open Source > Real-time
   - Learning Curve:
     - Real-time > Open Source > Commercial
   - Integration:
     - Open Source > Real-time > Commercial

3. **Cost & Support**
   - Initial Cost:
     - Commercial >>> Open Source > Real-time
   - Maintenance:
     - Commercial > Open Source > Real-time
   - Support:
     - Commercial > Open Source > Real-time

### Best Practices
1. **Dashboard Design**
   - User experience
   - Performance optimization
   - Mobile responsiveness
   - Accessibility

2. **Data Architecture**
   - Data modeling
   - Query optimization
   - Caching strategies
   - Real-time updates

3. **Security & Governance**
   - Access control
   - Data privacy
   - Audit logging
   - Compliance

4. **Performance Optimization**
   - Query tuning
   - Data aggregation
   - Caching layers
   - Resource management

## Tuần 10: Workflow Orchestration & Pipeline Management

### Apache Airflow
- Core Concepts
- DAG Development
- Operators & Sensors
- Custom Components

#### Bài tập
1. **Airflow DAGs**
   - DAG construction
   - Task dependencies
   - Scheduling
   - Error handling
   - Path: `/week10/exercises/airflow_dags.py`

2. **Custom Operators**
   - Operator development
   - Hook integration
   - Sensor creation
   - Path: `/week10/exercises/custom_operators.py`

### Prefect
- Flow Development
- Task Management
- State Handling
- Deployment Options

#### Bài tập
1. **Prefect Flows**
   - Flow definition
   - Task configuration
   - State handlers
   - Monitoring
   - Path: `/week10/exercises/prefect_flows.py`

2. **Advanced Features**
   - Mapping
   - Caching
   - Notifications
   - Path: `/week10/exercises/prefect_advanced.py`

### Parallel Computing
- NVIDIA DASK
- PySpark
- Ray
- Modin

#### Bài tập
1. **DASK Operations**
   - Distributed computing
   - DataFrame operations
   - Cluster management
   - Path: `/week10/exercises/dask_ops.py`

2. **Ray Processing**
   - Task parallel
   - Actor pattern
   - Distributed training
   - Path: `/week10/exercises/ray_processing.py`

### Workflow Management
- Luigi
- Argo
- Kubeflow
- Dagster

#### Bài tập
1. **Luigi Tasks**
   - Pipeline design
   - Task dependencies
   - Output targets
   - Path: `/week10/exercises/luigi_tasks.py`

2. **Kubeflow Pipelines**
   - Component creation
   - Pipeline assembly
   - Kubernetes deployment
   - Path: `/week10/exercises/kubeflow_pipelines.py`

### Mini Project: ETL Orchestration System
- Focus on a real production sample
- Add readme, requirements
- Describe database structure and interactions on readme
- Multiple workflow engines
- Pipeline monitoring
- Error recovery
- Performance optimization
- Path: `/week10/mini_project/etl_orchestration.py`

### Tool Selection Guide
1. **Traditional Orchestrators**
   - Use cases:
     - Complex workflows
     - Scheduled jobs
     - Dependencies management
   - Options:
     - Airflow: Mature, widely used
     - Prefect: Modern, pythonic
     - Luigi: Simple, lightweight
     - Dagster: Data-aware

2. **Cloud-Native Solutions**
   - Use cases:
     - Container orchestration
     - Microservices
     - Kubernetes integration
   - Options:
     - Argo: Kubernetes native
     - Kubeflow: ML workflows
     - AWS Step Functions
     - Azure Data Factory

3. **Parallel Computing**
   - Use cases:
     - Big data processing
     - Scientific computing
     - ML training
   - Options:
     - DASK: NumPy/Pandas parallel
     - Ray: Distributed Python
     - PySpark: Spark in Python
     - Modin: Drop-in Pandas

### Feature Comparison
1. **Functionality**
   - Scheduling:
     - Airflow > Prefect > Luigi
   - Monitoring:
     - Prefect > Airflow > Kubeflow
   - Scalability:
     - DASK > Ray > PySpark

2. **Implementation**
   - Learning Curve:
     - Luigi < Prefect < Airflow
   - Development Speed:
     - Prefect > Airflow > Kubeflow
   - Flexibility:
     - Ray > DASK > PySpark

3. **Operations**
   - Deployment:
     - Argo > Airflow > Prefect
   - Maintenance:
     - Prefect < Airflow < Kubeflow
   - Resource Usage:
     - Luigi < Prefect < DASK

### Best Practices
1. **Architecture Design**
   - Modularity
   - Reusability
   - Error handling
   - Idempotency

2. **Development**
   - Testing strategies
   - Version control
   - Documentation
   - Code review

3. **Operations**
   - Monitoring setup
   - Alert configuration
   - Resource management
   - Backup strategies

4. **Performance**
   - Task granularity
   - Parallel execution
   - Resource allocation
   - Caching strategies

## Tuần 11: Modern Data Lake Formats & Architecture

### Delta Lake
- Transaction Support
- ACID Properties
- Time Travel
- Schema Evolution

#### Bài tập
1. **Delta Operations**
   - Table management
   - Versioning
   - Optimization
   - Vacuum operations
   - Path: `/week11/exercises/delta_ops.py`

2. **Delta Features**
   - Schema enforcement
   - Merge operations
   - Streaming support
   - Path: `/week11/exercises/delta_features.py`

### Apache Iceberg
- Table Format
- Schema Evolution
- Partition Evolution
- Time Travel

#### Bài tập
1. **Iceberg Tables**
   - Table creation
   - Data writing
   - Query optimization
   - Path: `/week11/exercises/iceberg_tables.py`

2. **Iceberg Features**
   - Snapshots
   - Partition specs
   - Maintenance
   - Path: `/week11/exercises/iceberg_features.py`

### Apache Hudi
- Copy-on-Write Tables
- Merge-on-Read Tables
- Incremental Processing
- Upsert Support

#### Bài tập
1. **Hudi Operations**
   - Table types
   - Write operations
   - Incremental queries
   - Path: `/week11/exercises/hudi_ops.py`

2. **Hudi Features**
   - Clustering
   - Compaction
   - Cleaning
   - Path: `/week11/exercises/hudi_features.py`

### Data Lake Architecture
- Multi-Table Transactions
- Data Quality
- Performance Optimization
- Governance

#### Bài tập
1. **Lake Architecture**
   - Design patterns
   - Integration
   - Optimization
   - Path: `/week11/exercises/lake_architecture.py`

2. **Lake Management**
   - Monitoring
   - Maintenance
   - Security
   - Path: `/week11/exercises/lake_management.py`

### Mini Project: Modern Data Lake Platform
- Focus on a real production sample, like image related applications
- Add readme, requirements
- Describe database structure and interactions on readme
- Multiple format support
- Data ingestion
- Query optimization
- Governance framework
- Path: `/week11/mini_project/`

### Format Selection Guide
1. **Delta Lake**
   - Use cases:
     - ACID transactions
     - Real-time analytics
     - Data quality enforcement
   - Features:
     - Strong consistency
     - Schema enforcement
     - Spark integration
     - Azure integration

2. **Apache Iceberg**
   - Use cases:
     - Large scale analytics
     - Schema evolution
     - Multi-engine support
   - Features:
     - Format specification
     - Engine independence
     - AWS integration
     - Fine-grained partitioning

3. **Apache Hudi**
   - Use cases:
     - Incremental processing
     - Near real-time ingestion
     - Upsert heavy workloads
   - Features:
     - Record-level updates
     - Multiple table types
     - Incremental queries
     - AWS integration

### Feature Comparison
1. **Data Management**
   - Transaction Support:
     - Delta Lake > Iceberg > Hudi
   - Schema Evolution:
     - Iceberg > Delta Lake > Hudi
   - Time Travel:
     - Delta Lake = Iceberg > Hudi

2. **Performance**
   - Query Speed:
     - Iceberg > Delta Lake > Hudi
   - Write Performance:
     - Hudi > Delta Lake > Iceberg
   - Storage Efficiency:
     - Hudi > Iceberg > Delta Lake

3. **Integration**
   - Engine Support:
     - Iceberg > Hudi > Delta Lake
   - Cloud Integration:
     - Delta Lake > Hudi > Iceberg
   - Tool Ecosystem:
     - Delta Lake > Iceberg > Hudi

### Architecture Patterns
1. **Bronze-Silver-Gold**
   - Bronze: Raw data
   - Silver: Cleaned data
   - Gold: Business views

2. **Data Quality Layers**
   - Ingestion validation
   - Processing validation
   - Business validation

3. **Access Patterns**
   - Batch processing
   - Streaming updates
   - Interactive queries

4. **Optimization Strategies**
   - File size optimization
   - Partition optimization
   - Compaction policies
   - Caching strategies

### Best Practices
1. **Design Considerations**
   - Schema design
   - Partition strategy
   - File format selection
   - Security model

2. **Operational Excellence**
   - Monitoring setup
   - Backup procedures
   - Disaster recovery
   - Performance tuning

3. **Governance & Compliance**
   - Data cataloging
   - Access control
   - Audit logging
   - Retention policies

## Tuần 12: Data Optimization & Advanced Storage

### Data Encryption & Security
- Encryption at Rest
- Encryption in Transit
- Key Management
- Access Control

#### Bài tập
1. **Data Encryption**
   - Symmetric encryption
   - Asymmetric encryption
   - Key rotation
   - Secure storage
   - Path: `/week12/exercises/data_encryption.py`

2. **Security Management**
   - Access policies
   - Audit logging
   - Compliance checks
   - Path: `/week12/exercises/security_mgmt.py`

### Query Optimization
- Index Strategies
- Partitioning
- Query Planning
- Caching Mechanisms

#### Bài tập
1. **Index Optimization**
   - B-tree indexes
   - Bitmap indexes
   - Hash indexes
   - Custom indexes
   - Path: `/week12/exercises/index_optimization.py`

2. **Query Performance**
   - Query plans
   - Statistics collection
   - Plan caching
   - Path: `/week12/exercises/query_performance.py`

### Advanced Storage Patterns
- Columnar Storage
- Vector Storage
- Time-series Optimization
- Multi-modal Data

#### Bài tập
1. **Storage Formats**
   - Parquet optimization
   - Vector indexing
   - Compression strategies
   - Path: `/week12/exercises/storage_formats.py`

2. **Specialized Storage**
   - Image storage (DeepLake)
   - Time-series optimization
   - Graph data storage
   - Path: `/week12/exercises/specialized_storage.py`

### Feature-based Optimization
- Feature Stores
- Materialized Views
- Pre-aggregation
- Dynamic Filtering

#### Bài tập
1. **Feature Engineering**
   - Feature extraction
   - Feature storage
   - Online serving
   - Path: `/week12/exercises/feature_engineering.py`

2. **Query Acceleration**
   - View materialization
   - Aggregation tables
   - Dynamic filters
   - Path: `/week12/exercises/query_acceleration.py`

### Mini Project: Optimized Data Platform
- Focus on a real production sample
- Add readme, requirements
- Describe database structure and interactions on readme
- Multi-modal storage
- Advanced indexing
- Query optimization
- Security implementation
- Path: `/week12/mini_project/optimized_platform.py`

### Optimization Techniques Guide
1. **Data Encryption**
   - Approaches:
     - Transparent Data Encryption (TDE)
     - Application-Level Encryption
     - Column-Level Encryption
   - Key Management:
     - HSM Integration
     - Key Rotation
     - Access Control

2. **Index Optimization**
   - Types:
     - B-tree: General purpose
     - Bitmap: Low cardinality
     - Hash: Exact match
     - GiST: Geometric/Custom
   - Strategies:
     - Covering indexes
     - Partial indexes
     - Expression indexes

3. **Storage Optimization**
   - Formats:
     - Row-oriented: OLTP
     - Column-oriented: OLAP
     - Hybrid: Mixed workload
   - Specialized:
     - Vector: Similarity search
     - Graph: Relationship data
     - Time-series: Sequential data

4. **Feature-based**
   - Feature Store:
     - Online/Offline serving
     - Version control
     - Monitoring
   - Query Optimization:
     - Materialized views
     - Dynamic filtering
     - Adaptive execution

### Implementation Patterns
1. **Data Encryption**
   ```python
   # Example encryption strategy
   class DataEncryption:
       def encrypt_column(self, data, key):
           # Column-level encryption
           pass
       
       def rotate_keys(self, old_key, new_key):
           # Key rotation logic
           pass
   ```

2. **Index Management**
   ```python
   # Custom index implementation
   class CustomIndex:
       def build_index(self, data):
           # Index construction
           pass
       
       def query_index(self, condition):
           # Fast lookup
           pass
   ```

3. **Storage Optimization**
   ```python
   # Specialized storage handler
   class ImageStorage:
       def store_image(self, image_data):
           # Efficient image storage
           pass
       
       def retrieve_image(self, image_id):
           # Fast image retrieval
           pass
   ```

### Best Practices
1. **Performance Optimization**
   - Monitoring:
     - Query performance
     - Resource usage
     - Cache hit rates
   - Tuning:
     - Index selection
     - Partition strategy
     - Cache configuration

2. **Security Implementation**
   - Encryption:
     - Key management
     - Algorithm selection
     - Performance impact
   - Access Control:
     - Role-based access
     - Row-level security
     - Column-level security

3. **Storage Management**
   - Data Layout:
     - Partition design
     - Compression selection
     - File format choice
   - Optimization:
     - Auto-vacuum
     - Statistics update
     - Index maintenance

4. **Query Optimization**
   - Analysis:
     - Query patterns
     - Data distribution
     - Access patterns
   - Implementation:
     - Index creation
     - View materialization
     - Partition pruning

## Final Projects

### Basic Data Engineering Project Options

1. **ETL Pipeline System**
   - Requirements:
     - Extract data từ multiple sources (APIs, databases, files)
     - Transform data với validation và cleaning
     - Load vào data warehouse
     - Basic monitoring và error handling
   - Tech Stack:
     - Python
     - SQL Database (PostgreSQL/MySQL)
     - Basic ETL tools (pandas, SQLAlchemy)
     - Simple scheduling (cron/scheduler)
   - Deliverables:
     - Source code
     - Documentation
     - Test cases
     - Demo video

2. **Data Quality System**
   - Requirements:
     - Data validation rules
     - Quality checks và reporting
     - Alert system
     - Basic dashboard
   - Tech Stack:
     - Python
     - SQL Database
     - Basic visualization (matplotlib/seaborn)
     - Email notifications
   - Deliverables:
     - Validation framework
     - Quality reports
     - Alert system
     - Documentation

3. **Simple Analytics Platform**
   - Requirements:
     - Data ingestion
     - Basic analytics
     - Report generation
     - Simple visualizations
   - Tech Stack:
     - Python
     - SQL Database
     - Basic BI tools
     - Web framework (Flask/FastAPI)
   - Deliverables:
     - Analytics system
     - Reports
     - Basic dashboard
     - Documentation

### Advanced Data Engineering Project Options

1. **Real-time Data Platform**
   - Requirements:
     - Streaming data ingestion
     - Real-time processing
     - Multiple storage layers
     - Advanced monitoring
     - Security implementation
   - Tech Stack:
     - Apache Kafka
     - Apache Spark
     - Delta Lake/Iceberg
     - Elasticsearch
     - Grafana
   - Advanced Features:
     - Schema evolution
     - Data versioning
     - ACID compliance
     - Security & encryption
   - Deliverables:
     - Platform architecture
     - Implementation code
     - Performance metrics
     - Documentation
     - Deployment guide

2. **ML Feature Platform**
   - Requirements:
     - Feature engineering pipeline
     - Feature store
     - Online/offline serving
     - Version control
     - Monitoring system
   - Tech Stack:
     - Feature store (Feast/Hopsworks)
     - Apache Airflow
     - Redis/MongoDB
     - PostgreSQL
     - MLflow
   - Advanced Features:
     - Feature computation
     - Point-in-time correctness
     - Feature sharing
     - Access control
   - Deliverables:
     - Feature platform
     - API documentation
     - Performance analysis
     - Usage examples
     - Monitoring setup

3. **Data Lake System**
   - Requirements:
     - Multi-format data ingestion
     - Data catalog
     - Query engine
     - Governance framework
     - Security implementation
   - Tech Stack:
     - Apache Spark
     - Delta Lake/Hudi
     - Apache Atlas
     - Trino/Presto
     - Ranger/Knox
   - Advanced Features:
     - ACID transactions
     - Time travel
     - Column encryption
     - Access control
   - Deliverables:
     - System architecture
     - Implementation code
     - Governance framework
     - Security setup
     - Documentation

### Evaluation Criteria

#### Basic Projects (100 points)
1. **Functionality (40 points)**
   - Core features implementation
   - Error handling
   - Data accuracy
   - Performance

2. **Code Quality (30 points)**
   - Clean code
   - Documentation
   - Testing
   - Best practices

3. **Project Structure (20 points)**
   - Organization
   - Modularity
   - Configuration
   - Deployment

4. **Documentation (10 points)**
   - Setup guide
   - API documentation
   - Usage examples
   - Architecture diagram

#### Advanced Projects (100 points)
1. **Architecture (30 points)**
   - System design
   - Scalability
   - Reliability
   - Integration

2. **Implementation (30 points)**
   - Feature completeness
   - Code quality
   - Performance
   - Security

3. **Operations (20 points)**
   - Monitoring
   - Error handling
   - Recovery
   - Maintenance

4. **Documentation (20 points)**
   - Technical documentation
   - API specifications
   - Deployment guide
   - Performance analysis


### Timeline
1. **Basic Projects**
   - Week 1-2: Design & Setup
   - Week 3-4: Implementation
   - Week 5: Testing & Documentation
   - Week 6: Presentation

2. **Advanced Projects**
   - Week 1-2: Architecture & Design
   - Week 3-4: Core Implementation
   - Week 5-6: Advanced Features
   - Week 7: Testing & Documentation
   - Week 8: Presentation

### Submission Requirements
1. **Source Code**
   - GitHub repository
   - Clean commit history
   - README documentation
   - Setup instructions

2. **Documentation**
   - System architecture
   - API documentation
   - User guide
   - Performance analysis

3. **Presentation**
   - System demo
   - Architecture overview
   - Technical decisions
   - Lessons learned

4. **Optional**
   - Blog post
   - Video demo
   - Performance benchmarks
   - Advanced features