# Complex E-commerce System Analysis

## 1. System Architecture

```
ecommerce-platform/
├── services/
│   ├── order/
│   │   ├── domain/
│   │   │   ├── entities/
│   │   │   ├── value_objects/
│   │   │   ├── aggregates/
│   │   │   └── events/
│   │   ├── application/
│   │   │   ├── commands/
│   │   │   ├── queries/
│   │   │   └── services/
│   │   ├── infrastructure/
│   │   │   ├── repositories/
│   │   │   ├── adapters/
│   │   │   └── persistence/
│   │   └── interfaces/
│   │       ├── rest/
│   │       ├── grpc/
│   │       └── events/
│   ├── payment/
│   │   ├── domain/
│   │   │   ├── payment_processor/
│   │   │   ├── fraud_detection/
│   │   │   └── compliance/
│   │   ├── infrastructure/
│   │   │   ├── gateways/
│   │   │   ├── encryption/
│   │   │   └── audit/
│   │   └── interfaces/
│   ├── inventory/
│   │   ├── domain/
│   │   │   ├── stock_management/
│   │   │   ├── warehouse/
│   │   │   └── suppliers/
│   │   ├── infrastructure/
│   │   └── interfaces/
│   └── user/
│       ├── domain/
│       │   ├── authentication/
│       │   ├── authorization/
│       │   └── profile/
│       ├── infrastructure/
│       └── interfaces/
├── shared/
│   ├── kernel/
│   │   ├── events/
│   │   ├── commands/
│   │   └── queries/
│   ├── infrastructure/
│   │   ├── messaging/
│   │   ├── caching/
│   │   ├── logging/
│   │   └── monitoring/
│   └── utils/
└── infrastructure/
    ├── database/
    │   ├── migrations/
    │   ├── repositories/
    │   └── models/
    ├── cache/
    │   ├── redis/
    │   └── memcached/
    ├── queue/
    │   ├── kafka/
    │   └── rabbitmq/
    └── monitoring/
        ├── prometheus/
        ├── grafana/
        └── elastic/
```

## 2. Dependency Analysis Prompts

### 2.1 Generate Service Interaction Graph
```
PROMPT: "Analyze the service interactions in this e-commerce system:
1. Map all service-to-service communications
2. Identify:
   - Synchronous calls
   - Asynchronous events
   - Shared resources
   - External dependencies
3. Show interaction frequency and criticality
4. Highlight potential bottlenecks"

Expected Output:
OrderService [critical] 
  → PaymentService [sync, high-freq]
  → InventoryService [sync, high-freq]
  → NotificationService [async, medium-freq]
  → UserService [sync, low-freq]

PaymentService [critical]
  → FraudDetectionService [sync, high-freq]
  → ComplianceService [async, low-freq]
  → AuditService [async, high-freq]
```

### 2.2 Identify Complex Dependencies
```
PROMPT: "For the OrderService domain:
1. Map internal dependencies between:
   - Domain entities
   - Value objects
   - Aggregates
   - Domain events
2. Show lifecycle hooks
3. Identify transaction boundaries
4. Highlight invariant checks"

Example Complex Dependency:
Order Aggregate
  → OrderItems [strong]
  → Payment [lifecycle]
  → Inventory [invariant]
  → Shipping [async]
```

## 3. Code Smell Detection

### 3.1 Domain Layer Analysis
```python
# Example problematic code
class OrderService:
    def __init__(self, 
                 payment_svc, 
                 inventory_svc, 
                 notification_svc,
                 user_svc,
                 shipping_svc,
                 audit_svc,
                 cache,
                 db,
                 event_bus):
        # Too many dependencies - God Class smell
        self.payment_svc = payment_svc
        self.inventory_svc = inventory_svc
        # ... more dependencies

    async def process_order(self, order_data):
        # Complex method with multiple responsibilities
        # Validate order
        if not self._validate_order(order_data):
            raise ValidationError()
            
        # Check inventory
        inventory_status = await self.inventory_svc.check(
            order_data.items
        )
        
        # Process payment
        payment_result = await self.payment_svc.process(
            order_data.payment
        )
        
        # Update inventory
        await self.inventory_svc.reserve(
            order_data.items
        )
        
        # Send notifications
        await self.notification_svc.notify(
            order_data.user,
            'ORDER_CREATED'
        )
        
        # Update analytics
        await self.audit_svc.log(
            'ORDER_PROCESSED',
            order_data
        )
```

### 3.2 Code Smell Analysis Prompt
```
PROMPT: "Analyze this OrderService implementation:
1. Identify code smells:
   - God Class characteristics
   - High coupling points
   - Single Responsibility violations
   - DRY violations
2. For each smell:
   - Impact assessment
   - Refactoring suggestions
   - Implementation priority
3. Propose cleaner architecture"

Expected Analysis:
1. God Class Smell:
   - Too many dependencies
   - Too many responsibilities
   - Solution: Split into smaller services

2. High Coupling:
   - Direct service dependencies
   - Solution: Use event-driven pattern

3. Transaction Script:
   - Procedural order processing
   - Solution: Use domain events
```

## 4. Refactoring Strategy

### 4.1 Proposed Clean Architecture
```python
# Domain Events
class OrderCreated(DomainEvent):
    pass

class PaymentProcessed(DomainEvent):
    pass

# Order Aggregate
class Order(AggregateRoot):
    def create(self, order_data):
        self._validate(order_data)
        self._set_initial_state(order_data)
        self.raise_event(OrderCreated(self))

    def process_payment(self, payment_data):
        payment = Payment.process(payment_data)
        self.raise_event(PaymentProcessed(payment))

# Application Service
class OrderApplicationService:
    def __init__(self, order_repository, event_bus):
        self.repository = order_repository
        self.event_bus = event_bus

    async def create_order(self, command):
        order = Order.create(command.order_data)
        await self.repository.save(order)
        await self.event_bus.publish(order.events)
```

### 4.2 Event Handlers
```python
@event_handler(OrderCreated)
async def handle_order_created(event):
    await inventory_service.reserve(event.items)
    await notification_service.notify_customer(event.user_id)

@event_handler(PaymentProcessed)
async def handle_payment_processed(event):
    await shipping_service.schedule(event.order_id)
    await analytics_service.track(event)
```

## 5. Documentation Generation

### 5.1 Architecture Documentation Prompt
```
PROMPT: "Generate comprehensive documentation for this e-commerce system:
1. Architecture Overview
   - System components
   - Design patterns
   - Technology stack
2. Service Descriptions
   - Responsibilities
   - Dependencies
   - APIs
3. Data Flow
   - Main scenarios
   - Error handling
4. Deployment
   - Infrastructure requirements
   - Scaling considerations
5. Monitoring
   - Key metrics
   - Alert thresholds"
```

### 5.2 API Documentation Prompt
```
PROMPT: "For the OrderService REST API:
1. Document all endpoints
2. Include:
   - Request/Response formats
   - Authentication requirements
   - Rate limits
   - Error scenarios
3. Provide examples
4. Note dependencies"
```

## 6. Monitoring Strategy

### 6.1 Metrics Collection
```yaml
metrics:
  order_service:
    - name: order_creation_rate
      type: counter
      labels: [status, payment_method]
    - name: order_processing_time
      type: histogram
      buckets: [0.1, 0.5, 1, 2, 5]
    - name: active_orders
      type: gauge
```

### 6.2 Alert Rules
```yaml
alerts:
  - name: high_order_failure_rate
    condition: rate(order_failures[5m]) > 0.1
    severity: critical
    
  - name: slow_order_processing
    condition: histogram_quantile(0.95, order_processing_time) > 2
    severity: warning
``` 