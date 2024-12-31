# Complex Dependencies Analysis Guide

## 1. Dependency Graph Visualization & Analysis

### Initial Analysis Prompt
```
PROMPT: "Analyze this codebase's dependency structure:
1. Generate a complete dependency graph showing:
   - Direct dependencies
   - Indirect dependencies
   - Circular dependencies
   - Dependency strength (frequency of use)
2. Identify critical paths and bottlenecks
3. Highlight potential architectural issues"

Example Output:
ServiceA [weight=3] → ServiceB [weight=5] → ServiceC [weight=2] → ServiceA
                   ↘ Cache [weight=4] ↗
                   → Database [weight=6] ↗
```

### Deep Dive Analysis
```
PROMPT: "For each identified circular dependency:
1. Map complete dependency chain
2. Show all interaction points
3. Identify:
   - Shared resources
   - Communication patterns
   - State dependencies
   - Transaction boundaries
4. Assess impact on:
   - Testing
   - Deployment
   - Scalability
   - Maintenance"
```

## 2. Breaking Complex Dependencies

### Strategy Development
```
PROMPT: "For this circular dependency chain:
[Show specific chain]

1. Identify potential breaking points:
   - Interface abstractions
   - Event-based decoupling
   - Shared service extraction
   - State management refactoring

2. For each solution, analyze:
   - Implementation complexity
   - Migration effort
   - Testing requirements
   - Production impact"

Example Analysis:
ServiceA → ServiceB → ServiceC → ServiceA

Breaking Points:
1. Extract shared logic to new service
2. Implement event-based communication
3. Create interface abstractions
4. Use dependency injection
```

### Implementation Planning
```
PROMPT: "Create implementation plan for breaking dependency:
1. Current state analysis
2. Target architecture design
3. Step-by-step migration plan
4. Rollback procedures
5. Testing strategy
6. Monitoring requirements"
```

## 3. Microservices Dependency Analysis

### Communication Pattern Analysis
```
PROMPT: "Analyze microservices communication patterns:
1. Map all interaction types:
   - Synchronous calls
   - Asynchronous messages
   - Shared resources
   - Database dependencies

2. For each interaction:
   - Latency requirements
   - Failure scenarios
   - Retry strategies
   - Circuit breaker needs"

Example:
OrderService:
- Sync: PaymentService (timeout=2s)
- Async: NotificationService (retry=3)
- Resource: OrderDB (shared with InventoryService)
```

### Failure Impact Analysis
```
PROMPT: "For this microservices architecture:
1. Create failure impact map
2. Identify:
   - Cascade failure paths
   - Recovery sequences
   - Circuit breaker points
   - Fallback options

3. Design:
   - Resilience patterns
   - Recovery procedures
   - Monitoring strategy"
```

## 4. Database Dependencies

### Schema Dependency Analysis
```
PROMPT: "Analyze database schema dependencies:
1. Map table relationships:
   - Foreign key constraints
   - Shared columns
   - Inherited structures
   - View dependencies

2. Identify:
   - Circular references
   - Complex joins
   - Performance bottlenecks
   - Locking patterns"

Example:
Orders → OrderItems → Products
     → Customers → Addresses
     → Payments → PaymentMethods
```

### Query Dependency Analysis
```
PROMPT: "Analyze query dependencies:
1. Map complex queries:
   - Join patterns
   - Subquery usage
   - View dependencies
   - Function calls

2. Identify:
   - Performance issues
   - Locking problems
   - Deadlock potential
   - Optimization options"
```

## 5. Dependency Monitoring

### Monitoring Setup
```
PROMPT: "Design dependency monitoring:
1. Define metrics:
   - Response times
   - Error rates
   - Circuit breaker status
   - Resource usage

2. Create alerts for:
   - Cascade failures
   - Performance degradation
   - Resource exhaustion
   - Circuit breaker trips"
```

### Health Check Design
```
PROMPT: "Design health check system:
1. For each dependency:
   - Check method
   - Frequency
   - Timeout values
   - Failure thresholds

2. Define:
   - Recovery procedures
   - Fallback options
   - Alert conditions
   - Escalation paths"
```

## 6. Best Practices

### Design Principles
1. **Single Responsibility**
   - Each service owns its data
   - Clear boundaries
   - Minimal shared state

2. **Interface Segregation**
   - Specific interfaces
   - Minimal dependencies
   - Clear contracts

3. **Dependency Inversion**
   - Abstract dependencies
   - Pluggable implementations
   - Testable components

### Implementation Guidelines
1. **Service Communication**
   - Prefer async when possible
   - Use circuit breakers
   - Implement timeouts
   - Handle partial failures

2. **State Management**
   - Minimize shared state
   - Use event sourcing
   - Implement CQRS
   - Version interfaces

3. **Testing Strategy**
   - Unit tests with mocks
   - Integration tests
   - Contract tests
   - Chaos testing 