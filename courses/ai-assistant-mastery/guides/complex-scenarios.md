# Complex Scenarios Analysis Guide

## 1. Complex Dependency Graph Analysis

### Analyzing Circular Dependencies
```
EFFECTIVE PROMPT: "Analyze this complex dependency graph:
1. Generate visual representation of dependencies
2. Identify:
   - Circular dependencies
   - Tight coupling points
   - Dependency chains
3. For each circular dependency:
   - Show dependency chain
   - Impact analysis
   - Breaking points
   - Refactoring strategy
4. Propose:
   - Short-term fixes
   - Long-term architectural changes
   - Implementation steps
   - Risk assessment"

Example circular dependency:
ServiceA -> ServiceB -> ServiceC -> ServiceA

Analysis steps:
1. Map complete dependency chain
2. Identify shared resources
3. Find breaking points
4. Design interface abstractions
5. Plan refactoring phases
```

### Microservices Interaction Analysis
```
EFFECTIVE PROMPT: "Analyze this microservices architecture:
1. Create interaction graph showing:
   - Sync/async communications
   - Data flow patterns
   - Shared resources
   - Transaction boundaries
2. Identify:
   - Potential deadlocks
   - Race conditions
   - Performance bottlenecks
   - Single points of failure
3. For each critical path:
   - Failure scenarios
   - Recovery strategies
   - Scaling implications
   - Monitoring points"
```

## 2. Code Quality Analysis

### Complex Code Smell Detection
```
EFFECTIVE PROMPT: "Deep dive code quality analysis:
1. Identify advanced code smells:
   - Hidden coupling
   - Temporal coupling
   - Feature envy
   - Shotgun surgery patterns
   - God classes/methods
2. For each issue:
   - Impact assessment
   - Refactoring difficulty
   - Technical debt cost
3. Generate:
   - Dependency matrices
   - Coupling graphs
   - Complexity metrics
4. Propose refactoring strategy"

Example analysis:
Class: PaymentProcessor
Issues:
- Temporal coupling with OrderSystem
- Feature envy with UserAccount
- Hidden coupling through global state
- High cyclomatic complexity in processPayment()

Refactoring Strategy:
1. Extract PaymentState class
2. Implement Observer pattern
3. Inject dependencies
4. Break down processPayment()
```

### Legacy Code Analysis
```
EFFECTIVE PROMPT: "Analyze this legacy codebase:
1. Create architectural map:
   - Component relationships
   - Data flow
   - External dependencies
   - Business logic distribution
2. Identify:
   - Dead code
   - Duplicate logic
   - Security vulnerabilities
   - Performance issues
3. Generate:
   - Risk assessment matrix
   - Technical debt inventory
   - Migration difficulty score
4. Propose modernization strategy"
```

## 3. Tool Integration Analysis

### CI/CD Pipeline Integration
```
EFFECTIVE PROMPT: "Design CI/CD pipeline integration:
1. Analyze current tools:
   - Source control: Git
   - Build: Maven/Gradle
   - CI: Jenkins/GitHub Actions
   - Testing: JUnit/TestNG
   - Deployment: Kubernetes
2. For each integration point:
   - Data flow requirements
   - Authentication needs
   - Failure scenarios
   - Recovery procedures
3. Generate:
   - Integration architecture
   - Configuration templates
   - Migration scripts
   - Validation tests"

Example Pipeline:
Git -> Jenkins -> SonarQube -> Artifactory -> Kubernetes

Integration Points Analysis:
1. Git Webhooks:
   - Event types
   - Security considerations
   - Rate limiting
2. Jenkins Integration:
   - Build triggers
   - Environment variables
   - Secret management
3. Quality Gates:
   - Metrics collection
   - Threshold configuration
   - Failure handling
```

### Monitoring Stack Integration
```
EFFECTIVE PROMPT: "Design monitoring integration:
1. Analyze tools:
   - Metrics: Prometheus
   - Logging: ELK Stack
   - Tracing: Jaeger
   - Alerting: AlertManager
2. For each component:
   - Data collection points
   - Integration requirements
   - Performance impact
   - Storage needs
3. Generate:
   - Architecture diagram
   - Configuration templates
   - Dashboard designs
   - Alert rules"
```

## 4. Code Base Deep Dive

### Complex Feature Analysis
```
EFFECTIVE PROMPT: "Analyze this complex feature:
1. Create detailed flow:
   - Entry points
   - Business logic paths
   - External interactions
   - Error scenarios
2. Identify:
   - Decision points
   - State changes
   - Resource usage
   - Performance impacts
3. Generate:
   - Sequence diagrams
   - State machines
   - Decision trees
   - Performance profiles"

Example Feature: Payment Processing
Analysis:
1. Entry Points:
   - API endpoints
   - Webhook handlers
   - Scheduled jobs
2. State Transitions:
   - Payment states
   - Order updates
   - Inventory changes
3. External Systems:
   - Payment gateway
   - Fraud detection
   - Notification service
```

### Performance Bottleneck Analysis
```
EFFECTIVE PROMPT: "Analyze performance bottlenecks:
1. Create performance map:
   - CPU usage patterns
   - Memory allocation
   - I/O operations
   - Network calls
2. For each hotspot:
   - Root cause analysis
   - Impact assessment
   - Optimization options
   - Implementation cost
3. Generate:
   - Profiling data
   - Optimization plan
   - Benchmark suite
   - Monitoring setup"
```

## 5. Documentation Analysis

### API Documentation Review
```
EFFECTIVE PROMPT: "Review API documentation:
1. Analyze coverage:
   - Endpoints
   - Request/Response formats
   - Authentication
   - Error handling
2. Identify gaps:
   - Missing scenarios
   - Unclear descriptions
   - Outdated examples
   - Security considerations
3. Generate:
   - Coverage report
   - Update priorities
   - Example collection
   - Testing scenarios"
```

### Architecture Documentation
```
EFFECTIVE PROMPT: "Create architecture documentation:
1. Generate views:
   - Logical view
   - Process view
   - Deployment view
   - Physical view
2. For each component:
   - Purpose
   - Responsibilities
   - Interactions
   - Constraints
3. Include:
   - Decision records
   - Trade-off analysis
   - Evolution path
   - Migration guide"
```

## Best Practices for Complex Analysis

1. **Break Down Complexity**
   - Start with high-level view
   - Identify independent components
   - Analyze interactions
   - Deep dive critical parts

2. **Iterative Analysis**
   - Begin with broad analysis
   - Identify areas needing focus
   - Deep dive specific areas
   - Validate findings

3. **Documentation Strategy**
   - Use multiple formats
   - Include diagrams
   - Provide examples
   - Link related documents

4. **Validation Approach**
   - Review findings
   - Test assumptions
   - Verify impacts
   - Document uncertainties 