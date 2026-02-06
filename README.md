# Distributed Transactions Kafka Integration Tests

This repository contains comprehensive integration tests for the [distributed-transactions-kafka](https://github.com/rakeshyadalla/distributed-transactions-kafka) system - a distributed transaction implementation using the Saga pattern with Kafka.

## System Under Test

The distributed-transactions-kafka system consists of three microservices:
- **order-service** - Sends Order events to Kafka and orchestrates distributed transactions
- **payment-service** - Performs local transactions on customer accounts based on Order price
- **stock-service** - Performs local transactions on store inventory based on product count

### Transaction Flow
1. `order-service` sends a new Order with `status == NEW`
2. `payment-service` and `stock-service` receive the Order and perform local transactions
3. Both services send response Orders with `status == ACCEPT` or `status == REJECT`
4. `order-service` joins responses by Order ID and sends final status: `CONFIRMED`, `ROLLBACK`, or `REJECTED`
5. Services receive final status and commit or rollback their local transactions

## Prerequisites

- **Java 17** or higher
- **Docker** (for Testcontainers)
- **Gradle 8.x** (wrapper included)

## Test Categories

### 1. Positive Scenario Tests (`PositiveScenarioTests.java`)
- Happy Path: Order creation -> Payment acceptance -> Stock acceptance -> Confirmation
- Successful Order Query: Verify GET endpoint returns orders from state store
- Multiple Orders: Process multiple orders concurrently
- Valid Customer and Product: Orders accepted by both services
- Minimum Valid Values: Orders with minimum valid values processed successfully

### 2. Rejection Scenario Tests (`RejectionScenarioTests.java`)
- Payment Rejection: Order with insufficient customer balance -> Rollback
- Stock Rejection: Order with insufficient stock -> Rollback
- Mixed Rejection (Payment accepts, Stock rejects): Rollback with STOCK source
- Mixed Rejection (Stock accepts, Payment rejects): Rollback with PAYMENT source
- Both Reject: Payment and Stock both reject -> REJECT status

### 3. Invalid Data Scenario Tests (`InvalidDataScenarioTests.java`)
- Non-existent Customer: Order with invalid customerId
- Non-existent Product: Order with invalid productId
- Negative Price: Order with negative price value
- Negative Product Count: Order with negative productCount value
- Zero Price: Order with zero price value
- Zero Product Count: Order with zero productCount value
- Insufficient Funds: Order price exceeds customer balance
- Insufficient Stock: Order quantity exceeds available stock
- Duplicate Order ID: Idempotency test with same order ID
- Large Order Values: Order with maximum integer values

### 4. Infrastructure Failure Tests (`InfrastructureFailureTests.java`)
- Timeout Scenario: Service doesn't respond within expected time
- Kafka Connection Failure: Invalid bootstrap servers
- Schema Registry Failure: Invalid schema registry URL
- State Store Query: GET endpoint when Kafka Streams may not be ready
- Consumer Timeout: Short poll timeout behavior
- Concurrent Order Processing: System under concurrent load
- Message Ordering: Messages maintain order within partition
- Recovery After Failure: System recovery after temporary failure
- Empty Topic: Consuming from empty topic
- High Volume: Processing high volume of orders

## Running the Tests

### Start the Target System

Before running integration tests, you need to start the distributed-transactions-kafka system:

1. Start Kafka infrastructure:
```bash
cd /path/to/distributed-transactions-kafka
export DOCKER_HOST_IP=127.0.0.1
docker compose -f zk-single-kafka-multiple-schema-registry-ui.yml up -d
```

2. Build common-lib:
```bash
cd common-lib
gradle clean build
```

3. Start all three services (in separate terminals):
```bash
# Terminal 1 - Order Service
cd order-service
gradle clean bootRun

# Terminal 2 - Payment Service
cd payment-service
gradle clean bootRun

# Terminal 3 - Stock Service
cd stock-service
gradle clean bootRun
```

### Run Integration Tests

```bash
# Run all tests
./gradlew test

# Run specific test class
./gradlew test --tests "PositiveScenarioTests"
./gradlew test --tests "RejectionScenarioTests"
./gradlew test --tests "InvalidDataScenarioTests"
./gradlew test --tests "InfrastructureFailureTests"

# Run with detailed output
./gradlew test --info
```

### Test Infrastructure

The tests use **Testcontainers** to automatically provision:
- Zookeeper
- Kafka (Confluent Platform 7.5.1)
- Schema Registry

This means you don't need to manually start Kafka for the infrastructure failure tests - they will spin up their own containers.

## Project Structure

```
src/
├── main/
│   ├── avro/
│   │   ├── Order.avsc          # Avro schema for Order
│   │   └── OrderKey.avsc       # Avro schema for OrderKey
│   └── java/
│       └── com/ashu/practice/common/
│           └── Constants.java   # Kafka topic constants
└── test/
    ├── java/
    │   └── com/ashu/practice/integration/
    │       ├── base/
    │       │   └── BaseIntegrationTest.java    # Base test class with common utilities
    │       ├── client/
    │       │   └── OrderServiceClient.java     # HTTP client for order-service
    │       ├── config/
    │       │   ├── KafkaTestContainersConfig.java  # Testcontainers setup
    │       │   ├── TestKafkaConsumerConfig.java    # Kafka consumer configuration
    │       │   └── TestKafkaProducerConfig.java    # Kafka producer configuration
    │       ├── dto/
    │       │   ├── OrderDto.java               # Order DTO for REST responses
    │       │   └── OrderRequest.java           # Order request DTO
    │       └── tests/
    │           ├── PositiveScenarioTests.java
    │           ├── RejectionScenarioTests.java
    │           ├── InvalidDataScenarioTests.java
    │           └── InfrastructureFailureTests.java
    └── resources/
        └── application-test.yml    # Test configuration
```

## Technical Stack

- **Spring Boot 3.2.0**
- **Java 17**
- **Apache Kafka with Avro serialization**
- **Confluent Schema Registry**
- **Testcontainers 1.19.3**
- **JUnit 5**
- **AssertJ**
- **Awaitility**
- **REST Assured**

## Configuration

All environment-specific settings are externalized to properties files. You can configure the tests without modifying Java code.

### Configuration Files

1. **`src/test/resources/integration-test.properties`** - Main configuration file
2. **`src/test/resources/application-test.yml`** - YAML format configuration

### Using Testcontainers (Default)

By default, tests use Testcontainers to automatically provision Kafka infrastructure:

```properties
integration.use-testcontainers=true
```

### Using External Kafka Services

To run tests against your actual distributed-transactions-kafka services:

1. Edit `src/test/resources/integration-test.properties`:
```properties
# Disable Testcontainers
integration.use-testcontainers=false

# Your Kafka configuration
integration.kafka.bootstrap-servers=localhost:9092
integration.kafka.schema-registry-url=http://localhost:8081

# Service URLs
integration.services.order-service-url=http://localhost:8080
integration.services.payment-service-port=8081
integration.services.stock-service-port=8082
```

2. Start your distributed-transactions-kafka services
3. Run the tests: `./gradlew test`

### Environment Variable Overrides

You can also override any property using environment variables:

```bash
INTEGRATION_USE_TESTCONTAINERS=false \
INTEGRATION_KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
INTEGRATION_KAFKA_SCHEMA_REGISTRY_URL=http://localhost:8081 \
INTEGRATION_SERVICES_ORDER_SERVICE_URL=http://localhost:8080 \
./gradlew test
```

### Configuration Priority

1. Environment variables (highest priority)
2. System properties (`-D` flags)
3. `integration-test.properties` file
4. Default values (lowest priority)

## Notes

- Tests use unique consumer group IDs to avoid conflicts
- Asynchronous message processing is handled with Awaitility for reliable waiting
- The system uses Kafka Streams with queryable state stores for order retrieval
- All Avro schemas are compatible with the source system

## Troubleshooting

### Docker Issues
Ensure Docker is running and you have sufficient resources allocated.

### Port Conflicts
If port 8080 is in use, update `integration.services.order-service-url` in `integration-test.properties`.

### Timeout Issues
Increase timeout values in test methods if running on slower machines.

## License

This project is for testing purposes for the distributed-transactions-kafka system.
