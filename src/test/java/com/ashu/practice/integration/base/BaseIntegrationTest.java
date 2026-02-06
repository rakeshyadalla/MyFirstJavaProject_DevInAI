package com.ashu.practice.integration.base;

import com.ashu.practice.common.Constants;
import com.ashu.practice.common.model.Order;
import com.ashu.practice.common.model.OrderKey;
import com.ashu.practice.integration.client.OrderServiceClient;
import com.ashu.practice.integration.config.IntegrationTestProperties;
import com.ashu.practice.integration.config.KafkaTestContainersConfig;
import com.ashu.practice.integration.config.TestKafkaConsumerConfig;
import com.ashu.practice.integration.config.TestKafkaProducerConfig;
import com.ashu.practice.integration.dto.OrderDto;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.awaitility.Awaitility.await;

public abstract class BaseIntegrationTest {

    protected static final Logger log = LoggerFactory.getLogger(BaseIntegrationTest.class);

    protected static String kafkaBootstrapServers;
    protected static String schemaRegistryUrl;
    protected static KafkaProducer<OrderKey, Order> producer;
    protected static OrderServiceClient orderServiceClient;

    protected static String orderServiceBaseUrl;
    protected static int paymentServicePort;
    protected static int stockServicePort;

    @BeforeAll
    static void setUpInfrastructure() {
        orderServiceBaseUrl = IntegrationTestProperties.getOrderServiceUrl();
        paymentServicePort = IntegrationTestProperties.getPaymentServicePort();
        stockServicePort = IntegrationTestProperties.getStockServicePort();

        if (IntegrationTestProperties.useTestcontainers()) {
            log.info("Starting Kafka infrastructure using Testcontainers...");
            KafkaTestContainersConfig.startContainers();
            kafkaBootstrapServers = KafkaTestContainersConfig.getKafkaBootstrapServers();
            schemaRegistryUrl = KafkaTestContainersConfig.getSchemaRegistryUrl();
        } else {
            log.info("Using external Kafka infrastructure from properties...");
            kafkaBootstrapServers = IntegrationTestProperties.getKafkaBootstrapServers();
            schemaRegistryUrl = IntegrationTestProperties.getSchemaRegistryUrl();
        }

        log.info("Kafka bootstrap servers: {}", kafkaBootstrapServers);
        log.info("Schema registry URL: {}", schemaRegistryUrl);
        log.info("Order service URL: {}", orderServiceBaseUrl);

        createTopics();

        producer = TestKafkaProducerConfig.createProducer(kafkaBootstrapServers, schemaRegistryUrl);
        orderServiceClient = new OrderServiceClient(orderServiceBaseUrl);
    }

    @AfterAll
    static void tearDownInfrastructure() {
        if (producer != null) {
            producer.close();
        }
    }

    protected static void createTopics() {
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);

        try (AdminClient adminClient = AdminClient.create(adminProps)) {
            List<NewTopic> topics = Arrays.asList(
                    new NewTopic(Constants.TOPIC_ORDERS, 3, (short) 1),
                    new NewTopic(Constants.TOPIC_ORDERS_PAYMENT, 3, (short) 1),
                    new NewTopic(Constants.TOPIC_ORDERS_STOCK, 3, (short) 1)
            );

            adminClient.createTopics(topics).all().get(30, TimeUnit.SECONDS);
            log.info("Created topics: {}", topics.stream().map(NewTopic::name).toList());
        } catch (ExecutionException | InterruptedException | TimeoutException e) {
            if (e.getCause() instanceof org.apache.kafka.common.errors.TopicExistsException) {
                log.info("Topics already exist");
            } else {
                log.error("Failed to create topics", e);
            }
        }
    }

    protected void sendOrderToKafka(Order order) {
        ProducerRecord<OrderKey, Order> record = new ProducerRecord<>(
                Constants.TOPIC_ORDERS,
                new OrderKey(order.getId()),
                order
        );
        try {
            producer.send(record).get(10, TimeUnit.SECONDS);
            producer.flush();
            log.info("Sent order to Kafka: {}", order);
        } catch (Exception e) {
            log.error("Failed to send order to Kafka", e);
            throw new RuntimeException(e);
        }
    }

    protected Order createNewOrder(long id, long customerId, long productId, int productCount, int price) {
        return Order.newBuilder()
                .setId(id)
                .setCustomerId(customerId)
                .setProductId(productId)
                .setProductCount(productCount)
                .setPrice(price)
                .setStatus("NEW")
                .setSource("order")
                .build();
    }

    protected List<Order> consumeOrdersFromTopic(String topic, String groupId, int expectedCount, Duration timeout) {
        List<Order> orders = new ArrayList<>();
        try (KafkaConsumer<OrderKey, Order> consumer = TestKafkaConsumerConfig.createConsumer(
                kafkaBootstrapServers, schemaRegistryUrl, groupId)) {
            consumer.subscribe(Collections.singletonList(topic));

            await().atMost(timeout).until(() -> {
                ConsumerRecords<OrderKey, Order> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<OrderKey, Order> record : records) {
                    orders.add(record.value());
                    log.info("Consumed order from {}: {}", topic, record.value());
                }
                return orders.size() >= expectedCount;
            });
        }
        return orders;
    }

    protected Optional<Order> waitForOrderWithStatus(String topic, String groupId, long orderId, String expectedStatus, Duration timeout) {
        try (KafkaConsumer<OrderKey, Order> consumer = TestKafkaConsumerConfig.createConsumer(
                kafkaBootstrapServers, schemaRegistryUrl, groupId)) {
            consumer.subscribe(Collections.singletonList(topic));

            long startTime = System.currentTimeMillis();
            while (System.currentTimeMillis() - startTime < timeout.toMillis()) {
                ConsumerRecords<OrderKey, Order> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<OrderKey, Order> record : records) {
                    Order order = record.value();
                    log.info("Consumed order from {}: id={}, status={}", topic, order.getId(), order.getStatus());
                    if (order.getId() == orderId && order.getStatus().equals(expectedStatus)) {
                        return Optional.of(order);
                    }
                }
            }
        }
        return Optional.empty();
    }

    protected void waitForOrderInStateStore(long orderId, String expectedStatus, Duration timeout) {
        await().atMost(timeout)
                .pollInterval(Duration.ofMillis(500))
                .until(() -> {
                    List<OrderDto> orders = orderServiceClient.getAllOrders();
                    return orders.stream()
                            .anyMatch(o -> o.getId() == orderId && expectedStatus.equals(o.getStatus()));
                });
    }

    protected boolean isOrderServiceHealthy() {
        return orderServiceClient.isHealthy();
    }
}
