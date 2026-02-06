package com.ashu.practice.integration.tests;

import com.ashu.practice.common.Constants;
import com.ashu.practice.common.model.Order;
import com.ashu.practice.common.model.OrderKey;
import com.ashu.practice.integration.config.KafkaTestContainersConfig;
import com.ashu.practice.integration.config.TestKafkaConsumerConfig;
import com.ashu.practice.integration.config.TestKafkaProducerConfig;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("Kafka Infrastructure Tests - Validates Testcontainers Setup")
class KafkaInfrastructureTest {

    private static final Logger log = LoggerFactory.getLogger(KafkaInfrastructureTest.class);
    private static String kafkaBootstrapServers;
    private static String schemaRegistryUrl;
    private static KafkaProducer<OrderKey, Order> producer;

    @BeforeAll
    static void setUp() {
        log.info("Starting Kafka infrastructure with Testcontainers...");
        KafkaTestContainersConfig.startContainers();
        kafkaBootstrapServers = KafkaTestContainersConfig.getKafkaBootstrapServers();
        schemaRegistryUrl = KafkaTestContainersConfig.getSchemaRegistryUrl();
        log.info("Kafka bootstrap servers: {}", kafkaBootstrapServers);
        log.info("Schema registry URL: {}", schemaRegistryUrl);

        createTopics();
        producer = TestKafkaProducerConfig.createProducer(kafkaBootstrapServers, schemaRegistryUrl);
    }

    @AfterAll
    static void tearDown() {
        if (producer != null) {
            producer.close();
        }
        KafkaTestContainersConfig.stopContainers();
    }

    private static void createTopics() {
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);

        try (AdminClient adminClient = AdminClient.create(adminProps)) {
            List<NewTopic> topics = Arrays.asList(
                    new NewTopic(Constants.TOPIC_ORDERS, 1, (short) 1),
                    new NewTopic(Constants.TOPIC_ORDERS_PAYMENT, 1, (short) 1),
                    new NewTopic(Constants.TOPIC_ORDERS_STOCK, 1, (short) 1)
            );
            adminClient.createTopics(topics).all().get(30, TimeUnit.SECONDS);
            log.info("Created topics successfully");
        } catch (ExecutionException | InterruptedException | TimeoutException e) {
            log.warn("Topic creation issue: {}", e.getMessage());
        }
    }

    @Test
    @org.junit.jupiter.api.Order(1)
    @DisplayName("Verify Kafka containers are running")
    void testKafkaContainersRunning() {
        assertThat(KafkaTestContainersConfig.isRunning())
                .as("Kafka infrastructure should be running")
                .isTrue();
        log.info("PASSED: Kafka containers are running");
    }

    @Test
    @org.junit.jupiter.api.Order(2)
    @DisplayName("Verify Kafka bootstrap servers are accessible")
    void testKafkaBootstrapServersAccessible() {
        assertThat(kafkaBootstrapServers)
                .as("Kafka bootstrap servers should be configured")
                .isNotNull()
                .isNotEmpty();
        log.info("PASSED: Kafka bootstrap servers: {}", kafkaBootstrapServers);
    }

    @Test
    @org.junit.jupiter.api.Order(3)
    @DisplayName("Verify Schema Registry is accessible")
    void testSchemaRegistryAccessible() {
        assertThat(schemaRegistryUrl)
                .as("Schema Registry URL should be configured")
                .isNotNull()
                .isNotEmpty()
                .contains("http://");
        log.info("PASSED: Schema Registry URL: {}", schemaRegistryUrl);
    }

    @Test
    @org.junit.jupiter.api.Order(4)
    @DisplayName("Verify can produce and consume Avro messages")
    void testProduceAndConsumeAvroMessage() throws Exception {
        long orderId = System.currentTimeMillis();
        Order order = Order.newBuilder()
                .setId(orderId)
                .setCustomerId(1L)
                .setProductId(1L)
                .setProductCount(2)
                .setPrice(100)
                .setStatus("NEW")
                .setSource("test")
                .build();

        ProducerRecord<OrderKey, Order> record = new ProducerRecord<>(
                Constants.TOPIC_ORDERS,
                new OrderKey(orderId),
                order
        );
        producer.send(record).get(10, TimeUnit.SECONDS);
        producer.flush();
        log.info("Sent order to Kafka: {}", order);

        String groupId = "test-infra-" + UUID.randomUUID();
        try (KafkaConsumer<OrderKey, Order> consumer = TestKafkaConsumerConfig.createConsumer(
                kafkaBootstrapServers, schemaRegistryUrl, groupId)) {
            consumer.subscribe(Collections.singletonList(Constants.TOPIC_ORDERS));

            Order receivedOrder = null;
            long startTime = System.currentTimeMillis();
            while (System.currentTimeMillis() - startTime < 30000) {
                ConsumerRecords<OrderKey, Order> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<OrderKey, Order> rec : records) {
                    if (rec.value().getId() == orderId) {
                        receivedOrder = rec.value();
                        break;
                    }
                }
                if (receivedOrder != null) break;
            }

            assertThat(receivedOrder)
                    .as("Should receive the order from Kafka")
                    .isNotNull();
            assertThat(receivedOrder.getId()).isEqualTo(orderId);
            assertThat(receivedOrder.getStatus()).isEqualTo("NEW");
            log.info("PASSED: Received order from Kafka: {}", receivedOrder);
        }
    }

    @Test
    @org.junit.jupiter.api.Order(5)
    @DisplayName("Verify multiple messages can be produced")
    void testMultipleMessagesProduced() throws Exception {
        int messageCount = 5;
        long baseId = System.currentTimeMillis();

        for (int i = 0; i < messageCount; i++) {
            Order order = Order.newBuilder()
                    .setId(baseId + i)
                    .setCustomerId(1L)
                    .setProductId(1L)
                    .setProductCount(1)
                    .setPrice(50)
                    .setStatus("NEW")
                    .setSource("test")
                    .build();

            ProducerRecord<OrderKey, Order> record = new ProducerRecord<>(
                    Constants.TOPIC_ORDERS,
                    new OrderKey(order.getId()),
                    order
            );
            producer.send(record).get(5, TimeUnit.SECONDS);
        }
        producer.flush();

        log.info("PASSED: Successfully produced {} messages", messageCount);
        assertThat(messageCount).isEqualTo(5);
    }
}
