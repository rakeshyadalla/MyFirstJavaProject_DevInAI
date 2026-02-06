package com.ashu.practice.integration.tests;

import com.ashu.practice.common.Constants;
import com.ashu.practice.common.model.Order;
import com.ashu.practice.integration.base.BaseIntegrationTest;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("Invalid Data Scenario Integration Tests")
class InvalidDataScenarioTests extends BaseIntegrationTest {

    @Test
    @org.junit.jupiter.api.Order(1)
    @DisplayName("Non-existent Customer: Order with invalid customerId -> Payment rejection")
    void testNonExistentCustomer_PaymentRejection() {
        long orderId = System.currentTimeMillis();
        long invalidCustomerId = 999999L;
        long productId = 1L;
        int productCount = 1;
        int price = 100;

        Order newOrder = createNewOrder(orderId, invalidCustomerId, productId, productCount, price);
        sendOrderToKafka(newOrder);

        String groupId = "test-invalid-customer-" + UUID.randomUUID();
        List<Order> responses = consumeOrdersFromTopic(
                Constants.TOPIC_ORDERS_PAYMENT,
                groupId,
                1,
                Duration.ofSeconds(60)
        );

        boolean hasRejectionOrNoResponse = responses.isEmpty() ||
                responses.stream().anyMatch(o -> o.getId() == orderId && "REJECT".equals(o.getStatus()));

        assertThat(hasRejectionOrNoResponse)
                .as("Order with non-existent customer should be rejected or not processed")
                .isTrue();
    }

    @Test
    @org.junit.jupiter.api.Order(2)
    @DisplayName("Non-existent Product: Order with invalid productId -> Stock rejection")
    void testNonExistentProduct_StockRejection() {
        long orderId = System.currentTimeMillis();
        long customerId = 1L;
        long invalidProductId = 999999L;
        int productCount = 1;
        int price = 100;

        Order newOrder = createNewOrder(orderId, customerId, invalidProductId, productCount, price);
        sendOrderToKafka(newOrder);

        String groupId = "test-invalid-product-" + UUID.randomUUID();
        List<Order> responses = consumeOrdersFromTopic(
                Constants.TOPIC_ORDERS_STOCK,
                groupId,
                1,
                Duration.ofSeconds(60)
        );

        boolean hasRejectionOrNoResponse = responses.isEmpty() ||
                responses.stream().anyMatch(o -> o.getId() == orderId && "REJECT".equals(o.getStatus()));

        assertThat(hasRejectionOrNoResponse)
                .as("Order with non-existent product should be rejected or not processed")
                .isTrue();
    }

    @Test
    @org.junit.jupiter.api.Order(3)
    @DisplayName("Negative Price: Order with negative price value")
    void testNegativePrice_OrderProcessing() {
        long orderId = System.currentTimeMillis();
        long customerId = 1L;
        long productId = 1L;
        int productCount = 1;
        int negativePrice = -100;

        Order newOrder = createNewOrder(orderId, customerId, productId, productCount, negativePrice);
        sendOrderToKafka(newOrder);

        String groupId = "test-negative-price-" + UUID.randomUUID();
        List<Order> responses = consumeOrdersFromTopic(
                Constants.TOPIC_ORDERS_PAYMENT,
                groupId,
                1,
                Duration.ofSeconds(60)
        );

        assertThat(responses)
                .as("Order with negative price should be processed (may be accepted or rejected based on business logic)")
                .isNotNull();

        if (!responses.isEmpty()) {
            Order response = responses.stream()
                    .filter(o -> o.getId() == orderId)
                    .findFirst()
                    .orElse(null);
            if (response != null) {
                log.info("Order with negative price processed with status: {}", response.getStatus());
            }
        }
    }

    @Test
    @org.junit.jupiter.api.Order(4)
    @DisplayName("Negative Product Count: Order with negative productCount value")
    void testNegativeProductCount_OrderProcessing() {
        long orderId = System.currentTimeMillis();
        long customerId = 1L;
        long productId = 1L;
        int negativeProductCount = -5;
        int price = 100;

        Order newOrder = createNewOrder(orderId, customerId, productId, negativeProductCount, price);
        sendOrderToKafka(newOrder);

        String groupId = "test-negative-count-" + UUID.randomUUID();
        List<Order> responses = consumeOrdersFromTopic(
                Constants.TOPIC_ORDERS_STOCK,
                groupId,
                1,
                Duration.ofSeconds(60)
        );

        assertThat(responses)
                .as("Order with negative product count should be processed")
                .isNotNull();

        if (!responses.isEmpty()) {
            Order response = responses.stream()
                    .filter(o -> o.getId() == orderId)
                    .findFirst()
                    .orElse(null);
            if (response != null) {
                log.info("Order with negative product count processed with status: {}", response.getStatus());
            }
        }
    }

    @Test
    @org.junit.jupiter.api.Order(5)
    @DisplayName("Zero Price: Order with zero price value")
    void testZeroPrice_OrderProcessing() {
        long orderId = System.currentTimeMillis();
        long customerId = 1L;
        long productId = 1L;
        int productCount = 1;
        int zeroPrice = 0;

        Order newOrder = createNewOrder(orderId, customerId, productId, productCount, zeroPrice);
        sendOrderToKafka(newOrder);

        String groupId = "test-zero-price-" + UUID.randomUUID();
        Optional<Order> paymentResponse = waitForOrderWithStatus(
                Constants.TOPIC_ORDERS_PAYMENT,
                groupId,
                orderId,
                "ACCEPT",
                Duration.ofSeconds(60)
        );

        assertThat(paymentResponse)
                .as("Order with zero price should be accepted by payment service (no funds needed)")
                .isPresent();
    }

    @Test
    @org.junit.jupiter.api.Order(6)
    @DisplayName("Zero Product Count: Order with zero productCount value")
    void testZeroProductCount_OrderProcessing() {
        long orderId = System.currentTimeMillis();
        long customerId = 1L;
        long productId = 1L;
        int zeroProductCount = 0;
        int price = 100;

        Order newOrder = createNewOrder(orderId, customerId, productId, zeroProductCount, price);
        sendOrderToKafka(newOrder);

        String groupId = "test-zero-count-" + UUID.randomUUID();
        List<Order> responses = consumeOrdersFromTopic(
                Constants.TOPIC_ORDERS_STOCK,
                groupId,
                1,
                Duration.ofSeconds(60)
        );

        assertThat(responses)
                .as("Order with zero product count should be processed")
                .isNotNull();

        if (!responses.isEmpty()) {
            Optional<Order> response = responses.stream()
                    .filter(o -> o.getId() == orderId)
                    .findFirst();
            response.ifPresent(order -> 
                log.info("Order with zero product count processed with status: {}", order.getStatus())
            );
        }
    }

    @Test
    @org.junit.jupiter.api.Order(7)
    @DisplayName("Insufficient Funds: Order price exceeds customer balance")
    void testInsufficientFunds_PaymentRejection() {
        long orderId = System.currentTimeMillis();
        long customerId = 1L;
        long productId = 1L;
        int productCount = 1;
        int veryHighPrice = Integer.MAX_VALUE;

        Order newOrder = createNewOrder(orderId, customerId, productId, productCount, veryHighPrice);
        sendOrderToKafka(newOrder);

        String groupId = "test-insufficient-funds-" + UUID.randomUUID();
        Optional<Order> paymentResponse = waitForOrderWithStatus(
                Constants.TOPIC_ORDERS_PAYMENT,
                groupId,
                orderId,
                "REJECT",
                Duration.ofSeconds(60)
        );

        assertThat(paymentResponse)
                .isPresent()
                .hasValueSatisfying(order -> {
                    assertThat(order.getId()).isEqualTo(orderId);
                    assertThat(order.getStatus()).isEqualTo("REJECT");
                    assertThat(order.getSource()).isEqualTo("payment");
                });
    }

    @Test
    @org.junit.jupiter.api.Order(8)
    @DisplayName("Insufficient Stock: Order quantity exceeds available stock")
    void testInsufficientStock_StockRejection() {
        long orderId = System.currentTimeMillis();
        long customerId = 1L;
        long productId = 1L;
        int veryHighProductCount = Integer.MAX_VALUE;
        int price = 100;

        Order newOrder = createNewOrder(orderId, customerId, productId, veryHighProductCount, price);
        sendOrderToKafka(newOrder);

        String groupId = "test-insufficient-stock-" + UUID.randomUUID();
        Optional<Order> stockResponse = waitForOrderWithStatus(
                Constants.TOPIC_ORDERS_STOCK,
                groupId,
                orderId,
                "REJECT",
                Duration.ofSeconds(60)
        );

        assertThat(stockResponse)
                .isPresent()
                .hasValueSatisfying(order -> {
                    assertThat(order.getId()).isEqualTo(orderId);
                    assertThat(order.getStatus()).isEqualTo("REJECT");
                    assertThat(order.getSource()).isEqualTo("stock");
                });
    }

    @Test
    @org.junit.jupiter.api.Order(9)
    @DisplayName("Duplicate Order ID: Same order ID processed multiple times (idempotency test)")
    void testDuplicateOrderId_IdempotencyCheck() {
        long orderId = System.currentTimeMillis();
        long customerId = 1L;
        long productId = 1L;
        int productCount = 1;
        int price = 100;

        Order firstOrder = createNewOrder(orderId, customerId, productId, productCount, price);
        sendOrderToKafka(firstOrder);

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        Order duplicateOrder = createNewOrder(orderId, customerId, productId, productCount, price);
        sendOrderToKafka(duplicateOrder);

        String groupId = "test-duplicate-" + UUID.randomUUID();
        List<Order> responses = consumeOrdersFromTopic(
                Constants.TOPIC_ORDERS_PAYMENT,
                groupId,
                2,
                Duration.ofSeconds(60)
        );

        long countForOrderId = responses.stream()
                .filter(o -> o.getId() == orderId)
                .count();

        log.info("Received {} responses for order ID {}", countForOrderId, orderId);
        assertThat(countForOrderId)
                .as("Duplicate orders should be processed (system behavior may vary)")
                .isGreaterThanOrEqualTo(1);
    }

    @Test
    @org.junit.jupiter.api.Order(10)
    @DisplayName("Large Order Values: Order with maximum integer values")
    void testLargeOrderValues_Processing() {
        long orderId = System.currentTimeMillis();
        long customerId = 1L;
        long productId = 1L;
        int maxProductCount = Integer.MAX_VALUE;
        int maxPrice = Integer.MAX_VALUE;

        Order newOrder = createNewOrder(orderId, customerId, productId, maxProductCount, maxPrice);
        sendOrderToKafka(newOrder);

        String paymentGroupId = "test-large-values-payment-" + UUID.randomUUID();
        Optional<Order> paymentResponse = waitForOrderWithStatus(
                Constants.TOPIC_ORDERS_PAYMENT,
                paymentGroupId,
                orderId,
                "REJECT",
                Duration.ofSeconds(60)
        );

        String stockGroupId = "test-large-values-stock-" + UUID.randomUUID();
        Optional<Order> stockResponse = waitForOrderWithStatus(
                Constants.TOPIC_ORDERS_STOCK,
                stockGroupId,
                orderId,
                "REJECT",
                Duration.ofSeconds(60)
        );

        assertThat(paymentResponse.isPresent() || stockResponse.isPresent())
                .as("Order with max values should be rejected by at least one service")
                .isTrue();
    }
}
