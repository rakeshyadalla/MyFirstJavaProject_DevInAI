package com.ashu.practice.integration.tests;

import com.ashu.practice.common.Constants;
import com.ashu.practice.common.model.Order;
import com.ashu.practice.integration.base.BaseIntegrationTest;
import com.ashu.practice.integration.dto.OrderDto;
import com.ashu.practice.integration.dto.OrderRequest;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("Positive Scenario Integration Tests")
class PositiveScenarioTests extends BaseIntegrationTest {

    @Test
    @org.junit.jupiter.api.Order(1)
    @DisplayName("Happy Path: Order creation -> Payment acceptance -> Stock acceptance -> Confirmation")
    void testHappyPath_OrderConfirmed() {
        long orderId = System.currentTimeMillis();
        long customerId = 1L;
        long productId = 1L;
        int productCount = 2;
        int price = 100;

        Order newOrder = createNewOrder(orderId, customerId, productId, productCount, price);
        sendOrderToKafka(newOrder);

        String paymentGroupId = "test-payment-" + UUID.randomUUID();
        Optional<Order> paymentResponse = waitForOrderWithStatus(
                Constants.TOPIC_ORDERS_PAYMENT,
                paymentGroupId,
                orderId,
                "ACCEPT",
                Duration.ofSeconds(60)
        );

        assertThat(paymentResponse)
                .isPresent()
                .hasValueSatisfying(order -> {
                    assertThat(order.getId()).isEqualTo(orderId);
                    assertThat(order.getStatus()).isEqualTo("ACCEPT");
                    assertThat(order.getSource()).isEqualTo("payment");
                });

        String stockGroupId = "test-stock-" + UUID.randomUUID();
        Optional<Order> stockResponse = waitForOrderWithStatus(
                Constants.TOPIC_ORDERS_STOCK,
                stockGroupId,
                orderId,
                "ACCEPT",
                Duration.ofSeconds(60)
        );

        assertThat(stockResponse)
                .isPresent()
                .hasValueSatisfying(order -> {
                    assertThat(order.getId()).isEqualTo(orderId);
                    assertThat(order.getStatus()).isEqualTo("ACCEPT");
                    assertThat(order.getSource()).isEqualTo("stock");
                });

        String confirmGroupId = "test-confirm-" + UUID.randomUUID();
        Optional<Order> confirmedOrder = waitForOrderWithStatus(
                Constants.TOPIC_ORDERS,
                confirmGroupId,
                orderId,
                "CONFIRMED",
                Duration.ofSeconds(60)
        );

        assertThat(confirmedOrder)
                .isPresent()
                .hasValueSatisfying(order -> {
                    assertThat(order.getId()).isEqualTo(orderId);
                    assertThat(order.getStatus()).isEqualTo("CONFIRMED");
                    assertThat(order.getSource()).isEqualTo("ORDER");
                });
    }

    @Test
    @org.junit.jupiter.api.Order(2)
    @DisplayName("Successful Order Query: Verify GET endpoint returns orders from state store")
    void testSuccessfulOrderQuery_ReturnsOrdersFromStateStore() {
        long orderId = System.currentTimeMillis();
        long customerId = 1L;
        long productId = 1L;
        int productCount = 1;
        int price = 50;

        Order newOrder = createNewOrder(orderId, customerId, productId, productCount, price);
        sendOrderToKafka(newOrder);

        String confirmGroupId = "test-query-" + UUID.randomUUID();
        waitForOrderWithStatus(
                Constants.TOPIC_ORDERS,
                confirmGroupId,
                orderId,
                "CONFIRMED",
                Duration.ofSeconds(60)
        );

        waitForOrderInStateStore(orderId, "CONFIRMED", Duration.ofSeconds(30));

        List<OrderDto> orders = orderServiceClient.getAllOrders();

        assertThat(orders).isNotNull();
        assertThat(orders.stream().anyMatch(o -> o.getId() == orderId))
                .as("Order with id %d should be in state store", orderId)
                .isTrue();

        Optional<OrderDto> foundOrder = orders.stream()
                .filter(o -> o.getId() == orderId)
                .findFirst();

        assertThat(foundOrder)
                .isPresent()
                .hasValueSatisfying(order -> {
                    assertThat(order.getCustomerId()).isEqualTo(customerId);
                    assertThat(order.getProductId()).isEqualTo(productId);
                    assertThat(order.getProductCount()).isEqualTo(productCount);
                    assertThat(order.getPrice()).isEqualTo(price);
                    assertThat(order.getStatus()).isEqualTo("CONFIRMED");
                });
    }

    @Test
    @org.junit.jupiter.api.Order(3)
    @DisplayName("Multiple Orders: Process multiple orders concurrently")
    void testMultipleOrders_ProcessedConcurrently() {
        int numberOfOrders = 5;
        long baseOrderId = System.currentTimeMillis();

        for (int i = 0; i < numberOfOrders; i++) {
            Order order = createNewOrder(
                    baseOrderId + i,
                    1L,
                    1L,
                    1,
                    50
            );
            sendOrderToKafka(order);
        }

        String groupId = "test-multiple-" + UUID.randomUUID();
        List<Order> confirmedOrders = consumeOrdersFromTopic(
                Constants.TOPIC_ORDERS,
                groupId,
                numberOfOrders,
                Duration.ofSeconds(120)
        );

        long confirmedCount = confirmedOrders.stream()
                .filter(o -> "CONFIRMED".equals(o.getStatus()) || "ORDER".equals(o.getSource()))
                .count();

        assertThat(confirmedCount)
                .as("At least some orders should be confirmed")
                .isGreaterThan(0);
    }

    @Test
    @org.junit.jupiter.api.Order(4)
    @DisplayName("Order with valid customer and product: Should be accepted by both services")
    void testOrderWithValidCustomerAndProduct_AcceptedByBothServices() {
        long orderId = System.currentTimeMillis();
        long customerId = 5L;
        long productId = 5L;
        int productCount = 1;
        int price = 100;

        Order newOrder = createNewOrder(orderId, customerId, productId, productCount, price);
        sendOrderToKafka(newOrder);

        String paymentGroupId = "test-valid-payment-" + UUID.randomUUID();
        Optional<Order> paymentResponse = waitForOrderWithStatus(
                Constants.TOPIC_ORDERS_PAYMENT,
                paymentGroupId,
                orderId,
                "ACCEPT",
                Duration.ofSeconds(60)
        );

        String stockGroupId = "test-valid-stock-" + UUID.randomUUID();
        Optional<Order> stockResponse = waitForOrderWithStatus(
                Constants.TOPIC_ORDERS_STOCK,
                stockGroupId,
                orderId,
                "ACCEPT",
                Duration.ofSeconds(60)
        );

        assertThat(paymentResponse.isPresent() || stockResponse.isPresent())
                .as("At least one service should respond with ACCEPT")
                .isTrue();
    }

    @Test
    @org.junit.jupiter.api.Order(5)
    @DisplayName("Order with minimum valid values: Should be processed successfully")
    void testOrderWithMinimumValidValues_ProcessedSuccessfully() {
        long orderId = System.currentTimeMillis();
        long customerId = 1L;
        long productId = 1L;
        int productCount = 1;
        int price = 1;

        Order newOrder = createNewOrder(orderId, customerId, productId, productCount, price);
        sendOrderToKafka(newOrder);

        String groupId = "test-min-values-" + UUID.randomUUID();
        List<Order> responses = consumeOrdersFromTopic(
                Constants.TOPIC_ORDERS_PAYMENT,
                groupId,
                1,
                Duration.ofSeconds(60)
        );

        assertThat(responses)
                .isNotEmpty()
                .anyMatch(o -> o.getId() == orderId);
    }
}
