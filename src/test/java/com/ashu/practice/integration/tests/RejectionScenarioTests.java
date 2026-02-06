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
@DisplayName("Rejection Scenario Integration Tests")
class RejectionScenarioTests extends BaseIntegrationTest {

    @Test
    @org.junit.jupiter.api.Order(1)
    @DisplayName("Payment Rejection: Order with insufficient customer balance -> Rollback")
    void testPaymentRejection_InsufficientBalance_Rollback() {
        long orderId = System.currentTimeMillis();
        long customerId = 1L;
        long productId = 1L;
        int productCount = 1;
        int price = 999999;

        Order newOrder = createNewOrder(orderId, customerId, productId, productCount, price);
        sendOrderToKafka(newOrder);

        String paymentGroupId = "test-payment-reject-" + UUID.randomUUID();
        Optional<Order> paymentResponse = waitForOrderWithStatus(
                Constants.TOPIC_ORDERS_PAYMENT,
                paymentGroupId,
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

        String rollbackGroupId = "test-rollback-payment-" + UUID.randomUUID();
        Optional<Order> rollbackOrder = waitForOrderWithStatus(
                Constants.TOPIC_ORDERS,
                rollbackGroupId,
                orderId,
                "ROLLBACK",
                Duration.ofSeconds(60)
        );

        if (rollbackOrder.isEmpty()) {
            Optional<Order> rejectOrder = waitForOrderWithStatus(
                    Constants.TOPIC_ORDERS,
                    rollbackGroupId + "-reject",
                    orderId,
                    "REJECT",
                    Duration.ofSeconds(30)
            );
            assertThat(rollbackOrder.isPresent() || rejectOrder.isPresent())
                    .as("Order should be either ROLLBACK or REJECT")
                    .isTrue();
        }
    }

    @Test
    @org.junit.jupiter.api.Order(2)
    @DisplayName("Stock Rejection: Order with insufficient stock -> Rollback")
    void testStockRejection_InsufficientStock_Rollback() {
        long orderId = System.currentTimeMillis();
        long customerId = 1L;
        long productId = 1L;
        int productCount = 999999;
        int price = 100;

        Order newOrder = createNewOrder(orderId, customerId, productId, productCount, price);
        sendOrderToKafka(newOrder);

        String stockGroupId = "test-stock-reject-" + UUID.randomUUID();
        Optional<Order> stockResponse = waitForOrderWithStatus(
                Constants.TOPIC_ORDERS_STOCK,
                stockGroupId,
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

        String rollbackGroupId = "test-rollback-stock-" + UUID.randomUUID();
        Optional<Order> rollbackOrder = waitForOrderWithStatus(
                Constants.TOPIC_ORDERS,
                rollbackGroupId,
                orderId,
                "ROLLBACK",
                Duration.ofSeconds(60)
        );

        if (rollbackOrder.isEmpty()) {
            Optional<Order> rejectOrder = waitForOrderWithStatus(
                    Constants.TOPIC_ORDERS,
                    rollbackGroupId + "-reject",
                    orderId,
                    "REJECT",
                    Duration.ofSeconds(30)
            );
            assertThat(rollbackOrder.isPresent() || rejectOrder.isPresent())
                    .as("Order should be either ROLLBACK or REJECT")
                    .isTrue();
        }
    }

    @Test
    @org.junit.jupiter.api.Order(3)
    @DisplayName("Mixed Rejection: Payment accepts, Stock rejects -> Rollback with STOCK source")
    void testMixedRejection_PaymentAcceptsStockRejects_Rollback() {
        long orderId = System.currentTimeMillis();
        long customerId = 1L;
        long productId = 1L;
        int productCount = 999999;
        int price = 50;

        Order newOrder = createNewOrder(orderId, customerId, productId, productCount, price);
        sendOrderToKafka(newOrder);

        String paymentGroupId = "test-mixed-payment-" + UUID.randomUUID();
        Optional<Order> paymentResponse = waitForOrderWithStatus(
                Constants.TOPIC_ORDERS_PAYMENT,
                paymentGroupId,
                orderId,
                "ACCEPT",
                Duration.ofSeconds(60)
        );

        String stockGroupId = "test-mixed-stock-" + UUID.randomUUID();
        Optional<Order> stockResponse = waitForOrderWithStatus(
                Constants.TOPIC_ORDERS_STOCK,
                stockGroupId,
                orderId,
                "REJECT",
                Duration.ofSeconds(60)
        );

        assertThat(stockResponse)
                .isPresent()
                .hasValueSatisfying(order -> {
                    assertThat(order.getStatus()).isEqualTo("REJECT");
                });

        String rollbackGroupId = "test-mixed-rollback-" + UUID.randomUUID();
        Optional<Order> rollbackOrder = waitForOrderWithStatus(
                Constants.TOPIC_ORDERS,
                rollbackGroupId,
                orderId,
                "ROLLBACK",
                Duration.ofSeconds(60)
        );

        assertThat(rollbackOrder)
                .isPresent()
                .hasValueSatisfying(order -> {
                    assertThat(order.getId()).isEqualTo(orderId);
                    assertThat(order.getStatus()).isEqualTo("ROLLBACK");
                    assertThat(order.getSource()).isEqualTo("STOCK");
                });
    }

    @Test
    @org.junit.jupiter.api.Order(4)
    @DisplayName("Mixed Rejection: Stock accepts, Payment rejects -> Rollback with PAYMENT source")
    void testMixedRejection_StockAcceptsPaymentRejects_Rollback() {
        long orderId = System.currentTimeMillis();
        long customerId = 1L;
        long productId = 1L;
        int productCount = 1;
        int price = 999999;

        Order newOrder = createNewOrder(orderId, customerId, productId, productCount, price);
        sendOrderToKafka(newOrder);

        String stockGroupId = "test-mixed2-stock-" + UUID.randomUUID();
        Optional<Order> stockResponse = waitForOrderWithStatus(
                Constants.TOPIC_ORDERS_STOCK,
                stockGroupId,
                orderId,
                "ACCEPT",
                Duration.ofSeconds(60)
        );

        String paymentGroupId = "test-mixed2-payment-" + UUID.randomUUID();
        Optional<Order> paymentResponse = waitForOrderWithStatus(
                Constants.TOPIC_ORDERS_PAYMENT,
                paymentGroupId,
                orderId,
                "REJECT",
                Duration.ofSeconds(60)
        );

        assertThat(paymentResponse)
                .isPresent()
                .hasValueSatisfying(order -> {
                    assertThat(order.getStatus()).isEqualTo("REJECT");
                });

        String rollbackGroupId = "test-mixed2-rollback-" + UUID.randomUUID();
        Optional<Order> rollbackOrder = waitForOrderWithStatus(
                Constants.TOPIC_ORDERS,
                rollbackGroupId,
                orderId,
                "ROLLBACK",
                Duration.ofSeconds(60)
        );

        assertThat(rollbackOrder)
                .isPresent()
                .hasValueSatisfying(order -> {
                    assertThat(order.getId()).isEqualTo(orderId);
                    assertThat(order.getStatus()).isEqualTo("ROLLBACK");
                    assertThat(order.getSource()).isEqualTo("PAYMENT");
                });
    }

    @Test
    @org.junit.jupiter.api.Order(5)
    @DisplayName("Both Reject: Payment and Stock both reject -> REJECT status")
    void testBothReject_PaymentAndStockReject_RejectStatus() {
        long orderId = System.currentTimeMillis();
        long customerId = 1L;
        long productId = 1L;
        int productCount = 999999;
        int price = 999999;

        Order newOrder = createNewOrder(orderId, customerId, productId, productCount, price);
        sendOrderToKafka(newOrder);

        String paymentGroupId = "test-both-reject-payment-" + UUID.randomUUID();
        Optional<Order> paymentResponse = waitForOrderWithStatus(
                Constants.TOPIC_ORDERS_PAYMENT,
                paymentGroupId,
                orderId,
                "REJECT",
                Duration.ofSeconds(60)
        );

        String stockGroupId = "test-both-reject-stock-" + UUID.randomUUID();
        Optional<Order> stockResponse = waitForOrderWithStatus(
                Constants.TOPIC_ORDERS_STOCK,
                stockGroupId,
                orderId,
                "REJECT",
                Duration.ofSeconds(60)
        );

        assertThat(paymentResponse.isPresent() && stockResponse.isPresent())
                .as("Both payment and stock should reject")
                .isTrue();

        String rejectGroupId = "test-both-reject-final-" + UUID.randomUUID();
        Optional<Order> rejectOrder = waitForOrderWithStatus(
                Constants.TOPIC_ORDERS,
                rejectGroupId,
                orderId,
                "REJECT",
                Duration.ofSeconds(60)
        );

        assertThat(rejectOrder)
                .isPresent()
                .hasValueSatisfying(order -> {
                    assertThat(order.getId()).isEqualTo(orderId);
                    assertThat(order.getStatus()).isEqualTo("REJECT");
                });
    }
}
