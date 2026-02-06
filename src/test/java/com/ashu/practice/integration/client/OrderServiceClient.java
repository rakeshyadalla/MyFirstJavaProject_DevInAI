package com.ashu.practice.integration.client;

import com.ashu.practice.integration.dto.OrderDto;
import com.ashu.practice.integration.dto.OrderRequest;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;

public class OrderServiceClient {

    private final WebClient webClient;

    public OrderServiceClient(String baseUrl) {
        this.webClient = WebClient.builder()
                .baseUrl(baseUrl)
                .build();
    }

    public OrderDto createOrder(OrderRequest orderRequest) {
        return webClient.post()
                .uri("/orders")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(orderRequest)
                .retrieve()
                .bodyToMono(OrderDto.class)
                .block(Duration.ofSeconds(30));
    }

    public Mono<OrderDto> createOrderAsync(OrderRequest orderRequest) {
        return webClient.post()
                .uri("/orders")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(orderRequest)
                .retrieve()
                .bodyToMono(OrderDto.class);
    }

    public void generateOrders() {
        webClient.post()
                .uri("/orders/generate")
                .retrieve()
                .bodyToMono(Void.class)
                .block(Duration.ofSeconds(30));
    }

    public List<OrderDto> getAllOrders() {
        return webClient.get()
                .uri("/orders")
                .retrieve()
                .bodyToFlux(OrderDto.class)
                .collectList()
                .block(Duration.ofSeconds(30));
    }

    public Mono<List<OrderDto>> getAllOrdersAsync() {
        return webClient.get()
                .uri("/orders")
                .retrieve()
                .bodyToFlux(OrderDto.class)
                .collectList();
    }

    public boolean isHealthy() {
        try {
            webClient.get()
                    .uri("/actuator/health")
                    .retrieve()
                    .bodyToMono(String.class)
                    .block(Duration.ofSeconds(5));
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
