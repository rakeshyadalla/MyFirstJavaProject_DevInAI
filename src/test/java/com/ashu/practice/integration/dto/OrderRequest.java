package com.ashu.practice.integration.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OrderRequest {

    private long customerId;
    private long productId;
    private int productCount;
    private int price;
    private String status;
    private String source;
}
