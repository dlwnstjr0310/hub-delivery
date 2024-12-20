package com.sparta.order.infrastructure.client;

import com.sparta.order.application.dto.response.ProductInfoResponseDto;
import com.sparta.order.presentation.api.response.Response;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

import java.util.List;
import java.util.UUID;

@FeignClient(name="product-service")
public interface ProductClient {

    @GetMapping("/products/ids")
    List<ProductInfoResponseDto> findProductsByIds(@RequestBody List<UUID> ids);

    //TODO: 여기도 api 협의 안됨 성호님과 상의
    @GetMapping("/products/{productId}/feign")
    ProductInfoResponseDto findProductById(@PathVariable("productId") UUID productId);
}
