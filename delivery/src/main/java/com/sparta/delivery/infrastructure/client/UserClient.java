package com.sparta.delivery.infrastructure.client;

import org.springframework.cloud.openfeign.FeignClient;

@FeignClient(name="user-service")
public interface UserClient {
}
