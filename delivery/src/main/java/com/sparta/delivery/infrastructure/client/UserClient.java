package com.sparta.delivery.infrastructure.client;

import com.sparta.delivery.application.dto.response.UserResponseDto;
import com.sparta.delivery.presentation.api.response.Response;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;

import java.util.UUID;

@FeignClient(name = "user-service")
public interface UserClient {
    @GetMapping("/users/{userId}/feign")
    UserResponseDto getUserById(@PathVariable UUID userId);
}
