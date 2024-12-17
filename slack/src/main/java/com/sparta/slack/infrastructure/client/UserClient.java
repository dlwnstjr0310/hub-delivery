package com.sparta.slack.infrastructure.client;

import com.sparta.slack.application.dto.UserDetails;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;

import java.util.UUID;

@FeignClient(name = "user-service", url = "${user.service.url}")
public interface UserClient {

    @GetMapping("/users/{id}")
    UserDetails.Response getUserById(
            @PathVariable UUID id
    );

}
