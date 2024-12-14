package com.sparta.gateway.infrastructure.jwt;

import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class RedisService {

	private static final String ACCESS_TOKEN_PREFIX = "blacklist:accessToken:";
	private static final String REFRESH_TOKEN_PREFIX = "blacklist:refreshToken:";

	private final RedisTemplate<String, Object> redisTemplate;

	public Boolean isBlackList(String token) {
		return Boolean.TRUE.equals(redisTemplate.hasKey(ACCESS_TOKEN_PREFIX + token));
	}

}
