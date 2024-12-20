package com.sparta.company.application.exception

open class FeignException(val error: Error) : RuntimeException()

class CircuitBreakerOpenException : FeignException(Error.CIRCUIT_BREAKER_OPEN)

class ServerTimeoutException : FeignException(Error.SERVER_TIMEOUT)

class InternalServerErrorException : FeignException(Error.INTERNAL_SERVER_ERROR)