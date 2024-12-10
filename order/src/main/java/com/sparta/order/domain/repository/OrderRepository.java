package com.sparta.order.domain.repository;

import com.sparta.order.domain.model.Order;

import java.util.Optional;
import java.util.UUID;

public interface OrderRepository{
    Order save(Order order);

    Optional<Order> findById(UUID orderId);

}
