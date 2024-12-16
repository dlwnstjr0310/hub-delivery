package com.sparta.order.application.service;

import com.sparta.order.application.event.CreateOrderEvent;
import com.sparta.order.application.dto.request.OrderCreateRequestDto;
import com.sparta.order.application.dto.request.OrderSearchRequestDto;
import com.sparta.order.application.dto.request.OrderUpdateRequestDto;
import com.sparta.order.application.dto.response.*;
import com.sparta.order.application.event.DeleteEvent;
import com.sparta.order.infrastructure.client.CompanyClient;
import com.sparta.order.infrastructure.client.DeliveryClient;
import com.sparta.order.infrastructure.client.ProductClient;
import com.sparta.order.domain.exception.Error;
import com.sparta.order.domain.exception.OrderException;
import com.sparta.order.domain.model.Order;
import com.sparta.order.domain.model.OrderStatus;
import com.sparta.order.domain.repository.OrderRepository;
import com.sparta.order.domain.service.OrderService;
import com.sparta.order.infrastructure.message.producer.KafkaProducer;
import jakarta.servlet.http.HttpServletRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class OrderServiceImpl implements OrderService {

    private final OrderRepository orderRepository;
    private final CompanyClient companyClient;
    private final ProductClient productClient;
    private final DeliveryClient deliveryClient;
    private final KafkaProducer kafkaProducer;


    @Override
    public OrderResponseDto createOrder(OrderCreateRequestDto request, HttpServletRequest servletRequest) {


        ProductInfoResponseDto product = productClient.findProductById(request.productId()).getData();

        if (product.amount() < request.quantity()){
            throw new OrderException(Error.OUT_OF_STOCK);
        }

        Order order = orderRepository.save(
            Order.builder()
                    .productId(request.productId())
                    .status(OrderStatus.RECEIVED)
                    .totalPrice(request.price()* request.quantity())
                    .specialRequests(request.specialRequests())
                    .quantity(request.quantity())
                    .price(request.price())
                    .recipientCompanyId(request.recipientCompanyId())
                    .build()
        );

        CompanyResponseDto recipientCompany = companyClient.findCompanyById(order.getRecipientCompanyId());
        CompanyResponseDto requestCompany = companyClient.findCompanyById(product.companyId());

        CreateOrderEvent eventDto = CreateOrderEvent.from(order, product, recipientCompany, requestCompany);
        kafkaProducer.send(eventDto);

        UUID deliveryId = deliveryClient.getDeliveryByOrderId(order.getId());

        return OrderResponseDto.builder()
                .deliveryId(deliveryId)
                .orderId(order.getId()).build();

    }

    @Override
    @Transactional
    public UUID deleteOrder(UUID orderId, HttpServletRequest servletRequest) {
        Order order = orderRepository.findByIdAndIsDeleteFalse(orderId).orElseThrow(
                () -> new OrderException(Error.NOT_FOUND_ORDER)
        );

        //TODO : userID 로 바꾸기
        order.deleteOrder(orderId);

        kafkaProducer.delete(new DeleteEvent(orderId));

        return orderId;
    }

    @Transactional(readOnly = true)
    @Override
    public OrderDetailResponseDto getOrderById(UUID orderId, HttpServletRequest servletRequest) {
        Order order = orderRepository.findByIdAndIsDeleteFalse(orderId).orElseThrow(
                () -> new OrderException(Error.NOT_FOUND_ORDER)
        );

        CompanyResponseDto recipientCompany = companyClient.findCompanyById(order.getRecipientCompanyId());
        ProductInfoResponseDto product = productClient.findProductById(order.getProductId()).getData();
        CompanyResponseDto requestCompany = companyClient.findCompanyById(product.companyId());
        UUID deliveryId = deliveryClient.getDeliveryByOrderId(orderId);

        return OrderDetailResponseDto.from(order, recipientCompany,requestCompany ,product, deliveryId);
    }

    @Transactional(readOnly = true)
    @Override
    public PageResponseDto<OrderListResponseDto> getOrders(OrderSearchRequestDto requestDto, HttpServletRequest servletRequest) {
        Page<Order> orders = findOrders(requestDto);

        List<UUID> recipientCompanyIds = orders.map(Order::getRecipientCompanyId).stream().distinct().toList();
        List<CompanyResponseDto> recipientCompanies = companyClient.findCompaniesByIds(recipientCompanyIds);

        List<UUID> productIds = orders.map(Order::getProductId).stream().distinct().toList();
        List<ProductInfoResponseDto> products = productClient.findProductsByIds(productIds);

        // 응답값 반환
        Map<UUID, CompanyResponseDto> recipientCompanyMap = recipientCompanies.stream().collect(Collectors.toMap(CompanyResponseDto::companyId, c -> c));
        Map<UUID, ProductInfoResponseDto> productMap = products.stream().collect(Collectors.toMap(ProductInfoResponseDto::productId, p -> p));

        Page<OrderListResponseDto> results = orders.map(order -> {
            CompanyResponseDto recipientCompany = recipientCompanyMap.get(order.getRecipientCompanyId());
            ProductInfoResponseDto product = productMap.get(order.getProductId());
            return OrderListResponseDto.from(order, recipientCompany, product);
        });

        return PageResponseDto.of(results);
    }


    @Transactional
    @Override
    public OrderResponseDto updateOrder(UUID orderId, OrderUpdateRequestDto requestDto, HttpServletRequest servletRequest) {

        Order order = orderRepository.findByIdAndIsDeleteFalse(orderId).orElseThrow(
                () -> new OrderException(Error.NOT_FOUND_ORDER)
        );

        UUID deliveryId = deliveryClient.getDeliveryByOrderId(orderId);

        order.updateOrder(requestDto);

        return OrderResponseDto.builder()
                .orderId(order.getId())
                .deliveryId(deliveryId)
                .build();
    }

    private Page<Order> findOrders(OrderSearchRequestDto requestDto) {
        if ("RECIPIENT_NAME".equals(requestDto.searchType())) {

            List<CompanyResponseDto> findCompanies = companyClient.findCompaniesByName(requestDto.searchValue());
            List<UUID> companyIds = findCompanies.stream().map(CompanyResponseDto::companyId).toList();

            // 업체 ID 리스트를 조건으로 필터링
            return orderRepository.searchOrdersByCompanyIds(requestDto, companyIds);
        }
        return orderRepository.searchOrders(requestDto);
    }
}
