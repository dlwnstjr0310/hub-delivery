package com.sparta.delivery.application.service;

import com.sparta.delivery.application.dto.request.DeliveryComplateRequestDto;
import com.sparta.delivery.application.dto.request.DeliverySearchRequestDto;
import com.sparta.delivery.application.dto.response.*;
import com.sparta.delivery.application.event.DeleteEvent;
import com.sparta.delivery.domain.exception.DeliveryException;
import com.sparta.delivery.domain.exception.Error;
import com.sparta.delivery.domain.model.Delivery;
import com.sparta.delivery.domain.model.DeliveryStatus;
import com.sparta.delivery.domain.repository.DeliveryRepository;
import com.sparta.delivery.domain.service.DeliveryService;
import com.sparta.delivery.infrastructure.client.DeliveryRouteClient;
import com.sparta.delivery.infrastructure.client.HubClient;
import com.sparta.delivery.infrastructure.client.UserClient;
import com.sparta.delivery.infrastructure.message.producer.KafkaProducer;
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
public class DeliveryServiceImpl implements DeliveryService {

    private final DeliveryRepository deliveryRepository;
    private final HubClient hubClient;
    private final DeliveryRouteClient deliveryRouteClient;
    private final UserClient userClient;
    private final KafkaProducer kafkaProducer;

    @Override
    @Transactional(readOnly = true)
    public DeliveryDetailResponseDto getDeliveryById(UUID deliveryId, HttpServletRequest servletRequest) {

        UUID userId = UUID.fromString(servletRequest.getHeader("X-Authenticated-User-Id"));
        String userRole = servletRequest.getHeader("X-Authenticated-User-Role");

        if(userRole == null){
            throw new DeliveryException(Error.FORBIDDEN);
        }

        Delivery delivery = deliveryRepository.findById(deliveryId).orElseThrow(
                () -> new DeliveryException(Error.NOT_FOUND_DELIVERY)
        );
        

        if(userRole.equals("HUB_ADMIN")){
            checkHubAdmin(userId, delivery);
        } else if (userRole.equals("DELIVERY_PERSON")) {
            checkDeliveryPerson(userId, delivery);
        }

        HubResponseDto originHub = hubClient.findHubById(delivery.getStartHubId());
        HubResponseDto destinationHub = hubClient.findHubById(delivery.getEndHubId());

        List<DeliveryRouteResponseDto> deliveryRoutes = deliveryRouteClient.getDeliveryRoutesByDeliveryId(deliveryId);

        return DeliveryDetailResponseDto.from(delivery, originHub, destinationHub, deliveryRoutes);
    }

    @Override
    @Transactional
    public UUID updateDelivery(UUID deliveryId, DeliveryStatus status, HttpServletRequest servletRequest) {
        UUID userId = UUID.fromString(servletRequest.getHeader("X-Authenticated-User-Id"));
        String userRole = servletRequest.getHeader("X-Authenticated-User-Role");

        if(userRole == null || userRole.equals("COMPANY_ADMIN")){
            throw new DeliveryException(Error.FORBIDDEN);
        }

        Delivery delivery = deliveryRepository.findById(deliveryId).orElseThrow(
                () -> new DeliveryException(Error.NOT_FOUND_DELIVERY)
        );


        if(userRole.equals("HUB_ADMIN")){
            checkHubAdmin(userId, delivery);
        } else if (userRole.equals("DELIVERY_PERSON")) {
            checkDeliveryPerson(userId, delivery);
        }


        delivery.updateDelivery(status);

        return deliveryId;
    }

    @Override
    @Transactional
    public UUID deleteDelivery(UUID deliveryId, HttpServletRequest servletRequest) {
        UUID userId = UUID.fromString(servletRequest.getHeader("X-Authenticated-User-Id"));
        String userRole = servletRequest.getHeader("X-Authenticated-User-Role");

        if(userRole == null || userRole.equals("DELIVERY_PERSON")||userRole.equals("COMPANY_ADMIN")){
            throw new DeliveryException(Error.FORBIDDEN);
        }

        Delivery delivery = deliveryRepository.findById(deliveryId).orElseThrow(
                () -> new DeliveryException(Error.NOT_FOUND_DELIVERY)
        );

        if(userRole.equals("HUB_ADMIN")){
            checkHubAdmin(userId, delivery);
        }

        delivery.deleteDelivery(delivery.getCreatedBy());

        kafkaProducer.delete(new DeleteEvent(deliveryId, userId));

        return deliveryId;
    }

    @Transactional(readOnly = true)
    @Override
    public PageResponseDto<DeliveryListResponseDto> getDeliveries(DeliverySearchRequestDto requestDto, HttpServletRequest servletRequest) {
        Page<Delivery> deliveries = findDeliveries(requestDto);

        List<UUID> originHubIds = deliveries.map(Delivery::getStartHubId).stream().distinct().toList();
        List<HubResponseDto> originHubs = hubClient.findHubsByIds(originHubIds);

        List<UUID> destinationHubIds = deliveries.map(Delivery::getEndHubId).stream().distinct().toList();
        List<HubResponseDto> destinationHubs = hubClient.findHubsByIds(destinationHubIds);

        Map<UUID, HubResponseDto> originHubMap = originHubs.stream().collect(Collectors.toMap(HubResponseDto::hubId, c -> c));
        Map<UUID, HubResponseDto> destinationHubMap = destinationHubs.stream().collect(Collectors.toMap(HubResponseDto::hubId, c -> c));

        Page<DeliveryListResponseDto> results = deliveries.map(delivery -> {
            HubResponseDto originHub = originHubMap.get(delivery.getStartHubId());
            HubResponseDto destinationHub = destinationHubMap.get(delivery.getEndHubId());
            return DeliveryListResponseDto.from(delivery, originHub, destinationHub);
        });

        return PageResponseDto.of(results);
    }



    @Transactional(readOnly = true)
    @Override
    public UUID getDeliveryByOrderId(UUID orderId) {
        Delivery delivery = deliveryRepository.findByOrderIdAndIsDeleteFalse(orderId).orElseThrow(
                () -> new DeliveryException(Error.NOT_FOUND_DELIVERY)
        );
        return delivery.getId();
    }

    @Transactional
    @Override
    public UUID complateDelivery(UUID deliveryId, DeliveryComplateRequestDto requestDto, HttpServletRequest servletRequest) {
        UUID userId = UUID.fromString(servletRequest.getHeader("X-Authenticated-User-Id"));
        String userRole = servletRequest.getHeader("X-Authenticated-User-Role");

        if(userRole == null || userRole.equals("COMPANY_ADMIN")){
            throw new DeliveryException(Error.FORBIDDEN);
        }

        Delivery delivery = deliveryRepository.findById(deliveryId).orElseThrow(
                () -> new DeliveryException(Error.NOT_FOUND_DELIVERY)
        );


        if(userRole.equals("HUB_ADMIN")){
            checkHubAdmin(userId, delivery);
        } else if (userRole.equals("DELIVERY_PERSON")) {
            checkDeliveryPerson(userId, delivery);
        }

        delivery.complateDelivery(requestDto);

        return deliveryId;
    }


    private Page<Delivery> findDeliveries(DeliverySearchRequestDto requestDto) {
        if ("DESTINATION_HUB_NAME".equals(requestDto.searchType()) || "ORIGIN_HUB_NAME".equals(requestDto.searchType())) {

            List<HubResponseDto> findhubs = hubClient.findHubsByName(requestDto.searchValue());
            List<UUID> hubIds = findhubs.stream().map(HubResponseDto::hubId).toList();

            return deliveryRepository.searchDeliveriesByHubIds(requestDto, hubIds);
        }
        return deliveryRepository.searchDeliveries(requestDto);
    }

    private void checkHubAdmin(UUID userId, Delivery delivery){
        UUID targetHubId = hubClient.findHubByUserId(userId);

        if(!targetHubId.equals(delivery.getStartHubId()) && !targetHubId.equals(delivery.getEndHubId())){
            throw new DeliveryException(Error.FORBIDDEN);
        }

    }

    private void checkDeliveryPerson(UUID userId, Delivery delivery){
        if(!userId.equals(delivery.getDeliveryPersonId())){
            throw new DeliveryException(Error.FORBIDDEN);
        }
    }
    




}
