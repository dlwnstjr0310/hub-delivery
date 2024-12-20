package com.sparta.delivery.presentation.api.controller;

import com.sparta.delivery.application.dto.request.DeliveryComplateRequestDto;
import com.sparta.delivery.application.dto.request.DeliverySearchRequestDto;
import com.sparta.delivery.application.dto.response.DeliveryListResponseDto;
import com.sparta.delivery.application.dto.response.DeliveryDetailResponseDto;
import com.sparta.delivery.application.dto.response.PageResponseDto;
import com.sparta.delivery.domain.model.DeliveryStatus;
import com.sparta.delivery.domain.service.DeliveryService;
import com.sparta.delivery.presentation.api.controller.docs.DeliveryControllerDocs;
import com.sparta.delivery.presentation.api.response.Response;
import jakarta.servlet.http.HttpServletRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import java.util.UUID;

@RestController
@RequestMapping("/deliveries")
@RequiredArgsConstructor
public class DeliveryController extends DeliveryControllerDocs {

    private final DeliveryService deliveryService;


    @Override
    @GetMapping("/{deliveryId}")
    public Response<DeliveryDetailResponseDto> getDeliveryById(@PathVariable UUID deliveryId, HttpServletRequest servletRequest){
        return new Response<>(HttpStatus.OK.value(), HttpStatus.OK.getReasonPhrase(), deliveryService.getDeliveryById(deliveryId, servletRequest));
    }



    @Override
    @PatchMapping("/{deliveryId}")
    public Response<UUID> updateDelivery(@PathVariable UUID deliveryId, @RequestBody DeliveryStatus status, HttpServletRequest servletRequest){
        return new Response<>(HttpStatus.OK.value(), HttpStatus.OK.getReasonPhrase(),deliveryService.updateDelivery(deliveryId, status, servletRequest));
    }


    @Override
    @DeleteMapping("/{deliveryId}")
    public Response<UUID> deleteDelivery(@PathVariable UUID deliveryId, HttpServletRequest servletRequest){
        return new Response<>(HttpStatus.NO_CONTENT.value(), HttpStatus.NO_CONTENT.getReasonPhrase(), deliveryService.deleteDelivery(deliveryId, servletRequest));
    }


    @GetMapping()
    @Override
    public Response<PageResponseDto<DeliveryListResponseDto>> getDeliveries(@ModelAttribute DeliverySearchRequestDto requestDto, HttpServletRequest servletRequest) {
        return new Response<>(HttpStatus.OK.value(), HttpStatus.OK.getReasonPhrase(), deliveryService.getDeliveries(requestDto, servletRequest));
    }

    @PatchMapping("/{deliveryId}/complated")
    @Override
    public Response<UUID> complateDelivery(@PathVariable UUID deliveryId, DeliveryComplateRequestDto requestDto, HttpServletRequest servletRequest) {
        return new Response<>(HttpStatus.OK.value(), HttpStatus.OK.getReasonPhrase(), deliveryService.complateDelivery(deliveryId, requestDto, servletRequest));
    }







}
