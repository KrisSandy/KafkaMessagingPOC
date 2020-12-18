package com.sandy.order.processor.service;

import com.sandy.order.models.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class OrderProcessorService {

    private static final Logger logger = LoggerFactory.getLogger(OrderProcessorService.class);

    @KafkaListener(topics = "${kafka.orders.topic}", containerFactory = "kafkaListenerContainerFactory", groupId = "${spring.kafka.consumer.group-id}")
    public void processOrders(Order order) {
        logger.info("Successfully received order '{}'", order.toString());
    }

}
