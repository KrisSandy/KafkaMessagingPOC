package com.sandy.order.producer.service;

import com.sandy.order.models.Order;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

@Service
public class OrdersService {

    private static final Logger logger = LoggerFactory.getLogger(OrdersService.class);

    @Value(value = "${kafka.orders.topic}")
    private String ordersTopic;

    @Autowired
    private KafkaTemplate<String, Order> kafkaTemplate;

    public void publishOrder(Order order) {
        logger.info("Sending order ID: '{}' to kafka topic '{}'", order.getId(), ordersTopic);
        ProducerRecord<String, Order> record = new ProducerRecord<>(ordersTopic, order.getId().toString(), order);
        ListenableFuture<SendResult<String, Order>> future = this.kafkaTemplate.send(record);

        future.addCallback(new ListenableFutureCallback<>() {
            @Override
            public void onFailure(Throwable throwable) {
                logger.error("Order with ID: '{}' failed to send to kafka topic '{}'", order.getId(), ordersTopic);
            }

            @Override
            public void onSuccess(SendResult<String, Order> stringObjectSendResult) {
                logger.info("Order wit ID: '{}' sent successfully to topic '{}'", order.getId(), ordersTopic);
            }
        });
    }
}
