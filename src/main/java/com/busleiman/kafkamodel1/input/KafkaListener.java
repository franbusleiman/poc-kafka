package com.busleiman.kafkamodel1.input;

import com.busleiman.kafkamodel1.model.Order;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/consumer")
public class KafkaListener {

    @Autowired
    private ConcurrentKafkaListenerContainerFactory<String, GenericRecord> concurrentKafkaListenerContainerFactory;

    @Autowired
    private KafkaListenerEndpointRegistry registry;

    private static final Logger logger = LoggerFactory.getLogger(KafkaListener.class);

    private int counter = 0;


    @org.springframework.kafka.annotation.KafkaListener(topics = "learningKafka", groupId = "consumer-1",
            containerFactory = "listenerContainerFactory",
            properties = {"max.poll.records:100"},
            id = "autoStartup1", autoStartup = "false")
    public void receiveMessages1(List<ConsumerRecord<String, GenericRecord>> messages) {


        logger.info(String.valueOf(counter++));

        messages.forEach(x -> {
            logger.info("message received by, consumer 1, value: " + x.value());
            logger.info("message received by, consumer 1, key: " + x.key());
            logger.info("message received by, consumer 1, topic: " + x.topic());
            logger.info("message received by, consumer 1, partition: " + x.partition());
            logger.info("message received by, consumer 1, offset: " + x.offset());
        });

        List<Order> orders = messages.stream()
                .map(ConsumerRecord::value)
                .map(genericRecord -> {
                    return Order.builder()
                            .orderId(genericRecord.get("order_id").toString())
                            .customerId(genericRecord.get("customer_id").toString())
                            .supplierId(genericRecord.get("supplier_id").toString())
                            .firstName(genericRecord.get("first_name").toString())
                            .lastName(genericRecord.get("last_name").toString())
                            .items((Integer) genericRecord.get("items"))
                            .price((Float) genericRecord.get("price"))
                            .weight((Float) genericRecord.get("weight"))
                            .automatedEmail((Boolean) genericRecord.get("automated_email"))
                            .build();
                }).collect(Collectors.toList());


        orders.forEach(order -> {
            logger.info(order.toString());
        });
    }

    @org.springframework.kafka.annotation.KafkaListener(topics = "learningKafka", groupId = "consumer-2",
            containerFactory = "listenerContainerFactory",
            properties = {"max.poll.records:100"},
            id = "autoStartup2", autoStartup = "false")
    public void receiveMessages2(List<ConsumerRecord<String, GenericRecord>> messages) {

        logger.info(String.valueOf(counter++));

        messages.forEach(x -> {
            logger.info("message received by, consumer 2, value: " + x.value());
            logger.info("message received by, consumer 2, key: " + x.key());
            logger.info("message received by, consumer 2, topic: " + x.topic());
            logger.info("message received by, consumer 2, partition: " + x.partition());
            logger.info("message received by, consumer 2, offset: " + x.offset());
        });

        List<Order> orders = messages.stream()
                .map(ConsumerRecord::value)
                .map(genericRecord -> {
                    return Order.builder()
                            .orderId(genericRecord.get("order_id").toString())
                            .customerId(genericRecord.get("customer_id").toString())
                            .supplierId(genericRecord.get("supplier_id").toString())
                            .firstName(genericRecord.get("first_name").toString())
                            .lastName(genericRecord.get("last_name").toString())
                            .items((Integer) genericRecord.get("items"))
                            .price((Float) genericRecord.get("price"))
                            .weight((Float) genericRecord.get("weight"))
                            .automatedEmail((Boolean) genericRecord.get("automated_email"))
                            .build();
                }).collect(Collectors.toList());


        orders.forEach(order -> {
            logger.info(order.toString());
        });
    }

    @GetMapping(value = "/activate", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Void> activateListener() {

        logger.info("activating listener");
        registry.getListenerContainer("autoStartup1").start();
        registry.getListenerContainer("autoStartup2").start();
        logger.info("listener activated");

        return ResponseEntity.ok().build();
    }

    @GetMapping(value = "/stop", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Void> stopListener() {

        logger.info("stopping listener");
        registry.getListenerContainer("autoStartup1").stop();
        registry.getListenerContainer("autoStartup2").stop();
        logger.info("listener stopped");

        return ResponseEntity.ok().build();
    }
}
