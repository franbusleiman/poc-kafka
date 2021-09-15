package com.busleiman.kafkamodel1.input;

import com.busleiman.kafkadto.model.Message;
import com.busleiman.kafkadto.model.Order;
import com.busleiman.kafkadto.model.Response;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Objects;

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
            id = "autoStartup1", autoStartup = "false")
    @SendTo
    public Message receiveMessages(Message message) {

        logger.info("request received");

        Message messageResponse = new Message();



        if (message.getMessageContent() instanceof Order) {

            Order order = (Order) message.getMessageContent();

            if(Objects.equals(order.getFirstName(), "secret")){

                messageResponse.setMessageCode(200);
                messageResponse.setMessageContent(2L);
                logger.info("returning long correct");
            }
            else {

                logger.info("returning response correct");
                Response response = Response.builder()
                        .details(order.getOrderId() + " processed correctly")
                        .processedCorrectly(true)
                        .build();

                messageResponse.setMessageCode(200);
                messageResponse.setMessageContent(response);

            }
        } else {
            logger.info("returning response incorrect");
            Response response = Response.builder()
                    .details("Incorrect request")
                    .processedCorrectly(false)
                    .build();

            messageResponse.setMessageCode(400);
            messageResponse.setMessageContent(response);
        }

        return messageResponse;
    }

    @GetMapping(value = "/activate", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Void> activateListener() {

        logger.info("activating listener");
        registry.getListenerContainer("autoStartup1").start();
        logger.info("listener activated");

        return ResponseEntity.ok().build();
    }

    @GetMapping(value = "/stop", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Void> stopListener() {

        logger.info("stopping listener");
        registry.getListenerContainer("autoStartup1").stop();
        logger.info("listener stopped");

        return ResponseEntity.ok().build();
    }
}
