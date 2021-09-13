package com.busleiman.kafkamodel1.config;


import com.busleiman.kafkamodel1.model.Order;
import com.busleiman.kafkamodel1.model.Response;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfigurations {

    public Map<String, Object> consumerProperties() {

        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "15000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, new JsonDeserializer<Response>());
        return props;
    }

    public Map<String, Object> producerProperties() {

        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        return props;
    }

    @Bean
    public ConsumerFactory<String, Order> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerProperties(),  new StringDeserializer(),
                new JsonDeserializer<>(Order.class,false));
    }


    @Bean(name = "listenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, Order> concurrentKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Order> listener = new ConcurrentKafkaListenerContainerFactory<>();

        listener.setConsumerFactory(consumerFactory());
        listener.setReplyTemplate(getKafkaTemplate());

        return listener;
    }

    @Bean
    public ProducerFactory<String, Object> getProducerFactory() {
        ProducerFactory<String, Object> producerFactory = new DefaultKafkaProducerFactory<String, Object>(producerProperties());
        return producerFactory;

    }

    @Bean
    public KafkaTemplate getKafkaTemplate() {
        KafkaTemplate<String, Object> kafkaTemplate = new KafkaTemplate<>(getProducerFactory());

        return kafkaTemplate;
    }
}
