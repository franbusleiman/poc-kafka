package com.busleiman.kafkamodel1.config;


import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

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
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put("schema.registry.url", "http://localhost:8081");

        return props;
    }

    @Bean
    public ConsumerFactory<String, GenericRecord> getConsumerFactory() {
        ConsumerFactory<String, GenericRecord> consumerFactory = new DefaultKafkaConsumerFactory<>(consumerProperties());

        return consumerFactory;
    }

    @Bean(name = "listenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, GenericRecord> concurrentKafkaListenerContainerFactory(){
        ConcurrentKafkaListenerContainerFactory<String, GenericRecord> listener = new ConcurrentKafkaListenerContainerFactory<>();

        listener.setConsumerFactory(getConsumerFactory());
        listener.setBatchListener(true);

        return listener;
    }
}
