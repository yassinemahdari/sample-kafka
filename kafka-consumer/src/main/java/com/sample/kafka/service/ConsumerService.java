package com.sample.kafka.service;

import com.sample.kafka.entity.BrokerMessage;
import com.sample.kafka.repository.BrokerMessageRepository;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Properties;

@Service
public class ConsumerService implements CommandLineRunner {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerService.class);

    private final BrokerMessageRepository brokerMessageRepository;

    public ConsumerService(BrokerMessageRepository brokerMessageRepository) {
        this.brokerMessageRepository = brokerMessageRepository;
    }

    @Override
    public void run(String... args) {
        final String topic = "myTopic";

        final Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-consumer");

        try (final Consumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    String key = record.key();
                    String value = record.value();
                    BrokerMessage brokerMessage = BrokerMessage.builder()
                            .key(key)
                            .content(value)
                            .partition(record.partition())
                            .partitionOffset(record.offset())
                            .timestamp(LocalDateTime.now())
                            .build();
                    brokerMessageRepository.save(brokerMessage);
                }
            }
        }

    }
}
