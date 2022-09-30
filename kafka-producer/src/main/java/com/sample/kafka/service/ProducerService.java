package com.sample.kafka.service;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Service;
import org.springframework.util.StopWatch;

import java.util.Properties;
import java.util.UUID;

@Service
public class ProducerService implements CommandLineRunner {

    private static final Logger logger = LoggerFactory.getLogger(ProducerService.class);

    private final KafkaProducer<String, String> kafkaProducer;

    public ProducerService() {
        logger.info("Initializing Kafka producer");
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProducer = new KafkaProducer<>(properties);
    }

    @Override
    public void run(String... args) {
        logger.info("Start publishing messages");
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        int count = 0;
        while (count < 500000) {
            ProducerRecord<String, String> record = new ProducerRecord<>("myTopic", UUID.randomUUID().toString(), "this is value : (" + count + ") " + UUID.randomUUID());
            kafkaProducer.send(record, (recordMetadata, exception) -> {
                if (exception == null) {
                    logger.info("Record written to offset " +
                            recordMetadata.offset() + " timestamp " +
                            recordMetadata.timestamp());
                } else {
                    logger.error("an error occurred", exception);
                }
            });
            count++;
        }
        kafkaProducer.flush();
        kafkaProducer.close();
        stopWatch.stop();
        logger.info("Publishing all messages in seconds : {}", stopWatch.getTotalTimeSeconds());
    }
}
