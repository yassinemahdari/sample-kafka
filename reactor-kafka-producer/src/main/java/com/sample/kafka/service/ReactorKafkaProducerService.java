package com.sample.kafka.service;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Service;
import org.springframework.util.StopWatch;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.util.Properties;
import java.util.UUID;

@Service
public class ReactorKafkaProducerService implements CommandLineRunner {

    private static final Logger logger = LoggerFactory.getLogger(ReactorKafkaProducerService.class);

    private FluxSink<SenderRecord<String, String, String>> fluxSink;

    public ReactorKafkaProducerService() {
        logger.info("Initializing Reactor Kafka producer");
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        SenderOptions<String, String> senderOptions = SenderOptions.create(properties);

        KafkaSender<String, String> sender = KafkaSender.create(senderOptions);

        Flux<SenderRecord<String, String, String>> recordFlux = Flux.create(s -> fluxSink = s);

        sender.send(recordFlux)
                .doOnError(e -> {
                    logger.error("Send failed", e);
                })
                .subscribe(record -> {
                    RecordMetadata recordMetadata = record.recordMetadata();
                    logger.info("Message sent successfully, topic partition: {}, offset: {}",
                            recordMetadata.partition(),
                            recordMetadata.offset()
                    );
                });
    }

    @Override
    public void run(String... args) {
        logger.info("Start publishing messages");
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        int count = 0;
        while (count < 500000) {
            ProducerRecord<String, String> record = new ProducerRecord<>("myTopic", UUID.randomUUID().toString(), "this is value : (" + count + ") " + UUID.randomUUID());
            fluxSink.next(SenderRecord.create(record, record.key()));
            count++;
        }
        stopWatch.stop();
        logger.info("Publishing all messages in seconds : {}", stopWatch.getTotalTimeSeconds());
    }

}
