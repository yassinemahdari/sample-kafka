package com.sample.kafka.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Entity;
import javax.persistence.Id;
import java.time.LocalDateTime;

@NoArgsConstructor
@AllArgsConstructor
@Builder
@Data
@Entity
public class BrokerMessage {
    @Id
    private String key;
    private String content;
    private int partition;
    private long partitionOffset;
    private LocalDateTime timestamp;
}
