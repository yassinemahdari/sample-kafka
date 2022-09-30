package com.sample.kafka.repository;

import com.sample.kafka.entity.BrokerMessage;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface BrokerMessageRepository extends JpaRepository<BrokerMessage, String> {
}
