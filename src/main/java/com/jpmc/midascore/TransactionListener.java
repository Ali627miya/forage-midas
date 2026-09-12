package com.jpmc.midascore;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.jpmc.midascore.foundation.Transaction;

@Component
public class TransactionListener {

    @KafkaListener(
        topics = "${general.kafka-topic}",
        groupId = "midas-core",
        properties = "auto.offset.reset:earliest"
    )
    public void receive(Transaction transaction) {
        System.out.println("RECEIVED: " + transaction);
    }
}