package com.jpmc.midascore;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.jpmc.midascore.component.IncentiveClient;
import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRecordRepository;
import com.jpmc.midascore.repository.UserRepository;

@Component
public class TransactionListener {

    private final UserRepository userRepository;
    private final TransactionRecordRepository transactionRecordRepository;
    private final IncentiveClient incentiveClient;

    public TransactionListener(
            UserRepository userRepository,
            TransactionRecordRepository transactionRecordRepository, 
            IncentiveClient incentiveClient) {
        this.userRepository = userRepository;
        this.transactionRecordRepository = transactionRecordRepository;
        this.incentiveClient = incentiveClient;
    }

    @KafkaListener(
            topics = "${general.kafka-topic}",
            groupId = "midas-core",
            properties = "auto.offset.reset:earliest"
    )
    public void receive(Transaction transaction) {

        UserRecord sender = userRepository.findById(transaction.getSenderId());
        UserRecord recipient = userRepository.findById(transaction.getRecipientId());


        if (sender == null || recipient == null) {
            return;
        }

        if (sender.getBalance() < transaction.getAmount()) {
            return;
        }

        Incentive incentive = incentiveClient.getIncentive(transaction);
        float incentiveAmount = incentive.getAmount();

        sender.setBalance(sender.getBalance() - transaction.getAmount());

        System.out.println("SENDER: " + sender);
        System.out.println("RECIPIENT: " + recipient);

        recipient.setBalance(
                recipient.getBalance()
                        + transaction.getAmount()
                        + incentiveAmount
        );

        userRepository.save(sender);
        userRepository.save(recipient);

        transactionRecordRepository.save(
                new TransactionRecord(sender, recipient, transaction.getAmount(), incentiveAmount)
        );
    }
}