package com.jpmc.midascore.component;

import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;

@Component
public class IncentiveClient {

    private final RestTemplate restTemplate = new RestTemplate();

    public Incentive getIncentive(Transaction transaction) {
        return restTemplate.postForObject(
                "http://localhost:8080/incentive",
                transaction,
                Incentive.class
        );
    }
}