package com.example.kraken.kafka;

import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.reactive.messaging.Incoming;

@ApplicationScoped
public class KrakenKafkaConsumer {

    @Incoming("kraken-adapter-in")
    public void consume(String payload){
        System.out.println("Normalizing data from kraken topic: " + payload);
    }
}
