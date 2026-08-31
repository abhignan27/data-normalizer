package com.example.coinbase.kafka;

import com.example.normalizer.constants.Exchange;
import com.example.normalizer.dto.NormalizedData;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Incoming;

import java.math.BigDecimal;
import java.time.Instant;

@ApplicationScoped
public class CoinbaseKafkaConsumer {

    @Inject
    ObjectMapper objectMapper;

    @Incoming("coinbase-adapter-in")
    public void consume(String payload){
        try {
            JsonNode rootNode = objectMapper.readTree(payload);
            String receivedTimeStamp = rootNode.path("timestamp").asText();

            JsonNode eventsArray = rootNode.path("events");

            if (eventsArray.isArray()) {
                for (JsonNode eventNode : eventsArray) {
                    JsonNode tickersArray = eventNode.path("tickers");
                    if (tickersArray.isArray()) {
                        for (JsonNode tickerNode : tickersArray) {
                            NormalizedData normalizedData = new NormalizedData();
                            normalizedData.setExchange(Exchange.COINBASE);
                            normalizedData.setReceivedTimeStamp(receivedTimeStamp);
                            normalizedData.setAskPrice(new BigDecimal(tickerNode.path("best_ask").asText()));
                            normalizedData.setBidPrice(new BigDecimal(tickerNode.path("best_bid").asText()));
                            normalizedData.setLastPrice(new BigDecimal(tickerNode.path("price").asText()));
                            normalizedData.setAskQuantity(new BigDecimal(tickerNode.path("best_ask_quantity").asText()));
                            normalizedData.setBidQuantity(new BigDecimal(tickerNode.path("best_bid_quantity").asText()));
                            normalizedData.setSymbol(tickerNode.path("product_id").asText());
                            normalizedData.setRecordedTimeStamp(Instant.now().toString());
                            System.out.println("Normalized coinbase data -> " + normalizedData.toString());
                        }
                    }
                }
            }
        } catch (Exception e){
            throw new RuntimeException("Error while processing coinbase kafka topic", e);
        }
    }
}
