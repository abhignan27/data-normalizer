package com.example.coinbase.kafka;

import com.example.normalizer.constants.Exchange;
import com.example.normalizer.dto.NormalizedData;
import com.example.normalizer.entity.CanonicalData;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.hibernate.orm.panache.Panache;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import org.eclipse.microprofile.reactive.messaging.Incoming;

import java.math.BigDecimal;
import java.time.Instant;

@ApplicationScoped
public class CoinbaseKafkaConsumer {

    @Inject
    ObjectMapper objectMapper;

    @Incoming("coinbase-adapter-in")
    @Transactional
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
                            CanonicalData canonicalData = new CanonicalData();
                            canonicalData.exchange = Exchange.COINBASE;
                            canonicalData.receivedTimeStamp = receivedTimeStamp;
                            canonicalData.askPrice = (new BigDecimal(tickerNode.path("best_ask").asText()));
                            canonicalData.bidPrice = (new BigDecimal(tickerNode.path("best_bid").asText()));
                            canonicalData.lastPrice = (new BigDecimal(tickerNode.path("price").asText()));
                            canonicalData.askQuantity = (new BigDecimal(tickerNode.path("best_ask_quantity").asText()));
                            canonicalData.bidQuantity = (new BigDecimal(tickerNode.path("best_bid_quantity").asText()));
                            canonicalData.symbol = (tickerNode.path("product_id").asText());
                            canonicalData.recordedTimeStamp = (Instant.now().toString());
                            Panache.getEntityManager().merge(canonicalData);
                        }
                    }
                }
            }
        } catch (Exception e){
            throw new RuntimeException("Error while processing coinbase kafka topic", e);
        }
    }
}
