package com.example.coinbase.kafka;

import com.example.normalizer.constants.Exchange;
import com.example.normalizer.entity.CanonicalData;
import com.example.normalizer.entity.HistoricData;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.hibernate.orm.panache.Panache;
import io.quarkus.logging.Log;
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
    public void consume(String payload) {
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
                            canonicalData.id = 1L;
                            canonicalData.exchange = Exchange.COINBASE;
                            canonicalData.symbol = tickerNode.path("product_id").asText();
                            canonicalData.receivedTimeStamp = receivedTimeStamp;
                            canonicalData.recordedTimeStamp = Instant.now().toString();
                            canonicalData.askPrice = new BigDecimal(tickerNode.path("best_ask").asText());
                            canonicalData.bidPrice = new BigDecimal(tickerNode.path("best_bid").asText());
                            canonicalData.lastPrice = new BigDecimal(tickerNode.path("price").asText());
                            canonicalData.askQuantity = new BigDecimal(tickerNode.path("best_ask_quantity").asText());
                            canonicalData.bidQuantity = new BigDecimal(tickerNode.path("best_bid_quantity").asText());

                            Panache.getEntityManager().merge(canonicalData);

                            HistoricData historicData = new HistoricData();
                            historicData.exchange = Exchange.COINBASE;
                            historicData.symbol = tickerNode.path("product_id").asText();
                            historicData.receivedTimeStamp = receivedTimeStamp;
                            historicData.recordedTimeStamp = Instant.now().toString();
                            historicData.askPrice = canonicalData.askPrice;
                            historicData.bidPrice = canonicalData.bidPrice;
                            historicData.lastPrice = canonicalData.lastPrice;
                            historicData.askQuantity = canonicalData.askQuantity;
                            historicData.bidQuantity = canonicalData.bidQuantity;

                            Panache.getEntityManager().persist(historicData);
                        }
                    }
                }
            }
        } catch (Exception e) {
            Log.error(String.format("Error parsing or persisting Coinbase stream data due to exception %s with message %s", e.getClass().toString(), e.getMessage()));
            throw new RuntimeException("Error while parsing or persisting coinbase stream data", e);
        }
    }
}
