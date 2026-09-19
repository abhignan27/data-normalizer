package com.example.coinbase.kafka;

import com.example.normalizer.constants.Exchange;
import com.example.normalizer.entity.CanonicalData;
import com.example.normalizer.entity.HistoricData;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.hibernate.orm.panache.Panache;
import io.quarkus.logging.Log;
import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import org.eclipse.microprofile.reactive.messaging.Incoming;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

@ApplicationScoped
public class CoinbaseKafkaConsumer {

    @Inject
    ObjectMapper objectMapper;

    private final Map<String, CanonicalData> latestPriceCache = new HashMap<>();

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
                            latestPriceCache.put(canonicalData.symbol, canonicalData);
                        }
                    }
                }
            }
        } catch (Exception e) {
            Log.error(String.format("Error parsing or persisting Coinbase stream data due to exception %s with message %s", e.getClass().toString(), e.getMessage()));
            throw new RuntimeException("Error while parsing or persisting coinbase stream data", e);
        }
    }

    @Scheduled(every = "1s")
    @Transactional
    public void saveHistorySnapshot(){
        if(latestPriceCache.isEmpty()){
            Log.info("Latest Coinbase price cache is empty returning");
            return;
        }
        Log.info("Saving Coinbase history snapshot");
        for(Map.Entry<String, CanonicalData> entry: latestPriceCache.entrySet()){
            CanonicalData latest = entry.getValue();

            HistoricData historicData = new HistoricData();

            historicData.exchange = Exchange.COINBASE;
            historicData.symbol = entry.getKey();
            historicData.receivedTimeStamp = latest.receivedTimeStamp;
            historicData.recordedTimeStamp = latest.recordedTimeStamp;
            historicData.askPrice = latest.askPrice;
            historicData.bidPrice = latest.bidPrice;
            historicData.lastPrice = latest.lastPrice;
            historicData.askQuantity = latest.askQuantity;
            historicData.bidQuantity = latest.bidQuantity;

            Panache.getEntityManager().persist(historicData);

        }
    }
}
