package com.example.kraken.kafka;

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
import java.util.Objects;

@ApplicationScoped
public class KrakenKafkaConsumer {

    @Inject
    ObjectMapper objectMapper;

    @Incoming("kraken-adapter-in")
    @Transactional
    public void consume(String payload){
        try{
            JsonNode rootNode = objectMapper.readTree(payload);
            if(!rootNode.isArray()){
                return;
            }

            JsonNode detailsPayload = rootNode.get(1);
            if(Objects.isNull(detailsPayload) || !detailsPayload.isObject()){
                Log.error("Null or improper data received from Kraken exchange");
                return;
            }

            CanonicalData canonicalData = new CanonicalData();

            canonicalData.id = 2L;
            canonicalData.exchange = Exchange.KRAKEN;
            canonicalData.symbol = rootNode.get(3).asText();
            canonicalData.recordedTimeStamp = Instant.now().toString();
            canonicalData.askPrice = new BigDecimal(detailsPayload.path("a").get(0).asText());
            canonicalData.askQuantity = new BigDecimal(detailsPayload.path("a").get(2).asText());
            canonicalData.bidPrice = new BigDecimal(detailsPayload.path("b").get(0).asText());
            canonicalData.bidQuantity = new BigDecimal(detailsPayload.path("b").get(2).asText());
            canonicalData.lastPrice = new BigDecimal(detailsPayload.path("c").get(0).asText());

            Panache.getEntityManager().merge(canonicalData);

            HistoricData historicData = new HistoricData();
            historicData.exchange = Exchange.KRAKEN;
            historicData.symbol = rootNode.get(3).asText();
            historicData.askPrice = new BigDecimal(detailsPayload.path("a").get(0).asText());
            historicData.askQuantity = new BigDecimal(detailsPayload.path("a").get(2).asText());
            historicData.bidPrice = new BigDecimal(detailsPayload.path("b").get(0).asText());
            historicData.bidQuantity = new BigDecimal(detailsPayload.path("b").get(2).asText());
            historicData.lastPrice = new BigDecimal(detailsPayload.path("c").get(0).asText());
            historicData.recordedTimeStamp = Instant.now().toString();

            Panache.getEntityManager().persist(historicData);
        }
        catch (Exception e){
            Log.error(String.format("Error parsing or persisting Kraken stream data due to exception %s with message %s", e.getClass().toString(), e.getMessage()));
            throw new RuntimeException("Error while parsing or persisting kraken stream data", e);
        }
    }
}
