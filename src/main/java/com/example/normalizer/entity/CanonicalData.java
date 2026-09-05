package com.example.normalizer.entity;

import com.example.normalizer.constants.Exchange;
import jakarta.persistence.*;

import java.math.BigDecimal;

@Entity
@Table(name = "CANONICAL_DATA")
public class CanonicalData {

    @Id
    public Long id = 1L;

    @Enumerated(EnumType.STRING)
    @Column(name = "EXCHANGE")
    public Exchange exchange;

    @Column(name = "SYMBOL")
    public String symbol;

    @Column(name = "ASK_PRICE")
    public BigDecimal askPrice;

    @Column(name = "BID_PRICE")
    public BigDecimal bidPrice;

    @Column(name = "LAST_PRICE")
    public BigDecimal lastPrice;

    @Column(name = "ASK_QUANTITY")
    public BigDecimal askQuantity;

    @Column(name = "BID_QUANTITY")
    public BigDecimal bidQuantity;

    @Column(name = "RECEIVED_TIMESTAMP")
    public String receivedTimeStamp;

    @Column(name = "RECORDED_TIMESTAMP")
    public String recordedTimeStamp;
}
