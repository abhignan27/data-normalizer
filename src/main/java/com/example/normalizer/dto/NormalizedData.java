package com.example.normalizer.dto;

import com.example.normalizer.constants.Exchange;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class NormalizedData {

    private Exchange exchange;
    private String symbol;
    private BigDecimal bidPrice;
    private BigDecimal bidQuantity;
    private BigDecimal askPrice;
    private BigDecimal askQuantity;
    private BigDecimal lastPrice;
    private String recordedTimeStamp;
    private String receivedTimeStamp;

}
