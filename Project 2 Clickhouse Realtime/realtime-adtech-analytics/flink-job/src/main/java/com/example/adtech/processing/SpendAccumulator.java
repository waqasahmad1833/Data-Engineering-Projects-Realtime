package com.example.adtech.processing;

/** Window accumulator for campaign spend (primitive fields only: a plain Flink POJO). */
public class SpendAccumulator {

    public long impressions;
    public long spendMicros;
    public int advertiserId;

    public SpendAccumulator() {
    }
}
