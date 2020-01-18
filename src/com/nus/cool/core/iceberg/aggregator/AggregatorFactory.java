package com.nus.cool.core.iceberg.aggregator;

/**
 * @author yhb
 */
public class AggregatorFactory {

    public enum AggregatorType {

        COUNT,

        SUM,

        AVERAGE,

        MAX,

        MIN,

        DISTINCTCOUNT
    }

    public Aggregator create(AggregatorType operator) {
        switch (operator) {
            case COUNT:
                return new CountAggregator();
            case SUM:
                return new SumAggregator();
            case AVERAGE:
                return new AverageAggregator();
            case MAX:
                return new MaxAggregator();
            case MIN:
                return new MinAggregator();
            case DISTINCTCOUNT:
                return new CountDistinctAggregator();
            default:
                throw new IllegalArgumentException("Unknown aggregator operator: " + operator);
        }
    }
}
