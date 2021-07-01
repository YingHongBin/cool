package com.nus.cohana.core.iceberg.query;

import com.nus.cohana.core.iceberg.aggregator.AggregatorFactory;

import java.util.List;

/**
 * @author yhb
 */
public class Aggregation {
    private String fieldName;

    private List<AggregatorFactory.AggregatorType> operators;

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public List<AggregatorFactory.AggregatorType> getOperators() {
        return operators;
    }

    public void setOperators(List<AggregatorFactory.AggregatorType> operators) {
        this.operators = operators;
    }

    public Aggregation(String fieldName, List<AggregatorFactory.AggregatorType> operators) {
        this.fieldName = fieldName;
        this.operators = operators;
    }

    public Aggregation() {}
}
