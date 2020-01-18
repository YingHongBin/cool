package com.nus.cool.core.iceberg.result;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.HashSet;
import java.util.Set;

/**
 * @author yhb
 */
public class AggregatorResult {

    private Float count;

    private Long sum;

    private Float average;

    private Float max;

    private Float min;

    private Float countDistinct;

    @JsonIgnore
    private Set<String> distinctSet = new HashSet<>();

    public Float getCount() {
        return count;
    }

    public void setCount(float count) {
        this.count = count;
    }

    public Long getSum() {
        return sum;
    }

    public void setSum(Long sum) {
        this.sum = sum;
    }

    public Float getAverage() {
        return average;
    }

    public void setAverage(float average) {
        this.average = average;
    }

    public Float getMax() {
        return max;
    }

    public void setMax(float max) {
        this.max = max;
    }

    public Float getMin() {
        return min;
    }

    public void setMin(float min) {
        this.min = min;
    }

    public Float getCountDistinct() {
        return countDistinct;
    }

    public void setCountDistinct(Float countDistinct) {
        this.countDistinct = countDistinct;
    }

    public Set<String> getDistinctSet() {
        return distinctSet;
    }

    public void setDistinctSet(Set<String> distinctSet) {
        this.distinctSet = distinctSet;
    }

    public void merge(AggregatorResult res) {
        if (this.countDistinct != null) {
            this.distinctSet.addAll(res.getDistinctSet());
            this.countDistinct = (float) this.distinctSet.size();
        }
        if (this.max != null) this.max = this.max >= res.getMax() ? this.max : res.getMax();
        if (this.min != null) this.min = this.min <= res.getMin() ? this.min : res.getMin();
        if (this.count != null) this.count += res.getCount();
        if (this.sum != null) this.sum += res.getSum();
        if (this.average != null && this.average != 0) this.average = this.sum / this.count;
     }
}
