package com.nus.cohana.core.iceberg.result;

import java.util.*;

public class BaseResult {

    private String timeRange;

    private String key;

    private String fieldName;

    private AggregatorResult aggregatorResult;

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public String getTimeRange() {
        return timeRange;
    }

    public void setTimeRange(String timeRange) {
        this.timeRange = timeRange;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public AggregatorResult getAggregatorResult() {
        return aggregatorResult;
    }

    public void setAggregatorResult(AggregatorResult aggregatorResult) {
        this.aggregatorResult = aggregatorResult;
    }

    private boolean equalsKey(BaseResult another) {
        Set<String> set1 = new HashSet<>(Arrays.asList(this.getKey().split("|")));
        Set<String> set2 = new HashSet<>(Arrays.asList(another.getKey().split("|")));
        return set1.equals(set2);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if ((o instanceof BaseResult)) {
            BaseResult another = (BaseResult) o;
            if (this.equalsKey(another)) {
                if (this.getTimeRange().equals(another.getTimeRange()) && this.getFieldName().equals(another.getFieldName())) {
                    return true;
                } else {
                    return false;
                }
            } else {
                return false;
            }
        } else {
            return false;
        }
    }

    public static List<BaseResult> merge(List<BaseResult> results) {
        BitSet bs = new BitSet();
        bs.set(0, results.size());
        for (int i = 0; i < bs.size(); i++) {
            i = bs.nextSetBit(i);
            if (i < 0) break;
            BaseResult res1 = results.get(i);
            for (int j = i + 1; j < bs.size(); j++) {
                j = bs.nextSetBit(j);
                if (j < 0) break;
                BaseResult res2 = results.get(j);
                if (res1.equals(res2)) {
                    res1.getAggregatorResult().merge(res2.getAggregatorResult());
                    bs.clear(j);
                }
            }
        }
        List<BaseResult> finalRes = new ArrayList<>();
        for (int i = 0; i < bs.size(); i++) {
            i = bs.nextSetBit(i);
            if (i < 0) break;
            finalRes.add(results.get(i));
        }
        //System.out.println("bs: " + finalRes.size());
        return finalRes;
    }
}
