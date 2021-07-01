package com.nus.cohanaService.model;

public enum QueryType {
    COHORT("cohort"),
    ICEBERG("iceberg");

    private final String queryType;

    QueryType(String queryType) {
        this.queryType = queryType;
    }

    public String getValue() {
        return queryType;
    }

    public static QueryType getEnum(String queryType) {
        for (QueryType e : values()) {
            if (e.getValue().equals(queryType)) {
                return e;
            }
        }
        return null;
    }
} 


