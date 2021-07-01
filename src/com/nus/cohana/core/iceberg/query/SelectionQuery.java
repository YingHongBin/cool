package com.nus.cohana.core.iceberg.query;

import java.util.ArrayList;
import java.util.List;

/**
 * @author yhb
 */
public class SelectionQuery {

    public enum SelectionType {
        and,
        or,
        filter
    }

    private SelectionType type;

    private String dimension;

    private List<String> values;

    private List<SelectionQuery> fields = new ArrayList<>();

    public SelectionType getType() {
        return type;
    }

    public void setType(SelectionType type) {
        this.type = type;
    }

    public String getDimension() {
        return dimension;
    }

    public void setDimension(String dimension) {
        this.dimension = dimension;
    }

    public List<String> getValues() {
        return values;
    }

    public void setValues(List<String> values) {
        this.values = values;
    }

    public List<SelectionQuery> getFields() {
        return fields;
    }

    public void setFields(List<SelectionQuery> fields) {
        this.fields = fields;
    }
}
