package com.nus.cool.core.iceberg.query;

import com.nus.cool.core.cohort.filter.FieldFilter;

import java.util.ArrayList;
import java.util.List;

/**
 * @author yhb
 */
public class SelectionFilter {

    private SelectionQuery.SelectionType type;

    private FieldFilter filter;

    private String dimension;

    private List<SelectionFilter> fields = new ArrayList<>();

    public SelectionQuery.SelectionType getType() {
        return type;
    }

    public void setType(SelectionQuery.SelectionType type) {
        this.type = type;
    }

    public FieldFilter getFilter() {
        return filter;
    }

    public void setFilter(FieldFilter filter) {
        this.filter = filter;
    }

    public List<SelectionFilter> getFields() {
        return fields;
    }

    public void setFields(List<SelectionFilter> fields) {
        this.fields = fields;
    }

    public String getDimension() {
        return dimension;
    }

    public void setDimension(String dimension) {
        this.dimension = dimension;
    }
}
