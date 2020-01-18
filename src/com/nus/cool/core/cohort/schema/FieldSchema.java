/**
 * 
 */
package com.nus.cool.core.cohort.schema;

import static com.google.common.base.Preconditions.checkNotNull;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * @author david, qingchao
 *
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class FieldSchema {
    
    private String name;
    
    private FieldType fieldType;
    
    private DataType dataType;
    
    private String aggregator;

    private String baseField;
    
    private int scale = 1;

    private boolean preCal;
    
    FieldSchema() {
        
    }
    
    public FieldSchema(String name, FieldType fieldType, DataType dataType, boolean preCal) {
        this.name = checkNotNull(name);
        this.fieldType = fieldType;
        this.dataType = dataType;
        this.preCal = preCal;
    }

    public FieldSchema(String name, FieldType fieldType, DataType dataType, 
            String aggregator, String baseField, boolean preCal) {
        this.name = checkNotNull(name);
        this.fieldType = checkNotNull(fieldType);
        this.dataType = checkNotNull(dataType);
        this.aggregator = aggregator;
        this.baseField = baseField;
        this.preCal = preCal;
    }

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * @param name the name to set
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return the fieldType
     */
    public FieldType getFieldType() {
        return fieldType;
    }

    /**
     * @param fieldType the fieldType to set
     */
    public void setFieldType(FieldType fieldType) {
        this.fieldType = fieldType;
    }

    /**
     * @return the dataType
     */
    public DataType getDataType() {
        return dataType;
    }

    /**
     * @param dataType the dataType to set
     */
    public void setDataType(DataType dataType) {
        this.dataType = dataType;
    }

    /**
     * @return the aggregator
     */
    public String getAggregator() {
        return aggregator;
    }

    /**
     * @param aggregator the aggregator to set
     */
    public void setAggregator(String aggregator) {
        this.aggregator = aggregator;
    }

    /**
     * @return the baseField
     */
    public String getBaseField() {
        return baseField;
    }

    /**
     * @param baseField the baseField to set
     */
    public void setBaseField(String baseField) {
        this.baseField = baseField;
    }

	public int getScale() {
		return scale;
	}

	public void setScale(int scale) {
		this.scale = scale;
	}

	public boolean getPreCal() {
        return this.preCal;
    }

    public void setPreCal(boolean preCal) {
        this.preCal = preCal;
    }

  @JsonIgnore
  public boolean isNumeric() {
    return (fieldType == FieldType.Day ||
      fieldType == FieldType.Metric ||
      fieldType == FieldType.Week ||
      fieldType == FieldType.Month);
  }
}
