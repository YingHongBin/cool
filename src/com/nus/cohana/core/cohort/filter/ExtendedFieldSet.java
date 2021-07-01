/**
 * 
 */
package com.nus.cohana.core.cohort.filter;

import java.util.ArrayList;
import java.util.List;

/**
 * @author qingchao
 *
 */
public class ExtendedFieldSet {
    
    public static enum FieldValueType {
        
        AbsoluteValue,

        IncreaseByAbsoluteValue,

        IncreaseByPercentage

    }

    public static enum FieldSetType {
        
        Set,
        
        Range,

        Birth

    }

    public static class FieldValue {

        private FieldValueType type;

        private List<String> values = new ArrayList<>(0);

        private String baseField;

        private int baseEvent = -1;

        /**
         * @return the type
         */
        public FieldValueType getType() {
            return type;
        }

        /**
         * @param type the type to set
         */
        public void setType(FieldValueType type) {
            this.type = type;
        }

        /**
         * @return the value
         */
        public List<String> getValues() {
            return values;
        }

        /**
         * @param value the value to set
         */
        public void setValues(List<String> values) {
        	if (values != null)
        		this.values = values;
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

        /**
         * @return the baseEvent
         */
        public int getBaseEvent() {
            return baseEvent;
        }

        /**
         * @param baseEvent the baseEvent to set
         */
        public void setBaseEvent(int baseEvent) {
            this.baseEvent = baseEvent;
        }
    }
    
    private FieldSetType setType;
    
    private String field;
    
    private FieldValue fieldValue;
    
    /**
     * @return the setLevel
     */
    public FieldSetType getFilterType() {
        return setType;
    }

    /**
     * @param setType the setLevel to set
     */
    public void setFilterType(FieldSetType setType) {
        this.setType = setType;
    }

    /**
     * @return the field
     */
    public String getCubeField() {
        return field;
    }

    /**
     * @param field the field to set
     */
    public void setCubeField(String field) {
        this.field = field;
    }

    /**
     * @return the values
     */
    public FieldValue getFieldValue() {
        return fieldValue;
    }

    /**
     * @param values the values to set
     */
    public void setFieldValue(FieldValue value) {
        this.fieldValue = value;
    }
    
}
