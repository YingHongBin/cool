/**
 * 
 */
package com.nus.cohana.core.cohort.query;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;

/**
 * A set containing possible values (for filters) of a specific field
 * @author david
 *
 */
public class FieldSet {

	public static enum FieldSetType {
		
		Set,
		
		Range,
		
		Birth 
		
	}
	
	private FieldSetType setType;
	
	private String field;
	
	private List<String> values;
	
	public FieldSet() {
		
	}
	
	public FieldSet(FieldSetType setType, String field, List<String> values) {
		this.setType = setType;
		this.field = checkNotNull(field);
		this.values = checkNotNull(values);
	}

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
	public List<String> getValues() {
		return values;
	}

	/**
	 * @param values the values to set
	 */
	public void setValues(List<String> values) {
		this.values = values;
	}
	
}
