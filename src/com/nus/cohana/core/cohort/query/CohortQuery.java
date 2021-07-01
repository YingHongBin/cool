/**
 * 
 */
package com.nus.cohana.core.cohort.query;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author david
 *
 */
@JsonIgnoreProperties({"valid"})
public class CohortQuery {
	
	private String dataSource;
	
	private String appKey;
	
	private String metric;
	
	private String since;
		
	private String[] rowFields;
	
	private List<FieldSet> rowFilters;
	
	private String columnField;
	
	private List<FieldSet> columnFilters;

	/**
	 * @return the metric
	 */
	public String getMetric() {
		return metric;
	}

	/**
	 * @param metric the metric to set
	 */
	public void setMetric(String metric) {
		this.metric = metric;
	}


	/**
	 * @return the dataSource
	 */
	public String getDataSource() {
		return dataSource;
	}

	/**
	 * @param dataSource the dataSource to set
	 */
	public void setDataSource(String dataSource) {
		this.dataSource = dataSource;
	}

	/**
	 * @return the appKey
	 */
	public String getAppKey() {
		return appKey;
	}

	/**
	 * @param appKey the appKey to set
	 */
	public void setAppKey(String appKey) {
		this.appKey = appKey;
	}
	
	/**
	 * @return the since
	 */
	public String getSince() {
		return since;
	}

	/**
	 * @param since the since to set
	 */
	public void setSince(String since) {
		this.since = since;
	}

	
	public boolean isValid() {
		if(dataSource == null || appKey == null)
			return false;
		if(rowFields == null || rowFields.length != 1)
			return false;
		return true;
	}

	@Override
	public String toString() {
		ObjectMapper mapper = new ObjectMapper();
		//mapper.enable(SerializationFeature.INDENT_OUTPUT);
		try {
			return mapper.writeValueAsString(this);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * @return the rowFields
	 */
	public String[] getRowFields() {
		return rowFields;
	}

	/**
	 * @param rowFields the rowFields to set
	 */
	public void setRowFields(String[] rowFields) {
		this.rowFields = rowFields;
	}

	/**
	 * @return the rowFilters
	 */
	public List<FieldSet> getRowFilters() {
		return rowFilters;
	}

	/**
	 * @param rowFilters the rowFilters to set
	 */
	public void setRowFilters(List<FieldSet> rowFilters) {
		this.rowFilters = rowFilters;
	}

	/**
	 * @return the columnFilters
	 */
	public List<FieldSet> getColumnFilters() {
		return columnFilters;
	}

	/**
	 * @param columnFilters the columnFilters to set
	 */
	public void setColumnFilters(List<FieldSet> columnFilters) {
		this.columnFilters = columnFilters;
	}

	/**
	 * @return the columnField
	 */
	public String getColumnField() {
		return columnField;
	}

	/**
	 * @param columnField the columnField to set
	 */
	public void setColumnField(String columnField) {
		this.columnField = columnField;
	}

}
