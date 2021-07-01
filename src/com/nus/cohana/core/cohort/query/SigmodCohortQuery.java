/**
 * 
 */
package com.nus.cohana.core.cohort.query;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author david
 *
 */
public class SigmodCohortQuery {
	
	private static final Log LOG = LogFactory.getLog(SigmodCohortQuery.class);
	
	private String dataSource;
	
	private String appKey;
	
	private String[] birthActions;
	
	private String[] cohortFields;
	
	private List<FieldSet> birthSelection = new ArrayList<>();
	
	private List<FieldSet> ageSelection = new ArrayList<>();
	
	private int ageInterval = 1;
	
	private String metric;

	private String outSource;

	public String getOutSource() {
		return outSource;
	}

	public void setOutSource(String outSource) {
		this.outSource = outSource;
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
	 * @return the birthAction
	 */
	public String[] getBirthActions() {
		return birthActions;
	}

	/**
	 * @param birthActions the birthAction to set
	 */
	public void setBirthActions(String[] birthActions) {
		this.birthActions = birthActions;
	}

	/**
	 * @return the cohortFields
	 */
	public String[] getCohortFields() {
		return cohortFields;
	}

	/**
	 * @param cohortFields the cohortFields to set
	 */
	public void setCohortFields(String[] cohortFields) {
		this.cohortFields = cohortFields;
	}

	/**
	 * @return the birthSelection
	 */
	public List<FieldSet> getBirthSelection() {
		return birthSelection;
	}

	/**
	 * @param birthSelection the birthSelection to set
	 */
	public void setBirthSelection(List<FieldSet> birthSelection) {
		this.birthSelection = birthSelection;
	}

	/**
	 * @return the ageSelection
	 */
	public List<FieldSet> getAgeSelection() {
		return ageSelection;
	}

	/**
	 * @param ageSelection the ageSelection to set
	 */
	public void setAgeSelection(List<FieldSet> ageSelection) {
		this.ageSelection = ageSelection;
	}

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
	
	@JsonIgnore
	public boolean isValid() {
		boolean bValid = (appKey != null) &&
				(dataSource != null) &&
				(birthActions != null) &&
				(metric != null) &&
				(cohortFields != null) &&
				(cohortFields.length > 0) &&
				(ageInterval >= 1);
		return bValid;
	}

	@Override
	public String toString() {
		ObjectMapper mapper = new ObjectMapper();
		try {
			return mapper.writeValueAsString(this);
		} catch (JsonProcessingException e) {
			LOG.info(e);
		}
		return null;
	}
	
	public String toPrettyString() {
		ObjectMapper mapper = new ObjectMapper();
		try {
			return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(this);
		} catch (JsonProcessingException e) {
			LOG.info(e);
		}
		return null;
	}

	/**
	 * @return the ageInterval
	 */
	public int getAgeInterval() {
		return ageInterval;
	}

	/**
	 * @param ageInterval the ageInterval to set
	 */
	public void setAgeInterval(int ageInterval) {
		this.ageInterval = ageInterval;
	}
	
	public FieldSet getBirthSelectionField(String fieldName) {
		for(FieldSet fieldSet : birthSelection) 
			if(fieldSet.getCubeField().equals(fieldName))
				return fieldSet;
		return null;
	}

	public static SigmodCohortQuery read(InputStream in) throws IOException {
		ObjectMapper mapper = new ObjectMapper();
		return mapper.readValue(in, SigmodCohortQuery.class);
	}

}
