/**
 * 
 */
package com.nus.cohana.core.cohort.schema;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import jersey.repackaged.com.google.common.collect.Maps;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

/**
 * @author david
 *
 */
public class CubeSchema {
	
	private List<Dimension> dimensions;
	
	private List<Measure> measures;
	
	@JsonIgnore
	private Map<String, Integer> dimenMap = Maps.newHashMap();
	
	@JsonIgnore
	private Map<String, Integer> measureMap = Maps.newHashMap();
	
	public static CubeSchema read(InputStream in) throws IOException {
		ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
		return mapper.readValue(in, CubeSchema.class);
	}
	
	public static CubeSchema read(File schemaFile) throws IOException {
		return read(new FileInputStream(schemaFile));
	}

	/**
	 * @return the dimensions
	 */
	public List<Dimension> getDimensions() {
		return dimensions;
	}
	
	public Dimension getDimension(String name) {
		if(dimenMap.containsKey(name)) {
			return dimensions.get(dimenMap.get(name));
		}
		return null;
	}

	/**
	 * @param dimensions the dimensions to set
	 */
	public void setDimensions(List<Dimension> dimensions) {
		this.dimensions = dimensions;
		int i = 0;
		for(Dimension dim : dimensions) 
			dimenMap.put(dim.getName(), i++);
	}

	/**
	 * @return the measures
	 */
	public List<Measure> getMeasures() {
		return measures;
	}
	
	public Measure getMeasure(String measureName) {
		if(measureMap.containsKey(measureName)) {
			return measures.get(measureMap.get(measureName));
		}
		return null;
	}

	/**
	 * @param measures the measures to set
	 */
	public void setMeasures(List<Measure> measures) {
		this.measures = measures;
		int i = 0;
		for(Measure measure : measures)
			measureMap.put(measure.getName(), i++);
	}

	@Override
	public String toString() {
		ObjectMapper mapper = new ObjectMapper();
		mapper.enable(SerializationFeature.INDENT_OUTPUT);
		try {
			return mapper.writeValueAsString(this);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
		return null;
	}
	
}
