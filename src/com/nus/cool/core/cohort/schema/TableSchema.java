/**
 * 
 */
package com.nus.cool.core.cohort.schema;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.collect.Maps;

/**
 * @author xiezl, david
 *
 */
public class TableSchema {
	
	private String charset;
	
	private List<FieldSchema> fields;
	
	private Map<String, Integer> name2Id = Maps.newHashMap();
	
	private int userKeyField = -1;
	
	private int appKeyField = -1;
	
	private int actionField = -1;
	
	private int actionTimeField = -1;
	
	public static TableSchema read(InputStream in) throws IOException {
		ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
		return mapper.readValue(in, TableSchema.class);
	}
	
	public static TableSchema read(File inputFile) throws IOException {
		return read(new FileInputStream(inputFile));
	}

	/**
	 * @return the charset
	 */
	public String getCharset() {
		return charset;
	}

	/**
	 * @param charset the charset to set
	 */
	public void setCharset(String charset) {
		this.charset = charset;
	}

	/**
	 * @return the fields
	 */
	public List<FieldSchema> getFields() {
		return fields;
	}

	/**
	 * @param fields the fields to set
	 */
	public void setFields(List<FieldSchema> fields) {
		this.fields = fields;
		this.name2Id.clear();

		for (int i = 0; i < fields.size(); i++) {
			FieldSchema f = fields.get(i);
			FieldType fieldType = f.getFieldType();
			name2Id.put(f.getName(), i);
			switch (fieldType) {
			case AppKey:
				appKeyField = i;
				break;
			case UserKey:
				userKeyField = i;
				break;
			case Action:
				actionField = i;
				break;
			case ActionTime:
				actionTimeField = i;
				break;
			default:
				break;
			}
		}
	}
	
	public int getFieldID(String name) {
		Integer id = name2Id.get(name);
		if(id == null)
			return -1;
		return id;
	}
	
	public FieldSchema getFieldSchema(int i) {
		return fields.get(i);
	}
	
	public FieldSchema getFieldSchema(String name) {
		return getFieldSchema(getFieldID(name));
	}
	
	public FieldType getFieldType(String name) {
		return getFieldSchema(name).getFieldType();
	}
	
	@JsonIgnore
	public int getUserKeyField() {
		return userKeyField;
	}

	@JsonIgnore
	public int getAppKeyField() {
		return appKeyField;
	}
	
	@JsonIgnore
	public int getActionField() {
		return actionField;
	}
	
	public int fields() {
		return fields.size();
	}
	
	@JsonIgnore
	public String getActionTimeFieldName() {
		return fields.get(getActionTimeField()).getName();
	}

	/**
	 * @return the actionTimeField
	 */
	@JsonIgnore
	public int getActionTimeField() {
		return actionTimeField;
	}
	
}
