/**
 * 
 */
package com.nus.cool.core.io.readstore;

import com.nus.cool.core.cohort.schema.FieldType;
import com.nus.cool.core.io.Input;

import java.nio.ByteBuffer;

/**
 * For the moment, we only support Hash based meta field.
 * TODO: add range meta field later
 * 
 * @author david
 *
 */
public interface MetaFieldRS extends Input {
	
	FieldType getFieldType();
	
	/**
	 * Return the id of the given value
	 * 
	 * @param v
	 * @return
	 */
	int find(String v);
	
	/**
	 * Return number of values in the field
	 */
	int count();
	
	/**
	 * Return the value for the given id
	 * 
	 * @param i
	 * @return
	 */
	String getString(int i);
	
	/**
	 * Return the max value of the field
	 */
	int getMaxValue();
	
	/**
	 * Return the min value of the field
	 */
	int getMinValue();

	String toString();

	void readFromWithFieldType(ByteBuffer buffer, FieldType fieldType);
}
