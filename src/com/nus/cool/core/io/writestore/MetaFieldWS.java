/**
 * 
 */
package com.nus.cool.core.io.writestore;

import com.nus.cool.core.cohort.schema.FieldType;
import com.nus.cool.core.io.Output;

/**
 * @author david, xiezl
 *
 */
public interface MetaFieldWS extends Output {
	
	void put(String v);
	
	/**
	 * Find the position of value in this meta field, 
	 * return -1 if no=】 such value exists
	 * 
	 * @param v
	 * @return
	 */
	int find(String v);
	
	/**
	 * Number of entries in this field
	 * 
	 * @return
	 */
	int count();
	
	FieldType getFieldType();
	
	/**
	 * Call this method before writeTo when no more values
	 * are put into this meta field. After the method returns,
	 * this meta field is frozen for writing. 
	 */
	void complete();
	
}
