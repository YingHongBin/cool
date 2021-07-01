/**
 * 
 */
package com.nus.cohana.core.cohort.converter;

/**
 * @author david
 *
 */
public interface NumericConverter {
	
	int toInt(String v);
	
	double toDouble(String v);
	
	String getString(int i);

}
