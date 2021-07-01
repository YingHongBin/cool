/**
 * 
 */
package com.nus.cohana.core.cohort.converter;

/**
 * @author david
 *
 */
public class StringIntConverter implements NumericConverter {

	@Override
	public int toInt(String v) {
		return Integer.parseInt(v);
	}

	@Override
	public String getString(int i) {
		return String.valueOf(i);
	}

	@Override
	public double toDouble(String v) {
		return Double.parseDouble(v);
	}

}
