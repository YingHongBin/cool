/**
 * 
 */
package com.nus.cool.core.lang;

import java.nio.ByteOrder;

/**
 * Missing functions for java.lang.Integer
 * 
 * @author david, xiezl
 *
 */
public class Integers {

	/**
	 * Find the minimum number of bits to represent the integer
	 * 
	 * @param i
	 *          the integer
	 * @return the number of bits
	 */
	public static int minBits(int i) {
		i = (i == 0 ? 1 : i);
		return Integer.SIZE - Integer.numberOfLeadingZeros(i);
	}

	/**
	 * Return the minimum number of bytes to represent the integer
	 * 
	 * @param i
	 *          the integer
	 * @return the number of bytes
	 */
	public static int minBytes(int i) {
		return ((minBits(i) - 1) >>> 3) + 1;
		//int nonZeroBits = Integer.SIZE - Integer.numberOfLeadingZeros(i);
		//return nonZeroBits == 0 ? 1 : 
		//	(nonZeroBits & 0x7) == 0 ? nonZeroBits >>> 3 : (nonZeroBits >>> 3) + 1; 
	}

	/**
	 * Convert the input value into OS's native byte order
	 * 
	 * @param v
	 *          the integer
	 * @return the @param v in native order
	 */
	public static int toNativeByteOrder(int v) {
		boolean bLittle = (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN);
		return (bLittle ? Integer.reverseBytes(v) : v);
	}
	
}
