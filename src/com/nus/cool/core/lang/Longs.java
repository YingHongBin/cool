/**
 * 
 */
package com.nus.cool.core.lang;

import java.nio.ByteOrder;

/**
 * @author david, xiezl
 *
 */
public class Longs {

	/**
	 * Find the minimum number of bits to represent the long
	 * @param l the long 
	 * @return the mimimum number of bits
	 */
	public static int minBits(long l) {
		return Long.SIZE - Long.numberOfLeadingZeros(l);
	}
	
	/**
	 * Find the minimum number of bytes to represent the long
	 * @param l the long
	 * @return the minimum number of bites
	 */
	public static int minBytes(long l) {
		int bits = minBits(l);
		if (bits == 0)
			return 1;
		return ((bits - 1) >>> 3) + 1;
	}
	
    /**
    *
    * @param v the long integer
    * @return the native order
    */
   public static long toNativeByteOrder(long v) {
       boolean bLittle = (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN);
       return (bLittle ? Long.reverseBytes(v) : v);
   }
	
}
