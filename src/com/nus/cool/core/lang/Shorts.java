/**
 * 
 */
package com.nus.cool.core.lang;

import java.nio.ByteOrder;

/**
 * @author david
 *
 */
public class Shorts {

	/**
	 * Convert the input value into OS's native byte order
	 * 
	 * @param v
	 *          the integer
	 * @return the @param v in native order
	 */
	public static short toNativeByteOrder(short v) {
		boolean bLittle = (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN);
		return (bLittle ? Short.reverseBytes(v) : v);
	}
	
}
