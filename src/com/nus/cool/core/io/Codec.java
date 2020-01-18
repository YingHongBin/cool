/**
 * 
 */
package com.nus.cool.core.io;

/**
 * Encoding codec for compression
 * 
 * @author david, xiezl
 *
 */
public enum Codec {
	
	BitVector,
	
	INT8,
	
	INT16,
	
	INT24,
	
	INT32,
	
	INTBIT,
	
	RLE, // Run length encoding
	
	Delta,
	
	LZ4,
	
	Range,
	
	None,

	PreCal,
	
	Unknown;
	
	public static Codec fromInteger(int c) {
		switch(c) {
		case 0:
			return BitVector;
		case 1:
			return INT8;
		case 2:
			return INT16;
		case 3:
			return INT24;
		case 4:
			return INT32;
		case 5:
			return INTBIT;
		case 6:
			return RLE;
		case 7:
			return Delta;
		case 8:
			return LZ4;
		case 9:
			return Range;
		case 10:
			return None;
		case 11:
			return PreCal;
		}
		
		return Unknown;
	}
}
