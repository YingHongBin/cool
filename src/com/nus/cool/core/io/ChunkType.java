/**
 * 
 */
package com.nus.cool.core.io;

/**
 * @author david
 *
 */
public enum ChunkType {

	META,

	DATA;

	public static ChunkType fromInteger(int c) {
		switch (c) {
		case 0:
			return META;
		case 1:
			return DATA;
		default:
			throw new IllegalArgumentException("ChunkType: " + c);
		}
	}
}
