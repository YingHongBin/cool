/**
 * 
 */
package com.nus.cool.core.io.compression;

/**
 * @author David
 *
 */
public interface Compressor {

	/**
	 * Estimate the maximum size of compressed data
	 * 
	 * @param hist 
	 * @return
	 */
	int maxCompressedLength();

	/**
	 * Compress a byte array
	 * 
	 * @param src
	 * @param srcOff
	 * @param srcLen
	 * @param dest
	 * @param destOff
	 * @param maxDestLen
	 * @return
	 */
	int compress(byte[] src, int srcOff, int srcLen, byte[] dest, int destOff,
			int maxDestLen);

	/**
	 * Compress an integer array
	 * 
	 * @param src
	 * @param srcOff
	 * @param srcLen
	 * @param dest
	 * @param destOff
	 * @param maxDestLen
	 * @return
	 */
	int compress(int[] src, int srcOff, int srcLen, byte[] dest, int destOff,
			int maxDestLen);

	/**
	 * Compress a long array
	 * 
	 * @param src
	 * @param srcOff
	 * @param srcLen
	 * @param dest
	 * @param destOff
	 * @param maxDestLen
	 * @return
	 */
	int compress(long[] src, int srcOff, int srcLen, byte[] dest, int destOff,
			int maxDestLen);
	
	/**
	 * Compress a float array
	 * 
	 * @param src
	 * @param srcOff
	 * @param srcLen
	 * @param dest
	 * @param destOff
	 * @param maxDestLen
	 * @return
	 */
	int compress(float[] src, int srcOff, int srcLen, byte[] dest, int destOff,
			int maxDestLen);
	
//	byte[] compress(int[] src, int srcOff, int srcLen);
}
