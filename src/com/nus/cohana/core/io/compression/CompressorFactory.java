/**
 * 
 */
package com.nus.cohana.core.io.compression;

import com.nus.cohana.core.io.Codec;

/**
 * @author David
 *
 */
public interface CompressorFactory {
	
	Compressor newCompressor(Codec type, Histogram hist);
	
}
