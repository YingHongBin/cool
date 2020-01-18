/**
 * 
 */
package com.nus.cool.core.io.compression;

import com.nus.cool.core.io.Codec;

/**
 * @author David
 *
 */
public interface CompressorFactory {
	
	Compressor newCompressor(Codec type, Histogram hist);
	
}
