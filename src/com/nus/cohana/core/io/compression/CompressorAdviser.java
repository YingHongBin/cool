/**
 * 
 */
package com.nus.cohana.core.io.compression;

import com.nus.cohana.core.io.Codec;

/**
 * @author David
 *
 */
public interface CompressorAdviser {

	Codec advise(Histogram hist);
	
}
