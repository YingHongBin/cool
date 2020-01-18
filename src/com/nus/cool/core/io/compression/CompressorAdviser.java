/**
 * 
 */
package com.nus.cool.core.io.compression;

import com.nus.cool.core.io.Codec;

/**
 * @author David
 *
 */
public interface CompressorAdviser {

	Codec advise(Histogram hist);
	
}
