/**
 * 
 */
package com.nus.cohana.core.io;

import java.nio.ByteBuffer;

import com.nus.cohana.core.io.storevector.BitSetInputVector;
import com.nus.cohana.core.io.storevector.FoRInputVector;
import com.nus.cohana.core.io.storevector.LZ4InputVector;
import com.nus.cohana.core.io.storevector.RLEInputVector;
import com.nus.cohana.core.io.storevector.ZInt16Store;
import com.nus.cohana.core.io.storevector.ZInt32Store;
import com.nus.cohana.core.io.storevector.ZInt8Store;
import com.nus.cohana.core.io.storevector.ZIntBitInputVector;

/**
 * @author david
 *
 */
public class InputVectorFactory {
	
	private static final InputVectorFactory INSTANCE = new InputVectorFactory();
	
	public static final InputVectorFactory get() {
		return INSTANCE;
	}

	public InputVector readFrom(ByteBuffer buf) {
		Codec codec = Codec.fromInteger(buf.get());
		InputVector result = null;
		switch (codec) {
		case BitVector:
			result = new BitSetInputVector();
			result.readFrom(buf);
			break;
		case INTBIT:
			result = ZIntBitInputVector.load(buf);
			break;
		case INT8:
			result = (InputVector) ZInt8Store.load(buf, buf.getInt());
			break;
		case INT16:
			result = (InputVector) ZInt16Store.load(buf, buf.getInt());
			break;
		case INT24:
		case INT32:
			result = (InputVector) ZInt32Store.load(buf, buf.getInt());
			break;
		case Delta:
			result = new FoRInputVector();
			result.readFrom(buf);
			break;
		case RLE:
			result = new RLEInputVector();
			result.readFrom(buf);
			break;
		case LZ4:
			result = new LZ4InputVector();
			result.readFrom(buf);
			break;
		default:
			throw new IllegalArgumentException("Codec: " + codec);
		}
		return result;
	}

}
