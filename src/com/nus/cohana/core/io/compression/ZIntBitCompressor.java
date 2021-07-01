package com.nus.cohana.core.io.compression;

import com.nus.cohana.core.io.Codec;
import com.nus.cohana.core.lang.Integers;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import com.google.common.primitives.Longs;

/**
 *
 * @author qingchao
 */
public class ZIntBitCompressor implements Compressor {
    
    public static Codec CODEC = Codec.INTBIT;
    
    private int numOfVal;
    
    private int numOfBits;
    
    private int maxCompressedLength;
    
    class Pack{
        int offset;
        long pack;
        
        boolean hasSlot() {
            return offset >= numOfBits;
        }
        
        void pushNext(long val) {
            offset -= numOfBits;
            pack |= (val << offset);
        }
        
        Pack() {
            reset();
        }
        
        void reset() {
            offset = 64;
            pack = 0;
        }
    }   

    public ZIntBitCompressor(Histogram hist) {
//      numOfBits = Integers.minBits(hist.getNumberOfUniqueValues());
//      this.numOfVal = hist.count();
        if (hist.max() >= (1L << 32))
            numOfBits = 64;
        else
            this.numOfBits = Integers.minBits((int) (hist.max() + 1));          
        this.numOfVal = hist.count();
        
        int numOfValPerPack = 64 / numOfBits;
        int numOfPack = (numOfVal - 1) / numOfValPerPack + 2;
        this.maxCompressedLength = numOfPack * Longs.BYTES;
    }

    @Override
    public int maxCompressedLength() {
        return this.maxCompressedLength;
    }

    @Override
    public int compress(byte[] src, int srcOff, int srcLen, byte[] dest,
            int destOff, int maxDestLen) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int compress(int[] src, int srcOff, int srcLen, byte[] dest,
            int destOff, int maxDestLen) {

        ByteBuffer buffer = ByteBuffer.wrap(dest, destOff, maxDestLen);
        buffer.order(ByteOrder.nativeOrder());
        buffer.putInt(srcLen);
        buffer.putInt(numOfBits);
        
        Pack packer = new Pack();
        for (int i = 0; i < srcLen; i++) {          
            packer.pushNext(src[i + srcOff]);
            
            if (!packer.hasSlot()) {
                buffer.putLong(packer.pack);
                packer.reset();             
            }   
        }
        
        if (packer.offset < 64) {
            buffer.putLong(packer.pack);
        }
        
        //System.out.println(packer.offset);

        return buffer.position();
    }

    @Override
    public int compress(long[] src, int srcOff, int srcLen, byte[] dest,
            int destOff, int maxDestLen) {
        ByteBuffer buffer = ByteBuffer.wrap(dest, destOff, maxDestLen);
        buffer.order(ByteOrder.nativeOrder());
        buffer.putInt(srcLen);
        buffer.putInt(numOfBits);
        
        Pack pack = new Pack();
        for (int i = 0; i < srcLen; i++) {
            pack.pushNext(src[i + srcOff]);
            
            if (!pack.hasSlot()) {
                buffer.putLong(pack.pack);                
                pack.reset();             
            } 
        }
        
        if (pack.offset < 64) {
            buffer.putLong(pack.pack);
        }

        return buffer.position();
    }

    @Override
    public int compress(float[] src, int srcOff, int srcLen, byte[] dest,
            int destOff, int maxDestLen) {
        throw new UnsupportedOperationException();
    }


    @Override
    public String toString() {
        return String.format("(numOfBits = %d)", numOfBits);
    }
}
