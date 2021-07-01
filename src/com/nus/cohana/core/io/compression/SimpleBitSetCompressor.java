package com.nus.cohana.core.io.compression;

import com.google.common.io.Files;
import com.google.common.primitives.Ints;
import com.nus.cohana.core.lang.Integers;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

public class SimpleBitSetCompressor {

    public static int compress(BitSet bs, DataOutput out) throws IOException {
        int pos1 = 0, pos2 = 0, bytesWritten = 0;
        List<Integer> blks = new ArrayList<>();
        boolean sign = bs.get(pos1);
        while (true) {
            if (sign) {
                pos2 = bs.nextClearBit(pos1);
            } else {
                pos2 = bs.nextSetBit(pos1);
            }
            if (pos2 < 0 & !sign) {
                break;
            }
            blks.add(pos2 - pos1);
            pos1 = pos2;
            sign = !sign;
        }
        out.write(bs.get(0) ? 1 : 0);
        bytesWritten++;
        out.writeInt(Integers.toNativeByteOrder(blks.size()));
        bytesWritten += Ints.BYTES;
        for (int blk : blks) {
            out.writeInt(Integers.toNativeByteOrder(blk));
            bytesWritten += Ints.BYTES;
        }
        return bytesWritten;
    }

    public static BitSet read(ByteBuffer buff) {
        boolean sign = buff.get() != 0;
        int blks = buff.getInt();
        BitSet bs = new BitSet();
        int pos = 0, len;
        for (int i = 0; i < blks; i++) {
            len = buff.getInt();
            if (sign) {
                bs.set(pos, pos + len);
            }
            pos = pos + len;
            sign = !sign;
        }
        return bs;
    }

    public static void main(String[] args) throws IOException{
        BitSet bs = new BitSet(8);

        File file = new File("test");
        DataOutputStream out = new DataOutputStream(new FileOutputStream(file));
        compress(bs, out);
        BitSet bs2 = read(Files.map(new File("test")).order(ByteOrder.nativeOrder()));
        System.out.println(bs2);
    }

}
