/**
 * 
 */
package com.nus.cohana.core.io.writestore;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Map;

import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import com.planetj.math.rabinhash.RabinHashFunction32;

import com.nus.cohana.core.cohort.schema.FieldType;
import com.nus.cohana.core.io.DataOutputBuffer;
import com.nus.cohana.core.io.OutputCompressor;
import com.nus.cohana.core.io.compression.CompressType;
import com.nus.cohana.core.io.compression.Histogram;

/**
 * The data layout is as follows
 * 
 * ---------------------------------------------------
 * | fieldType | finger codec | fingers | value data |
 * ---------------------------------------------------
 * 
 * if fieldType == AppID, we do NOT store values
 * 
 * value data layout:
 *
 * change to:
 * --------------------------------------------------------------
 * | # globalIDs(values) | value codec | value offsets | values |
 * --------------------------------------------------------------
 *
 * was:
 * --------------------------------------------------------------
 * | # globalIDs(values) | value offsets | value codec | values |
 * --------------------------------------------------------------
 * 
 * @author david, xiezl
 *
 */
public class CohortMetaFieldWS implements MetaFieldWS {

    /**
     * Convert String to globalID
     * @author david, xiezl
     *
     */
    private static class Term {
        String term;
        int globalID;

        public Term(String term, int globalID) {
            this.term = term;
            this.globalID = globalID;
        }

        @Override
        public String toString() {
            return String.format("(%s, %d)", term, globalID);
        }

    }

    private Charset charset;

    private FieldType fieldType;

    private OutputCompressor outComp;

    private RabinHashFunction32 rhash = RabinHashFunction32.DEFAULT_HASH_FUNCTION;

    /**
     * Global dictionary
     * The keys are hashed by the indexed string
     */
    private Map<Integer, Term> dict = Maps.newTreeMap();

    public CohortMetaFieldWS(FieldType fieldType, Charset charset, OutputCompressor compressor) {
        this.fieldType = fieldType;
        this.charset = checkNotNull(charset);
        this.outComp = checkNotNull(compressor);
    }

    @Override
    public void put(String v) {
        // Set globalID as 0 for now
        // It will be changed in complete function
        dict.put(rhash.hash(v), new Term(v, 0));
    }

    @Override
    public int find(String v) {
        // TODO: need to handle the case where v is null
        int fp = rhash.hash(v);
        return dict.containsKey(fp) ? dict.get(fp).globalID : -1;
    }

    @Override
    public int count() {
        return dict.size();
    }

    @Override
    public FieldType getFieldType() {
        return this.fieldType;
    }

    @Override
    public void complete() {
        int gID = 0;
        // Set globalIDs
        for (Map.Entry<Integer, Term> en : dict.entrySet()) {
            en.getValue().globalID = gID++;
        }
    }

    @Override
    public int writeTo(DataOutput out) throws IOException {
        int bytesWritten = 0;

        //out.writeByte(fieldType.ordinal()); // write metaChunk -> metaChunkData -> fieldN -> fieldType
        //bytesWritten++;

        // Write fingers, i.e., the hash values of the original string, into the array
        // metaChunk -> metaChunkData -> fieldN -> fingers (fingers contains data's hash value)
        int[] fingers = new int[dict.size()];
        int i = 0;
        for (Map.Entry<Integer, Term> en : dict.entrySet()) {
            fingers[i++] = en.getKey();
        }
        Histogram h = Histogram.builder().min(fingers[0]).max(fingers[fingers.length - 1]).count(fingers.length)
                .rawSize(Ints.BYTES * fingers.length).compressType(CompressType.KeyFinger).build();
        outComp.reset(h, fingers, 0, fingers.length);
        // Compress and write the fingers.
        // The codec is written internally
        // Actually, the data is not compressed here.
        bytesWritten += outComp.writeTo(out);

        // Write values
        // TODO: checkDisconnected userkey field
        if (fieldType == FieldType.Segment || fieldType == FieldType.Action || fieldType == FieldType.UserKey) {
            try (DataOutputBuffer buf = new DataOutputBuffer()) {
                buf.writeInt(dict.size());  // write metaChunk -> metaChunkData -> fieldN -> value data -> # values

                // Store offsets into the buffer first
                i = 0;
                int off = 0;
                // value offsets begin with 0
                // store metaChunk -> metaChunkData -> fieldN -> value data -> value offsets into dataOutputBuffer
                for (Map.Entry<Integer, Term> en : dict.entrySet()) {
                    buf.writeInt(off);
                    // Calculate the String offset
                    off += en.getValue().term.getBytes(charset).length;
                }

                // Store string values into the buffer
                // did not encrypt string
                for (Map.Entry<Integer, Term> en : dict.entrySet()) {
                    buf.write(en.getValue().term.getBytes(charset));
                }

                h = Histogram.builder().compressType(CompressType.KeyString).rawSize(buf.size()).build();
                outComp.reset(h, buf.getData(), 0, buf.size());
                // Compress and write the buffer.
                // The codec is written internally
                bytesWritten += outComp.writeTo(out);
            }
        }

        return bytesWritten;
    }

}
