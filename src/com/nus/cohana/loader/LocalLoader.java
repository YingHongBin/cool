/**
 * 
 */
package com.nus.cohana.loader;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

import com.google.common.primitives.Ints;

import jersey.repackaged.com.google.common.collect.Lists;
import com.nus.cohana.core.cohort.schema.TableSchema;
import com.nus.cohana.core.io.writestore.ChunkWS;
import com.nus.cohana.core.io.writestore.MetaChunkWS;
import com.nus.cohana.core.lang.Integers;
import com.nus.cohana.loader.reader.TupleReader;
import com.nus.cohana.loader.reader.LineTupleReader;

/**
 * Cublet Data Layout
 * -------------------------------------------------------------------------
 * | metaChunk | chunk 1 | chunk 2 |....| chunk N | header | header offset |
 * -------------------------------------------------------------------------
 * 
 * Header layout: 
 * --------------------------------- 
 * | chunks | chunk header offsets | 
 * --------------------------------- 
 * where chunks == number of chunks stored in this cublet.
 * 
 * Each cubelet has a size of roughly 1G in order to be memory mapped
 * 
 * @author david, caiqc, xiezl
 */
public class LocalLoader {

    /**
     * metaChunk offset
     */
    private static int offset = 0;
    
    /**
     * Header offsets
     */
    private static List<Integer> chunkOffsets = Lists.newArrayList();

    /**
     * Close current cublet. Write chunk header offsets and header offset into the
     * cublet.
     * 
     * @param out The output stream for data
     * @throws IOException
     */
    private static void closeCubelet(DataOutputStream out) throws IOException {
        int headOffset = offset;
        out.writeInt(Integers.toNativeByteOrder(chunkOffsets.size()));

        // Write chunk header Offsets
        for (int chunkOff : chunkOffsets) {
            out.writeInt(Integers.toNativeByteOrder(chunkOff));
        }
        // Write header offset
        out.writeInt(Integers.toNativeByteOrder(headOffset));
        out.flush();
        out.close();
    }

    /**
     * Create a new cublet for input data
     * 
     * @param dir The output dir
     * @param metaChunk The generated metaChunk
     * @return A DataOutputStream for further writing. Note the metaChunk has been
     *         already written into the stream once the function returns.
     * @throws IOException
     */
    private static DataOutputStream newCubelet(File dir, MetaChunkWS metaChunk) throws IOException {
        File cubelet = new File(dir, Long.toHexString(System.currentTimeMillis()) + ".dz");
        DataOutputStream out = new DataOutputStream(new FileOutputStream(cubelet));

        // first write the meta chunk and update offset and chunkOffsets
        // metaChunk -> header offset
        offset = metaChunk.writeTo(out);    //write metaChunk
        chunkOffsets.clear();
        // Question: Why minus Ints.BYTES
        // Answer: Move offset marker to metaChunk Header Offset to help decompression
        chunkOffsets.add(offset - Ints.BYTES);
        return out;
    }

    /**
     * Create a metaChunk for current Cublet
     * @param inputMetaFile the dimension file
     * @param schema the table schema
     * @param parser the table schema parser
     * @return metaChunkWS the generated metaChunk
     * @throws IOException
     */
    private static MetaChunkWS newMetaChunk(File inputMetaFile, TableSchema schema, TupleParser parser)
            throws IOException {
        // Read dimension file
        MetaChunkWS metaChunk = MetaChunkWS.newCohortMetaChunkWS(schema, offset);

        try (TupleReader reader = new LineTupleReader(inputMetaFile)) {
            while (reader.hasNext()) {
                metaChunk.put(parser.parse(reader.next()));
            }
        }
        metaChunk.complete();
        return metaChunk;
    }

    /**
     * Convert local data file (csv) to compression store
     * @param args there are five arguments for localLoader. List in input order:
     *            (1)table.yaml; (2)dimension.csv; (3)data.csv; (4) output folder;
     *            (5)chunkSize(Int), chunkSize = number of tuples in a chunk.
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        long beg = System.currentTimeMillis();
        // Read table schema
        File schemaFile = new File(args[0]);
        TableSchema schema = TableSchema.read(schemaFile);
        TupleParser parser = new CsvTupleParser();

        File metaChunkFile = new File(args[1]);
        MetaChunkWS metaChunk = newMetaChunk(metaChunkFile, schema, parser);

        // Generate data compress file
        File inputDataFile = new File(args[2]);
        File outputDir = new File(args[3]);
        // String fileName = Long.toHexString(System.currentTimeMillis());
        // File outputFile = new File(outputDir, fileName + ".dz");

        int chunkSize = Integer.parseInt(args[4]);
        int numChunk = 0;
        int splitColumn = schema.getUserKeyField();
        String lastUser = null;

        DataOutputStream out = newCubelet(outputDir, metaChunk);
        try (TupleReader reader = new LineTupleReader(inputDataFile)) {
            int noTuples = 0;
            ChunkWS chunk = ChunkWS.newCohortChunk(schema, metaChunk.getMetaFields(), offset);
            int totalUsers = 1;
            while (reader.hasNext()) {
                String tmp = (String) reader.next();
                String[] tuple = parser.parse(tmp);
                 String curUser = tuple[splitColumn];
                if (lastUser == null) {
                    lastUser = curUser;
                }
                if (!curUser.equals(lastUser)) {
                    totalUsers++;
                    lastUser = curUser;
                    if (noTuples >= chunkSize) {
                        // Write current chunk into output file
                        offset += chunk.writeTo(out);
                        // Move the offset cursor of the current chunk to chunk header offset
                        chunkOffsets.add(offset - Ints.BYTES);
                        // check if need to addWorker a new cubelet (2^30 = 1 Giga Byte)
                        if (offset >= (1 << 30)) {
                            closeCubelet(out);
                            out = newCubelet(outputDir, metaChunk);
                        }
                        chunk = ChunkWS.newCohortChunk(schema, metaChunk.getMetaFields(), offset);
                        noTuples = 0;
                        numChunk++;
                    }
                }
                /*
                if (noTuples >= chunkSize) {
                    offset += chunk.writeTo(out);
                    chunkOffsets.add(offset - Ints.BYTES);
                    System.out.println(offset);
                    if (offset >= (1 << 30)) {
                    //if (numChunk == 2) {
                        closeCubelet(out);
                        out = newCubelet(outputDir, metaChunk);
                    //    numChunk = 0;
                    }
                    chunk = ChunkWS.newCohortChunk(schema, metaChunk.getMetaFields(), offset);
                    noTuples = 0;
                    numChunk++;
                }
                */
                chunk.put(tuple);
                ++noTuples;
            }

            // all tuples processed; write the current chunk and close cubelet
            offset += chunk.writeTo(out);
            // Move the offset cursor of the current chunk to chunk header offset
            chunkOffsets.add(offset - Ints.BYTES);
            numChunk++;
            closeCubelet(out);
            System.out.println("Total processed users: " + totalUsers + ", # of chunks: " + numChunk );
                    // + " Output: " + outputFile.getAbsolutePath());
            long end = System.currentTimeMillis();
            //System.out.println("compressed elapsed: " + (end - beg));
        }
    }
}
