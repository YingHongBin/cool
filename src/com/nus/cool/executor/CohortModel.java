package com.nus.cool.executor;

// import static com.google.common.base.Preconditions.checkNotNull;

import java.io.*;
// import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.nus.cool.core.cohort.filter.ExtendedFieldSet;
import com.nus.cool.core.cohort.filter.FieldFilter;
import com.nus.cool.core.cohort.filter.FieldFilterFactory;
import com.nus.cool.core.cohort.schema.FieldSchema;
import com.nus.cool.core.io.InputVector;
import com.nus.cool.core.io.readstore.ChunkRS;
import com.nus.cool.core.io.readstore.CubletRS;
import com.sun.jersey.server.impl.container.filter.FilterFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.nus.cool.core.cohort.schema.CubeSchema;
import com.nus.cool.core.cohort.schema.TableSchema;
import com.nus.cool.core.io.readstore.CubeRS;

import com.google.common.collect.Maps;

// import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricSet;
/*
import com.codahale.metrics.jvm.BufferPoolMetricSet;
import com.codahale.metrics.jvm.ClassLoadingGaugeSet;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.google.common.collect.Maps;
*/

/**
 * Model component of MVC for QueryServer
 * 
 * @author david
 * 
 */
public class CohortModel implements Closeable {
	
	//private static final Log LOG = LogFactory.getLog(CohortModel.class);

	// The following map is used for jvm metrics
	// NOT USED in current package
	private Map<String, MetricSet> metrics = Maps.newHashMap();

	private Map<String, CubeRS> metaStore = Maps.newHashMap();
	
	/**
	 *  The directory to the compressed dz files
	 */
	private File localRepo;

	public String getLocalRepo() {
		return this.localRepo.getPath();
	}

	public CohortModel(String path) {
	    this.localRepo = new File(path);
	}

	public CohortModel(ByteBuffer buffer, TableSchema tableSchema, CubeSchema cubeSchema, String source, String file) throws IOException {
		CubeRS cubeStore = new CubeRS(cubeSchema, tableSchema);
		cubeStore.addCublet(buffer, file);
		metaStore.put(source, cubeStore);
	}
	
	/**
	 * To get the metric set by category
	 * NOT USED in current package
	 * @param cat
	 * @return
	 */
	public MetricSet findMetricSetByCategory(String cat) {
		return metrics.get(cat);
	}

	/**
	 * To load the cube file (dz file) into memory
	 * @param cube The cube name
	 * @throws IOException
	 */
	public synchronized void reload(String cube) throws IOException {
		//LOG.info("Reloading cube " + cube + " ...");
		
		// Step 1: checkDisconnected the old cube info
		metaStore.remove(cube);

		File cubeRoot = new File(localRepo, cube);
		
		if(cubeRoot.exists() == false)
			throw new FileNotFoundException(cube + " cube was not found!");
		
		TableSchema tableSchema = TableSchema.read(new File(cubeRoot, "table.yaml"));
		CubeSchema cubeSchema = CubeSchema.read(new File(cubeRoot, "cube.yaml"));
		CubeRS cubeStore = new CubeRS(cubeSchema, tableSchema);
		
		File[] versions = cubeRoot.listFiles(new FileFilter() {

			@Override
			public boolean accept(File pathname) {
				if(pathname.isDirectory())
					return true;
				return false;
			}
			
		});
		
		if(versions.length == 0)
			return;
		
		Arrays.sort(versions);
		//LOG.info(cube + "Cube versions: " + Arrays.toString(versions));
		
		File currentVersion = versions[versions.length - 1];
		
		for(File cubletFile : currentVersion.listFiles(new FilenameFilter() {

			@Override
			public boolean accept(File dir, String name) {
				return name.endsWith(".dz");
			}
			
		})) {
			cubeStore.addCublet(cubletFile);
		}
		metaStore.put(cube, cubeStore);

		// Step 3: tell all cublet workers to unload old cube data (For server)
	}

	public void addCube(String cube, String cubletPath) throws IOException {
		CubeRS cubeStore = this.metaStore.get(cube);
		File cublet = new File(cubletPath);
		cubeStore.addCublet(cublet);
	}
	
	public synchronized CubeRS getCube(String cube) {
		return metaStore.get(cube);
	}
	
	@Override
	public void close() throws IOException {
	}

	public void init(String dataSource) {
		CubeRS cube = this.getCube(dataSource);
		List<CubletRS> cublets = cube.getCublets();
		TableSchema tableSchema = cube.getTableSchema();
		List<FieldSchema> fieldSchemas = tableSchema.getFields();
		for (FieldSchema fieldSchema : fieldSchemas) {
            //System.out.println(fieldSchema.getName());
			FieldFilter filter = initFilter(fieldSchema);
			for (CubletRS cublet : cublets) {
			    filter.accept(cublet.getMetaChunk().getMetaField(fieldSchema.getName()));
			    for (ChunkRS dataChunk : cublet.getDataChunks()) {
			        filter.accept(dataChunk.getField(fieldSchema.getName()));
			        if (dataChunk.getField(fieldSchema.getName()).isPreCal()) continue;
			        InputVector in = dataChunk.getField(fieldSchema.getName()).getValueVector();
			        while (in.hasNext()) {
			            int value = in.next();
			            filter.accept(value);
                    }
                }
            }
		}
	}

	private FieldFilter initFilter(FieldSchema fieldSchema) {
        FieldFilterFactory factory = new FieldFilterFactory();
        switch (fieldSchema.getFieldType()) {
            case AppKey:
            case UserKey:
            case Action:
            case Segment: {
                List<String> values = new ArrayList<>();
                values.add("aaa");
                FieldFilter filter = factory.create(fieldSchema, null, values);
                return filter;
            }
            case ActionTime: {
                List<String> values = new ArrayList<>();
                values.add("1991-01-01|1999-01-01");
                FieldFilter filter = factory.create(fieldSchema, null, values);
                return filter;
            }
            case Metric: {
                List<String> values = new ArrayList<>();
                values.add("1|100");
                ExtendedFieldSet fs = new ExtendedFieldSet();
                ExtendedFieldSet.FieldValue v = new ExtendedFieldSet.FieldValue();
                v.setType(ExtendedFieldSet.FieldValueType.AbsoluteValue);
                fs.setFieldValue(v);
                FieldFilter filter = factory.create(fieldSchema, fs, values);
                return filter;
            }
            default:
                throw new IllegalArgumentException();
        }
    }

}
