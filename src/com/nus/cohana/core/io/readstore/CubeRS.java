/**
 * 
 */
package com.nus.cohana.core.io.readstore;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.io.Files;

import com.nus.cohana.core.cohort.schema.CubeSchema;
import com.nus.cohana.core.cohort.schema.TableSchema;
import com.nus.cohana.core.io.readstore.CubletRS;

/**
 * The main in-memory data structure for cube. 
 * For the moment it only loads cohort cube.
 * 
 * @author david
 *
 */
@JsonIgnoreProperties({ "cubeSchema", "tableSchema", "cublets" })
public class CubeRS {

	private CubeSchema cubeSchema;

	private TableSchema tableSchema;

	private List<File> cubletFiles = new ArrayList<>();

	private List<CubletRS> cublets = new ArrayList<>();

	public CubeRS(CubeSchema cubeSchema, TableSchema tableSchema) {
		this.cubeSchema = checkNotNull(cubeSchema);
		this.tableSchema = checkNotNull(tableSchema);
	}

	public static CubeRS load(File cubeDir) throws IOException {
		File cubeRoot = cubeDir;
		if (cubeRoot.exists() == false)
			throw new FileNotFoundException(cubeRoot.getName()
					+ " cube was not found!");

		TableSchema tableSchema = TableSchema.read(new File(cubeRoot, "table.yaml"));
		CubeSchema cubeSchema = CubeSchema.read(new File(cubeRoot, "cube.yaml"));
		CubeRS cubeStore = new CubeRS(cubeSchema, tableSchema);

		File[] versions = cubeRoot.listFiles(new FileFilter() {

			@Override
			public boolean accept(File pathname) {
				if (pathname.isDirectory())
					return true;
				return false;
			}

		});

		if (versions.length == 0)
			return null;

		Arrays.sort(versions);

		File currentVersion = versions[versions.length - 1];

        File[] cublets = currentVersion.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(".dz");
            }
        });
        Arrays.sort(cublets);

		for (File cubletFile : cublets) {
            cubeStore.addCublet(cubletFile);
		}
		return cubeStore;
	}

	/**
	 * @return the cubletFiles
	 */
	public List<File> getCubletFiles() {
		return cubletFiles;
	}

	public CubeSchema getCubeSchema() {
		return cubeSchema;
	}

	public TableSchema getTableSchema() {
		return tableSchema;
	}

	public void addCublet(File cubletFile) throws IOException {
		cubletFiles.add(cubletFile);
		CubletRS cubletRS = new CubletRS(tableSchema);
		cubletRS.readFrom(Files.map(cubletFile).order(ByteOrder.nativeOrder()));
		cubletRS.setFile(cubletFile.getName());
		cublets.add(cubletRS);
	}

	public void addCublet(ByteBuffer buffer) {
		CubletRS cubletRS = new CubletRS(tableSchema);
		cubletRS.readFrom(buffer);
		cublets.add(cubletRS);
	}

	public void addCublet(ByteBuffer buffer, String file) {
		CubletRS cubletRS = new CubletRS(tableSchema);
		cubletRS.readFrom(buffer);
		cubletRS.setFile(file);
		cublets.add(cubletRS);
	}

	public List<CubletRS> getCublets() {
		return this.cublets;
	}

}
