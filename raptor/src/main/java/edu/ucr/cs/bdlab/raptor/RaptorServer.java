/*
 * Copyright 2018 University of California, Riverside
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.ucr.cs.bdlab.raptor;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.ucr.cs.bdlab.beast.common.BeastOptions;
import edu.ucr.cs.bdlab.beast.common.WebMethod;
import edu.ucr.cs.bdlab.beast.geolite.EnvelopeNDLite;
import edu.ucr.cs.bdlab.beast.geolite.IFeature;
import edu.ucr.cs.bdlab.beast.io.GeoJSONFeatureWriter;
import edu.ucr.cs.bdlab.beast.io.SpatialReader;
import edu.ucr.cs.bdlab.beast.util.WebHandler;
import edu.ucr.cs.bdlab.beast.util.OperationParam;
import edu.ucr.cs.bdlab.beast.util.Parallel;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.locationtech.jts.geom.Geometry;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.awt.geom.Rectangle2D;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * A web handler that handles visualization methods
 */
public class RaptorServer extends WebHandler {
  private static final Log LOG = LogFactory.getLog(RaptorServer.class);

  /**User options given at the application startup*/
  protected BeastOptions opts;

  /**The Spark context of the underlying application*/
  protected JavaSparkContext sc;

  /**Hadoop environment configuration*/
  protected Configuration conf;

  @OperationParam(
      description = "The relative path in which the vector layers are stored",
      defaultValue = "vectors"
  )
  public static final String VectorLayers = "vectors";

  @OperationParam(
      description = "The relative path in which the raster layers are stored",
      defaultValue = "raster"
  )
  public static final String RasterLayers = "rasters";

  /**A source for vector or raster data*/
  static abstract class DataLayer {
    /**A unique identified for the vector layer*/
    String id;
    /**A user friendly name*/
    String name;
    /**The relative path in which the vector layer is stored*/
    String path;

    void set(JsonNode node) {
      this.id = node.get("id").textValue();
      this.name = node.get("name").textValue();
      this.path = node.get("path").textValue();
    }
  }

  static class RasterLayer extends DataLayer {
    /**Name of the dataset in the HDF file*/
    String layerName;

    @Override
    void set(JsonNode node) {
      super.set(node);
      this.layerName = node.get("layer").textValue();
    }
  }

  static class VectorLayer extends DataLayer {
    /**Name of the field that contains the ID*/
    String idField;

    /**A list of fields that contain the name*/
    String[] nameFields;

    @Override
    void set(JsonNode node) {
      super.set(node);
      this.idField = node.get("idField").textValue();
      if (node.get("nameField") != null) {
        this.nameFields = new String[] {node.get("nameField").textValue()};
      } else if (node.get("nameFields") != null) {
        Iterator<JsonNode> iterator = node.get("nameFields").iterator();
        List<String> names = new ArrayList<>();
        while (iterator.hasNext()) {
          names.add(iterator.next().textValue());
        }
        this.nameFields = names.toArray(new String[names.size()]);
      }
    }
  }

  /**The base directory for all vector layers*/
  protected String vectorBaseDir;

  /**Maps each vector layer ID to its data*/
  protected List<VectorLayer> vectorLayers;

  /**The base directory for all raster layers*/
  protected String rasterBaseDir;

  /**Maps each raster layer ID to its data*/
  protected List<RasterLayer> rasterLayers;

  @Override
  public void setup(SparkContext sc, BeastOptions opts) {
    super.setup(sc, opts);
    this.opts = opts;
    this.conf = opts.loadIntoHadoopConf(null);
    this.sc = new JavaSparkContext(sc);
    try {
      // Read raster layers
      this.rasterBaseDir = opts.getString(RasterLayers, "rasters");
      this.rasterLayers = populateLayers(new Path(rasterBaseDir), conf, RasterLayer.class);
      // Read the list of vector layers
      this.vectorBaseDir = opts.getString(VectorLayers, "vectors");
      this.vectorLayers = populateLayers(new Path(vectorBaseDir), conf, VectorLayer.class);
    } catch (IOException e) {
      throw new RuntimeException("Error initializing the Raptor server", e);
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    } catch (InstantiationException e) {
      e.printStackTrace();
    }
  }

  /**
   * Populate the list of layers from a directory
   * @param dataPath the path of the input data
   * @param conf the system configuration
   * @param layerClass the class that represents the data layer, either vector or aster
   * @param <T> the type of the class, either {@link RasterLayer} or {@link VectorLayer}
   * @return the list of layers
   * @throws IOException if an error happens while reading from disk
   * @throws IllegalAccessException if the layer could not be initialized
   * @throws InstantiationException if the layer class could not be initialized
   */
  protected static <T extends DataLayer> List<T> populateLayers(
      Path dataPath, Configuration conf, Class<T> layerClass) throws IOException, IllegalAccessException, InstantiationException {
    List<T> layers = new ArrayList<>();
    Path indexFile = new Path(dataPath, "index.json");
    try (FileSystem fs = indexFile.getFileSystem(conf)) {
      if (fs.exists(indexFile)) {
        ObjectMapper mapper = new ObjectMapper();
        try (FSDataInputStream input = fs.open(indexFile)) {
          JsonNode rootNode = mapper.readTree(input);
          JsonNode layersNode = rootNode.path("layers");
          Iterator<JsonNode> iLayers = layersNode.elements();
          while (iLayers.hasNext()) {
            JsonNode layerNode = iLayers.next();
            T layer = layerClass.newInstance();
            layer.set(layerNode);
            layers.add(layer);
          }
        }
      } else {
        // No index file, use the directory names instead
        if (!fs.exists(dataPath))
          throw new RuntimeException(String.format("Layer path '%s' not found", dataPath));
        FileStatus[] directories = fs.listStatus(dataPath);
        for (FileStatus dir : directories) {
          if (dir.isDirectory()) {
            T layer = layerClass.newInstance();
            layer.id = layer.name = layer.path = dir.getPath().getName();
            layers.add(layer);
          }
        }
        // Write the layers back as a JSON file to facilitate future runs and edits
        try (JsonGenerator jsonGenerator = new JsonFactory().createGenerator((OutputStream) fs.create(indexFile))) {
          jsonGenerator.setPrettyPrinter(new DefaultPrettyPrinter());
          jsonGenerator.writeStartObject();
          jsonGenerator.writeFieldName("layers"); // Start root object
          jsonGenerator.writeStartArray(); // Start array of layers
          for (DataLayer layer : layers) {
            jsonGenerator.writeStartObject(); // Start layer object
            jsonGenerator.writeStringField("id", layer.id);
            jsonGenerator.writeStringField("name", layer.name);
            jsonGenerator.writeStringField("path", layer.path);
            jsonGenerator.writeEndObject(); // End layer object
          }
          jsonGenerator.writeEndArray(); // End array of layers
          jsonGenerator.writeEndObject(); // End root object
        }
      }
    }
    return layers;
  }

  /**
   * Computes the zonal statistics for a geometry over a MODIS dataset given the following parameters:
   * <ul>
   *   <li><tt>raster-id</tt>: Relative path to the vector file</li>
   *   <li><tt>feature-id</tt>: The ID of the polygon to retrieve from the vector file</li>
   *   <li><tt>raster-id</tt>: Relative path to the raster data</li>
   *   <li><tt>start-date</tt>: The start date for computing the results in yyyy.MM.dd format</li>
   *   <li><tt>end-date</tt>: The last date for computing the results in yyyy.MM.dd format</li>
   * </ul>
   * @param target the target path requested by the user
   * @param request the HTTP request
   * @param response the HTTP response
   * @return {@code true if the request was handled}
   * @throws IOException if an error happens while handling the request
   * @throws InterruptedException if the processing fails because of some interruption
   */
  @WebMethod(url = "/dynamic/zonalstats")
  public boolean handleZonalStatisticsRequest(String target,
                                       HttpServletRequest request,
                                       HttpServletResponse response) throws IOException, InterruptedException {
    ensureRequiredParameters(request, response, new String[] {"vector-id", "feature-id", "raster-id",
        "start-date", "end-date"});
    long t1 = System.nanoTime();
    // 1- Read the vector file and retrieve the desired polygon
    String vectorID = request.getParameter("vector-id");
    VectorLayer vectorLayer = findLayer(vectorLayers, vectorID);
    String vectorPath = new File(vectorBaseDir, vectorLayer.path).getPath();
    String featureID = request.getParameter("feature-id");
    JavaRDD<IFeature> polygons = SpatialReader.readInput(sc, opts, vectorPath, "shapefile");
    final String idField = vectorLayer.idField;
    IFeature feature = polygons.filter(f -> f.getAs(idField).toString().equals(featureID)).first();
    EnvelopeNDLite mbr = new EnvelopeNDLite(2);
    mbr.merge(feature.getGeometry());

    // 2- Select all subdirectories based on the date range
    String rasterID = request.getParameter("raster-id");
    RasterLayer rasterLayer = findLayer(rasterLayers, rasterID);
    Path rasterPath = new Path(rasterBaseDir, rasterLayer.path);
    String startDate = request.getParameter("start-date").replaceAll("-", ".");
    String endDate = request.getParameter("end-date").replaceAll("-", ".");
    // The final query result
    Statistics finalResult = new Statistics();
    // The list of all raster files
    List<Path> allRasterFiles = new ArrayList<>();
    try (FileSystem rFileSystem = rasterPath.getFileSystem(conf)) {
      FileStatus[] matchingDirs = rFileSystem.listStatus(rasterPath, HDF4Reader.createDateFilter(startDate, endDate));
      LOG.info(String.format("Matched %d dates", matchingDirs.length));

      // 3- In each dir, match the files based on the MBR of the geometry and process the files
      PathFilter tileRangeFilter = HDF4Reader.createTileIDFilter(new Rectangle2D.Double(mbr.getMinCoord(0), mbr.getMinCoord(1),
          mbr.getSideLength(0), mbr.getSideLength(1)));

      for (FileStatus matchingDir : matchingDirs) {
        FileStatus[] matchingTiles = rFileSystem.listStatus(matchingDir.getPath(), tileRangeFilter);
        for (FileStatus p : matchingTiles)
          allRasterFiles.add(p.getPath());
      }
      LOG.info(String.format("Matched a total of %d HDF files", allRasterFiles.size()));

      final String method = request.getParameter("method") == null ? "scanline" : request.getParameter("method");

      // 4- Process all tiles to compute the desired statistics
      finalResult.setNumBands(1);
      Parallel.forEach(allRasterFiles.size(), (i1, i2) -> {
        for (int iFile = i1; iFile < i2; iFile++) {
          try (HDF4Reader raster = new HDF4Reader()) {
            raster.initialize(rFileSystem, allRasterFiles.get(iFile), rasterLayer.layerName);
            Collector[] stats = {new Statistics()};
            stats[0].setNumBands(raster.getNumComponents());
            switch (method) {
              case "scanline":
                Intersections intersections = new Intersections();
                intersections.compute(new Geometry[] {feature.getGeometry()}, raster);
                stats = ZonalStatisticsCore.computeZonalStatisticsScanline(raster, intersections, Statistics.class);
                break;
              case "pip":
                ZonalStatisticsCore.computeZonalStatisticsNaive(raster, feature.getGeometry(), stats[0]);
                break;
              case "quadsplit":
                ZonalStatisticsCore.computeZonalStatisticsQuadSplit(raster, feature.getGeometry(), stats[0]);
                break;
              case "mask":
                ZonalStatisticsCore.computeZonalStatisticsMasking(raster, feature.getGeometry(), stats[0]);
                break;
              default:
                throw new RuntimeException(String.format("Unsupported method '%s'", method));
            }
            finalResult.accumulate(stats[0]);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
        return null;
      });
    }
    long t2 = System.nanoTime();

    // 5- Report the results back as JSON
    response.setStatus(HttpServletResponse.SC_OK);
    response.setContentType("application/json");
    try (JsonGenerator jsonGenerator = new JsonFactory().createGenerator(response.getOutputStream())) {
      jsonGenerator.writeStartObject();
      // Write the results of the zonal statistics query
      jsonGenerator.writeFieldName("results");
      jsonGenerator.writeStartObject();
      jsonGenerator.writeNumberField("min", finalResult.min[0]/50.0);
      jsonGenerator.writeNumberField("max", finalResult.max[0]/50.0);
      jsonGenerator.writeNumberField("sum", finalResult.sum[0]/50.0);
      jsonGenerator.writeNumberField("count", finalResult.count[0]);
      jsonGenerator.writeEndObject();
      // Write runtime statistics
      jsonGenerator.writeFieldName("runtime");
      jsonGenerator.writeStartObject();
      jsonGenerator.writeNumberField("total_time", (t2 - t1) * 1E-9);
      jsonGenerator.writeNumberField("num_files", allRasterFiles.size());
      jsonGenerator.writeEndObject();

      jsonGenerator.writeEndObject();
    }
    return true;
  }

  private <T extends DataLayer> T findLayer(Iterable<T> layers, String id) {
    for (T l : layers)
      if (id.equals(l.id))
        return l;
    return null;
  }

  /**
   * List all available vector layers
   *
   * @param target the target path requested by the user
   * @param request the HTTP request
   * @param response the HTTP response
   * @return {@code true if the request was handled}
   * @throws IOException if an error happens while handling the request
   */
  @WebMethod(url = "/dynamic/vectors")
  public boolean handleListVectorLayers(String target,
                                        HttpServletRequest request,
                                        HttpServletResponse response) throws IOException {
    listDataLayers(vectorLayers, response);
    return true;
  }

  /**
   * List all available raster layers
   * @param target the target path requested by the user
   * @param request the HTTP request
   * @param response the HTTP response
   * @return {@code true if the request was handled}
   * @throws IOException if an error happens while handling the request
   */
  @WebMethod(url = "/dynamic/rasters")
  public boolean handleListRasterLayers(String target,
                                        HttpServletRequest request,
                                        HttpServletResponse response) throws IOException {
    listDataLayers(rasterLayers, response);
    return true;
  }

  /**
   * List data layers and return in JSON format
   * @param layers the set of layers, either a list of vector or raster layers
   * @param response the HTTP response
   * @throws IOException if an error happens while handling the request
   */
  protected void listDataLayers(Iterable<? extends DataLayer> layers, HttpServletResponse response) throws IOException {
    response.setStatus(HttpServletResponse.SC_OK);
    response.setContentType("application/json");
    try (JsonGenerator jsonGenerator = new JsonFactory().createGenerator(response.getOutputStream())) {
      jsonGenerator.writeStartObject(); // Start root object
      jsonGenerator.writeFieldName("layers");
      jsonGenerator.writeStartArray(); // Start array of layers
      for (DataLayer layer : layers) {
        jsonGenerator.writeStartObject(); // Start of layer object
        jsonGenerator.writeStringField("id", layer.id);
        jsonGenerator.writeStringField("name", layer.name);
        jsonGenerator.writeEndObject(); // End of layer object
      }
      jsonGenerator.writeEndArray(); // End array of layers
      jsonGenerator.writeEndObject(); // End root object
    }
  }

  /**
   * Lists all features in a given vector file
   * @param target the target path requested by the user
   * @param request the HTTP request
   * @param response the HTTP response
   * @param vectorID the ID of the vector dataset
   * @return {@code true if the request was handled}
   * @throws IOException if an error happens while handling the request
   */
  @WebMethod(url = "/dynamic/vectors/{vectorID}/features")
  public boolean handleListFeaturesRequest(String target,
                                           HttpServletRequest request,
                                           HttpServletResponse response,
                                           String vectorID) throws IOException {
    // 1- Read the vector file and retrieve the desired polygon
    VectorLayer vectorLayer = findLayer(vectorLayers, vectorID);
    String vectorPath = new File(vectorBaseDir, vectorLayer.path).getPath();
    String[] nameFields = vectorLayer.nameFields;
    List<IFeature> features = SpatialReader.readInput(sc, opts, vectorPath, "shapefile").
        sortBy(f -> f.getAs(nameFields[0]), true, 1).collect();

    // 2- Write the result back in JSON format
    response.setStatus(HttpServletResponse.SC_OK);
    response.setContentType("application/json");
    try (JsonGenerator jsonGenerator = new JsonFactory().createGenerator(response.getOutputStream())) {
      jsonGenerator.writeStartObject();
      jsonGenerator.writeFieldName("features");
      jsonGenerator.writeStartArray();
      for (IFeature feature : features) {
        jsonGenerator.writeStartObject();
        jsonGenerator.writeStringField("id", feature.getAs(vectorLayer.idField).toString());
        String name = feature.getAs(nameFields[0]).toString();
        for (int i = 1; i < nameFields.length; i++)
          name += ", " + feature.getAs(nameFields[i]);
        jsonGenerator.writeStringField("name", name);
        jsonGenerator.writeEndObject();
      }
      jsonGenerator.writeEndArray();
      jsonGenerator.writeEndObject();
    }
    return true;
  }

  /**
   * Retrieve the feature in GeoJSON format
   * @param target the target path requested by the user
   * @param request the HTTP request
   * @param response the HTTP response
   * @param vectorID the ID of the vector dataset
   * @param featureID the ID of the raster dataset
   * @return {@code true if the request was handled}
   * @throws IOException if an error happens while handling the request
   * @throws InterruptedException if the processing fails
   */
  @WebMethod(url = "/dynamic/vectors/{vectorID}/features/{featureID}.geojson")
  public boolean handleRetrieveFeature(String target,
                                       HttpServletRequest request,
                                       HttpServletResponse response,
                                       String vectorID,
                                       String featureID) throws IOException, InterruptedException {
    // 1- Read the vector file and retrieve the desired polygon
    VectorLayer vectorLayer = findLayer(vectorLayers, vectorID);
    String vectorPath = new File(vectorBaseDir, vectorLayer.path).getPath();
    JavaRDD<IFeature> features = SpatialReader.readInput(sc, opts, vectorPath, "shapefile");
    final String idField = vectorLayer.idField;
    // 2- Filter by ID
    IFeature feature = features.filter(f -> f.getAs(idField).toString().equals(featureID)).first();

    // 3- Write in GeoJSON format
    response.setStatus(HttpServletResponse.SC_OK);
    response.setContentType("application/geo+json");
    GeoJSONFeatureWriter writer = new GeoJSONFeatureWriter();
    writer.initialize(response.getOutputStream(), conf);
    writer.write(null, feature);
    writer.close(null);
    return true;
  }
}
