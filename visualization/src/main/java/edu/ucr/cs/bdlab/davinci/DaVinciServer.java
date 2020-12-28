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
package edu.ucr.cs.bdlab.davinci;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import edu.ucr.cs.bdlab.beast.common.BeastOptions;
import edu.ucr.cs.bdlab.beast.common.WebMethod;
import edu.ucr.cs.bdlab.beast.geolite.EnvelopeND;
import edu.ucr.cs.bdlab.beast.geolite.EnvelopeNDLite;
import edu.ucr.cs.bdlab.beast.geolite.IFeature;
import edu.ucr.cs.bdlab.beast.io.CSVFeatureWriter;
import edu.ucr.cs.bdlab.beast.io.FeatureReader;
import edu.ucr.cs.bdlab.beast.io.FeatureWriter;
import edu.ucr.cs.bdlab.beast.io.GeoJSONFeatureWriter;
import edu.ucr.cs.bdlab.beast.io.KMLFeatureWriter;
import edu.ucr.cs.bdlab.beast.io.KMZFeatureWriter;
import edu.ucr.cs.bdlab.beast.io.SpatialFileRDD;
import edu.ucr.cs.bdlab.beast.io.SpatialOutputFormat;
import edu.ucr.cs.bdlab.beast.io.SpatialReader;
import edu.ucr.cs.bdlab.beast.io.shapefile.CompressedShapefileWriter;
import edu.ucr.cs.bdlab.beast.util.CounterOutputStream;
import edu.ucr.cs.bdlab.beast.util.FileUtil;
import edu.ucr.cs.bdlab.beast.util.OperationParam;
import edu.ucr.cs.bdlab.beast.util.WebHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import javax.imageio.ImageIO;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;

/**
 * A web handler that handles visualization methods
 */
public class DaVinciServer extends WebHandler {
  private static final Log LOG = LogFactory.getLog(DaVinciServer.class);

  @OperationParam(
      description = "Enable/Disable server-side caching of generated tiles",
      defaultValue = "true"
  )
  public static final String ServerCache = "cache";

  @OperationParam(
      description = "Path to the directory that contains the plots and data",
      defaultValue = "."
  )
  public static final String DataDirectory = "datadir";

  // The next two configurations allow running the server on data that reside in AWS
  @OperationParam(description = "AWS Access Key ID")
  public static final String AWS_ACCESS_KEY_ID = "fs.s3a.access.key";

  @OperationParam(description = "AWS Access Key ID")
  public static final String AWS_SECRET_ACCESS_KEY = "fs.s3a.secret.key";

  /**The file system that contains the data dir*/
  protected FileSystem dataFileSystem;

  /**The Spark context of the underlying application*/
  protected JavaSparkContext sc;

  static final byte[] EmptyImage;

  private static final Map<Class<? extends FeatureWriter>, String> MimeTypes = new HashMap<>();

  static {
    MimeTypes.put(CSVFeatureWriter.class, "text/csv");
    MimeTypes.put(GeoJSONFeatureWriter.class, "application/geo+json");
    MimeTypes.put(KMLFeatureWriter.class, "application/vnd.google-earth.kml+xml");
    MimeTypes.put(KMZFeatureWriter.class, "application/vnd.google-earth.kmz");
    MimeTypes.put(CompressedShapefileWriter.class, "application/zip");
  }

  static {
    try {
      BufferedImage img = new BufferedImage(1, 1, BufferedImage.TYPE_INT_ARGB);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ImageIO.write(img, "png", baos);
      EmptyImage = baos.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException("Error creating empty image", e);
    }
  }

  /**
   * A server-side cache that caches copies of generated tiles to avoid regenerating the same tile for multiple users.
   */
  private Cache<String,byte[]> cache;

  /**Path to the data directory*/
  protected Path datadir;

  @Override
  public void setup(SparkContext sc, BeastOptions opts) {
    super.setup(sc, opts);
    this.sc = new JavaSparkContext(sc);
    if (opts.getBoolean(ServerCache, true)) {
      cache = CacheBuilder.newBuilder()
          .maximumSize(100000) // the cache size is 100,000 tiles
          .expireAfterAccess(30, TimeUnit.DAYS)
          .build();
    }
    this.datadir = new Path(opts.getString(DataDirectory, "."));
    try {
      this.dataFileSystem = this.datadir.getFileSystem(opts.loadIntoHadoopConf(sc.hadoopConfiguration()));
    } catch (IOException e) {
      throw new RuntimeException(
              String.format("Cannot retrieve the file system of the data directory '%s'", this.datadir), e);
    }
  }

  /**
   * Returns metadata of the given dataset to create the layer
   * @param target the target path requested by the user
   * @param request the HTTP request
   * @param response the HTTP response
   * @param datasetID the path to the visualization index
   * @throws IOException if an error happens while handling the request
   * @return {@code true if the request was handled}
   */
  @WebMethod(url = "/dynamic/metadata.cgi/{datasetID}")
  public boolean handleMetadataRequest(String target,
                                       HttpServletRequest request,
                                       HttpServletResponse response,
                                       String datasetID) throws IOException {
    Path propertiesFile = new Path(new Path(datadir, datasetID), "_visualization.properties");
    BeastOptions opts = new BeastOptions().loadFromTextFile(dataFileSystem, propertiesFile);
    // Write the response as JSON
    response.setStatus(HttpServletResponse.SC_OK);
    response.setContentType("application/json");
    JsonGenerator jsonGenerator = new JsonFactory().createGenerator(response.getOutputStream());
    jsonGenerator.writeStartObject();
    jsonGenerator.writeNumberField("width", opts.getInt(MultilevelPlot.TileWidth(), 256));
    jsonGenerator.writeNumberField("height", opts.getInt(MultilevelPlot.TileHeight(), 256));
    jsonGenerator.writeBooleanField("mercator", opts.getBoolean(CommonVisualizationHelper.UseMercatorProjection, false));
    String levels = opts.getString(MultilevelPlot.NumLevels(), "7");
    String[] parts = levels.split("\\.\\.");
    int minLevel, maxLevel;
    if (parts.length == 2) {
      minLevel = Integer.parseInt(parts[0]);
      maxLevel = Integer.parseInt(parts[1]);
    } else {
      minLevel = 0;
      maxLevel = Integer.parseInt(parts[0]) - 1;
    }
    jsonGenerator.writeFieldName("levels");
    jsonGenerator.writeStartArray();
    jsonGenerator.writeNumber(minLevel);
    jsonGenerator.writeNumber(maxLevel);
    jsonGenerator.writeEndArray();
    jsonGenerator.writeEndObject();
    jsonGenerator.close();
    return true;
  }

  @WebMethod(url = "/dynamic/visualize.cgi/{datasetID}")
  public boolean handleVisualizeIndex(String target,
                                      HttpServletRequest request,
                                      HttpServletResponse response,
                                      String datasetID) throws IOException {
    return handleStaticResource(new Path(new Path(datadir, datasetID), "index.html").toString(), request, response);
  }

  /**
   * Returns an image tile that is either materialized on disk, cached in memory, or generated on the fly.
   * @param target the target path requested by the user
   * @param request the HTTP request
   * @param response the HTTP response
   * @param datasetID the ID of the dataset
   * @param z the zoom level of the tile
   * @param x the column of the tile in that level
   * @param y the row of the tile at that level
   * @return {@code true if the request was handled}
   * @throws IOException if an error happens while handling the request
   */
  @WebMethod(url = "/dynamic/visualize.cgi/{datasetID}/tile-{z}-{x}-{y}\\.(.+)")
  public boolean getTile(String target,
                         HttpServletRequest request,
                         HttpServletResponse response,
                         String datasetID,
                         int z, int x, int y) throws IOException {
    long startTime = System.nanoTime();
    byte[] tileData = null;
    String howHandled;
    String tilePath = target.substring(23); // Remove prefix "/dynamic/visualize.cgi/" (23 bytes)
    Path resourcePath = new Path(datadir, tilePath);
    if (this.dataFileSystem.isFile(resourcePath)) {
      howHandled = "static-file";
      handleStaticResource(resourcePath.toString(), request, response);
    } else {
      if (cache != null && (tileData = cache.getIfPresent(tilePath)) != null) {
        // A cached version is found on the server cache. Use it
        howHandled = "server-cached";
      } else {
        // Either no cache is configured or the tile does not exist in the cache
        // Generate the tile or inform the client to use their cached version (if applicable)
        long tileID = TileIndex.encode(z, x, y);

        // Retrieve path to the AID visualization index
        Path pathToAIDIndex = new Path(datadir, datasetID);
        if (!pathToAIDIndex.getFileSystem(conf).exists(pathToAIDIndex)) {
          // Return an empty tile
          tileData = EmptyImage;
          howHandled = "non-existent";
        } else {
          // Create the output stream where the image will be written
          // We need to create the intermediate output stream for two reasons:
          // 1- To be able to set the response headers based on whether the cached version was newer or not.
          // 2- To be able to add it to the cache if a cache is configured
          ByteArrayOutputStream interimOutput = new ByteArrayOutputStream();
          // Extract the timestamp of a possible cached version on the browser to avoid recreating the image when possible
          LongWritable requesterCachedTimestamp = new LongWritable();
          if (request.getHeader("If-Modified-Since") != null)
            requesterCachedTimestamp.set(request.getDateHeader("If-Modified-Since"));
          else
            requesterCachedTimestamp.set(0); // Oldest timestamp ensures sending a new version
          // Call the function that plots the tile from the raw data
          boolean tileGenerated = MultilevelPyramidPlotHelper.plotTile(dataFileSystem, pathToAIDIndex, tileID,
                  interimOutput, requesterCachedTimestamp);
          interimOutput.close();
          if (!tileGenerated) {
            // Tile was skipped as it was not modified since the browser cached it
            howHandled = "skipped-client-cached";
            response.setStatus(HttpServletResponse.SC_NOT_MODIFIED);
          } else {
            howHandled = "generated on-the-fly";
            tileData = interimOutput.toByteArray();//change interimOutput to byte type which can be used as the value in cache
            // First time returning this tile or a newer version is returned, add to the cache
            cache.put(tilePath, tileData);
          }
        }
      }

      if (tileData != null) {
        // Either a server-side cached version is returned or a tile was generated on the fly
        // TODO: Change the content type depending on the image format (e.g., SVG)
        response.setContentType("image/png");
        response.setStatus(HttpServletResponse.SC_OK);
        OutputStream finalOutput = response.getOutputStream();
        finalOutput.write(tileData);
        finalOutput.close();
      }
    }
    long endTime = System.nanoTime();
    LOG.info(String.format("Requested tile '%s' processed in %f seconds (%s)",
        tilePath, (endTime - startTime) * 1E-9, howHandled));
    return true;
  }


  /**
   * Returns the contents of the master file of an index in GeoJSON format
   * @param target the target path requested by the user
   * @param request the HTTP request
   * @param response the HTTP response
   * @param datasetID path to the AID index
   * @return {@code true if the request was handled}
   * @throws IOException if an error happens while handling the request
   * @throws InterruptedException if the processing fails
   */
  @WebMethod(url = "/dynamic/index.cgi/{datasetID}")
  public boolean handleIndex(String target, HttpServletRequest request, HttpServletResponse response,
                           String datasetID) throws IOException, InterruptedException {
    // Retrieve the URL of the data
    Path indexPath = new Path(datadir, datasetID);
    Path masterFilePath = SpatialFileRDD.getMasterFilePath(dataFileSystem, indexPath);

    if (masterFilePath != null) {
      long masterFileModificationDate = dataFileSystem.getFileStatus(masterFilePath).getModificationTime();
      // TODO skip the response if the client-cached version is up-to-date

      // Set the response header
      response.setStatus(HttpServletResponse.SC_OK);
      response.setContentType("application/vnd.geo+json");
      response.addDateHeader("Last-Modified", masterFileModificationDate);
      response.addDateHeader("Expires", masterFileModificationDate + WebHandler.OneDay);

      // Limit the returned partition boundaries to the web mercator boundaries to avoid bad visualization
      final EnvelopeNDLite worldMercatorMBR = new EnvelopeNDLite(CommonVisualizationHelper.MercatorMapBoundariesEnvelope);
      JavaRDD<IFeature> partitions = SpatialReader.readInput(this.sc, new BeastOptions(),
          masterFilePath.toString(), "envelope(xmin)");

      GeoJSONFeatureWriter writer = new GeoJSONFeatureWriter();
      try {
        for (IFeature f : partitions.collect()) {
          ((EnvelopeND)f.getGeometry()).shrink(worldMercatorMBR);
          writer.write(null, f);
        }
      } catch (IOException e) {
        reportError(response, "Error writing the output", e);
      } finally {
        if (writer != null)
          writer.close(null);
      }
    }
    return true;
  }


  /**
   * Download a subset of the dataset given by its visualization ID.
   * @param target the target path requested by the user
   * @param request the HTTP request
   * @param response the HTTP response
   * @param datasetPath the ID of the dataset to download
   * @return {@code true if the request was handled}
   * @throws IOException if an error happens while handling the request
   * @throws InterruptedException if the processing fails
   * @throws IllegalAccessException if the constructor the FeatureWriter is inaccisble
   * @throws InstantiationException if the creation of the FeatureWriter fails
   */
  @WebMethod(url = "/dynamic/download.cgi/{datasetPath}", async = true)
  public boolean handleDownload(String target, HttpServletRequest request, HttpServletResponse response,
                             String datasetPath)
      throws IOException, InterruptedException, IllegalAccessException, InstantiationException {
    // The final answer is a GZIP compressed file
    boolean returnGZ = false;
    // The data is compressed (in any form) which is used to avoid extra compression of the transmitted data
    boolean dataIsCompressed = false;
    // Retrieve the URL of the data
    String extension = FileUtil.getExtension(datasetPath);
    String datasetID = datasetPath.substring(0, datasetPath.length() - extension.length());
    if (extension.equals(".gz")) {
      returnGZ = true;
      dataIsCompressed = true;
      extension = FileUtil.getExtension(datasetID);
      datasetID = datasetID.substring(0, datasetID.length() - extension.length());
    }
    // Search for the writer that supports the given format
    Class<? extends FeatureWriter> matchingWriter = null;
    Iterator<Class<? extends FeatureWriter>> writerIter = FeatureWriter.featureWriters.values().iterator();
    while (matchingWriter == null && writerIter.hasNext()) {
      Class<? extends FeatureWriter> writerClass = writerIter.next();
      FeatureWriter.Metadata metadata = writerClass.getAnnotation(FeatureWriter.Metadata.class);
      if (metadata.extension().equalsIgnoreCase(extension)) {
        matchingWriter = writerClass;
      }
    }
    if (matchingWriter == null) {
      reportError(response, String.format("Unrecognized extension '%s'", extension));
      return false;
    }
    // Mark the data as compressed to avoid the overhead of double compression
    if (matchingWriter == KMZFeatureWriter.class || matchingWriter == CompressedShapefileWriter.class)
      dataIsCompressed = true;

    // Now, find the corresponding index and search for the matching records
    Path propertiesFile = new Path(new Path(datadir, datasetID), "_visualization.properties");
    BeastOptions opts;
    Path dataPath;
    long masterFileModificationTime = 0;

    if (dataFileSystem.exists(propertiesFile)) {
      LOG.warn("Deprecated access of data download through a URL for the visualization");
      opts = new BeastOptions().loadFromTextFile(dataFileSystem, propertiesFile);
      dataPath = new Path(new Path(datadir, datasetID), opts.getString("data", ""));
      Path masterFile = SpatialFileRDD.getMasterFilePath(dataFileSystem, dataPath);
      if (masterFile != null)
        masterFileModificationTime = dataFileSystem.getFileStatus(masterFile).getModificationTime();
    } else {
      // Check if the path is for an index
      dataPath = new Path(datadir, datasetID);
      opts = new BeastOptions(conf);
      Path masterFile = SpatialFileRDD.getMasterFilePath(dataFileSystem, dataPath);
      if (masterFile == null) {
        reportMissingFile(target, response);
        return true;
      }
      masterFileModificationTime = dataFileSystem.getFileStatus(masterFile).getModificationTime();
    }

    String mbrString = request.getParameter("mbr");
    if (mbrString == null && masterFileModificationTime != 0) {
      // A client version can only be used if there is no filter
      long clientCachedTimestamp = request.getHeader("If-Modified-Since") != null ?
          request.getDateHeader("If-Modified-Since") : 0;
      if (clientCachedTimestamp >= masterFileModificationTime) {
        // The client already has a version. Skip.
        response.setStatus(HttpServletResponse.SC_NOT_MODIFIED);
        LOG.info(String.format("Not returning data GeoJSON layer for '%s' since the client has an up-to-date version", target));
        return true;
      }
    }

    // Initialize the writer and set it to write to the servlet
    String mimetype = MimeTypes.get(matchingWriter);
    if (mimetype == null)
      LOG.warn("Unknown Mime type for "+matchingWriter.getSimpleName());
    // Special handling for CSV files to write the output as X,Y if the 'point' parameter is specified
    if (matchingWriter == CSVFeatureWriter.class) {
      if (request.getParameter("point") != null) {
        // Write point data as x,y
        opts.set(SpatialOutputFormat.OutputFormat, "point");
      } else {
        // Write any geometry attribute as WKT-encoded text
        opts.set(SpatialOutputFormat.OutputFormat, "wkt");
      }
      opts.set(CSVFeatureWriter.FieldSeparator, "\t");
      opts.setBoolean(CSVFeatureWriter.WriteHeader, true);
    }
    // Set the part size in ShapefileWriter to 100 MB to make the download start asap.
    opts.setLong(CompressedShapefileWriter.PartSize, 100 * 1024 * 1024);
    // Prepare the writing
    FeatureWriter featureWriter = matchingWriter.newInstance();
    response.setStatus(HttpServletResponse.SC_OK);
    if (mimetype != null)
      response.setContentType(mimetype);
    CounterOutputStream counterOut = new CounterOutputStream(response.getOutputStream());
    OutputStream out = returnGZ? new GZIPOutputStream(counterOut) : counterOut;
    // Check if the intermediate data can be compressed to reduce the transmission bandwidth
    if (!dataIsCompressed && isGZIPAcceptable(request)) {
      response.setHeader("Content-Encoding", "gzip");
      out = new GZIPOutputStream(counterOut);
    }
    if (masterFileModificationTime != 0 && mbrString == null) {
      // If the entire file is downloaded, add a header to make the file available for a day
      // This is helpful when the file is loaded for visualization as a GeoJSON file
      response.addDateHeader("Last-Modified", masterFileModificationTime);
      response.addDateHeader("Expires", masterFileModificationTime + WebHandler.OneDay);
    }
    // Initialize the feature writer
    featureWriter.initialize(out, opts.loadIntoHadoopConf(this.conf));
    String downloadFileName = datasetID.replaceAll("[/\\s]", "_");
    downloadFileName += extension;
    if (returnGZ)
      downloadFileName += ".gz";
    response.setHeader("Content-Disposition", String.format("attachment; filename=\"%s\"", downloadFileName));

    // Now, read all input splits one-by-one and write their contents to the writer
    EnvelopeNDLite[] mbrs = null;
    if (mbrString != null) {
      EnvelopeNDLite mbr = EnvelopeNDLite.decodeString(mbrString, new EnvelopeNDLite());
      if (mbr.getSideLength(0) > 360.0) {
        mbr.setMinCoord(0, -180.0);
        mbr.setMaxCoord(0, +180.0);
      } else {
        // Adjust the minimum and maximum longitude to be in the range [-180.0, +180.0]
        mbr.setMinCoord(0, mbr.getMinCoord(0) - 360.0 * Math.floor((mbr.getMinCoord(0) + 180.0) / 360.0));
        mbr.setMaxCoord(0, mbr.getMaxCoord(0) - 360.0 * Math.floor((mbr.getMaxCoord(0) + 180.0) / 360.0));
      }
      if (mbr.getMinCoord(0) > mbr.getMaxCoord(0)) {
        // The MBR crosses the international day line, split it into two MBRs
        mbrs = new EnvelopeNDLite[2];
        mbrs[0] = new EnvelopeNDLite(2, mbr.getMinCoord(0), mbr.getMinCoord(1), +180.0, mbr.getMaxCoord(1));
        mbrs[1] = new EnvelopeNDLite(2, -180.0, mbr.getMinCoord(1), mbr.getMaxCoord(0), mbr.getMaxCoord(1));
      } else {
        // A simple MBR that does not cross the line.
        mbrs = new EnvelopeNDLite[]{mbr};
      }
    }

    int iMBR = 0;
    do {
      BeastOptions readOpts = new BeastOptions(opts.loadIntoHadoopConf(this.conf));
      if (mbrs != null)
        readOpts.set(SpatialFileRDD.FilterMBR(), mbrs[iMBR].encodeAsString());

      Class<? extends FeatureReader> featureReaderClass = SpatialFileRDD.getFeatureReaderClass(dataPath.toString(), readOpts);
      SpatialFileRDD.SpatialFilePartition[] partitions = SpatialFileRDD.createPartitions(dataPath.toString(),
          readOpts, this.conf);
      for (SpatialFileRDD.SpatialFilePartition partition : partitions) {
        Iterator<IFeature> features = SpatialFileRDD.readPartitionJ(partition, featureReaderClass, readOpts);
        while (features.hasNext())
          featureWriter.write(null, features.next());
      }
    } while (mbrs != null && ++iMBR < mbrs.length);
    featureWriter.close(null);
    long numWrittenBytes = counterOut.getCount();
    LOG.info(String.format("Request '%s' resulted in %d bytes", target, numWrittenBytes));
    return true;
  }

}
