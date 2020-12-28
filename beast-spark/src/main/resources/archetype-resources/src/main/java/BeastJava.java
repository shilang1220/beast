package ${groupId};

import edu.ucr.cs.bdlab.beast.JavaSpatialRDDHelper;
import edu.ucr.cs.bdlab.beast.cg.SpatialJoinAlgorithms;
import edu.ucr.cs.bdlab.beast.common.BeastOptions;
import edu.ucr.cs.bdlab.beast.geolite.Feature;
import edu.ucr.cs.bdlab.beast.geolite.IFeature;
import edu.ucr.cs.bdlab.beast.io.SpatialReader;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

public class BeastJava {
  public static void main(String[] args) {
    // Initialize Spark
    SparkConf conf = new SparkConf().setAppName("Beast Example");

    // Set Spark master to local if not already set
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]");

    // Create Spark session (for Dataframe API) and Spark context (for RDD API)
    SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
    JavaSparkContext sparkContext = new JavaSparkContext(sparkSession.sparkContext());

    try {
      // Load spatial datasets
      // Load a shapefile. Download from: ftp://ftp2.census.gov/geo/tiger/TIGER2018/STATE/
      JavaRDD<IFeature> polygons = SpatialReader.readInput(sparkContext, new BeastOptions(), "tl_2018_us_state.zip", "shapefile");

      // Load points in GeoJSON format.
      // Download from https://star.cs.ucr.edu/dynamic/download.cgi/Tweets/data_index.geojson.gz?mbr=-117.8538,33.2563,-116.8142,34.4099
      JavaRDD<IFeature> points = SpatialReader.readInput(sparkContext, new BeastOptions(), "Tweets_index.geojson", "geojson");

      // Run a range query
      GeometryFactory geometryFactory = new GeometryFactory();
      Geometry range = geometryFactory.toGeometry(new Envelope(-117.337182, -117.241395, 33.622048, 33.72865));
      JavaRDD<IFeature> matchedPolygons = JavaSpatialRDDHelper.rangeQuery(polygons, range);
      JavaRDD<IFeature> matchedPoints = JavaSpatialRDDHelper.rangeQuery(points, range);

      // Run a spatial join operation between points and polygons (point-in-polygon) query
      JavaPairRDD<IFeature, IFeature> sjResults = JavaSpatialRDDHelper.spatialJoin(matchedPolygons, matchedPoints,
          SpatialJoinAlgorithms.ESJPredicate.Contains, SpatialJoinAlgorithms.ESJDistributedAlgorithm.PBSM);

      // Keep point coordinate, text, and state name
      JavaRDD<IFeature> finalResults = sjResults.map(pip -> {
        IFeature polygon = pip._1;
        IFeature point = pip._2;
        Object[] values = new Object[point.length() + 1];
        point.toSeq().copyToArray(values);
        values[values.length - 1] = (String) polygon.getAs("NAME");
        StructField[] schema = new StructField[values.length];
        point.schema().copyToArray(schema);
        schema[schema.length - 1] = new StructField("state", StringType$.MODULE$, true, null);
        return new Feature(values, new StructType(schema));
      });

      // Write the output in CSV format
      JavaSpatialRDDHelper.saveAsCSVPoints(finalResults, "output", 0, 1, ';', true);
    } finally {
      // Clean up Spark session
      sparkSession.stop();
    }
  }
}