package ${groupId}

import edu.ucr.cs.bdlab.beast.cg.SpatialJoinAlgorithms.{ESJDistributedAlgorithm, ESJPredicate}
import edu.ucr.cs.bdlab.beast.geolite.{Feature, IFeature}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.locationtech.jts.geom.{Envelope, GeometryFactory}

/**
 * Scala examples for Beast
 */
object BeastScala {
  def main(args: Array[String]): Unit = {
    // Initialize Spark context

    val conf = new SparkConf().setAppName("Beast Example")
    // Set Spark master to local if not already set
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")

    val spark: SparkSession.Builder = SparkSession.builder().config(conf)

    val sparkSession = spark.getOrCreate()
    val sparkContext = sparkSession.sparkContext

    try {
      // Import Beast features
      import edu.ucr.cs.bdlab.beast._

      // Load spatial datasets
      // Load a shapefile.
      // Download from: ftp://ftp2.census.gov/geo/tiger/TIGER2018/STATE/
      val polygons = sparkContext.shapefile("tl_2018_us_state.zip")
      // Load points in GeoJSON format.
      // Download from https://star.cs.ucr.edu/dynamic/download.cgi/Tweets/index.geojson?mbr=-117.8538,33.2563,-116.8142,34.4099&point
      val points = sparkContext.geojsonFile("Tweets_index.geojson")

      // Run a range query
      val geometryFactory: GeometryFactory = new GeometryFactory()
      val range = geometryFactory.toGeometry(new Envelope(-117.337182, -117.241395, 33.622048, 33.72865))
      val matchedPolygons: RDD[IFeature] = polygons.rangeQuery(range)
      val matchedPoints: RDD[IFeature] = points.rangeQuery(range)

      // Run a spatial join operation between points and polygons (point-in-polygon) query
      val sjResults: RDD[(IFeature, IFeature)] =
        matchedPolygons.spatialJoin(matchedPoints, ESJPredicate.Contains, ESJDistributedAlgorithm.PBSM)

      // Keep point coordinate, text, and state name
      val finalResults: RDD[IFeature] = sjResults.map(pip => {
        val polygon: IFeature = pip._1
        val point: IFeature = pip._2
        val values = point.toSeq :+ polygon.getAs[String]("NAME")
        val schema = StructType(point.schema :+ StructField("state", StringType))
        new Feature(values.toArray, schema)
      })

      // Write the output in CSV format
      finalResults.saveAsCSVPoints(filename = "output", xColumn = 0, yColumn = 1, delimiter = ';')
    } finally {
      sparkSession.stop()
    }
  }
}