package ${groupId}

import org.apache.spark.test.ScalaSparkTest
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import edu.ucr.cs.bdlab.beast._

@RunWith(classOf[JUnitRunner])
class BeastScalaTest extends FunSuite with ScalaSparkTest {
  test("Simple test") {
    // Make a copy of the input file from the resources directory
    val input = makeFileCopy("/input.txt")

    // Read the features
    val features = sparkContext.readCSVPoint(input.getPath, delimiter=',')

    // Add your assertions here
    assert(features.count() == 2)
  }
}