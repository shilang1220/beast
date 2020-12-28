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
package edu.ucr.cs.bdlab.beast.operations

import edu.ucr.cs.bdlab.beast.common.{BeastOptions, CLIOperation}
import edu.ucr.cs.bdlab.beast.util.OperationHelper
import edu.ucr.cs.bdlab.beast.util.OperationHelper.{ParsedCommandLineOptions, printOperationUsage}
import org.apache.spark.SparkConf
import org.apache.spark.beast.CRSServer
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

/**
  * A main class that runs all supported operations from the command line
  */
object Main extends Logging {

  def main(args: Array[String]): Unit = {
    // Get the operation to run
    if (args.length == 0) {
      OperationHelper.printUsage(System.err)
      System.exit(1)
    }

    val parsedCLO: ParsedCommandLineOptions = OperationHelper.parseCommandLineArguments(args: _*)
    if (parsedCLO == null) {
      OperationHelper.printUsage(System.err)
      System.exit(1)
    }

    // Check if the parameters are invalid
    if (!OperationHelper.checkOptions(parsedCLO, System.err)) {
      printOperationUsage(parsedCLO.operation, parsedCLO.options, System.err)
      System.exit(1)
    }

    // Create the Spark context
    val conf = new SparkConf
    conf.setAppName("Beast/" + parsedCLO.operation.metadata.shortName)

    // Set Spark master to local if not already set
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")
    logInfo(s"Using master '${conf.get("spark.master")}'")

    val opInstance: CLIOperation =
      try {
        // 1- Test the operation as a Scala operation
        // See: https://stackoverflow.com/questions/1913092/getting-object-instance-by-string-name-in-scala
        val opClass = Class.forName(parsedCLO.operation.klass.getName)
        opClass.getField("MODULE$").get(opClass).asInstanceOf[CLIOperation]
      } catch {
        // 2- Fall back to Java operation
        case _: Exception => parsedCLO.operation.klass.asSubclass(classOf[CLIOperation]).newInstance
      }
    // Initialize the spark context
    val crsServerPort = CRSServer.startServer()
    // Set the CRSServer information in both Spark Configuration and BeastOptions
    conf.set(CRSServer.CRSServerPort, crsServerPort.toString)
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sparkContext = sparkSession.sparkContext
    import edu.ucr.cs.bdlab.beast._
    val states = sparkContext.shapefile("tl_2018_us_state.zip").toDataFrame(sparkSession)
    states.createOrReplaceTempView("states")
    states.printSchema()
    sparkSession.sql("SELECT count(*) FROM states WHERE ST_IsSimple(thegeom)").show(5)
    System.exit(0)
    val t1 = System.nanoTime
    try {
      parsedCLO.options.setInt(CRSServer.CRSServerPort, crsServerPort)
      if (conf.contains("spark.driver.host"))
        parsedCLO.options.set("spark.driver.host", conf.get("spark.driver.host"))
      val opts: BeastOptions = parsedCLO.options
      opInstance.setup(opts)
      opInstance.run(opts, parsedCLO.inputs, parsedCLO.outputs, sparkContext)
      val t2 = System.nanoTime
      logInfo(f"The operation ${parsedCLO.operation.metadata.shortName} finished in ${(t2 - t1) * 1E-9}%f seconds")
    } catch {
      case _other: Exception =>
        val t2 = System.nanoTime
        logError(f"The operation ${parsedCLO.operation.metadata.shortName} failed after ${(t2 - t1) * 1E-9}%f seconds")
        throw _other
    } finally {
      sparkContext.stop
      CRSServer.stopServer(false)
    }
  }
}
