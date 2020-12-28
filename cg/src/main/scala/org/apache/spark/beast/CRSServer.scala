/*
 * Copyright 2020 University of California, Riverside
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
package org.apache.spark.beast

import java.io.IOException
import java.net.{BindException, URL}
import java.util.regex.Pattern

import edu.ucr.cs.bdlab.beast.util.OperationParam
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}
import org.apache.hadoop.conf.Configuration
import org.apache.http.HttpEntity
import org.apache.http.client.HttpClient
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.spark.internal.Logging
import org.apache.spark.{SparkConf, SparkContext}
import org.geotools.referencing.CRS
import org.mortbay.jetty.{Request, Server}
import org.mortbay.jetty.handler.AbstractHandler
import org.mortbay.thread.{QueuedThreadPool, ThreadPool}
import org.opengis.referencing.crs.CoordinateReferenceSystem

/**
 * A server that handles non-standard coordinate reference systems (CRS) between executor
 * nodes and the driver. It has three main methods.
 * - `setPort(port: Int)` - Sets the port where the server is listening (or should listen)
 * - `startServer` - Starts the server. Should run on the driver node before the executors start.
 * - `stopServer` - Stops the server
 * - `crsToSRID(crs: CoordinateReferenceSystem): Int` - puts the given CRS into the cache
 *     and returns a unique ID for it.
 * - `sridToCRS(srid: Int): CoordinateReferenceSystem` - Returns the CRS with the given SRID if it exists.
 */
object CRSServer extends AbstractHandler with Logging {

  @OperationParam(description = "The port on which the CRSServer is listening", defaultValue = "21345")
  val CRSServerPort = "crsserver.port"

  val DefaultPort: Int = 21345

  def getPort(conf: SparkConf) = conf.getInt(CRSServerPort, DefaultPort)
  def getServerAddress(conf: SparkConf) = conf.get("spark.driver.host", "127.0.0.1")

  /**The running Jetty server instance*/
  var server: Server = _

  /**
   * A cache (at both the client and server) to avoid unnecessary calls.
   * A negative SRID maps to the index `-srid-1` in this array.
   * Note that we use java.util.Vector for thread safety.
   */
  val crsCache: java.util.Vector[CoordinateReferenceSystem] = new java.util.Vector[CoordinateReferenceSystem]()

  /**A pattern for HTTP requests*/
  val pattern: Pattern = Pattern.compile("(?i)/?crs(/?(-?\\d+))?/?")

  /**
   * Starts the server and returns the port on which it is listening
   * @return the port on which the server is started
   */
  def startServer(defaultPort: Int = DefaultPort): Int = {
    val numThreads: Int = Runtime.getRuntime().availableProcessors() * 2
    val threadPool: ThreadPool = new QueuedThreadPool(numThreads)
    var started: Boolean = false
    var attempt: Int = 0
    do {
      try {
        server = new Server(defaultPort + attempt)
        server.setThreadPool(threadPool)
        server.setHandler(this)
        server.start()
        // Wait until the server is running
        while (!server.isRunning)
          Thread.sleep(1000)
        // If no exception was thrown, then store this port and use it
        val port: Int = defaultPort + attempt
        log.info(s"Successfully started CRSServer on port $port")
        started = true
        return port
      } catch {
        case e: BindException => {
          logWarning(s"Could not start CRSServer on port ${defaultPort + attempt}")
          attempt += 1
        }
      }
    } while (!started && attempt < 10)
    logWarning("Could not start CRSServer on all attempted ports. Failing ...")
    -1
  }

  def stopServer(wait: Boolean = false): Unit = {
    server.stop()
    if (wait)
      server.join()
  }

  override def handle(path: String, request: HttpServletRequest, response: HttpServletResponse, i: Int): Unit = {
    val matcher = pattern.matcher(path)
    val method: String = request.getMethod.toLowerCase
    if (matcher.matches()) {
      val sridStr = matcher.group(2)
      if (sridStr == null) {
        // No ID in the URL
        if (method == "get") {
          // TODO list all stored CRS
          throw new RuntimeException("Not supported")
        } else if (method == "post") {
          handleInsert(request, response)
        }
      } else {
        // There is an ID
        val srid = sridStr.toInt
        if (method == "get") {
          // Retrieve that CRS
          handleRetrieve(srid, request, response)
        }
      }
    }
  }

  /**
   * Inserts a CRS given in the HTTP request into the cache. If the CRS is already in the cache, its key (SRID) is
   * returned in the response. If it does not exist, it is inserted in the cache with a new ID and the ID is returned.
   * @param request
   * @param response
   */
  protected def handleInsert(request: HttpServletRequest, response: HttpServletResponse): Unit = {
    this.synchronized {
      logInfo("Inserting a CRS into the CRS store")
      val length: Int = request.getContentLength
      val wktArray = new Array[Byte](length)
      val inputStream = request.getInputStream
      var offset = 0
      while (offset < length)
        offset += inputStream.read(wktArray, offset, length - offset)
      inputStream.close()
      val wkt = new String(wktArray)
      val crs = CRS.parseWKT(wkt)
      val index = crsCache.indexOf(crs)
      val srid = if (index != -1) {
        // Found in the list
        -index - 1
      } else {
        crsCache.synchronized {
          crsCache.add(crs)
          -crsCache.size
        }
      }
      logInfo(s"A new SRID was assigned at the server ${srid}")
      // Return the SRID in the response
      request.asInstanceOf[Request].setHandled(true)
      response.setStatus(HttpServletResponse.SC_OK)
      response.setContentType("text/plain")
      val writer = response.getWriter
      writer.print(srid)
      writer.close()
    }
  }

  protected def handleRetrieve(srid: Int, request: HttpServletRequest, response: HttpServletResponse): Unit = {
    val index = -srid - 1
    logInfo(s"Retrieving a CRS with SRID $srid at index $index")
    if (index < 0 || index >= crsCache.size) {
      // Not found
      request.asInstanceOf[Request].setHandled(true)
      response.setStatus(HttpServletResponse.SC_NOT_FOUND)
    } else {
      val crs: CoordinateReferenceSystem = crsCache.elementAt(index)
      request.asInstanceOf[Request].setHandled(true)
      response.setStatus(HttpServletResponse.SC_OK)
      val writer = response.getWriter
      writer.print(crs.toWKT)
      writer.close()
    }
  }

  /**
   * Get an integer SRID that corresponds to the given CRS according to the following logic.
   * - Lookup the EPSG database to find an SRID code and return it
   * - If no EPSG code is found, search the local cache. If found, return srid=-index - 1, where index is the index
   * of the CRS in the cache.
   * - If not found in the local cache, send to the server to request an SRID
   * @param crs the CRS to find an SRID for
   * @param sparkConf the Spark configuration to retrieve the server address and port from
   * @return
   */
  def crsToSRID(crs: CoordinateReferenceSystem, sparkConf: SparkConf) : Int = {
    try {
      // Try to look up an EPSG code to avoid an unnecessary network call
      val epsgCode: Integer = CRS.lookupEpsgCode(crs, false)
      if (epsgCode != null)
        return epsgCode
      var listIndex: Int = crsCache.indexOf(crs)
      if (listIndex >= 0)
        return -listIndex - 1
      logInfo(s"Assigning a new SRID to CRS '${crs.toString}'")
      val url: String = s"http://${getServerAddress(sparkConf)}:${getPort(sparkConf)}/crs"
      logInfo(s"Calling '$url'")
      val httpClient: HttpClient = new DefaultHttpClient()
      val httpPost: HttpPost = new HttpPost(url);
      httpPost.setEntity(new StringEntity(crs.toWKT))
      val response = httpClient.execute(httpPost)
      val responseEntity: HttpEntity = response.getEntity

      if (responseEntity != null && responseEntity.getContentLength > 0) {
        val buffer = new Array[Byte](responseEntity.getContentLength.toInt)
        val inputStream = responseEntity.getContent
        var offset = 0
        while (offset < buffer.length) {
          offset += inputStream.read(buffer, offset, buffer.length - offset)
        }
        inputStream.close()
        val srid = new String(buffer).toInt
        logInfo(s"A new SRID was assigned at the client $srid")
        listIndex = -srid - 1
        crsCache.synchronized {
          while (listIndex >= crsCache.size)
            crsCache.add(null)
          crsCache.set(listIndex, crs)
        }
        srid
      } else {
        logError(s"Did not receive a proper response for $url for CRS '$crs''")
        0
      }
    } catch {
      case e: IOException => {
        logError(s"Did not received a response for CRS '$crs'. Returning 0 to signal error")
        0
      }
    }
  }

  /**
   * Convert the given SRID to CRS according to the following logic.
   * - If the SRID is zero, it indicates an invalid SRID and `null` is returned.
   * - If the SRID is positive, it tries to parse it as an EPSG code.
   * - If it is negative, it searches the local cache for this SRID.
   * - If not found in local cache, it calls the server to get the corresponding CRS.
   * @param srid the SRID that needs to be converted to a CRS
   * @param sparkConf the Spark configuration to retrieve the server address and port
   * @return the converted CRS.
   */
  def sridToCRS(srid: Int, sparkConf: SparkConf): CoordinateReferenceSystem = {
    try {
      logInfo(s"Finding CRS for SRID:$srid")
      // SRID = 0 marks an invalid or non-existent CRS
      if (srid == 0)
        return null
      // A positive SRID is treated as an EPSG code
      if (srid > 0)
        return CRS.decode("EPSG:"+srid, true)
      // A negative SRID is first looked up in the cache
      val listIndex: Int = -srid - 1
      if (listIndex < crsCache.size && crsCache.elementAt(listIndex) != null)
        return crsCache.elementAt(listIndex)
      // Request from the server
      val path: String = s"http://${getServerAddress(sparkConf)}:${getPort(sparkConf)}/crs/$srid"
      val url = new URL(path)
      val inputStream = url.openStream()
      var length = 0
      var wktArray = new Array[Byte](4096)
      var bytesRead: Int = 0
      while (length < wktArray.length) {
        bytesRead = inputStream.read(wktArray, length, wktArray.length - length)
        if (bytesRead <= 0) {
          // Done reading. End the loop by setting the array length to the current length
          wktArray = wktArray.slice(0, length)
        } else {
          length += bytesRead
          if (length == wktArray.length) {
            // Expand the array to read more data
            val newArray = new Array[Byte](wktArray.length * 2)
            wktArray.copyToArray(newArray)
            wktArray = newArray
          }
        }
      }
      inputStream.close()
      val wkt = new String(wktArray)
      val crs = CRS.parseWKT(wkt)
      crsCache.synchronized {
        while (listIndex >= crsCache.size)
          crsCache.add(null)
        crsCache.set(listIndex, crs)
      }
      crs
    } catch {
      case e: Exception => null
    }
  }

  /**
   * Implicit conversion from Hadoop Configuration to Spark Configuration to allow the methods above to accept
   * Hadoop Configuration
   * @param hadoopConf
   * @return
   */
  implicit def sparkConfFromHadoopConf(hadoopConf: Configuration): SparkConf = {
    val sparkConf = new SparkConf()
    val hadoopConfIterator = hadoopConf.iterator()
    while (hadoopConfIterator.hasNext) {
      val hadoopConfEntry = hadoopConfIterator.next()
      sparkConf.set(hadoopConfEntry.getKey, hadoopConfEntry.getValue)
    }
    sparkConf
  }
}
