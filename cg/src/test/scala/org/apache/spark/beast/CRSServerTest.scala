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

import org.apache.spark.test.ScalaSparkTest
import org.geotools.referencing.CRS
import org.geotools.referencing.crs.{DefaultGeographicCRS, DefaultProjectedCRS}
import org.geotools.referencing.cs.{DefaultCartesianCS, DefaultEllipsoidalCS}
import org.geotools.referencing.datum.{DefaultEllipsoid, DefaultGeodeticDatum, DefaultPrimeMeridian}
import org.geotools.referencing.operation.DefaultMathTransformFactory
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CRSServerTest extends FunSuite with ScalaSparkTest {

  test("Standard CRS") {
    val port = CRSServer.startServer()
    sparkContext.conf.set(CRSServer.CRSServerPort, port.toString)
    try {
      val mercator = CRS.decode("EPSG:3857")
      val sridMercator = CRSServer.crsToSRID(mercator, sparkContext.conf)
      val wgs84 = CRS.decode("EPSG:4326")
      val sridWGS84 = CRSServer.crsToSRID(wgs84, sparkContext.conf)
      // Now retrieve them back
      assert(CRS.lookupEpsgCode(CRSServer.sridToCRS(sridMercator, sparkContext.conf), false) == 3857)
      assert(CRS.lookupEpsgCode(CRSServer.sridToCRS(sridWGS84, sparkContext.conf), false) == 4326)
    } finally {
      CRSServer.stopServer(true)
    }
  }

  test("Non-standard CRS") {
    val port = CRSServer.startServer()
    sparkContext.conf.set(CRSServer.CRSServerPort, port.toString)
    try {
      val sinusoidal = new DefaultProjectedCRS("Sinusoidal", new DefaultGeographicCRS(new DefaultGeodeticDatum("World", DefaultEllipsoid.WGS84, DefaultPrimeMeridian.GREENWICH), DefaultEllipsoidalCS.GEODETIC_2D), //sinus.getConversionFromBase.getMathTransform,
        new DefaultMathTransformFactory().createFromWKT("PARAM_MT[\"Sinusoidal\", \n  PARAMETER[\"semi_major\", 6371007.181], \n  PARAMETER[\"semi_minor\", 6371007.181], \n  PARAMETER[\"central_meridian\", 0.0], \n  PARAMETER[\"false_easting\", 0.0], \n  PARAMETER[\"false_northing\", 0.0]]"), DefaultCartesianCS.PROJECTED)
      // Create a new SRID
      val sridSinusoidal = CRSServer.crsToSRID(sinusoidal, sparkContext.conf)
      assert(sridSinusoidal == -1)
      // Now retrieve it back
      assert(CRSServer.sridToCRS(sridSinusoidal, sparkContext.conf).toWKT == sinusoidal.toWKT())
    } finally {
      CRSServer.stopServer(true)
    }
  }
}
