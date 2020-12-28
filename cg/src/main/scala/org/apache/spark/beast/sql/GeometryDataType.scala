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
package org.apache.spark.beast.sql

import edu.ucr.cs.bdlab.beast.geolite.{GeometryReader, GeometryWriter}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.types.{ArrayType, ByteType, DataType, UserDefinedType}
import org.locationtech.jts.geom.Geometry

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

/**
 * A user-defined data type for geometry. Notice that due to a limitation in Spark API, we have to put this class
 * in a package under org.apache.spark. See [SPARK-7768](https://issues.apache.org/jira/browse/SPARK-7768) for details
 */
class GeometryDataType extends UserDefinedType[Geometry] {
  @transient val geometryWriter = new GeometryWriter
  @transient val geometryReader = new GeometryReader(GeometryReader.DefaultGeometryFactory)

  override def sqlType: DataType = ArrayType(ByteType, containsNull = false)

  override def serialize(geometry: Geometry): GenericArrayData = {
    val baos = new ByteArrayOutputStream()
    val dataoutput = new DataOutputStream(baos)
    geometryWriter.write(geometry, dataoutput, true)
    dataoutput.close()
    new GenericArrayData(baos.toByteArray)
  }

  override def deserialize(datum: Any): Geometry = {
    val datainput = new DataInputStream(new ByteArrayInputStream(datum.asInstanceOf[ArrayData].toByteArray()))
    geometryReader.parse(datainput)
  }

  override def userClass: Class[Geometry] = classOf[Geometry]

  override def typeName: String = "geometry"
}

object GeometryDataType extends GeometryDataType {

  def getGeometryFromRow(record: InternalRow, i: Int): Geometry =
    getGeometryFromArray(record.get(i, GeometryDataType).asInstanceOf[ArrayData])

  def getGeometryFromArray(array: ArrayData): Geometry = {
    val datainput = new DataInputStream(new ByteArrayInputStream(array.toByteArray()))
    geometryReader.parse(datainput)
  }

  def setGeometryInRow(geometry: Geometry): GenericArrayData = {
    val baos = new ByteArrayOutputStream()
    val dataoutput = new DataOutputStream(baos)
    geometryWriter.write(geometry, dataoutput, true)
    dataoutput.close()
    new GenericArrayData(baos.toByteArray)
  }
}