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
package edu.ucr.cs.bdlab.beast.geolite

import org.apache.spark.beast.sql.GeometryDataType
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{BooleanType, DoubleType, IntegerType, LongType, StringType, TimestampType}
import org.locationtech.jts.geom.Geometry

import java.io.{DataInput, DataOutput, Externalizable, IOException}

/**
 * An interface to a geometric feature (geometry + other attributes)
 */
trait IFeature extends Serializable with Row {

  /**
   * The index of the geometry field
   */
  lazy val iGeom: Int = schema.indexWhere(_.dataType == GeometryDataType)

  /**
   * The geometry contained in the feature.
   *
   * @return the geometry in this attribute
   */
  def getGeometry: Geometry = getAs[Geometry](iGeom)

  /**
   * Returns the value of the attribute at position {@code i} 0-based.
   *
   * @param i the index of the attribute to return its value
   * @return the value of the given attribute or {@code null} if it has no value.
   */
  def getAttributeValue(i: Int): Any = get(i + 1)

  /**
   * The type of the given attribute.
   *
   * @param i the index of the attribute
   * @return the type of the attribute
   */
  def getAttributeType(i: Int): FieldType = schema(i + 1).dataType match {
    case StringType => FieldType.StringType
    case IntegerType => FieldType.IntegerType
    case LongType => FieldType.LongType
    case DoubleType => FieldType.DoubleType
    case TimestampType => FieldType.TimestampType
    case BooleanType => FieldType.BooleanType
  }

  /**
   * Returns the total number of attributes
   *
   * @return the number of attributes
   */
  def getNumAttributes: Int = length - 1

  /**
   * If names are associated with attributes, this function returns the name of the attribute at the given position
   * (0-based).
   *
   * @param i the index of the attribute to return its name
   * @return the name of the given attribute index or {@code null} if it does not exist
   */
  def getAttributeName(i: Int): String = schema(i + 1).name

  /**
   * The estimated total size of the feature in bytes including the geometry and features
   *
   * @return the storage size in bytes
   */
  def getStorageSize: Int = {
    var size: Int = 0
    for (i <- 0 until length) {
      schema(i).dataType match {
        case StringType => size += getAs[String](i).length
        case IntegerType => size += 4
        case LongType | DoubleType | TimestampType => size += 8
        case BooleanType => size += 1
        case GeometryDataType => size += GeometryHelper.getGeometryStorageSize(getAs[Geometry](i))
      }
    }
    size
  }
}

object IFeature {
  def toString(feature: IFeature): String = {
    import java.util.GregorianCalendar
    val b: StringBuilder = new StringBuilder
    b.append(if (feature.getGeometry != null) {
      feature.getGeometry.toText
    }
    else {
      "EMPTY"
    })
    var $i: Int = 0
    while ( {
      $i < feature.getNumAttributes
    }) {
      b.append(';')
      val value: Any = feature.getAttributeValue($i)
      if (value.isInstanceOf[GregorianCalendar]) {
        b.append(value.asInstanceOf[GregorianCalendar].toZonedDateTime.toString)
      }
      else {
        b.append(value)
      }

      $i += 1
    }
    b.toString()
  }

  def equals(f1: IFeature, f2: IFeature): Boolean = {
    val numAttrs = f1.getNumAttributes
    if (numAttrs != f2.getNumAttributes) return false
    for (iAttr <- 0 until numAttrs) {
      val name1 = f1.getAttributeName(iAttr)
      val name2 = f2.getAttributeName(iAttr)
      val namesEqual = ((name1 == null || name1.isEmpty) && (name2 == null || name2.isEmpty)) || (name1 != null && name1 == name2)
      if (!namesEqual) return false
      if (!(f1.getAttributeValue(iAttr) == f2.getAttributeValue(iAttr))) return false
    }
    true
  }

}