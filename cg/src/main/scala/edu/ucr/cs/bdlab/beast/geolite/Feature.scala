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
import org.apache.spark.sql.Row.unapplySeq
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{BooleanType, DataType, DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType}
import org.locationtech.jts.geom.Geometry

import java.util.{Calendar, SimpleTimeZone, TimeZone}

/**
 * A Row that contains a geometry
 * @param values an initial list of values that might or might not contain a [[Geometry]]
 * @param schema the schema of the given values or `null` to auto-detect the types from the values
 */
class Feature(@transient _values: Array[Any], @transient _schema: StructType)
  extends GenericRowWithSchema(Feature.fixValues(_values, _schema),
    Feature.fixSchema(_schema, names = null, _values)) with IFeature {

  def this() {
    this(_values = null, _schema = null)
  }

  def this(g: Geometry) {
    this(Array[Any](g), StructType(Array(StructField("thegeom", GeometryDataType))))
  }

  /**
   * Create a feature from the given row. The schema of the given row is used if present.
   * If `row.schema` is `null`, the schema is detected based on the types of values in the given row.
   * @param row
   */
  def this(row: Row) {
    this(unapplySeq(row).getOrElse(Seq()).toArray, row.schema)
  }

  /**
   * Initialize the feature by copying all the attributes from the given row and override the geometry
   * attribute to the given one. If the given row already contains a geometry, it will be discarded and replaced
   * with the given geometry. If it does not contain a geometry field, the given geometry will be prepended
   * as the first attribute in the created feature.
   * @param row a feature to copy all the values from except the geometry
   * @param g the geometry to set in this feature.
   */
  def this(row: Row, g: Geometry) {
    this(row)
    values(iGeom) = g
  }

  def this(g: Geometry, values: Array[Any], schema: StructType) {
    this(g +: values, StructType(StructField("thegeom", GeometryDataType) +: schema))
  }

  def this(geometry: Geometry, names: Array[String], types: Array[FieldType], values: Array[Any]) {
    this(Feature.makeValuesArray(geometry, names, types, values), Feature.makeSchema(names, types, values))
  }
}

object Feature {
  /**
   * Return a schema for a new Feature. If `schema` is not `null`, it is used as-is.
   * Otherwise, a schema is detected from the given values.
   * @param schema
   * @param names
   * @param values
   * @return
   */
  def fixSchema(schema: StructType, names: Array[String], values: Array[Any]): StructType = {
    val theSchema =  if (schema != null || values == null) {
      schema
    } else {
      val detectedTypes: Array[StructField] = new Array[StructField](values.length)
      for (i <- values.indices) {
        val detectedType: DataType = values(i) match {
          case null | _: String => StringType
          case _: Integer | _: Int | _: Byte | _: Short => IntegerType
          case _: java.lang.Long | _: Long => LongType
          case _: java.lang.Double | _: Double | _: Float => DoubleType
          case _: Calendar => TimestampType
          case _: java.lang.Boolean | _: Boolean => BooleanType
          case _: Geometry => GeometryDataType
        }
        detectedTypes(i) = StructField(if (names == null) null else names(i), detectedType)
      }
      StructType(detectedTypes)
    }
    // If the schema does not contain a geometry field, prepend it
    if (theSchema.indexWhere(_.dataType == GeometryDataType) != -1)
      theSchema
    else
      StructType(StructField("thegeom", GeometryDataType) +: theSchema)
  }


  val UTC: TimeZone = new SimpleTimeZone(0, "UTC")

  /**
   * Initialize the schema from the given parameters where the first field is always the geometry.
   * If names and types are not null, they are simply padded
   * together to create the schema. If any of the types is null, the value is used to detect the type.
   * If the value is also null, the type is set to [[StringType]] by default
   * @param names the list of names. Can be null and can contain nulls.
   * @param types the list of types. Can be null and can contain nulls.
   * @param values the list of values. Can be null and can contain nulls.
   * @return
   */
  def makeSchema(names: Array[String], types: Array[FieldType], values: Array[Any]): StructType = {
    val numAttributes: Int = if (names != null) names.length
      else if (types != null) types.length
      else if (values != null) values.length
      else 0
    val fields = new Array[StructField](numAttributes + 1)
    fields(0) = StructField("thegeom", GeometryDataType)
    for (i <- 0 until numAttributes) {
      var fieldType: DataType = null
      if (types != null && types(i) != null) {
        fieldType = types(i) match {
          case FieldType.StringType => StringType
          case FieldType.IntegerType => IntegerType
          case FieldType.LongType => LongType
          case FieldType.DoubleType => DoubleType
          case FieldType.TimestampType => TimestampType
          case FieldType.BooleanType => BooleanType
        }
      } else if (values != null && values(i) != null) {
        fieldType = values(i) match {
          case _: String => StringType
          case _: Integer | _: Int | _: Byte | _: Short => IntegerType
          case _: java.lang.Long | _: Long => LongType
          case _: java.lang.Double | _: Double | _: Float => DoubleType
          case _: Calendar => TimestampType
          case _: java.lang.Boolean | _: Boolean => BooleanType
          case _: Geometry => GeometryDataType
        }
      } else {
        fieldType = StringType
      }
      val name: String = if (names == null) null else names(i)
      fields(i + 1) = StructField(name, fieldType)
    }
    StructType(fields)
  }

  def makeValuesArray(geometry: Geometry, names: Array[String], types: Array[FieldType], values: Array[Any]): Array[Any] = {
    val numAttributes: Int = if (names != null) names.length
    else if (types != null) types.length
    else if (values != null) values.length
    else 0
    if (values != null && numAttributes == values.length)
      geometry +: values
    else {
      val retVal = new Array[Any](numAttributes + 1)
      retVal(0) = geometry
      if (values != null)
        System.arraycopy(values, 0, retVal, 1, values.length)
      retVal
    }
  }

  /**
   * Fix the given list of values by prepending an empty geometry attribute if the list does not contain
   * a geometry attribute and the schema is null or does not contain a geometry field.
   * @param values the list of values that might or might not contain a geometry field
   * @param schema the schema that can be null and might or might not contain a geometry field
   * @return
   */
  def fixValues(values: Array[Any], schema: StructType): Array[Any] = {
    val iGeometrySchema: Int = if (schema == null) -1 else schema.indexWhere(_.dataType == GeometryDataType)
    if (iGeometrySchema != -1)
      return values
    val iGeometryValue: Int = if (values == null) -1 else values.indexWhere(_.isInstanceOf[Geometry])
    if (iGeometryValue != -1)
      return values
    EmptyGeometry.instance +: values
  }
}