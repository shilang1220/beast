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

import edu.ucr.cs.bdlab.beast.util.BitArray
import org.apache.spark.beast.sql.GeometryDataType
import org.apache.spark.sql.Row
import org.apache.spark.sql.Row.unapplySeq
import org.apache.spark.sql.types._
import org.locationtech.jts.geom.Geometry

import java.io.{Externalizable, ObjectInput, ObjectOutput}
import java.util.{Calendar, SimpleTimeZone, TimeZone}

/**
 * A Row that contains a geometry
 * @param values an initial list of values that might or might not contain a [[Geometry]]
 * @param _schema the schema of the given values or `null` to auto-detect the types from the values
 */
class Feature(@transient private var values: Array[Any], @transient private var _schema: StructType)
  extends IFeature with Externalizable {
  // infer the schema from the values if null
  if (_schema == null && values != null)
    _schema = Feature.inferSchema(values)

  override def schema: StructType = _schema

  /**
   * Default constructor for serialization/deserialization
   */
  def this() {
    this(values = null, _schema = null)
  }

  override def fieldIndex(name: String): Int = schema.fieldIndex(name)

//
//  /**
//   * Creates a feature with a geometry and no additional attributes
//   * @param g the geometry to set in the features
//   */
//  def this(g: Geometry) {
//    this(Array[Any](g), StructType(Array(StructField("g", GeometryDataType))))
//  }
//
//  /**
//   * Create a feature from the given row. The schema of the given row is used if present.
//   * If `row.schema` is `null`, the schema is detected based on the types of values in the given row.
//   * @param row
//   */
//  def this(row: Row) {
//    this(unapplySeq(row).getOrElse(Seq()).toArray, row.schema)
//  }
//
//  /**
//   * Initialize the feature by copying all the attributes from the given row and override the geometry
//   * attribute to the given one. If the given row already contains a geometry, it will be discarded and replaced
//   * with the given geometry. If it does not contain a geometry field, the given geometry will be prepended
//   * as the first attribute in the created feature.
//   * @param row a feature to copy all the values from except the geometry
//   * @param g the geometry to set in this feature.
//   */
//  def this(row: Row, g: Geometry) {
//    this(g +: unapplySeq(row).getOrElse(Seq()).toArray,
//      StructType(StructField("g", GeometryDataType) +: (if (row.schema != null) row.schema else Feature.inferSchema(unapplySeq(row).getOrElse(Seq()).toArray))))
//  }
//
//  def this(g: Geometry, values: Array[Any], schema: StructType) {
//    this(g +: values, StructType(StructField("g", GeometryDataType) +: (if (schema != null) schema else Feature.inferSchema(values))))
//  }
//
//  def this(geometry: Geometry, names: Array[String], types: Array[FieldType], values: Array[Any]) {
//    this(Feature.makeValuesArray(geometry, names, types, values), Feature.makeSchema(names, types, values))
//  }

  /**
   * Efficient Java serialization/deserialization. A feature is serialized as follows.
   *  - Total number of attributes including the geometry, i.e., [[length]]
   *  - The names of the attributes in order. If the name does not exist, an empty string is written.
   *  - The types of the attributes, each written as a single byte.
   *  - A compact bit mask of which attribute values are null.
   *  - The values of the attributes in Java serialization form.
   *  - The geometry is written using GeometryWriter with the SRID in its position in the list of values
   *  - null values are skipped
   * @param out the output to write to
   */
  override def writeExternal(out: ObjectOutput): Unit = {
    // Number of attributes
    out.writeShort(length)
    if (length > 0) {
      // Attribute names
      for (field <- schema)
        out.writeUTF(if (field.name == null) "" else field.name)
      // Attribute types
      for (field <- schema) {
        val typeOrdinal = field.dataType match {
          case ByteType => 0
          case ShortType => 1
          case IntegerType => 2
          case LongType => 3
          case FloatType => 4
          case DoubleType => 5
          case StringType => 6
          case BooleanType => 7
          case GeometryDataType => 8
          case DateType => 9
          case TimestampType => 10
          case _ => -1 // Any other type will be written as a Java object
        }
        out.writeByte(typeOrdinal)
      }
      // Attribute exists (bit mask)
      val attributeExists = new BitArray(length)
      for (i <- 0 until length)
        attributeExists.set(i, !isNullAt(i))
      attributeExists.writeBitsMinimal(out)
      // Attribute values
      for (i <- 0 until length; if !isNullAt(i)) {
        val value = values(i)
        schema(i).dataType match {
          case ByteType => out.writeByte(value.asInstanceOf[Number].byteValue())
          case ShortType => out.writeShort(value.asInstanceOf[Number].shortValue())
          case IntegerType => out.writeInt(value.asInstanceOf[Number].intValue())
          case LongType => out.writeLong(value.asInstanceOf[Number].longValue())
          case FloatType => out.writeFloat(value.asInstanceOf[Number].floatValue())
          case DoubleType => out.writeDouble(value.asInstanceOf[Number].doubleValue())
          case StringType => out.writeUTF(value.asInstanceOf[String])
          case BooleanType => out.writeBoolean(value.asInstanceOf[Boolean])
          case GeometryDataType => new GeometryWriter().write(value.asInstanceOf[Geometry], out, true)
          case _ => out.writeObject(value)
        }
      }
    }
  }

  override def readExternal(in: ObjectInput): Unit = {
    // Read number of attributes
    val recordLength: Int = in.readShort()
    val attributeNames = new Array[String](recordLength)
    val attributeTypes = new Array[DataType](recordLength)
    // Read attribute names
    for (i <- 0 until recordLength)
      attributeNames(i) = in.readUTF()
    // Read attribute types
    for (i <- 0 until recordLength)
      attributeTypes(i) = in.readByte() match {
        case 0 => ByteType
        case 1 => ShortType
        case 2 => IntegerType
        case 3 => LongType
        case 4 => FloatType
        case 5 => DoubleType
        case 6 => StringType
        case 7 => BooleanType
        case 8 => GeometryDataType
        case 9 => DateType
        case 10 => TimestampType
        case -1 => null
      }
    this._schema = StructType((0 until recordLength).map(i => StructField(attributeNames(i), attributeTypes(i))))
    // Read attribute exists
    val attributeExists = new BitArray(recordLength)
    attributeExists.readBitsMinimal(in)
    // Read attribute values
    this.values = new Array[Any](recordLength)
    for (i <- 0 until recordLength; if attributeExists.get(i)) {
      values(i) = attributeTypes(i) match {
        case ByteType => in.readByte()
        case ShortType => in.readShort()
        case IntegerType => in.readInt()
        case LongType => in.readLong()
        case FloatType => in.readFloat()
        case DoubleType => in.readDouble()
        case StringType => in.readUTF()
        case BooleanType => in.readBoolean()
        case GeometryDataType => GeometryReader.DefaultInstance.parse(in)
        case _ => in.readObject()
      }
    }
  }

  override def length: Int = if (values == null) 0 else values.length

  override def get(i: Int): Any = values(i)

  /**
   * Make a copy of this row. Since Feature is immutable, we just return the same object.
   * @return the same object
   */
  override def copy(): Row = this
}

object Feature {

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
    fields(0) = StructField("g", GeometryDataType)
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

  /**
   * Create an array of values that contains the given geometry.
   * The list of values is not expected to include a geometry field.
   * @param geometry the geometry element to include in the array of values
   * @param types the list of data types. Can be null
   * @param values the list of values. Can be null
   * @return a list of values with the given geometry included in it
   */
  def makeValuesArray(geometry: Geometry, types: Array[FieldType], values: Array[Any]): Array[Any] = {
    val numAttributes = if (types != null) types.length
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
   * Infer schema from the values. If a value is `null`, the type is inferred as [[BinaryType]]
   * @param values the array of values
   * @return
   */
  def inferSchema(values: Array[Any]): StructType = StructType(values.map(v => {
    val valueType: DataType = v match {
      case null => BinaryType
      case _: Byte => ByteType
      case _: Short => ShortType
      case _: Int => IntegerType
      case _: Long => LongType
      case _: Float => FloatType
      case _: Double => DoubleType
      case _: String => StringType
      case x: java.math.BigDecimal => DecimalType(x.precision(), x.scale())
      case _: java.sql.Date | _: java.time.LocalDate => DateType
      case _: java.sql.Timestamp | _: java.time.Instant => TimestampType
      case _: Array[Byte] => BinaryType
      case r: Row => r.schema
      case _: Geometry => GeometryDataType
      case _ => BinaryType
    }
    StructField(null, valueType)
  }))

  /**
   * Create a [[Feature]] from the given row and the given geometry.
   * If the row already contains a geometry field, it is overridden.
   * If the row does not contain a geometry field, the geometry is prepended.
   * If the given geometry is null, the original geometry is kept intact.
   * @param row and existing row that might or might not contain a geometry
   * @param geometry the new geometry to use in the created feature
   * @return a [[Feature]] with the given values and geometry
   */
  def create(row: Row, geometry: Geometry): Feature =
    if (row == null)
      create(geometry, null, null)
    else
      create(geometry, Row.unapplySeq(row).getOrElse(Seq[Any]()).toArray, row.schema)

  def create(geometry: Geometry, _values: Array[Any], _schema: StructType): Feature = {
    var schema: StructType = if (_schema != null) _schema
    else if (_values != null) inferSchema(_values)
    else null
    val iGeom: Int = if (schema == null) -1 else schema.indexWhere(_.dataType == GeometryDataType)
    var values: Array[Any] = _values
    if (values == null)
      values = Array(geometry)
    else if (iGeom == -1)
      values = geometry +: values
    else if (geometry != null)
      values(iGeom) = geometry

    if (iGeom == -1 && schema == null)
      schema = StructType(Seq(StructField("g", GeometryDataType)))
    else if (iGeom == -1)
      schema = StructType(StructField("g", GeometryDataType) +: schema)

    new Feature(values, schema)
  }

  def create(geometry: Geometry, _names: Array[String], _types: Array[FieldType], _values: Array[Any]): Feature =
    new Feature(Feature.makeValuesArray(geometry, _types, _values), Feature.makeSchema(_names, _types, _values))

}