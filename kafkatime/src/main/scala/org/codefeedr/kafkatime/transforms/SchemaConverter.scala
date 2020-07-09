package org.codefeedr.kafkatime.transforms

import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}

import scala.collection.JavaConverters._

object SchemaConverter {

  /**
    * Getter for the nested Avro schema.
    *
    * @param name name of the schema
    * @param schema schema of current level
    * @return schema name with the corresponding Flink type information as a tuple
    */
  def getNestedSchema(
      name: String,
      schema: org.apache.avro.Schema): (String, TypeInformation[_]) =
    schema.getType match {
      case org.apache.avro.Schema.Type.NULL =>
        (name, Types.GENERIC(classOf[Null]))
      case org.apache.avro.Schema.Type.STRING  => (name, Types.STRING)
      case org.apache.avro.Schema.Type.FLOAT   => (name, Types.FLOAT)
      case org.apache.avro.Schema.Type.DOUBLE  => (name, Types.DOUBLE)
      case org.apache.avro.Schema.Type.INT     => (name, Types.INT)
      case org.apache.avro.Schema.Type.BOOLEAN => (name, Types.BOOLEAN)
      case org.apache.avro.Schema.Type.LONG    => (name, Types.LONG)
      case org.apache.avro.Schema.Type.UNION =>
        (name, getNestedSchema(name, schema.getTypes.asScala.last)._2)
      case org.apache.avro.Schema.Type.BYTES => (name, Types.BYTE)
      // The key for an Avro map must be a string. Avro maps supports only one attribute: values.
      case org.apache.avro.Schema.Type.MAP =>
        (name,
         Types.MAP(Types.STRING, getNestedSchema(name, schema.getValueType)._2))
      case org.apache.avro.Schema.Type.ARRAY =>
        val nestedType = getNestedSchema(name, schema.getElementType)._2
        nestedType match {
          case Types.BOOLEAN | Types.BYTE | Types.INT | Types.LONG |
              Types.FLOAT | Types.DOUBLE =>
            (name, Types.PRIMITIVE_ARRAY(nestedType))
          case Types.STRING =>
            (name, Types.LIST(nestedType))
          case _ => {
            if (nestedType == Types.PRIMITIVE_ARRAY(Types.INT)
                || nestedType == Types.PRIMITIVE_ARRAY(Types.BOOLEAN)
                || nestedType == Types.PRIMITIVE_ARRAY(Types.BYTE)
                || nestedType == Types.PRIMITIVE_ARRAY(Types.LONG)
                || nestedType == Types.PRIMITIVE_ARRAY(Types.FLOAT)
                || nestedType == Types.PRIMITIVE_ARRAY(Types.DOUBLE)
                || nestedType == Types.LIST(Types.STRING)) {
              (name,
               Types.LIST(
                 Types.LIST(getNestedSchema(
                   name,
                   schema.getElementType.getElementType)._2)))
            } else {
              (name, Types.OBJECT_ARRAY(nestedType))
            }
          }
        }
      case org.apache.avro.Schema.Type.RECORD =>
        val temp = schema.getFields.asScala
          .map(x => getNestedSchema(x.name(), x.schema()))
          .unzip
        (name,
         Types.ROW_NAMED(
           temp._1.toArray,
           temp._2.toArray: _*
         ))
      case _ => throw new RuntimeException("Unsupported type.")
    }

}
