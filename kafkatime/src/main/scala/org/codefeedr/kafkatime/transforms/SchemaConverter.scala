package org.codefeedr.kafkatime.transforms

import org.apache.avro.Schema.Type
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.types.DataType

import scala.collection.JavaConverters._

object SchemaConverter {

  /**
    * Getter for the nested Avro schema.
    *
    * @param name name of the schema
    * @param schema schema of current level
    * @return schema name with the corresponding Flink type information as a tuple
    */
  def getNestedSchema(name: String,
                      schema: org.apache.avro.Schema): (String, DataType) =
    schema.getType match {
      case Type.NULL    => (name, DataTypes.NULL())
      case Type.STRING  => (name, DataTypes.STRING())
      case Type.FLOAT   => (name, DataTypes.FLOAT())
      case Type.DOUBLE  => (name, DataTypes.DOUBLE())
      case Type.INT     => (name, DataTypes.INT())
      case Type.BOOLEAN => (name, DataTypes.BOOLEAN())
      case Type.LONG    => (name, DataTypes.BIGINT())
      case Type.BYTES   => (name, DataTypes.BYTES())

      case Type.UNION =>
        val foundType = schema.getTypes.asScala
          .map(getNestedSchema(name, _)._2)
          .find(x => x != DataTypes.NULL())
        (name, if (foundType.isDefined) foundType.get else DataTypes.NULL())

      // The key for an Avro map must be a string. Avro maps supports only one attribute: values.
      case Type.MAP =>
        (name,
         DataTypes.MAP(DataTypes.STRING(),
                       getNestedSchema(name, schema.getValueType)._2))

      case org.apache.avro.Schema.Type.ARRAY =>
        (name, DataTypes.ARRAY(getNestedSchema(name, schema.getElementType)._2))

      case org.apache.avro.Schema.Type.RECORD =>
        val fields = schema.getFields.asScala.map(x => {
          val res = getNestedSchema(x.name(), x.schema())
          DataTypes.FIELD(res._1, res._2)
        })
        (name, DataTypes.ROW(fields: _*))

      case _ => throw new RuntimeException("Unsupported type.")
    }

}
