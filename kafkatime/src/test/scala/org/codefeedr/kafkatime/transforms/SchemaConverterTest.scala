package org.codefeedr.kafkatime.transforms

import java.nio.ByteBuffer

import com.sksamuel.avro4s._
import org.apache.avro.Schema
import org.apache.avro.io.FastReaderBuilder.RecordReader.Stage
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.types.DataType
import org.codefeedr.kafkatime.transforms.SchemaConverter.getNestedSchema
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor2}

import scala.language.higherKinds

class SchemaConverterTest extends AnyFunSuite with TableDrivenPropertyChecks {

  case class Nested(someValue: Int)

  case class someStage(someStage: Stage)

  case class AllSupportedTypes(someString: String, someFloat: Float, someDouble: Double, someInt: Int,
                               someBoolean: Boolean, someLong: Long, someOptional: Option[String], someByte: ByteBuffer,
                               someMap: Map[String, Int], someArray: Array[Int], someList: List[Long], someNested: Nested,
                               someNestedList: List[Nested], someNull: Null)

  def getSchema: Schema = {
    implicit object ItemToSchema extends SchemaFor[Null] {
      override def schema(fieldMapper: FieldMapper): Schema = {
        Schema.create(Schema.Type.NULL)
      }
    }
    implicit object NullEncoder extends Encoder[Null] {
      override def encode(t: Null,
                          schema: Schema,
                          fieldMapper: FieldMapper): AnyRef = {
        t
      }
    }
    implicit object DateDecoder extends Decoder[Null] {
      override def decode(value: Any,
                          schema: Schema,
                          fieldMapper: FieldMapper): Null = {
        null
      }
    }
    AvroSchema[AllSupportedTypes]
  }
  val schema: Schema = getSchema

  val testData: TableFor2[DataType, String] =
    Table(
      ("expectedType", "FieldName"),
      (DataTypes.STRING(), "someString"),
      (DataTypes.FLOAT(), "someFloat"),
      (DataTypes.DOUBLE(), "someDouble"),
      (DataTypes.INT(), "someInt"),
      (DataTypes.BOOLEAN(), "someBoolean"),
      (DataTypes.BIGINT(), "someLong"),
      (DataTypes.STRING(), "someOptional"),
      (DataTypes.BYTES(), "someByte"),
      (DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()), "someMap"),
      (DataTypes.ARRAY(DataTypes.INT()), "someArray"),
      (DataTypes.ARRAY(DataTypes.BIGINT()), "someList"),
      (DataTypes.ROW(DataTypes.FIELD("someValue", DataTypes.INT())), "someNested"),
      (DataTypes.ARRAY(DataTypes.ROW(DataTypes.FIELD("someValue", DataTypes.INT()))), "someNestedList"),
      (DataTypes.NULL(), "someNull")
    )

  /**
   * Parameterized good weather tests for all supported types.
   */
  forAll(testData) { (t: DataType, name: String) =>
    assertResult((name, t)) {
      getNestedSchema(name, schema.getField(name).schema())
    }
  }
  /**
   * Unsupported type.
   */
  assertThrows[RuntimeException] {
    getNestedSchema("doesnt matter", AvroSchema[someStage])
  }

}
