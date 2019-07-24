package com.ngc.spark.sql.jdbc

import org.apache.spark.sql.jdbc.{ JdbcDialect, JdbcType }
import java.sql.Types
import org.apache.spark.sql.types._

object Db2Dialect {
  
  def apply(): Db2Dialect = new Db2Dialect()
  
}

class Db2Dialect extends JdbcDialect {

  override def canHandle(url: String): Boolean = url.startsWith("jdbc:db2")

  override def getCatalystType(
    sqlType: Int,
    typeName: String,
    size: Int,
    md: MetadataBuilder): Option[DataType] = sqlType match {
    case Types.REAL => Option(FloatType)
    case Types.OTHER =>
      typeName match {
        case "DECFLOAT" => Option(DecimalType(38, 18))
        case "XML" => Option(StringType)
        case t if (t.startsWith("TIMESTAMP")) => Option(TimestampType) // TIMESTAMP WITH TIMEZONE
        case _ => None
      }
    case _ => None
  }

  override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
    case StringType => Option(JdbcType("CLOB", java.sql.Types.CLOB))
    case BooleanType => Option(JdbcType("CHAR(1)", java.sql.Types.CHAR))
    case ShortType | ByteType => Some(JdbcType("SMALLINT", java.sql.Types.SMALLINT))
    case _ => None
  }

  override def quoteIdentifier(colName: String): String = {
    s"$colName"
  }

  override def isCascadingTruncateTable(): Option[Boolean] = Some(false)

  override def getTruncateQuery(table: String): String = {
    s"TRUNCATE TABLE $table IMMEDIATE"
  }

}


