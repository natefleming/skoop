package com.ngc.spark.sql.jdbc

import org.scalatest._
import org.apache.spark.sql.jdbc.{ JdbcDialect, JdbcDialects }

class Db2DialectTest extends FlatSpec with Matchers {

  "Db2Dialect" should "successfully register with JdbDialects" in {
    JdbcDialects.registerDialect(Db2Dialect())
  }

  "Db2Dialect" should "return truncate table with IMMEDIATE modifier" in {
    assert(Db2Dialect().getTruncateQuery("mytable").endsWith("IMMEDIATE"))
  }

  "Db2Dialect" should "successfully register with JdbDialects using reflection" in {
    val clazz = java.lang.Class.forName("com.ngc.spark.sql.jdbc.Db2Dialect")
    val instance = clazz.newInstance().asInstanceOf[JdbcDialect]
    JdbcDialects.registerDialect(instance)
  }

  "Db2Dialect" should "successfully return from JdbcDialects" in {
    val clazz = java.lang.Class.forName("com.ngc.spark.sql.jdbc.Db2Dialect")
    val instance = clazz.newInstance().asInstanceOf[JdbcDialect]
    JdbcDialects.registerDialect(instance)
    val registeredDialect = JdbcDialects.get("jdbc:db2://hostname:port/FPS2_MTE")
    assert(registeredDialect.getTruncateQuery("mytable").endsWith("IMMEDIATE"))
  }
  
}
