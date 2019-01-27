package com.ibm.sentimentanalysis

import org.apache.spark.sql.{SQLContext, SparkSession}

object QueryDF {

  def main(args: Array[String]): Unit = {
    val warehouseLocation = "/user/hive/warehouse"
    val spark = SparkSession
      .builder()//.config("spark.speculation", "false")
      .appName("SparkSessionZipsExample").config("spark.master", "local[2]")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("hive.metastore.uris", "thrift://127.0.0.1:9083/metastore")
      .enableHiveSupport()
      .getOrCreate()

    val sqlco: SQLContext = spark.sqlContext

    sqlco.sql("CREATE TABLE IF NOT EXISTS employee12ka4(id INT, name STRING, age INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n'")
    //sqlco.sql("INSERT INTO table employee12ka4 values(1, 'Sudip', 34)")
    //sqlco.sql("show tables").show()
    sqlco.sql("select * from default.employee12ka4").show()

  }

}
