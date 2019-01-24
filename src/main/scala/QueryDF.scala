import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.hive.HiveContext

object QueryDF {

  def main(args: Array[String]): Unit = {
    val warehouseLocation = "/user/hive/warehouse"
    val spark = SparkSession
      .builder()
      .appName("SparkSessionZipsExample").config("spark.master", "local[2]")
      .config("spark.sql.warehouse.dir", warehouseLocation).config("hive.metastore.uris", "thrift://127.0.0.1:9083/metastore")
      .enableHiveSupport()
      .getOrCreate()

    val sqlco:SQLContext = spark.sqlContext
    sqlco.sql("CREATE TABLE IF NOT EXISTS employee12(id INT, name STRING, age INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n'")
    sqlco.sql("LOAD DATA INPATH 'hdfs://127.0.0.1:8020/spark/input/sample.txt' into table employee12")
    sqlco.sql("show tables").show()
    sqlco.sql("select * from employee12").show()

  }

}
