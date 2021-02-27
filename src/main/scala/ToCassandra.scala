import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object ToCassandra extends AppConfigs {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val mobileSchema: StructType = StructType(Array(
    StructField("model", StringType,nullable = true),
    StructField("amount", IntegerType,nullable = true),
    StructField("boughtAt", StringType, nullable = true)
  ))

  def SparkInitializer: SparkSession = {
    val spark = SparkSession.builder().appName("GetJSONStreams").master(MASTER_MODE)
      .config("spark.cassandra.connection.host", CASSANDRA_HOST)
      .config("spark.cassandra.connection.port", CASSANDRA_PORT)
      .getOrCreate()
    spark
  }
  def toCassandra(finalDF: DataFrame): DataStreamWriter[Row] = {

    val finalDFWriter = finalDF.writeStream.outputMode("update")
      .foreachBatch { (batchDF: DataFrame, _: Long) =>
      batchDF.write.mode(SaveMode.Append)
        .cassandraFormat(TABLE, KEYSPACE)
        .save()
    }
  finalDFWriter
  }

  def aggregatedDF(baseDF: DataFrame): DataFrame = {
    val aggStreamDF = baseDF.withWatermark("instance", "5 seconds")
      .groupBy(window(col("instance"), "5 seconds", "2 seconds"),col("model"))
      .agg(sum("amount").as("amount"))
      .select(col("window").getField("end").as("instance"),col("amount"),col("model"))
    aggStreamDF
  }

  def main(args: Array[String]): Unit = {

    val JStream = SparkInitializer.readStream.format("kafka")
      .option("kafka.bootstrap.servers", BootStrapServers)
      .option("subscribe", Topic)
      .option("failOnDataLoss","false").load

    val streamDF = JStream.select(from_json(col("value").cast("string"), mobileSchema).as("mobilesDF"))
      .select(col("mobilesDF.model").as("model"), col("mobilesDF.amount").as("amount"),
        to_timestamp(col("mobilesDF.boughtAt"), "yyyy-MM-dd HH:mm:ss.SSS").alias("instance"))

    val AggDF = aggregatedDF(streamDF)
    toCassandra(AggDF).start.awaitTermination

  }
}
