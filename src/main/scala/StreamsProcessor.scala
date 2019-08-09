import org.apache.spark.sql.SparkSession

class StreamsProcessor(brokers: String) {
  def process(): Unit = {
    val spark = SparkSession.builder()
      .appName("kafka-tutorials")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val inputDf = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokers)
      .option("subscribe", "test")
      .load()

    val personJsonDf = inputDf.selectExpr("CAST(value AS STRING)")

    personJsonDf.writeStream
      .outputMode("append")
      .format("console")
      .start()
      .awaitTermination()
  }
}
object StreamsProcessor extends App {
    new StreamsProcessor("localhost:9092").process()
}