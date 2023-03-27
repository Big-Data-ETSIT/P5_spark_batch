package es.upm.dit

import org.apache.spark.sql.{SparkSession, SaveMode}

object WordCountBatchJob{
  final val BASE_PATH = "../../P5_spark_batch"
  def main(args: Array[String]) {

    val spark = SparkSession
    .builder
    .appName("WordCountBatchJob")
    .master("local[*]")
    .getOrCreate()
    
    spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

    import spark.implicits._


    val folderPath = BASE_PATH + "/books"
    val resultFolder = BASE_PATH + "/result"


    val textFiles = spark
    .sparkContext
    .wholeTextFiles(folderPath)
    .map(_._2)

    // count words in each file and clean words
    val wordCounts = textFiles.flatMap(line => line.split("\\W+"))
                             .map(word => (
                                word
                                  .toLowerCase()
                                  .replaceAll("[^a-zA-Z0-9]", ""), 
                                1))
                             .reduceByKey(_ + _)
                             .sortBy(_._2, ascending = false)

    wordCounts.toDF("Word", "Count")
              .coalesce(1) //in a single file, not recommended for huge csv files
              .write
              .mode(SaveMode.Overwrite)
              .option("header", "true")
              .format("csv")
              .save(resultFolder)

    spark.stop()
  }
}