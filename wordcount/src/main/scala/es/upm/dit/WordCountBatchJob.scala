package es.upm.dit

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object WordCountBatchJob {
  final val BASE_PATH = "../"

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("WordCountBatchJob")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val folderPath = BASE_PATH + "/books"
    val resultFolder = BASE_PATH + "/result"

    // Lectura moderna y eficiente en Spark 4.x (lee todos los archivos de texto de la carpeta)
    val dfWords = spark.read
      .text(folderPath)
      .select(explode(split(lower($"value"), "\\s+")).as("Word"))
      // Limpieza de caracteres especiales usando expresiones regulares compatibles
      .withColumn("Word", regexp_replace($"Word", "[^a-zA-Z0-9áàâäéèêëíìîïóòôöúùûüñÁÀÂÄÉÈÊËÍÌÎÏÓÒÔÖÚÙÛÜÑ]", ""))
      .filter($"Word" =!= "")
      .groupBy("Word")
      .count()
      .withColumnRenamed("count", "Count")
      .sort(desc("Count"))

    dfWords.show()

    // Escritura del resultado
    dfWords
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .format("csv")
      .save(resultFolder)

    spark.stop()
  }
}
