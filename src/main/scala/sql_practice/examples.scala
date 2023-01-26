package sql_practice

import org.apache.spark.sql.functions._
import spark_helpers.SessionBuilder

object examples {
  def exec1(): Unit = {
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val toursDF = spark.read
      .option("multiline", true)
      .option("mode", "PERMISSIVE")
      .json("data/input/tours.json")
    toursDF.show

    println(toursDF
      .select(explode($"tourTags"))
      .groupBy("col")
      .count()
      .count()
    )

    toursDF
      .select(explode($"tourTags"), $"tourDifficulty")
      .groupBy($"col", $"tourDifficulty")
      .count()
      .orderBy($"count".desc)
      .show(10)

    toursDF.select($"tourPrice")
      .filter($"tourPrice" > 500)
      .orderBy($"tourPrice".desc)
      .show(20)


  }

  def exec2(): Unit = {
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val demoDF = spark.read
      .json("data/input/demographie_par_commune.json")
    demoDF.show


    // Number inhabitants France
    println(demoDF
      .agg(sum("Population").as("Population totale"))
      .show
    )

    // Top highly populated departments in France
    demoDF
      .groupBy("Departement")
      .agg(sum("Population").alias("Population"))
      .orderBy($"Population".desc)
      .show

    // join
    val depDF = spark.read
      .csv("data/input/departements.txt")

    depDF.printSchema()

    demoDF
      .groupBy("Departement")
      .agg(sum("Population").alias("Population"))
      .orderBy($"Population".desc)
      .join(depDF, demoDF("Departement")===depDF("_c1"), "inner")
      .show
  }


}
