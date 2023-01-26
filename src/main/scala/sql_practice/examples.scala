package sql_practice

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
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

  def exec3(): Unit = {
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val s07DF = spark.read
      .option("delimiter", "\t")
      .csv("data/input/sample_07")

//
//    s07DF.printSchema()
//    s07DF.show()

    // Top salaries in 2007 above $100k
    println(s07DF
      .withColumn("Total_emp", col("_c2").cast(IntegerType))
      .withColumn("Salary", col("_c3").cast(IntegerType))
      .drop("_c2")
      .drop("_c3")
      .withColumn("Description", col("_c1"))
      .select("Description","Salary").where(col("Salary")>100000)
      .orderBy($"Salary".desc)
      .show
    )

    // Salary Growth from 2007-2008
    val s08DF = spark.read
      .option("delimiter", "\t")
      .csv("data/input/sample_08")

    s08DF
      .withColumn("Code", s08DF("_c0"))
      .withColumn("Description", s08DF("_c1"))
      .join(s07DF, s08DF("_c0")===s07DF("_c0"))
      .withColumn("Growth", s08DF("_c3")-s07DF("_c3"))
      .drop(s07DF("_c0"))
      .drop(s07DF("_c1"))
      .drop(s07DF("_c2"))
      .drop(s07DF("_c3"))
      .drop(s08DF("_c0"))
      .drop(s08DF("_c1"))
      .drop(s08DF("_c2"))
      .drop(s08DF("_c3"))
      .show

    // Job loss among top earnings from 2007-2008
    s08DF
      .withColumn("Code", s08DF("_c0"))
      .withColumn("Description", s08DF("_c1"))
      .join(s07DF, s08DF("_c0")===s07DF("_c0"))
      .withColumn("Growth", s08DF("_c3") - s07DF("_c3"))
      .withColumn("Job loss", s08DF("_c2") - s07DF("_c2"))
      .where(col("Growth")>0 and col("Job loss")<0)
      .drop(s07DF("_c0"))
      .drop(s07DF("_c1"))
      .drop(s07DF("_c2"))
      .drop(s07DF("_c3"))
      .drop(s08DF("_c0"))
      .drop(s08DF("_c1"))
      .drop(s08DF("_c2"))
      .drop(s08DF("_c3"))
      .show
  }

  def exec4(): Unit = {
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val toursDF = spark.read
      .option("multiline", true)
      .option("mode", "PERMISSIVE")
      .json("data/input/tours.json")
    toursDF.printSchema()

    // Number of unique level of difficulties
    toursDF
      .groupBy($"tourDifficulty")
      .count()
      .orderBy($"count".desc)
      .show()

    // Min of tour prices
    toursDF
      .agg(min($"tourPrice").as("Min tour price"))
      .show()

    // Max of tour prices
    toursDF
      .agg(max($"tourPrice").as("Max tour price"))
      .show()

    // Avg of tour prices
    toursDF
      .agg(avg($"tourPrice").as("Avg tour price"))
      .show()

    // Min of tour prices for each level of difficulty
    toursDF
      .select("tourPrice", "tourDifficulty")
      .groupBy($"tourDifficulty")
      .agg(min($"tourPrice").as("Min tour price"))
      .show()

    // Max of tour prices for each level of difficulty
    toursDF
      .select("tourPrice", "tourDifficulty")
      .groupBy($"tourDifficulty")
      .agg(max($"tourPrice").as("Max tour price"))
      .show()

    // Avg of tour prices for each level of difficulty
    toursDF
      .select("tourPrice", "tourDifficulty")
      .groupBy($"tourDifficulty")
      .agg(avg($"tourPrice").as("Avg tour price"))
      .show()

    // min/max/avg of price and duration for each level of difficulty
    toursDF
      .select( "tourDifficulty", "tourPrice", "tourLength")
      .groupBy($"tourDifficulty")
      .agg(min($"tourPrice").as("Min tour price"),
            max($"tourPrice").as("Max tour price"),
            avg($"tourPrice").as("Avg tour price"),
            min($"tourLength").as("Min tour length"),
            max($"tourLength").as("Max tour length"),
            avg($"tourLength").as("Avg tour length"))
      .show()

    // Top 10 tour tags
    toursDF
      .select(explode($"tourTags"))
      .groupBy("col")
      .count()
      .orderBy($"count".desc)
      .show(10)

    // Relationship between top 10 tour tags and tour difficulty
    toursDF
      .select(explode($"tourTags"), $"tourDifficulty")
      .groupBy($"col", $"tourDifficulty")
      .count()
      .orderBy($"count".desc)
      .show(10)

    // min/max/avg of price in tour tags and tour difficulty relationship
    toursDF
      .select(explode($"tourTags"), $"tourDifficulty", $"tourPrice")
      .groupBy($"col", $"tourDifficulty")
      .agg(min($"tourPrice").as("Min"),
        max($"tourPrice").as("Max"),
        avg($"tourPrice").as("Avg"))
      .orderBy($"Avg".desc)
      .show(10)
  }

  }
