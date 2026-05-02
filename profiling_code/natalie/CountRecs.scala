import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object CountRecs {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("NYC Crashes CountRecs")
      .getOrCreate()

    import spark.implicits._

    val inputPath = "hdfs:///user/bh2514_nyu_edu/project/input/Motor_Vehicle_Collisions_-_Crashes_20260406.csv"

    // was using inferSchema, but it re-scanned the whole 563MB CSV and still
    // typed NUMBER OF PERSONS INJURED as string (a handful of rows had stray
    // "Unspecified" values), which made max() return "Unspecified". defining
    // the schema manually fixes both problems.
    //   old:
    //   val df = spark.read.option("header","true").option("inferSchema","true").csv(inputPath)
    val crashSchema = StructType(Array(
      StructField("CRASH DATE",                    StringType,  true),
      StructField("CRASH TIME",                    StringType,  true),
      StructField("BOROUGH",                       StringType,  true),
      StructField("ZIP CODE",                      StringType,  true),
      StructField("LATITUDE",                      DoubleType,  true),
      StructField("LONGITUDE",                     DoubleType,  true),
      StructField("LOCATION",                      StringType,  true),
      StructField("ON STREET NAME",                StringType,  true),
      StructField("CROSS STREET NAME",             StringType,  true),
      StructField("OFF STREET NAME",               StringType,  true),
      StructField("NUMBER OF PERSONS INJURED",     IntegerType, true),
      StructField("NUMBER OF PERSONS KILLED",      IntegerType, true),
      StructField("NUMBER OF PEDESTRIANS INJURED", IntegerType, true),
      StructField("NUMBER OF PEDESTRIANS KILLED",  IntegerType, true),
      StructField("NUMBER OF CYCLIST INJURED",     IntegerType, true),
      StructField("NUMBER OF CYCLIST KILLED",      IntegerType, true),
      StructField("NUMBER OF MOTORIST INJURED",    IntegerType, true),
      StructField("NUMBER OF MOTORIST KILLED",     IntegerType, true),
      StructField("CONTRIBUTING FACTOR VEHICLE 1", StringType,  true),
      StructField("CONTRIBUTING FACTOR VEHICLE 2", StringType,  true),
      StructField("CONTRIBUTING FACTOR VEHICLE 3", StringType,  true),
      StructField("CONTRIBUTING FACTOR VEHICLE 4", StringType,  true),
      StructField("CONTRIBUTING FACTOR VEHICLE 5", StringType,  true),
      StructField("COLLISION_ID",                  LongType,    true),
      StructField("VEHICLE TYPE CODE 1",           StringType,  true),
      StructField("VEHICLE TYPE CODE 2",           StringType,  true),
      StructField("VEHICLE TYPE CODE 3",           StringType,  true),
      StructField("VEHICLE TYPE CODE 4",           StringType,  true),
      StructField("VEHICLE TYPE CODE 5",           StringType,  true)
    ))

    val df = spark.read
      .option("header", "true")
      .option("mode", "PERMISSIVE")
      .schema(crashSchema)
      .csv(inputPath)

    // cache — we hit this DF with count, map-count, 3x distinct, missing check,
    // min/max and a length profile. without cache each pass re-reads from HDFS.
    df.cache()

    println("=== Total Records using DataFrame count() ===")
    println(df.count())

    println("=== Total Records using map() ===")
    val mapCount = df.rdd
      .map(_ => ("records", 1))
      .reduceByKey(_ + _)
    mapCount.collect().foreach(println)

    println("=== Distinct BOROUGH Values ===")
    df.select("BOROUGH").distinct().show(50, false)

    println("=== Distinct CONTRIBUTING FACTOR VEHICLE 1 Values ===")
    df.select("CONTRIBUTING FACTOR VEHICLE 1").distinct().show(50, false)

    println("=== Distinct VEHICLE TYPE CODE 1 Values ===")
    df.select("VEHICLE TYPE CODE 1").distinct().show(50, false)

    println("=== Missing BOROUGH Count ===")
    println(
      df.filter(col("BOROUGH").isNull || trim(col("BOROUGH")) === "").count()
    )

    // with the old string-typed column max returned "Unspecified" (lex order);
    // now the schema is int so this is real numeric min/max.
    println("=== Injury/Killed Range ===")
    df.select(
      min(col("NUMBER OF PERSONS INJURED")).alias("min_injured"),
      max(col("NUMBER OF PERSONS INJURED")).alias("max_injured"),
      min(col("NUMBER OF PERSONS KILLED")).alias("min_killed"),
      max(col("NUMBER OF PERSONS KILLED")).alias("max_killed")
    ).show(false)

    // free-text length check so we know whether to truncate in the analytic
    println("=== CONTRIBUTING FACTOR VEHICLE 1: length stats ===")
    df.filter(col("CONTRIBUTING FACTOR VEHICLE 1").isNotNull)
      .select(
        min(length(col("CONTRIBUTING FACTOR VEHICLE 1"))).alias("min_len"),
        max(length(col("CONTRIBUTING FACTOR VEHICLE 1"))).alias("max_len"),
        avg(length(col("CONTRIBUTING FACTOR VEHICLE 1"))).alias("avg_len")
      ).show(false)

    df.unpersist()
    spark.stop()
  }
}

CountRecs.main(Array())
