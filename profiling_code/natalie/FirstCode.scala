import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object FirstCode {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("HW7 First Code Drop - NYC Crashes")
      .getOrCreate()

    val inputPath  = "hdfs:///user/bh2514_nyu_edu/project/input/Motor_Vehicle_Collisions_-_Crashes_20260406.csv"
    val outputPath = "hdfs:///user/bh2514_nyu_edu/project/output/firstcode_cleaned_output"

    // explicit schema instead of inferSchema -- inferSchema triggered a second
    // pass of the 563MB file and still mis-typed NUMBER OF PERSONS INJURED as
    // string because of stray "Unspecified" tokens, which broke mean/stddev.
    //   old: spark.read.option("inferSchema","true").csv(inputPath)
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

    println("=== ORIGINAL DATA: SCHEMA ===")
    df.printSchema()

    println("=== ORIGINAL DATA: RECORD COUNT ===")
    println(df.count())

    println("=== ORIGINAL DATA: SAMPLE ===")
    df.show(20, false)

    println("=== ORIGINAL DATA: SUMMARY STATISTICS ===")
    df.describe().show(false)

    // cleaning option 1: date formatting (MM/dd/yyyy -> DateType)
    // cleaning option 2: text normalization (trim + upper on BOROUGH)
    // plus a binary column INJURY_OCCURRED derived from the injury count.
    //
    // older version needed a regex to filter out non-numeric junk in the
    // injury column (because inferSchema typed it as string); with the
    // explicit schema now the bad rows come in as null and a single coalesce
    // handles them:
    //   .withColumn("NUMBER_OF_PERSONS_INJURED_CLEAN",
    //     when(trim(col("NUMBER OF PERSONS INJURED").cast("string")).rlike("^[0-9]+$"),
    //          col("NUMBER OF PERSONS INJURED").cast("int"))
    //       .otherwise(lit(0)))
    val cleanDF = df
      .filter(col("CRASH DATE").isNotNull && trim(col("CRASH DATE")) =!= "")
      .filter(
        col("BOROUGH").isNotNull &&
        trim(col("BOROUGH")) =!= "" &&
        upper(trim(col("BOROUGH"))) =!= "NULL" &&
        trim(col("BOROUGH")) =!= "0"
      )
      .withColumn("CRASH_DATE_FORMATTED", to_date(col("CRASH DATE"), "MM/dd/yyyy"))
      .withColumn("BOROUGH_CLEAN", upper(trim(col("BOROUGH"))))
      .withColumn(
        "NUMBER_OF_PERSONS_INJURED_CLEAN",
        coalesce(col("NUMBER OF PERSONS INJURED"), lit(0))
      )
      .withColumn(
        "NUMBER_OF_PERSONS_KILLED_CLEAN",
        coalesce(col("NUMBER OF PERSONS KILLED"), lit(0))
      )
      .withColumn(
        "INJURY_OCCURRED",
        when(col("NUMBER_OF_PERSONS_INJURED_CLEAN") > 0, 1).otherwise(0)
      )
      .select(
        col("CRASH DATE"),
        col("CRASH_DATE_FORMATTED"),
        col("BOROUGH"),
        col("BOROUGH_CLEAN"),
        col("CONTRIBUTING FACTOR VEHICLE 1"),
        col("VEHICLE TYPE CODE 1"),
        col("NUMBER_OF_PERSONS_INJURED_CLEAN"),
        col("NUMBER_OF_PERSONS_KILLED_CLEAN"),
        col("INJURY_OCCURRED")
      )

    // cache before running mean/median/mode/stddev back-to-back
    cleanDF.cache()

    println("=== CLEANED DATA: RECORD COUNT ===")
    println(cleanDF.count())

    println("=== CLEANED DATA: SAMPLE ===")
    cleanDF.show(20, false)

    println("=== CLEANED DATA: SUMMARY STATISTICS ===")
    cleanDF.describe(
      "NUMBER_OF_PERSONS_INJURED_CLEAN",
      "NUMBER_OF_PERSONS_KILLED_CLEAN",
      "INJURY_OCCURRED"
    ).show(false)

    println("=== MEAN OF NUMBER_OF_PERSONS_INJURED_CLEAN ===")
    cleanDF.select(mean("NUMBER_OF_PERSONS_INJURED_CLEAN").alias("mean_injured")).show(false)

    println("=== MEDIAN OF NUMBER_OF_PERSONS_INJURED_CLEAN ===")
    cleanDF.select(expr("percentile_approx(NUMBER_OF_PERSONS_INJURED_CLEAN, 0.5)").alias("median_injured")).show(false)

    println("=== MODE OF NUMBER_OF_PERSONS_INJURED_CLEAN ===")
    cleanDF.groupBy("NUMBER_OF_PERSONS_INJURED_CLEAN")
      .count()
      .orderBy(desc("count"))
      .show(1, false)

    println("=== STANDARD DEVIATION OF NUMBER_OF_PERSONS_INJURED_CLEAN ===")
    cleanDF.select(stddev("NUMBER_OF_PERSONS_INJURED_CLEAN").alias("stddev_injured")).show(false)

    cleanDF.write
      .mode("overwrite")
      .option("header", "true")
      .csv(outputPath)

    println(s"=== Cleaned data written to: $outputPath ===")

    cleanDF.unpersist()
    spark.stop()
  }
}

FirstCode.main(Array())
