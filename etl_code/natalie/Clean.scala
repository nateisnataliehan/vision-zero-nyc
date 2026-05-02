import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Clean {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("NYC Crashes Clean")
      .getOrCreate()

    val inputPath  = "hdfs:///user/bh2514_nyu_edu/project/input/Motor_Vehicle_Collisions_-_Crashes_20260406.csv"
    val outputPath = "hdfs:///user/bh2514_nyu_edu/project/output/cleaned_crashes_v2"

    // switched off inferSchema -- it kept mis-typing the injury/killed columns
    // because of stray "Unspecified" values, and it cost an extra pass of the
    // file. with an explicit schema bad cells come in as null under PERMISSIVE
    // mode, then we coalesce them to 0 below.
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

    println("=== Original Record Count ===")
    println(df.count())

    // BOROUGH has four kinds of "missing" in the raw data: null, "", "NULL"
    // (literal string), and "0". early version only handled null/"" which left
    // thousands of junk rows. also normalizing upper+trim so group-bys later
    // don't split "Brooklyn"/"BROOKLYN "/"brooklyn" into separate buckets.
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
      .select(
        col("CRASH DATE"),
        col("BOROUGH_CLEAN").alias("BOROUGH"),
        col("CONTRIBUTING FACTOR VEHICLE 1"),
        col("VEHICLE TYPE CODE 1"),
        col("NUMBER_OF_PERSONS_INJURED_CLEAN"),
        col("NUMBER_OF_PERSONS_KILLED_CLEAN")
      )

    cleanDF.cache()
    println("=== Cleaned Record Count ===")
    println(cleanDF.count())

    println("=== Cleaned Data Preview ===")
    cleanDF.show(20, false)

    // rename columns Hive-friendly before writing
    val hiveReadyDF = cleanDF
      .withColumnRenamed("CRASH DATE", "crash_date")
      .withColumnRenamed("BOROUGH", "borough")
      .withColumnRenamed("CONTRIBUTING FACTOR VEHICLE 1", "contributing_factor_vehicle_1")
      .withColumnRenamed("VEHICLE TYPE CODE 1", "vehicle_type_code_1")
      .withColumnRenamed("NUMBER_OF_PERSONS_INJURED_CLEAN", "number_of_persons_injured")
      .withColumnRenamed("NUMBER_OF_PERSONS_KILLED_CLEAN", "number_of_persons_killed")

    // using CTRL-A (0x01) as the field delimiter because free-text columns
    // like CONTRIBUTING FACTOR can contain commas. matches the Hive DDL below.
    hiveReadyDF.write
      .mode("overwrite")
      .option("header", "false")
      .option("sep", "")
      .csv(outputPath)

    println(s"=== Cleaned data written to: $outputPath ===")

    spark.sql("CREATE DATABASE IF NOT EXISTS bh2514_db")
    spark.sql("DROP TABLE IF EXISTS bh2514_db.nyc_crashes_clean")
    spark.sql(s"""
      CREATE EXTERNAL TABLE bh2514_db.nyc_crashes_clean (
        crash_date STRING,
        borough STRING,
        contributing_factor_vehicle_1 STRING,
        vehicle_type_code_1 STRING,
        number_of_persons_injured INT,
        number_of_persons_killed INT
      )
      ROW FORMAT DELIMITED
      FIELDS TERMINATED BY '\\001'
      STORED AS TEXTFILE
      LOCATION '$outputPath'
    """)

    println("=== Hive table created: bh2514_db.nyc_crashes_clean ===")
    spark.sql("DESCRIBE bh2514_db.nyc_crashes_clean").show(false)
    spark.sql("SELECT COUNT(*) AS total FROM bh2514_db.nyc_crashes_clean").show()
    spark.sql("SELECT * FROM bh2514_db.nyc_crashes_clean LIMIT 5").show(false)

    cleanDF.unpersist()
    spark.stop()
  }
}

Clean.main(Array())
