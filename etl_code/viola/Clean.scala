import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

val basePath = "/user/jy4018_nyu_edu/project/input"

// columns we want — present in all 20 years (with minor name differences)
val commonCols = Seq(
  "ST_CASE", "STATE", "CITY", "COUNTY",
  "YEAR", "MONTH", "DAY", "DAY_WEEK", "HOUR", "MINUTE",
  "FATALS", "PERSONS", "PEDS",
  "VE_TOTAL", "VE_FORMS",
  "ROUTE", "HARM_EV", "MAN_COLL", "REL_ROAD",
  "LGT_COND", "WEATHER",
  "LATITUDE", "LONGITUD"
)

// load each year, select common columns, union them
var allData: DataFrame = null

for (year <- 2005 to 2024) {
  val path = basePath + "/accident_" + year + ".csv"
  val yearDF = spark.read.option("header", "true").option("inferSchema", "true").csv(path)

  // handle column name differences across years:
  // 2005-2007: lowercase latitude/longitud
  // 2022-2024: BOM prefix on STATE column
  val cols = commonCols.map { c =>
    if (c == "LATITUDE" && yearDF.columns.contains("latitude") && !yearDF.columns.contains("LATITUDE"))
      col("latitude").alias("LATITUDE")
    else if (c == "LONGITUD" && yearDF.columns.contains("longitud") && !yearDF.columns.contains("LONGITUD"))
      col("longitud").alias("LONGITUD")
    else if (c == "STATE" && !yearDF.columns.contains("STATE"))
      col(yearDF.columns.find(_.endsWith("STATE")).getOrElse("STATE")).alias("STATE")
    else
      col(c)
  }

  val selected = yearDF.select(cols: _*)

  if (allData == null) allData = selected
  else allData = allData.union(selected)
}

val rawDF = allData
val rawCount = rawDF.count()
println("Raw dataset: " + rawCount + " records, " + rawDF.columns.length + " columns")
rawDF.printSchema()

// filter to analysis window
val filteredDF = rawDF.filter(col("YEAR") >= 2005 && col("YEAR") <= 2024)
println("After year filter: " + filteredDF.count())

// check nulls before dropping
println("Nulls in STATE: " + filteredDF.filter(col("STATE").isNull).count())
println("Nulls in YEAR: " + filteredDF.filter(col("YEAR").isNull).count())
println("Nulls in FATALS: " + filteredDF.filter(col("FATALS").isNull).count())
println("Nulls in ST_CASE: " + filteredDF.filter(col("ST_CASE").isNull).count())

// drop rows where critical fields are null
val cleanedDF = filteredDF.na.drop(Seq("STATE", "YEAR", "MONTH", "FATALS", "ST_CASE"))
println("After dropping nulls in critical columns: " + cleanedDF.count())

// replace FARS sentinel values with null
// HOUR=99 unknown, MINUTE=99 unknown
// LATITUDE=0 or out of US range means not recorded
// LONGITUD=0 or out of range means not recorded
val finalDF = cleanedDF
  .withColumn("HOUR",
    when(col("HOUR") === 99, lit(null).cast("int")).otherwise(col("HOUR")))
  .withColumn("MINUTE",
    when(col("MINUTE") === 99, lit(null).cast("int")).otherwise(col("MINUTE")))
  .withColumn("LATITUDE",
    when(col("LATITUDE") === 0.0 || col("LATITUDE") > 77.0 || col("LATITUDE") < 17.0,
      lit(null).cast("double")).otherwise(col("LATITUDE")))
  .withColumn("LONGITUD",
    when(col("LONGITUD") === 0.0 || abs(col("LONGITUD")) > 180.0,
      lit(null).cast("double")).otherwise(col("LONGITUD")))

println("\nAfter cleaning sentinel values:")
println("Null HOUR: " + finalDF.filter(col("HOUR").isNull).count())
println("Null MINUTE: " + finalDF.filter(col("MINUTE").isNull).count())
println("Null LATITUDE: " + finalDF.filter(col("LATITUDE").isNull).count())
println("Null LONGITUD: " + finalDF.filter(col("LONGITUD").isNull).count())

println("\nFinal dataset: " + finalDF.count() + " records, " + finalDF.columns.length + " columns")
finalDF.printSchema()
finalDF.show(10, false)

// write cleaned data to HDFS
val outputPath = "/user/jy4018_nyu_edu/project/output/fars_cleaned"
finalDF.write.mode("overwrite").option("header", "true").csv(outputPath)
println("Wrote cleaned data to " + outputPath)

// read back to verify
val checkDF = spark.read.option("header", "true").option("inferSchema", "true").csv(outputPath)
println("Verification - read back: " + checkDF.count() + " records")
checkDF.show(5, false)

// create Hive table
spark.sql("CREATE DATABASE IF NOT EXISTS jy4018_nyu_edu")

spark.sql("""
  CREATE EXTERNAL TABLE IF NOT EXISTS jy4018_nyu_edu.fars_accident_cleaned (
    ST_CASE INT,
    STATE INT,
    CITY INT,
    COUNTY INT,
    YEAR INT,
    MONTH INT,
    DAY INT,
    DAY_WEEK INT,
    HOUR INT,
    MINUTE INT,
    FATALS INT,
    PERSONS INT,
    PEDS INT,
    VE_TOTAL INT,
    VE_FORMS INT,
    ROUTE INT,
    HARM_EV INT,
    MAN_COLL INT,
    REL_ROAD INT,
    LGT_COND INT,
    WEATHER INT,
    LATITUDE DOUBLE,
    LONGITUD DOUBLE
  )
  ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
  STORED AS TEXTFILE
  LOCATION '/user/jy4018_nyu_edu/project/output/fars_cleaned'
  TBLPROPERTIES ('skip.header.line.count'='1')
""")

println("Hive table created")
spark.sql("DESCRIBE jy4018_nyu_edu.fars_accident_cleaned").show(false)
spark.sql("SELECT COUNT(*) as total FROM jy4018_nyu_edu.fars_accident_cleaned").show()
spark.sql("SELECT * FROM jy4018_nyu_edu.fars_accident_cleaned LIMIT 5").show(false)
