import org.apache.spark.sql.functions._

// read cleaned data from Hive table
val df = spark.sql("SELECT * FROM jy4018_nyu_edu.fars_accident_cleaned WHERE ST_CASE IS NOT NULL")
println("Total records: " + df.count())

// Initial Analysis: mean, median, mode, stddev

// FATALS
println("\n--- FATALS stats ---")
val fatalMean = df.select(mean("FATALS")).first().getDouble(0)
val fatalStd = df.select(stddev("FATALS")).first().getDouble(0)
val fatalMedian = df.stat.approxQuantile("FATALS", Array(0.5), 0.001)(0)
val fatalModeRow = df.groupBy("FATALS").count().orderBy(desc("count")).first()
Seq(("mean", fatalMean.toString),
    ("median", fatalMedian.toString),
    ("mode", fatalModeRow(0).toString + " (" + fatalModeRow(1) + " times)"),
    ("stddev", fatalStd.toString)).toDF("stat", "value").show(false)

// PERSONS
println("--- PERSONS stats ---")
val personsMean = df.select(mean("PERSONS")).first().getDouble(0)
val personsStd = df.select(stddev("PERSONS")).first().getDouble(0)
val personsMedian = df.stat.approxQuantile("PERSONS", Array(0.5), 0.001)(0)
val personsModeRow = df.groupBy("PERSONS").count().orderBy(desc("count")).first()
Seq(("mean", personsMean.toString),
    ("median", personsMedian.toString),
    ("mode", personsModeRow(0).toString + " (" + personsModeRow(1) + " times)"),
    ("stddev", personsStd.toString)).toDF("stat", "value").show(false)

// PEDS
println("--- PEDS stats ---")
val pedsMean = df.select(mean("PEDS")).first().getDouble(0)
val pedsStd = df.select(stddev("PEDS")).first().getDouble(0)
val pedsMedian = df.stat.approxQuantile("PEDS", Array(0.5), 0.001)(0)
val pedsModeRow = df.groupBy("PEDS").count().orderBy(desc("count")).first()
Seq(("mean", pedsMean.toString),
    ("median", pedsMedian.toString),
    ("mode", pedsModeRow(0).toString + " (" + pedsModeRow(1) + " times)"),
    ("stddev", pedsStd.toString)).toDF("stat", "value").show(false)

// Cleaning option 1: Date formatting
// The raw FARS data stores the crash date as three separate integer columns (YEAR, MONTH, DAY). 
// This job combines them into a single proper DATE column called CRASH_DATE, formatted as "yyyy-MM-dd"

val withDate = df.withColumn("CRASH_DATE",
  to_date(concat_ws("-", col("YEAR"), col("MONTH"), col("DAY")), "yyyy-M-d"))

println("--- Cleaning 1: Date formatting ---")
withDate.select("YEAR", "MONTH", "DAY", "CRASH_DATE").show(10, false)

// Cleaning option 2: Binary column POST_VISION_ZERO
// This job adds a new binary column POST_VISION_ZERO based on the YEAR column.
// It equals 1 if the crash happened in 2014 or later (when NYC launched Vision Zero), and 0 otherwise. 

val finalDF = withDate.withColumn("POST_VISION_ZERO",
  when(col("YEAR") >= 2014, 1).otherwise(0))

println("--- Cleaning 2: Binary column POST_VISION_ZERO ---")
finalDF.groupBy("POST_VISION_ZERO").count().show()