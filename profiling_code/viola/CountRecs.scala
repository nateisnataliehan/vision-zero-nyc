import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

val basePath = "/user/jy4018_nyu_edu/project/input"

// columns we want (these exist in all 20 years)
val commonCols = Seq(
  "ST_CASE", "STATE", "CITY", "COUNTY",
  "YEAR", "MONTH", "DAY", "DAY_WEEK", "HOUR", "MINUTE",
  "FATALS", "PERSONS", "PEDS",
  "VE_TOTAL", "VE_FORMS",
  "ROUTE", "HARM_EV", "MAN_COLL", "REL_ROAD",
  "LGT_COND", "WEATHER",
  "LATITUDE", "LONGITUD"
)

// load each year, select common columns, and union them
var allData: DataFrame = null

for (year <- 2005 to 2024) {
  val path = basePath + "/accident_" + year + ".csv"
  val yearDF = spark.read.option("header", "true").option("inferSchema", "true").csv(path)

  // 2005-2007 have lowercase latitude/longitud
  // some years might not have LATITUDE column by that exact name
  val cols = commonCols.map { c =>
    if (c == "LATITUDE" && yearDF.columns.contains("latitude") && !yearDF.columns.contains("LATITUDE"))
      col("latitude").alias("LATITUDE")
    else if (c == "LONGITUD" && yearDF.columns.contains("longitud") && !yearDF.columns.contains("LONGITUD"))
      col("longitud").alias("LONGITUD")
    else if (c == "STATE" && !yearDF.columns.contains("STATE"))
      // 2022-2024 have BOM prefix on STATE column
      col(yearDF.columns.find(_.endsWith("STATE")).getOrElse("STATE")).alias("STATE")
    else
      col(c)
  }

  val selected = yearDF.select(cols: _*)

  if (allData == null) allData = selected
  else allData = allData.union(selected)
}

val rawDF = allData
rawDF.printSchema()

val totalRecords = rawDF.count()
println("Total records: " + totalRecords)

// map to (STATE, 1) and count with reduceByKey
val stateCountsRDD = rawDF.rdd
  .map(row => (row.getAs[Any]("STATE").toString, 1))
  .reduceByKey(_ + _)
  .sortByKey()

println("\nRecords per STATE:")
stateCountsRDD.collect().foreach { case (state, count) =>
  println("  " + state + " -> " + count)
}

val yearCountsRDD = rawDF.rdd
  .map(row => (row.getAs[Any]("YEAR").toString, 1))
  .reduceByKey(_ + _)
  .sortByKey()

println("\nRecords per YEAR:")
yearCountsRDD.collect().foreach { case (year, count) =>
  println("  " + year + " -> " + count)
}

// distinct values for each column used in the analytic
val analyticCols = Seq(
  "STATE", "CITY", "COUNTY",
  "YEAR", "MONTH", "DAY", "DAY_WEEK", "HOUR", "MINUTE",
  "FATALS", "PERSONS", "PEDS",
  "VE_TOTAL", "VE_FORMS",
  "ROUTE", "HARM_EV", "MAN_COLL", "REL_ROAD",
  "LGT_COND", "WEATHER"
)

analyticCols.foreach { c =>
  println("--- Distinct " + c + " ---")
  val d = rawDF.select(c).distinct()
  println("Count: " + d.count())
  d.orderBy(c).show(30, false)
}

// LATITUDE and LONGITUD are continuous, just show range
println("--- LATITUDE range ---")
println("Min: " + rawDF.agg(min("LATITUDE")).first()(0))
println("Max: " + rawDF.agg(max("LATITUDE")).first()(0))
println("Distinct count: " + rawDF.select("LATITUDE").distinct().count())

println("--- LONGITUD range ---")
println("Min: " + rawDF.agg(min("LONGITUD")).first()(0))
println("Max: " + rawDF.agg(max("LONGITUD")).first()(0))
println("Distinct count: " + rawDF.select("LONGITUD").distinct().count())

// basic stats
println("--- Summary Statistics ---")
rawDF.select("FATALS", "PERSONS", "PEDS", "VE_TOTAL", "HOUR").describe().show()

// null counts
println("--- Null Counts ---")
commonCols.foreach { c =>
  println(c + ": " + rawDF.filter(col(c).isNull).count())
}

println("\nDone. " + totalRecords + " records.")