// VisionZeroAnalysis.scala
// DiD analysis of Vision Zero's effect on NYC traffic fatalities
// spark-shell --deploy-mode client -i VisionZeroAnalysis.scala

import org.apache.spark.sql.functions._

val raw = spark.sql("SELECT * FROM jy4018_nyu_edu.fars_accident_cleaned WHERE ST_CASE IS NOT NULL")
println("Total records: " + raw.count())

// filter to NYC and control cities
// looked up GSA city codes from FARS data: NYC=4170, Chicago=1670, LA=1980, Houston=3280, Philly=6540
val df = raw.filter(
  (col("STATE") === 36 && col("CITY") === 4170) ||
  (col("STATE") === 17 && col("CITY") === 1670) ||
  (col("STATE") === 6 && col("CITY") === 1980) ||
  (col("STATE") === 48 && col("CITY") === 3280) ||
  (col("STATE") === 42 && col("CITY") === 6540)
).withColumn("CITY_NAME",
  when(col("STATE") === 36 && col("CITY") === 4170, "NYC")
  .when(col("STATE") === 17 && col("CITY") === 1670, "Chicago")
  .when(col("STATE") === 6 && col("CITY") === 1980, "LA")
  .when(col("STATE") === 48 && col("CITY") === 3280, "Houston")
  .when(col("STATE") === 42 && col("CITY") === 6540, "Philadelphia")
)

// DiD variables
val didDF = df
  .withColumn("TREATMENT", when(col("CITY_NAME") === "NYC", 1).otherwise(0))
  .withColumn("POST", when(col("YEAR") >= 2014, 1).otherwise(0))
  .withColumn("TREAT_POST", col("TREATMENT") * col("POST"))

println("Crashes in treatment + control cities: " + didDF.count())
didDF.groupBy("CITY_NAME").count().orderBy("CITY_NAME").show()

// yearly fatalities by city — can use this to check parallel trends
println("\n=== ANNUAL FATALITIES BY CITY ===")
val yearly = didDF.groupBy("CITY_NAME", "YEAR")
  .agg(sum("FATALS").alias("total_fatalities"),
       count("*").alias("num_crashes"))
  .orderBy("CITY_NAME", "YEAR")

yearly.show(100, false)

yearly.coalesce(1).write.mode("overwrite").option("header", "true")
  .csv("/user/jy4018_nyu_edu/project/output/yearly_trends")

// DiD on total fatalities
println("\n=== DiD: TOTAL FATALITIES ===")

val didTable = didDF.groupBy("CITY_NAME", "POST")
  .agg(sum("FATALS").alias("fatalities"),
       count("*").alias("crashes"))
  .withColumn("fatalities_per_year",
    col("fatalities") / when(col("POST") === 0, 9).otherwise(11))

didTable.orderBy("CITY_NAME", "POST").show(false)

// DiD = (NYC_post - NYC_pre) - (Control_post - Control_pre)
val nycPre = didTable.filter(col("CITY_NAME") === "NYC" && col("POST") === 0)
  .first().getAs[Double]("fatalities_per_year")
val nycPost = didTable.filter(col("CITY_NAME") === "NYC" && col("POST") === 1)
  .first().getAs[Double]("fatalities_per_year")

val controlPreAvg = didTable.filter(col("CITY_NAME") =!= "NYC" && col("POST") === 0)
  .agg(avg("fatalities_per_year")).first().getDouble(0)
val controlPostAvg = didTable.filter(col("CITY_NAME") =!= "NYC" && col("POST") === 1)
  .agg(avg("fatalities_per_year")).first().getDouble(0)

val nycChange = nycPost - nycPre
val controlChange = controlPostAvg - controlPreAvg
val didEstimate = nycChange - controlChange

println(f"\nNYC pre-2014 avg:       $nycPre%.2f fatalities/yr")
println(f"NYC post-2014 avg:      $nycPost%.2f fatalities/yr")
println(f"NYC change:             $nycChange%.2f")
println(f"Control pre-2014 avg:   $controlPreAvg%.2f fatalities/yr")
println(f"Control post-2014 avg:  $controlPostAvg%.2f fatalities/yr")
println(f"Control change:         $controlChange%.2f")
println(f"DiD estimate:           $didEstimate%.2f fatalities/yr attributable to Vision Zero")
val nycPct = (nycChange/nycPre)*100
val ctrlPct = (controlChange/controlPreAvg)*100
println(f"NYC %% change:           $nycPct%.1f%%")
println(f"Control %% change:       $ctrlPct%.1f%%")

// same thing but only for pedestrian crashes since Vision Zero mainly targeted pedestrians
println("\n=== DiD: PEDESTRIAN CRASHES ONLY (PEDS > 0) ===")

val pedDF = didDF.filter(col("PEDS") > 0)
println("Pedestrian-involved crashes: " + pedDF.count())

val pedTable = pedDF.groupBy("CITY_NAME", "POST")
  .agg(sum("FATALS").alias("ped_fatalities"),
       count("*").alias("ped_crashes"))
  .withColumn("ped_fatalities_per_year",
    col("ped_fatalities") / when(col("POST") === 0, 9).otherwise(11))

pedTable.orderBy("CITY_NAME", "POST").show(false)

val nycPedPre = pedTable.filter(col("CITY_NAME") === "NYC" && col("POST") === 0)
  .first().getAs[Double]("ped_fatalities_per_year")
val nycPedPost = pedTable.filter(col("CITY_NAME") === "NYC" && col("POST") === 1)
  .first().getAs[Double]("ped_fatalities_per_year")

val controlPedPreAvg = pedTable.filter(col("CITY_NAME") =!= "NYC" && col("POST") === 0)
  .agg(avg("ped_fatalities_per_year")).first().getDouble(0)
val controlPedPostAvg = pedTable.filter(col("CITY_NAME") =!= "NYC" && col("POST") === 1)
  .agg(avg("ped_fatalities_per_year")).first().getDouble(0)

val nycPedChange = nycPedPost - nycPedPre
val controlPedChange = controlPedPostAvg - controlPedPreAvg
val pedDidEstimate = nycPedChange - controlPedChange

println(f"\nNYC ped pre-2014:       $nycPedPre%.2f fatalities/yr")
println(f"NYC ped post-2014:      $nycPedPost%.2f fatalities/yr")
val nycPedPct = (nycPedChange/nycPedPre)*100
val ctrlPedPct = (controlPedChange/controlPedPreAvg)*100
println(f"NYC ped %% change:       $nycPedPct%.1f%%")
println(f"Control ped %% change:   $ctrlPedPct%.1f%%")
println(f"Ped DiD estimate:       $pedDidEstimate%.2f fatalities/yr")

// placebo test: pretend 2010 is the treatment year using only pre-2014 data
// if we get a big effect here too, our real result might just be noise
println("\n=== PLACEBO TEST: fake treatment year 2010 ===")

val placeboDF = didDF.filter(col("YEAR") < 2014)
  .withColumn("PLACEBO_POST", when(col("YEAR") >= 2010, 1).otherwise(0))

val placeboTable = placeboDF.groupBy("CITY_NAME", "PLACEBO_POST")
  .agg(sum("FATALS").alias("fatalities"))
  .withColumn("fatalities_per_year",
    col("fatalities") / when(col("PLACEBO_POST") === 0, 5).otherwise(4))

placeboTable.orderBy("CITY_NAME", "PLACEBO_POST").show(false)

val nycPlaceboPre = placeboTable.filter(col("CITY_NAME") === "NYC" && col("PLACEBO_POST") === 0)
  .first().getAs[Double]("fatalities_per_year")
val nycPlaceboPost = placeboTable.filter(col("CITY_NAME") === "NYC" && col("PLACEBO_POST") === 1)
  .first().getAs[Double]("fatalities_per_year")
val ctrlPlaceboPre = placeboTable.filter(col("CITY_NAME") =!= "NYC" && col("PLACEBO_POST") === 0)
  .agg(avg("fatalities_per_year")).first().getDouble(0)
val ctrlPlaceboPost = placeboTable.filter(col("CITY_NAME") =!= "NYC" && col("PLACEBO_POST") === 1)
  .agg(avg("fatalities_per_year")).first().getDouble(0)

val placeboEffect = (nycPlaceboPost - nycPlaceboPre) - (ctrlPlaceboPost - ctrlPlaceboPre)
println(f"\nPlacebo DiD (fake 2010 treatment): $placeboEffect%.2f")
println("(small value = good, means no spurious effect)")

// save results
val summary = Seq(
  ("Real DiD (2014) - total fatalities", didEstimate),
  ("Real DiD (2014) - pedestrian fatalities", pedDidEstimate),
  ("Placebo DiD (2010) - total fatalities", placeboEffect)
).toDF("analysis", "did_estimate_per_year")

summary.coalesce(1).write.mode("overwrite").option("header", "true")
  .csv("/user/jy4018_nyu_edu/project/output/did_summary")

println("\nDone. Results saved to yearly_trends and did_summary.")