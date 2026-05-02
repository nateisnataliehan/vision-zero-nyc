import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object VisionZeroAnalytic {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Vision Zero Analytic - NYC Crashes")
      .getOrCreate()

    import spark.implicits._

    val inputPath  = "hdfs:///user/bh2514_nyu_edu/project/input/Motor_Vehicle_Collisions_-_Crashes_20260406.csv"
    val outputBase = "hdfs:///user/bh2514_nyu_edu/project/output/vision_zero_analytic"

    // explicit schema -- inferSchema cost an extra full pass and mis-typed
    // the int columns because of stray non-numeric values.
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

    val raw = spark.read
      .option("header", "true")
      .option("mode", "PERMISSIVE")
      .schema(crashSchema)
      .csv(inputPath)

    println("=== RAW RECORD COUNT ===")
    println(raw.count())

    // ETL notes:
    //  - the raw CSV starts 2012-07-01. first version didn't filter on year
    //    and the "pre-Vision-Zero" bucket ended up being a partial 6-month
    //    window, which dragged the baseline down. restricting to >= 2013
    //    so every year is complete.
    //  - also publishing a monthly time series below so the 2012 gap and
    //    the 2020 COVID dip are visible instead of hidden by annualization.
    val injuryCols = Seq(
      "NUMBER OF PERSONS INJURED",
      "NUMBER OF PERSONS KILLED",
      "NUMBER OF PEDESTRIANS INJURED",
      "NUMBER OF PEDESTRIANS KILLED",
      "NUMBER OF CYCLIST INJURED",
      "NUMBER OF CYCLIST KILLED",
      "NUMBER OF MOTORIST INJURED",
      "NUMBER OF MOTORIST KILLED"
    )

    val withDate = raw
      .withColumn("CRASH_DATE_FORMATTED", to_date(col("CRASH DATE"), "MM/dd/yyyy"))
      .filter(col("CRASH_DATE_FORMATTED").isNotNull)
      .withColumn("CRASH_YEAR",  year(col("CRASH_DATE_FORMATTED")))
      .withColumn("CRASH_MONTH", date_format(col("CRASH_DATE_FORMATTED"), "yyyy-MM"))
      .filter(col("CRASH_YEAR") >= 2013)

    val coerced = injuryCols.foldLeft(withDate) { (df, colName) =>
      df.withColumn(colName, coalesce(col(colName), lit(0)))
    }

    val withPolicy = coerced.withColumn(
      "VISION_ZERO_ERA",
      when(col("CRASH_YEAR") >= 2014, lit("post")).otherwise(lit("pre"))
    )

    // cache -- 7 aggregations below all hit this DF, without cache each one
    // re-reads from HDFS.
    withPolicy.cache()
    println("=== CLEANED RECORD COUNT (>= 2013 full years) ===")
    println(withPolicy.count())

    // ----- 1. annual summary -----
    val annual = withPolicy.groupBy("CRASH_YEAR").agg(
      count(lit(1)).alias("crashes"),
      sum("NUMBER OF PERSONS KILLED").alias("persons_killed"),
      sum("NUMBER OF PERSONS INJURED").alias("persons_injured"),
      sum("NUMBER OF PEDESTRIANS KILLED").alias("ped_killed"),
      sum("NUMBER OF CYCLIST KILLED").alias("cyc_killed"),
      sum("NUMBER OF MOTORIST KILLED").alias("mot_killed")
    ).orderBy("CRASH_YEAR")

    println("=== ANNUAL SUMMARY ===")
    annual.show(50, false)
    annual.coalesce(1).write.mode("overwrite").option("header", "true")
      .csv(s"$outputBase/annual_summary")

    // ----- 2. pre/post-2014 headline (annualized) -----
    val preYears  = withPolicy.filter(col("VISION_ZERO_ERA") === "pre").select("CRASH_YEAR").distinct().count()
    val postYears = withPolicy.filter(col("VISION_ZERO_ERA") === "post").select("CRASH_YEAR").distinct().count()
    println(s"=== Pre years=$preYears, Post years=$postYears ===")

    val prePost = withPolicy.groupBy("VISION_ZERO_ERA").agg(
      count(lit(1)).alias("crashes_total"),
      sum("NUMBER OF PERSONS KILLED").alias("killed_total"),
      sum("NUMBER OF PERSONS INJURED").alias("injured_total"),
      sum("NUMBER OF PEDESTRIANS KILLED").alias("ped_killed_total"),
      sum("NUMBER OF CYCLIST KILLED").alias("cyc_killed_total"),
      sum("NUMBER OF MOTORIST KILLED").alias("mot_killed_total")
    ).withColumn(
      "years_in_era",
      when(col("VISION_ZERO_ERA") === "pre", lit(preYears)).otherwise(lit(postYears))
    ).withColumn("crashes_per_year",     col("crashes_total")    / col("years_in_era"))
     .withColumn("killed_per_year",      col("killed_total")     / col("years_in_era"))
     .withColumn("injured_per_year",     col("injured_total")    / col("years_in_era"))
     .withColumn("ped_killed_per_year",  col("ped_killed_total") / col("years_in_era"))
     .withColumn("cyc_killed_per_year",  col("cyc_killed_total") / col("years_in_era"))
     .withColumn("mot_killed_per_year",  col("mot_killed_total") / col("years_in_era"))

    println("=== PRE/POST SUMMARY (annualized) ===")
    prePost.show(false)
    prePost.coalesce(1).write.mode("overwrite").option("header", "true")
      .csv(s"$outputBase/pre_post_summary")

    // ----- 3. borough pre/post (raw + per-capita) -----
    // raw counts make Brooklyn look most dangerous, but it also has the most
    // people (2.7M vs Staten Island's 0.5M). dividing by population so we're
    // comparing risk, not population size. figures from NYC DCP ~2020 ACS.
    val boroughPop = Seq(
      ("BRONX",         1472654),
      ("BROOKLYN",      2736074),
      ("MANHATTAN",     1694251),
      ("QUEENS",        2405464),
      ("STATEN ISLAND",  495747)
    ).toDF("BOROUGH_CLEAN", "population")

    val boroughDF = withPolicy
      .filter(col("BOROUGH").isNotNull && trim(col("BOROUGH")) =!= "")
      .withColumn("BOROUGH_CLEAN", upper(trim(col("BOROUGH"))))

    val boroughRaw = boroughDF.groupBy("BOROUGH_CLEAN", "VISION_ZERO_ERA").agg(
      count(lit(1)).alias("crashes"),
      sum("NUMBER OF PERSONS KILLED").alias("killed"),
      sum("NUMBER OF PERSONS INJURED").alias("injured")
    ).withColumn(
      "years_in_era",
      when(col("VISION_ZERO_ERA") === "pre", lit(preYears)).otherwise(lit(postYears))
    ).withColumn("crashes_per_year", col("crashes") / col("years_in_era"))
     .withColumn("killed_per_year",  col("killed")  / col("years_in_era"))
     .withColumn("injured_per_year", col("injured") / col("years_in_era"))

    // broadcast the 5-row population table so the join is map-side.
    // a plain join would shuffle the ~1.5M-row boroughRaw unnecessarily.
    val borough = boroughRaw.join(broadcast(boroughPop), Seq("BOROUGH_CLEAN"), "left")
      .withColumn("killed_per_100k_per_year",
        col("killed_per_year") * lit(100000.0) / col("population"))
      .withColumn("injured_per_100k_per_year",
        col("injured_per_year") * lit(100000.0) / col("population"))
      .orderBy("BOROUGH_CLEAN", "VISION_ZERO_ERA")

    println("=== BOROUGH PRE/POST (raw + per-capita) ===")
    borough.show(false)
    borough.coalesce(1).write.mode("overwrite").option("header", "true")
      .csv(s"$outputBase/borough_pre_post")

    // ----- 4. road-user decomposition -----
    // Vision Zero's 2014 action plan explicitly prioritized pedestrians, so
    // if the policy worked the way it was advertised, pedestrian fatalities
    // should drop more than motorist fatalities.
    val roadUser = withPolicy.groupBy("VISION_ZERO_ERA").agg(
      sum("NUMBER OF PEDESTRIANS INJURED").alias("ped_injured"),
      sum("NUMBER OF PEDESTRIANS KILLED").alias("ped_killed"),
      sum("NUMBER OF CYCLIST INJURED").alias("cyc_injured"),
      sum("NUMBER OF CYCLIST KILLED").alias("cyc_killed"),
      sum("NUMBER OF MOTORIST INJURED").alias("mot_injured"),
      sum("NUMBER OF MOTORIST KILLED").alias("mot_killed")
    ).withColumn(
      "years_in_era",
      when(col("VISION_ZERO_ERA") === "pre", lit(preYears)).otherwise(lit(postYears))
    ).withColumn("ped_killed_per_year",  col("ped_killed")  / col("years_in_era"))
     .withColumn("cyc_killed_per_year",  col("cyc_killed")  / col("years_in_era"))
     .withColumn("mot_killed_per_year",  col("mot_killed")  / col("years_in_era"))
     .withColumn("ped_injured_per_year", col("ped_injured") / col("years_in_era"))
     .withColumn("cyc_injured_per_year", col("cyc_injured") / col("years_in_era"))
     .withColumn("mot_injured_per_year", col("mot_injured") / col("years_in_era"))

    println("=== ROAD USER PRE/POST ===")
    roadUser.show(false)
    roadUser.coalesce(1).write.mode("overwrite").option("header", "true")
      .csv(s"$outputBase/road_user_pre_post")

    // ----- 5. top contributing factors -----
    val factor = withPolicy
      .filter(col("CONTRIBUTING FACTOR VEHICLE 1").isNotNull)
      .withColumn("FACTOR_CLEAN", upper(trim(col("CONTRIBUTING FACTOR VEHICLE 1"))))
      .filter(col("FACTOR_CLEAN") =!= "" && col("FACTOR_CLEAN") =!= "UNSPECIFIED")
      .groupBy("FACTOR_CLEAN", "VISION_ZERO_ERA").agg(
        count(lit(1)).alias("crashes"),
        sum("NUMBER OF PERSONS KILLED").alias("killed")
      ).withColumn(
        "years_in_era",
        when(col("VISION_ZERO_ERA") === "pre", lit(preYears)).otherwise(lit(postYears))
      ).withColumn("crashes_per_year", col("crashes") / col("years_in_era"))

    // previously the orderBy on ties was non-deterministic -- added
    // FACTOR_CLEAN as a secondary sort key so the top 15 is stable across
    // runs. broadcast() on topFactors so the join doesn't shuffle.
    val topFactors = factor
      .filter(col("VISION_ZERO_ERA") === "post")
      .orderBy(desc("crashes_per_year"), asc("FACTOR_CLEAN"))
      .limit(15)
      .select("FACTOR_CLEAN")
      .cache()

    val factorOut = factor.join(broadcast(topFactors), Seq("FACTOR_CLEAN"), "inner")
      .orderBy("FACTOR_CLEAN", "VISION_ZERO_ERA")

    println("=== TOP CONTRIBUTING FACTORS — PRE/POST ===")
    factorOut.show(50, false)
    factorOut.coalesce(1).write.mode("overwrite").option("header", "true")
      .csv(s"$outputBase/factor_pre_post")

    // ----- 6. monthly time series -----
    // shows the 2012 gap and the 2020 COVID dip explicitly. useful because
    // the annual table averages across them.
    val monthly = withPolicy.groupBy("CRASH_MONTH").agg(
      count(lit(1)).alias("crashes"),
      sum("NUMBER OF PERSONS KILLED").alias("killed"),
      sum("NUMBER OF PERSONS INJURED").alias("injured")
    ).orderBy("CRASH_MONTH")

    println("=== MONTHLY TIME SERIES (head 24) ===")
    monthly.show(24, false)
    monthly.coalesce(1).write.mode("overwrite").option("header", "true")
      .csv(s"$outputBase/monthly_time_series")

    // ----- 7. COVID window -----
    // published national data (IIHS 2021, NHTSA 2022) shows US crashes dropped
    // in 2020 but fatality rates per mile driven went up -- fewer cars on the
    // road, more reckless driving among the remaining ones. checking whether
    // NYC followed the same pattern by looking at killed-per-1k-crashes.
    val covidWindow = withPolicy
      .filter(col("CRASH_YEAR").between(2018, 2022))
      .groupBy("CRASH_YEAR").agg(
        count(lit(1)).alias("crashes"),
        sum("NUMBER OF PERSONS KILLED").alias("killed"),
        sum("NUMBER OF PERSONS INJURED").alias("injured")
      ).withColumn("fatality_rate_per_1k_crashes",
        col("killed") * lit(1000.0) / col("crashes"))
      .orderBy("CRASH_YEAR")

    println("=== COVID ANOMALY WINDOW 2018-2022 ===")
    covidWindow.show(false)
    covidWindow.coalesce(1).write.mode("overwrite").option("header", "true")
      .csv(s"$outputBase/covid_anomaly")

    // ----- 8. unspecified-factor rate by borough -----
    // officers fill CONTRIBUTING FACTOR manually and skip it often. if the
    // skip rate varies by borough, the factor analysis above is biased.
    // reporting the per-borough share so a reader can judge.
    val unspecifiedRate = boroughDF
      .withColumn("FACTOR_CLEAN", upper(trim(col("CONTRIBUTING FACTOR VEHICLE 1"))))
      .groupBy("BOROUGH_CLEAN").agg(
        count(lit(1)).alias("total"),
        sum(when(col("FACTOR_CLEAN").isNull || col("FACTOR_CLEAN") === "" ||
                col("FACTOR_CLEAN") === "UNSPECIFIED", 1).otherwise(0)
        ).alias("unspecified")
      ).withColumn("unspecified_share",
        col("unspecified").cast("double") / col("total"))
      .orderBy(desc("unspecified_share"))

    println("=== UNSPECIFIED contributing-factor rate by borough ===")
    unspecifiedRate.show(false)
    unspecifiedRate.coalesce(1).write.mode("overwrite").option("header", "true")
      .csv(s"$outputBase/bias_unspecified_rate")

    println(s"=== Vision Zero analytic outputs written under: $outputBase ===")
    topFactors.unpersist()
    withPolicy.unpersist()
    spark.stop()
  }
}

VisionZeroAnalytic.main(Array())
