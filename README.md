# Vision Zero NYC — Did the Policy Save Lives?

NYU Big Data, Spring 2026 — final project, my (Natalie Han) half of the analysis.

I analyzed roughly 2 million NYC Motor Vehicle Collision records on NYU Dataproc (Spark Scala on HDFS) to test whether NYC's 2014 Vision Zero policy reduced traffic fatalities, especially for pedestrians. The full project also includes a cross-city difference-in-differences analysis on NHTSA FARS data done by my teammate Viola Yu (jy4018); her half is linked below.

## Medium write-up

Public first-person narrative of the project:
<https://medium.com/@YOUR_MEDIUM_HANDLE/REPLACE_WITH_PUBLISHED_URL>

## Animation

`animation/vision_zero.gif` — short animation built **only** from the within-NYC analytic output. Plots NYC pedestrian and total fatalities 2013–2025, marks the 2014 Vision Zero policy line, and ends on the within-NYC pre/post pedestrian numbers (pre-2014: 176, post-2014 avg: 126, −28%). Source: `animation/make_animation.py`.

## Repo layout

```
.
├── README.md
├── ana_code/natalie/
│   └── VisionZeroAnalytic.scala        within-NYC pre/post analysis on the cleaned Hive table
├── data_ingest/
│   └── data_ingest_README.md           how the raw CSV gets onto HDFS
├── etl_code/natalie/
│   └── Clean.scala                     schema-by-hand load, BOROUGH_CLEAN, year filter, Hive write
├── profiling_code/natalie/
│   ├── CountRecs.scala                 raw record count + null distribution
│   └── FirstCode.scala                 cleaned-data stats + derived columns
├── screenshots/                        evidence every Spark job ran on Dataproc
└── animation/
    ├── make_animation.py               matplotlib FuncAnimation source
    ├── vision_zero.gif
    └── vision_zero.mp4
```

## Pipeline

1. **Ingest** — `hdfs dfs -put` the NYC Open Data collisions CSV (~2.25M rows, July 2012 onward) to `/user/bh2514_nyu_edu/project/input/`.
2. **Profiling (raw)** — `CountRecs.scala`. Confirms record count, dumps null rates per column, surfaces the `"Unspecified"` / `"NULL"` / empty-string / `"0"` mess in BOROUGH and the int columns.
3. **ETL** — `Clean.scala`. Hand-written schema (because `inferSchema` typed killed/injured as string), `BOROUGH_CLEAN` derived column, `CRASH_YEAR >= 2013` filter so the pre-Vision-Zero baseline is a complete calendar year, written to a Hive table with CTRL-A delimiter (CONTRIBUTING FACTOR has commas in it).
4. **Profiling (cleaned)** — `FirstCode.scala`. Min/max/mean/distinct on the cleaned table, plus derived columns and groupBys.
5. **Analytic** — `VisionZeroAnalytic.scala`. Annual summary, pre/post (2013 vs 2014+), borough breakdown raw and per 100k residents, road-user split, top-15 contributing factors, monthly time series, COVID anomaly window, per-borough UNSPECIFIED rate.

## How to run on Dataproc

```bash
# 1. ingest
hdfs dfs -mkdir -p /user/bh2514_nyu_edu/project/input
hdfs dfs -put Motor_Vehicle_Collisions_-_Crashes_*.csv \
  /user/bh2514_nyu_edu/project/input/

# 2-5. profiling, ETL, profiling, analytic
spark-shell --deploy-mode client -i profiling_code/natalie/CountRecs.scala
spark-shell --deploy-mode client -i etl_code/natalie/Clean.scala
spark-shell --deploy-mode client -i profiling_code/natalie/FirstCode.scala
spark-shell --deploy-mode client -i ana_code/natalie/VisionZeroAnalytic.scala
```

Output lands under `/user/bh2514_nyu_edu/project/output/vision_zero_analytic/`.

## HDFS access

```
hdfs dfs -setfacl -R -m user:adm209:r-x   /user/bh2514_nyu_edu/project
hdfs dfs -setfacl -R -m user:pd2672:r-x   /user/bh2514_nyu_edu/project
hdfs dfs -setfacl -R -m user:jy4018:r-x   /user/bh2514_nyu_edu/project
```

## Key within-NYC results

- 2013 pedestrian deaths: 176. 2014–2025 average: 126/yr. **−28% sustained drop**, holds through COVID.
- COVID anomaly: 2019 → 2022 crashes fell 55% (231k → 104k) but **fatality rate per 1k crashes nearly tripled** (1.15 → 2.79). Fewer drivers, more reckless.
- Borough per-capita normalization: raw counts rank Brooklyn highest on absolute fatalities, but per-100k-residents reshuffles the ranking. Both columns published in `borough_pre_post/` so a reader can see the shift.
- UNSPECIFIED contributing-factor share ranges 30% (Manhattan) to 40% (Bronx). Top-15 contributing factor table should be read as approximate.

(Viola's separate cross-city analysis on NHTSA FARS is in her own repo, linked below — not what this repo is about.)

## Bias and ethics notes

- NYPD-reported, so subject to officer-level subjectivity and precinct-level reporting variation.
- Borough average can hide neighborhood-level differences (ecological fallacy). Findings are not statements about any specific street or zip code.
- Single pre-year (2013) is a thin baseline; results are reported as multi-year averages or year-over-year curves rather than single-year comparisons.
- 2020 COVID structural break overlaps the post period. The 2018–2022 window is published separately so a reader can judge policy effect vs lockdown effect.

## Teammate's half

Viola Yu (jy4018) — cross-city DiD on NHTSA FARS, 2005–2024, ~690k fatal crash records: <https://github.com/REPLACE_WITH_VIOLA_REPO>

## Course

NYU Tandon — Realtime and Big Data Analytics — Spring 2026.
