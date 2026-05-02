# Did Vision Zero Save Lives?
# Measuring the Causal Impact of NYC's Vision Zero Policy on Traffic Fatalities

## Team
- Viola Yu (jy4018) - FARS national crash data
- Natalie Han (bh2514) - NYC Motor Vehicle Collisions data

## Project Description
This project analyzes the impact of NYC's Vision Zero policy on traffic
fatalities using two complementary analyses:

1. A **difference-in-differences (DiD) analysis** across US cities, comparing
   NYC against Chicago, LA, Houston, and Philadelphia from 2005-2024 using
   NHTSA FARS data, to identify the causal effect of the policy.
2. A **within-NYC pre/post analysis** of the NYC Open Data collisions record
   from 2013 onward, broken down by borough, road-user type, and
   contributing factor, to characterize where and how the policy's effects
   show up on the ground.

The two analyses are on equal footing: the DiD gives a causal estimate at
the city level, and the within-NYC breakdown shows the distributional and
mechanism-level detail that a single DiD coefficient can't.

## Directory Structure
```
├── README.md
├── ana_code/
│   ├── viola/
│   │   └── VisionZeroAnalysis.scala  (DiD across cities on FARS)
│   └── natalie/
│       └── VisionZeroAnalytic.scala  (within-NYC pre/post on NYC collisions)
├── data_ingest/
│   └── data_ingest_README.md         (both sources: FARS + NYC Open Data)
├── etl_code/
│   ├── viola/
│   │   └── Clean.scala               (FARS cleaning + Hive table)
│   └── natalie/
│       └── Clean.scala               (NYC collisions cleaning + Hive table)
├── profiling_code/
│   ├── viola/
│   │   ├── CountRecs.scala           (raw FARS profiling)
│   │   └── FirstCode.scala           (cleaned FARS stats + derived columns)
│   └── natalie/
│       ├── CountRecs.scala           (raw NYC collisions profiling)
│       └── FirstCode.scala           (cleaned NYC stats + derived columns)
├── screenshots/                      (evidence every step ran)
└── animation/
    ├── make_animation.py               matplotlib FuncAnimation source
    ├── vision_zero.gif
    └── vision_zero.mp4
```

## Data Locations (HDFS)

### Viola — FARS
- Raw input: `/user/jy4018_nyu_edu/project/input/accident_*.csv`
- Cleaned output: `/user/jy4018_nyu_edu/project/output/fars_cleaned/`
- FirstCode output: `/user/jy4018_nyu_edu/project/output/fars_firstcode/`
- Analysis output: `/user/jy4018_nyu_edu/project/output/yearly_trends/`
- DiD summary: `/user/jy4018_nyu_edu/project/output/did_summary/`
- Hive table: `jy4018_nyu_edu.fars_accident_cleaned`

### Natalie — NYC Motor Vehicle Collisions
- Raw input: `/user/bh2514_nyu_edu/project/input/Motor_Vehicle_Collisions_-_Crashes_20260406.csv`
- Cleaned output: `/user/bh2514_nyu_edu/project/output/cleaned_crashes_v2/`
- Within-NYC analytic outputs under:
  `/user/bh2514_nyu_edu/project/output/vision_zero_analytic/`
    - `annual_summary/` — year-by-year totals
    - `pre_post_summary/` — annualized pre-2014 vs post-2014 headline
    - `borough_pre_post/` — borough breakdown, raw + per-capita (per 100k residents)
    - `road_user_pre_post/` — pedestrian / cyclist / motorist split
    - `factor_pre_post/` — top-15 contributing-factor shifts
    - `monthly_time_series/` — month-by-month counts (exposes 2012 partial year + 2020 COVID dip)
    - `covid_anomaly/` — 2018-2022 window with fatality rate per 1k crashes
    - `bias_unspecified_rate/` — per-borough share of UNSPECIFIED contributing-factor entries
- Hive table: `bh2514_db.nyc_crashes_clean`

## Data Sources
- **FARS:** NHTSA Fatality Analysis Reporting System — 20 years of data
  (2005-2024), ~690,000 fatal crash records.
  https://www.nhtsa.gov/file-downloads?p=nhtsa/downloads/FARS/
- **NYC Open Data — Motor Vehicle Collisions:** NYPD-reported collisions,
  July 2012 onward, ~2M rows.
  https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95

## How to Run

All commands run on NYU Dataproc: https://dataproc.hpc.nyu.edu/ssh

### Step 1: Data Ingestion
See `data_ingest/README.md`

### Step 2: Data Profiling (raw data)
```bash
spark-shell --deploy-mode client -i CountRecs.scala
```

### Step 3: Data Cleaning
```bash
spark-shell --deploy-mode client -i Clean.scala
```

### Step 4: Data Profiling (cleaned data)
```bash
spark-shell --deploy-mode client -i FirstCode.scala
```

### Step 5: Final Analysis
```bash
spark-shell --deploy-mode client -i VisionZeroAnalysis.scala
```


## Key Results

### Cross-city DiD (Viola / FARS)
- NYC total fatalities dropped 21.9% after Vision Zero (2014+)
- Control cities increased 16.1% in the same period
- DiD estimate: -92.42 fewer fatalities per year attributable to Vision Zero
- Pedestrian fatalities in NYC dropped 22.6% while control cities increased 43.9%
- Pedestrian DiD estimate: -66.11 fewer pedestrian fatalities per year
- Placebo test (fake 2010 treatment): 10.14 — small value confirms no spurious effect

### Within-NYC pre/post (Natalie / NYC Open Data)
- Manhattan has the highest per-capita fatality rate across both eras, not
  Brooklyn — raw counts hide this because Brooklyn has 5x Manhattan's
  daytime-resident population.
- Pedestrian fatalities per year fell faster than motorist fatalities per year
  post-2014, consistent with the policy's stated pedestrian focus.
- "Driver Inattention/Distraction" stays the dominant contributing factor in
  both eras; the rank of the top 15 doesn't shuffle much, so Vision Zero looks
  like a levels effect, not a cause-mix effect.
- COVID anomaly: 2020 crashes fell sharply vs 2019, but the fatality rate per
  1,000 crashes went up — consistent with the national pattern (IIHS 2021,
  NHTSA 2022) of fewer but more reckless drivers during lockdown.
- Bias audit: the UNSPECIFIED share of CONTRIBUTING FACTOR varies by borough,
  so the factor analysis should be read as approximate.

## Ethical Guardrails & Data Governance

### Data Bias
- FARS only records fatal crashes, not all accidents. Communities with less access
  to emergency services may have higher fatality rates due to slower response, not
  necessarily more crashes.
- NYC collisions are NYPD-reported. Minor injuries in lower-reporting precincts
  may be undercounted, and CONTRIBUTING FACTOR is filled in by the responding
  officer, so it inherits officer-level subjectivity.
- Crash reporting practices vary across jurisdictions and over time.

### Algorithmic-Bias Check (NYC)
- Raw borough counts scale with population and are misleading on their own, so
  the borough output also reports killed/injured per 100k residents. The
  per-capita numbers flip the ranking (Manhattan > Brooklyn on per-capita
  fatality rate, the opposite of the raw totals).
- The per-borough UNSPECIFIED rate for CONTRIBUTING FACTOR is published in
  `bias_unspecified_rate/` so a reader can see how much of the factor table
  is driven by missing entries before trusting it.

### Ecological Fallacy
- The NYC analysis goes down to the borough level, but no further. A borough
  average can hide very different patterns at the neighborhood or
  intersection level, so the findings shouldn't be read as statements about
  any specific street or zip code.

### Control City Limitation
- Chicago (2017) and Philadelphia (2017) later adopted Vision Zero. LA adopted it
  in 2015. This contaminates the control group after those dates, likely making our
  estimate conservative. Houston has no Vision Zero and is the cleanest control.

### Confounding (NYC)
- The raw CSV starts 2012-07-01, so the pre-2014 bucket would only be
  ~1.5 years of data and is dragged by partial-year sampling. We filter to
  CRASH_YEAR >= 2013 so pre and post are both complete calendar years, and we
  annualize everything (`per_year` columns) to keep them comparable. The single
  pre-year (2013) is still a weak baseline — this is why the cross-city DiD on
  FARS is the primary causal claim.
- The 2020 COVID structural break affects the post period. We publish the
  2018-2022 window separately so the reader can judge how much of the
  post-2014 drop is policy vs. lockdown.

### Alternative Approaches
- Could include more control cities to increase sample size and improve robustness.
- Could join the Vehicle table from FARS to get speed limit data (SP_LIMIT) for a
  more detailed analysis of speeding-related crashes before and after Vision Zero.
- Could cross-check NYC's NYPD-reported collisions against the same city's
  FARS fatalities to see whether borough-level reporting patterns match.

### Privacy
- FARS contains no personally identifiable information. Data is publicly
  available from NHTSA.
- NYC Open Data collisions don't include names, license plates, or VINs. We
  keep the analysis at the borough level so nothing in our output can be
  tied back to an individual crash.

## Code Improvements Over Time

### Viola — FARS
- Initially used mergeSchema to load all 20 years at once, but failed due to column
  name differences across years (lowercase latitude in 2005-2007, BOM in 2022-2024).
  Switched to loading each year individually with column name normalization.
- Added sentinel value handling (HOUR=99, bad lat/long) after finding them during profiling.
- Removed columns missing in many years (SP_LIMIT, DRUNK_DR, CF1-CF3) for consistency.

### Natalie — NYC Collisions
- `inferSchema` typed the int columns as string because a few rows have
  "Unspecified" in them, so `max()` returned "Unspecified" lexicographically.
  Wrote the schema out by hand.
- BOROUGH took three tries — null, then empty string, then the literal
  strings "NULL" and "0" that also show up in the raw CSV.
- Hive output uses CTRL-A (0x01) as the delimiter; comma broke on
  CONTRIBUTING FACTOR, which is free text and sometimes has commas in it.
- Year filter started at `>= 2012`, but the raw CSV begins 2012-07-01 and
  that partial year dragged the pre-Vision-Zero baseline down. Tightened to
  `>= 2013`.
- Added `cache()` on the post-ETL DataFrame — 7 aggregations were each
  re-reading from HDFS.
- Raw counts made Brooklyn look most dangerous, but it has ~5x Staten
  Island's population. Broadcast-joined a 5-row population table — Manhattan
  is actually worse per capita.


## HDFS Access
Read access granted to: adm209, pd2672, bh2514 (on Viola's paths) and
jy4018 (on Natalie's paths). 
