# Data Ingestion

## Source 1 — FARS Accident Data (Viola Yu)

### Data Source
NHTSA Fatality Analysis Reporting System (FARS)
https://www.nhtsa.gov/file-downloads?p=nhtsa/downloads/FARS/

### Steps

#### 1. Download data
Download the National CSV zip for each year (2005-2024) from the NHTSA website.
Extract only `accident.csv` from each zip and rename to `accident_YYYY.csv`.

#### 2. Upload to Dataproc cluster
Upload all 20 CSV files via browser SSH at https://dataproc.hpc.nyu.edu/ssh

#### 3. Put files into HDFS
```bash
hdfs dfs -mkdir -p /user/jy4018_nyu_edu/project/input
hdfs dfs -put ~/accident_*.csv /user/jy4018_nyu_edu/project/input/
```

#### 4. Verify
```bash
hdfs dfs -ls /user/jy4018_nyu_edu/project/input/
```

### HDFS Location
`/user/jy4018_nyu_edu/project/input/accident_*.csv`

---

## Source 2 — NYC Motor Vehicle Collisions (Natalie Han)

### Data Source
NYC Open Data — Motor Vehicle Collisions - Crashes
https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95

### Steps

#### 1. Download data
Downloaded as a single CSV (`Motor_Vehicle_Collisions_-_Crashes_20260406.csv`).

#### 2. Upload to Dataproc and put into HDFS
```bash
hdfs dfs -mkdir -p /user/bh2514_nyu_edu/project/input
hdfs dfs -put Motor_Vehicle_Collisions_-_Crashes_20260406.csv \
  /user/bh2514_nyu_edu/project/input/
```

#### 3. Verify
```bash
hdfs dfs -ls /user/bh2514_nyu_edu/project/input/
```

### HDFS Location
`/user/bh2514_nyu_edu/project/input/Motor_Vehicle_Collisions_-_Crashes_20260406.csv`