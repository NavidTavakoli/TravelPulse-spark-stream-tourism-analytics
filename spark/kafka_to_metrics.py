#!/usr/bin/env python3
# spark/kafka_to_metrics.py
#
# Structured Streaming job that reads Weather / Flights / Bookings topics from Kafka,
# computes near‑real‑time tourism metrics, and pushes them to Prometheus Pushgateway.

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window, when, lit, coalesce,
    avg, count, sum as _sum, expr,
    to_date, current_date, month, abs as _abs, greatest, least,
    broadcast
)
from pyspark.sql.types import *

print(">>> starting kafka_to_metrics_fixed.py ...")

# ============================== Config ==============================
PUSHGATEWAY   = os.environ.get("PUSHGATEWAY_URL", "http://localhost:9091")
JOB_NAME      = os.environ.get("JOB_NAME", "tourism_stream")

KAFKA_BOOTSTRAP  = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
STARTING_OFFSETS = os.environ.get("STARTING_OFFSETS", "earliest")  
FAIL_ON_DATALOSS = os.environ.get("FAIL_ON_DATALOSS", "false")

# Windows
WINDOW_SIZE_MINUTE = os.environ.get("WINDOW_SIZE", "1 minute")
WINDOW_30D         = os.environ.get("WINDOW_30D", "30 days")
WINDOW_365D        = os.environ.get("WINDOW_365D", "365 days")
WATERMARK          = os.environ.get("WATERMARK", "45 seconds")
TRIGGER            = os.environ.get("TRIGGER", "10 seconds")
DEBUG              = os.environ.get("DEBUG_STREAM","0") == "1"
TOPN               = int(os.environ.get("TOPN", "10"))

try:
    import requests
    _HAS_REQ = True
except Exception as e:
    print("!!! 'requests' not found; disabling Pushgateway metrics. (pip install requests) ->", e)
    _HAS_REQ = False


def push_metrics(metric_dict, grouping_key="instance", grouping_val="local"):
    """
    metric_dict: {metric_name: (labels_dict|None, value)}
    """
    if not _HAS_REQ or not metric_dict:
        return
    lines = []
    for mname, (labels, value) in metric_dict.items():
        try:
            val = float(value)
        except Exception:
            continue
        if labels:
            label_str = ",".join(f'{k}="{v}"' for k, v in labels.items())
            lines.append(f'{mname}{{{label_str}}} {val}')
        else:
            lines.append(f'{mname} {val}')
    if not lines:
        return
    body = "\n".join(lines) + "\n"
    url = f"{PUSHGATEWAY}/metrics/job/{JOB_NAME}/{grouping_key}/{grouping_val}"
    try:
        r = requests.post(url, data=body.encode("utf-8"), timeout=3)
        r.raise_for_status()
        print(f">>> pushed {len(lines)} metrics to {url}")
    except Exception as e:
        print("Pushgateway error:", e)

# ============================== Schemas ==============================
weather_schema = StructType([
    StructField("schema_version", StringType()),
    StructField("event_type", StringType()),
    StructField("event_ts", StringType()),
    StructField("ingest_ts", StringType()),
    StructField("trace_id", StringType()),
    StructField("producer", StringType()),
    StructField("city_id", StringType()),
    StructField("date", StringType()),
    StructField("t_min", DoubleType()),
    StructField("t_max", DoubleType()),
    StructField("precip_mm", DoubleType()),
    StructField("wind_kph", DoubleType()),
    StructField("humidity", DoubleType()),
    StructField("condition", StringType()),
    StructField("is_peak_season", BooleanType()),
    StructField("holiday_flag", BooleanType()),
])

flight_schema = StructType([
    StructField("schema_version", StringType()),
    StructField("event_type", StringType()),
    StructField("event_ts", StringType()),
    StructField("ingest_ts", StringType()),
    StructField("trace_id", StringType()),
    StructField("producer", StringType()),
    StructField("flight_id", StringType()),
    StructField("airline", StringType()),
    StructField("service_date", StringType()),
    StructField("origin_iata", StringType()),
    StructField("destination_iata", StringType()),
    StructField("destination_city_id", StringType()),
    StructField("scheduled_departure", StringType()),
    StructField("actual_departure", StringType()),
    StructField("scheduled_arrival", StringType()),
    StructField("actual_arrival", StringType()),
    StructField("status", StringType()),
    StructField("delay_min", IntegerType()),
    StructField("load_factor", DoubleType()),
    StructField("aircraft_type", StringType()),
    StructField("seats", IntegerType()),
    StructField("weather_factor", DoubleType()),
    StructField("airport_congestion", DoubleType()),
])

booking_schema = StructType([
    StructField("schema_version", StringType()),
    StructField("event_type", StringType()),
    StructField("event_ts", StringType()),
    StructField("ingest_ts", StringType()),
    StructField("trace_id", StringType()),
    StructField("producer", StringType()),
    StructField("booking_id", StringType()),
    StructField("hotel_id", StringType()),
    StructField("city_id", StringType()),
    StructField("city_name", StringType()),
    StructField("checkin_date", StringType()),
    StructField("checkout_date", StringType()),
    StructField("nights", IntegerType()),
    StructField("guests", IntegerType()),
    StructField("rooms", IntegerType()),
    StructField("channel", StringType()),
    StructField("lead_time_days", IntegerType()),
    StructField("adr_proxy", DoubleType()),
    StructField("currency", StringType()),
    StructField("is_refundable", BooleanType()),
    StructField("status", StringType()),
    StructField("cancel_ts", StringType()),
    StructField("flight_anchor", MapType(StringType(), IntegerType())),
])

# ============================== Spark ==============================
print(">>> building SparkSession ...")
spark = (SparkSession.builder
         .appName("kafka-to-prometheus")
         .config("spark.sql.shuffle.partitions", "4")
         # If needed, add Kafka package coord matching your Spark/Scala versions
         # .config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")
print(">>> SparkSession created.")


def read_kafka(topic: str):
    print(f">>> creating reader for topic {topic}")
    return (spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
            .option("subscribe", topic)
            .option("startingOffsets", STARTING_OFFSETS)
            .option("failOnDataLoss", FAIL_ON_DATALOSS)
            .load()
            .selectExpr("CAST(key AS STRING) as key", "CAST(value AS STRING) as value", "timestamp"))

weather_raw = read_kafka(os.environ.get("WEATHER_TOPIC", "weather.events.v1"))
flight_raw  = read_kafka(os.environ.get("FLIGHT_TOPIC",  "flight.events.v1"))
booking_raw = read_kafka(os.environ.get("BOOKING_TOPIC", "booking.events.v1"))
print(">>> Kafka sources created.")

# ======================= Parse + robust timestamps =======================

def parse(df, schema):
    parsed = df.select(
        col("key"),
        from_json(col("value"), schema).alias("js"),
        col("timestamp").alias("kafka_ts")
    ).select("key", "js.*", "kafka_ts")

    event_time_raw  = to_timestamp(col("event_ts"))
    ingest_time_raw = to_timestamp(col("ingest_ts"))

    event_time  = when(event_time_raw.isNull(), col("kafka_ts")).otherwise(event_time_raw)
    ingest_time = when(ingest_time_raw.isNull() | (ingest_time_raw < col("kafka_ts")), col("kafka_ts")).otherwise(ingest_time_raw)

    return parsed.withColumn("event_time", event_time) \
                 .withColumn("ingest_time", ingest_time)

weather_base  = parse(weather_raw, weather_schema)
flights_base  = parse(flight_raw,  flight_schema)
bookings_base = parse(booking_raw, booking_schema)

# ======================= Enrich bookings (spend, dates) ===================
bookings_enriched = (
    bookings_base
    .withColumn("spend_eur",
        coalesce(col("adr_proxy"), lit(0.0)) * coalesce(col("rooms"), lit(1)) * coalesce(col("nights"), lit(1))
    )
    .withColumn("arrival_day", to_date(col("checkin_date")))  
    .withColumn("event_day",   to_date(col("event_time")))
)

# ======================= Watermarks: ingest-time vs event-time =====================
# Ingest-time (1-minute operational monitors)
weather_ing  = weather_base.withWatermark("ingest_time", WATERMARK)
flights_ing  = flights_base.withWatermark("ingest_time", WATERMARK)
bookings_ing = bookings_base.withWatermark("ingest_time", WATERMARK)

# Event-time (30d/365d windows, seasonal scores)
weather_evt  = weather_base.withWatermark("event_time", WATERMARK)
flights_evt  = flights_base.withWatermark("event_time", WATERMARK) \
                        .withColumnRenamed("destination_city_id", "city_id")
bookings_evt = bookings_enriched.withWatermark("event_time", WATERMARK)

# ================== Debug counter (no window, just to ensure flow) =================
ingest_counter = (
    weather_ing.select(lit(1).alias("one"))
               .groupBy()
               .agg(count("one").alias("records_in_trigger"))
)

# ============================ Minute-level monitoring ==============================
w_cnt = (weather_ing.groupBy(window(col("ingest_time"), WINDOW_SIZE_MINUTE))
         .agg(count(lit(1)).alias("weather_count")))

f_cnt = (flights_ing.groupBy(window(col("ingest_time"), WINDOW_SIZE_MINUTE))
         .agg(count(lit(1)).alias("flights_count"),
              _sum(when(col("status")=="cancelled", 1).otherwise(0)).alias("flights_cancelled"),
              avg(when(col("delay_min").isNotNull(), col("delay_min"))).alias("avg_delay_min")))

b_cnt = (bookings_ing.groupBy(window(col("ingest_time"), WINDOW_SIZE_MINUTE))
         .agg(count(lit(1)).alias("bookings_count"),
              avg(col("adr_proxy")).alias("avg_adr")))

# ===================== Airports: inbound/outbound + total ===========================
inbound_by_airport = (
    flights_ing.groupBy(window(col("ingest_time"), WINDOW_SIZE_MINUTE), col("destination_iata").alias("airport"))
               .agg(count(lit(1)).alias("inbound"))
)
outbound_by_airport = (
    flights_ing.groupBy(window(col("ingest_time"), WINDOW_SIZE_MINUTE), col("origin_iata").alias("airport"))
               .agg(count(lit(1)).alias("outbound"))
)


def push_airports_top(df, direction, metric="tourism_airport_flights_per_min"):
    val_col = "inbound" if direction=="inbound" else "outbound"
    rows = df.orderBy(col(val_col).desc()).limit(TOPN).collect()
    metrics = {}
    for r in rows:
        ap = r["airport"] or "UNK"
        metrics[metric] = ({"airport": ap, "direction": direction}, r[val_col])
    push_metrics(metrics)


def push_total_flights(df):
    total = df.agg(_sum(col("flights_count")).alias("t")).collect()[0]["t"]
    if total is not None:
        push_metrics({"tourism_flights_total_per_min": (None, float(total))})

# ===================== Today: arrivals & hotel spend (Top cities) ===================


today_books = bookings_enriched.filter(col("arrival_day") == current_date())
arrivals_today = (
    today_books.groupBy(col("city_id"), col("city_name"))
               .agg(count(lit(1)).alias("arrivals_today"),
                    _sum(col("spend_eur")).alias("spend_today_eur"))
)


def push_city_today(df):
    top = df.orderBy(col("arrivals_today").desc()).limit(TOPN).collect()
    metrics = {}
    for r in top:
        labels = {"city_id": str(r["city_id"] or "NA")}
        if r["city_name"]:
            labels["city_name"] = r["city_name"]
        if r["arrivals_today"] is not None:
            metrics["tourism_city_arrivals_today"] = (labels, r["arrivals_today"])
        if r["spend_today_eur"] is not None:
            metrics["tourism_city_spend_today_eur"] = (labels, r["spend_today_eur"])
    push_metrics(metrics)

# ===================== Top cities: minute / 30d / 365d =============================
b_city_min = (
    bookings_ing.groupBy(window(col("ingest_time"), WINDOW_SIZE_MINUTE), col("city_id"), col("city_name"))
                .agg(count(lit(1)).alias("bookings"))
)


def push_city_topN(df, metric_name, value_col, period_label):
    top = df.orderBy(col(value_col).desc()).limit(TOPN).collect()
    metrics = {}
    for r in top:
        labels = {"city_id": str(r["city_id"] or "NA"), "period": period_label}
        if "city_name" in r.asDict() and r["city_name"]:
            labels["city_name"] = r["city_name"]
        metrics[metric_name] = (labels, r[value_col])
    push_metrics(metrics)

b_city_30d = (
    bookings_evt.groupBy(window(col("event_time"), WINDOW_30D), col("city_id"), col("city_name"))
                .agg(count(lit(1)).alias("bookings_30d"))
)

b_city_365d = (
    bookings_evt.groupBy(window(col("event_time"), WINDOW_365D), col("city_id"), col("city_name"))
                .agg(count(lit(1)).alias("bookings_365d"))
)

# ===================== Month / Season (Rolling 365d) ===============================

def add_season(df, date_col):
    m = month(date_col)
    return df.withColumn(
        "season",
        when(m.isin(12, 1, 2), "winter")
        .when(m.isin(3, 4, 5), "spring")
        .when(m.isin(6, 7, 8), "summer")
        .otherwise("autumn")
    )

books_with_month = bookings_evt.withColumn("arr_month", month(col("arrival_day")))

month_roll_365 = (
    books_with_month.groupBy(window(col("event_time"), WINDOW_365D), col("arr_month"))
                    .agg(
                        count(lit(1)).alias("bookings_m"),
                        _sum(col("spend_eur")).alias("spend_m_eur")
                    )
)


def push_month_roll(df):
    rows = df.collect()
    metrics = {}
    for r in rows:
        m = int(r["arr_month"]) if r["arr_month"] is not None else None
        if m is None:
            continue
        metrics["tourism_month_bookings_rolling"] = ({"month": f"{m:02d}"}, r["bookings_m"])
        if r["spend_m_eur"] is not None:
            metrics["tourism_month_spend_rolling_eur"] = ({"month": f"{m:02d}"}, r["spend_m_eur"])
    push_metrics(metrics)

season_roll_365 = (
    add_season(bookings_evt, col("arrival_day"))
    .groupBy(window(col("event_time"), WINDOW_365D), col("season"))
    .agg(
        count(lit(1)).alias("bookings_s"),
        _sum(col("spend_eur")).alias("spend_s_eur")
    )
)


def push_season_roll(df):
    rows = df.collect()
    metrics = {}
    for r in rows:
        s = r["season"]
        if not s:
            continue
        metrics["tourism_season_bookings_rolling"] = ({"season": s}, r["bookings_s"])
        if r["spend_s_eur"] is not None:
            metrics["tourism_season_spend_rolling_eur"] = ({"season": s}, r["spend_s_eur"])
    push_metrics(metrics)

# ===================== Geomap cities (Italy) via broadcast lookup ==================
# Static dimension table (replace/extend as needed)
city_lookup_rows = [
    ("3165524", "Roma",   41.9028, 12.4964),
    ("3173435", "Milano", 45.4642,  9.1900),
    ("3183560", "Torino", 45.0703,  7.6869),
    ("3169070", "Napoli", 40.8518, 14.2681),
    ("3164603", "Palermo",38.1157, 13.3613),
    ("3172394", "Bologna",44.4949, 11.3426),
    ("3176959", "Firenze",43.7699, 11.2556),
    ("3176219", "Genova", 44.4056,  8.9463),
    ("3170647", "Bari",   41.1171, 16.8719),
    ("3164527", "Verona", 45.4384, 10.9916),
    ("3164600", "Venezia",45.4408, 12.3155),
    ("3183299", "Trieste",45.6495, 13.7768),
    ("3176217", "Padova", 45.4064, 11.8768),
    ("3172397", "Bergamo",45.6983,  9.6773),
    ("3176218", "Parma",  44.8015, 10.3279),
    ("3176958", "Ferrara",44.8381, 11.6198),
    ("3172395", "Brescia",45.5416, 10.2118),
    ("3171457", "Catania",37.5079, 15.0830),
    ("3173331", "Messina",38.1938, 15.5540),
    ("3171180", "Bolzano",46.4983, 11.3548),
]
city_lookup_df = spark.createDataFrame(city_lookup_rows, ["city_id", "g_name", "g_lat", "g_lon"])

b_city_min_geo = (
    b_city_min
    .join(broadcast(city_lookup_df), on="city_id", how="left")
    .withColumn("g_name_final", when(col("g_name").isNotNull(), col("g_name")).otherwise(col("city_name")))
)


def push_city_geomap(df, metric="tourism_city_bookings_geo"):
    rows = (df.filter(col("g_lat").isNotNull() & col("g_lon").isNotNull())
              .orderBy(col("bookings").desc()).limit(TOPN).collect())
    metrics = {}
    for r in rows:
        labels = {
            "city_id": str(r["city_id"]),
            "city_name": r["g_name_final"] or (r["city_name"] or "NA"),
            "lat": f'{float(r["g_lat"]):.5f}',
            "lon": f'{float(r["g_lon"]):.5f}',
        }
        metrics[metric] = (labels, r["bookings"])
    push_metrics(metrics)

# ===================== Season Score (Rolling 365d per city × season) ================
# Build seasonal views (event-time)
w_with_season = add_season(weather_evt, to_date(col("event_time")))
f_with_season = add_season(flights_evt, to_date(col("event_time")))
b_with_season = add_season(bookings_evt, col("arrival_day"))

# 2) Stats per (window, city_id, season)
flights_cs = (
    f_with_season.groupBy(window(col("event_time"), WINDOW_365D), col("city_id"), col("season"))
                 .agg(
                     count(lit(1)).alias("flights_total"),
                     _sum(when(col("status")=="cancelled", 1).otherwise(0)).alias("flights_cancelled"),
                     avg(col("delay_min")).alias("delay_avg")
                 )
                 .withColumn("cancel_rate", when(col("flights_total") > 0, col("flights_cancelled")/col("flights_total")).otherwise(lit(0.0)))
)

weather_cs = (
    w_with_season.groupBy(window(col("event_time"), WINDOW_365D), col("city_id"), col("season"))
                 .agg(
                     avg((col("t_max")+col("t_min"))/2.0).alias("t_avg"),
                     avg(col("precip_mm")).alias("precip_avg")
                 )
)

bookings_cs = (
    b_with_season.groupBy(window(col("event_time"), WINDOW_365D), col("city_id"), col("season"), col("city_name"))
                 .agg(
                     count(lit(1)).alias("bookings"),
                     avg(col("adr_proxy")).alias("adr_avg")
                 )
)

# 3) Join
cs_join = (
    bookings_cs.alias("b")
    .join(weather_cs.alias("w"), on=["window", "city_id", "season"], how="left")
    .join(flights_cs.alias("f"), on=["window", "city_id", "season"], how="left")
)

# 4) Percentiles per (window, city_id)
city_percentiles = (
    cs_join.groupBy(col("window"), col("city_id"))
           .agg(
               expr("percentile_approx(adr_avg, 0.10)").alias("adr_p10"),
               expr("percentile_approx(adr_avg, 0.90)").alias("adr_p90"),
               expr("percentile_approx(bookings, 0.10)").alias("book_p10"),
               expr("percentile_approx(bookings, 0.90)").alias("book_p90"),
               expr("percentile_approx(precip_avg, 0.10)").alias("rain_p10"),
               expr("percentile_approx(precip_avg, 0.90)").alias("rain_p90"),
               expr("percentile_approx(cancel_rate, 0.10)").alias("cancel_p10"),
               expr("percentile_approx(cancel_rate, 0.90)").alias("cancel_p90"),
               expr("percentile_approx(delay_avg, 0.10)").alias("delay_p10"),
               expr("percentile_approx(delay_avg, 0.90)").alias("delay_p90")
           )
)

cs_with_p = cs_join.join(city_percentiles, on=["window", "city_id"], how="left")

# 5) Indexes + final score (pure SQL expr; no UDFs)

def minmax_norm(x, lo, hi):
    denom = (hi - lo)
    return when(denom <= lit(1e-9), lit(0.5)) \
           .otherwise(least(greatest((x - lo) / denom, lit(0.0)), lit(1.0)))

price_idx  = (lit(1.0) - minmax_norm(col("adr_avg"), col("adr_p10"), col("adr_p90")))

crowd_idx  = (lit(1.0) - minmax_norm(col("bookings"), col("book_p10"), col("book_p90")))

temp_penalty = least((_abs(coalesce(col("t_avg"), lit(21.0)) - lit(21.0)) / lit(12.0)), lit(1.0))
rain_norm    = minmax_norm(coalesce(col("precip_avg"), lit(0.0)), col("rain_p10"), col("rain_p90"))
weather_idx  = (lit(1.0) - (lit(0.6)*temp_penalty + lit(0.4)*rain_norm))

cancel_norm = minmax_norm(coalesce(col("cancel_rate"), lit(0.0)), col("cancel_p10"), col("cancel_p90"))
delay_norm  = minmax_norm(coalesce(col("delay_avg"),  lit(0.0)), col("delay_p10"),  col("delay_p90"))
reliab_idx  = (lit(1.0) - (lit(0.7)*cancel_norm + lit(0.3)*delay_norm))

season_score = lit(100.0) * (lit(0.40)*price_idx + lit(0.35)*weather_idx + lit(0.15)*crowd_idx + lit(0.10)*reliab_idx)

cs_score = (
    cs_with_p.select(
        col("window"), col("city_id"), col("season"),
        col("city_name"),
        price_idx.alias("price_idx"),
        weather_idx.alias("weather_idx"),
        crowd_idx.alias("crowd_idx"),
        reliab_idx.alias("reliab_idx"),
        season_score.alias("season_score")
    )
)


def push_season_score(df, metric="tourism_season_score"):
    rows = df.orderBy(col("season_score").desc()).limit(TOPN*4).collect()
    metrics = {}
    for r in rows:
        labels = {
            "city_id": str(r["city_id"]),
            "city_name": r["city_name"] or "NA",
            "season": r["season"] or "NA"
        }
        metrics[metric] = (labels, float(r["season_score"]))
    push_metrics(metrics)

# ========================= foreachBatch helpers ==========================

def push_batch_counts(batch_df, batch_id):
    n = batch_df.count()
    print(f">>> foreachBatch push, batch_id={batch_id}, count={n}")
    if n == 0:
        return
    metrics = {}
    for r in batch_df.collect():
        ad = r.asDict()
        if ad.get("records_in_trigger") is not None:
            metrics["tourism_ingest_records_per_trigger"] = (None, ad["records_in_trigger"])
        if ad.get("weather_count") is not None:
            metrics["tourism_weather_msgs_per_min"] = (None, ad["weather_count"])
        if ad.get("flights_count") is not None:
            total = ad["flights_count"] or 0
            canc  = ad.get("flights_cancelled") or 0
            metrics["tourism_flight_msgs_per_min"] = (None, total)
            if total > 0:
                metrics["tourism_flight_cancel_rate"] = (None, float(canc)/float(total))
            if ad.get("avg_delay_min") is not None:
                metrics["tourism_flight_delay_min_avg"] = (None, ad["avg_delay_min"])
        if ad.get("bookings_count") is not None:
            metrics["tourism_booking_msgs_per_min"] = (None, ad["bookings_count"])
            if ad.get("avg_adr") is not None:
                metrics["tourism_booking_adr_avg"] = (None, ad["avg_adr"])
    push_metrics(metrics)

# =============================== start streams ==========================

def start_query(df, name, chk, foreach_fn=push_batch_counts, mode="update"):
    w = (df.writeStream
         .outputMode(mode)
         .foreachBatch(foreach_fn)
         .option("checkpointLocation", chk))
    if TRIGGER:
        w = w.trigger(processingTime=TRIGGER)
    q = w.start()
    print(f">>> stream '{name}' started (mode={mode})")
    return q

# Core monitors
iq  = start_query(ingest_counter, "ingest_counter", "/tmp/chk_i")
wq  = start_query(w_cnt, "weather_cnt",  "/tmp/chk_w")
fq  = start_query(f_cnt, "flights_cnt",  "/tmp/chk_f", foreach_fn=lambda df, bid: (push_batch_counts(df, bid), push_total_flights(df)))
bq  = start_query(b_cnt, "bookings_cnt", "/tmp/chk_b")

# Airports
in_q = start_query(inbound_by_airport,  "airports_inbound",  "/tmp/chk_air_in", foreach_fn=lambda df, bid: push_airports_top(df, "inbound"))
out_q= start_query(outbound_by_airport, "airports_outbound", "/tmp/chk_air_out", foreach_fn=lambda df, bid: push_airports_top(df, "outbound"))

# Today arrivals & spend
tod_q = start_query(arrivals_today, "city_today", "/tmp/chk_city_today", foreach_fn=lambda df, bid: push_city_today(df))

# Top cities (minute / 30d / 365d)
bmin_q   = start_query(b_city_min,  "top_cities_minute", "/tmp/chk_city_min",  foreach_fn=lambda df, bid: push_city_topN(df, "tourism_city_bookings_top", "bookings", "1m"))
b30d_q   = start_query(b_city_30d,  "top_cities_30d",    "/tmp/chk_city_30d", foreach_fn=lambda df, bid: push_city_topN(df, "tourism_city_bookings_top", "bookings_30d", "30d"))
b365d_q  = start_query(b_city_365d, "top_cities_365d",   "/tmp/chk_city_365d", foreach_fn=lambda df, bid: push_city_topN(df, "tourism_city_bookings_top", "bookings_365d", "365d"))

# Month/Season rollups
mon_q = start_query(month_roll_365,  "month_roll_365",  "/tmp/chk_month_roll",  foreach_fn=lambda df, bid: push_month_roll(df))
sea_q = start_query(season_roll_365, "season_roll_365", "/tmp/chk_season_roll", foreach_fn=lambda df, bid: push_season_roll(df))

# Geomap
geo_q = start_query(b_city_min_geo, "cities_geomap", "/tmp/chk_city_geo", foreach_fn=lambda df, bid: push_city_geomap(df))

# Season Score
score_q = start_query(cs_score, "season_score", "/tmp/chk_season_score", foreach_fn=lambda df, bid: push_season_score(df), mode="append")

# Optional debug
if DEBUG:
    (bookings_enriched.select("kafka_ts","event_ts","ingest_ts","event_time","ingest_time","arrival_day","event_day","spend_eur","city_id","city_name")
            .writeStream.format("console")
            .outputMode("append")
            .option("truncate","false")
            .option("numRows","20")
            .start())

print(">>> streams started. waiting for termination ...")
spark.streams.awaitAnyTermination()
