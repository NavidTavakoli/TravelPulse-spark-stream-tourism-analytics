#!/usr/bin/env python3
# producers/orchestrator_producers.py
"""
Unified generator for flights, bookings and weather.

Examples:
  # timewarp: simulate N days from a given start date (batch per day)
  python producers/orchestrator_producers.py --mode timewarp --sim-start 2023-01-01 --days 30 --timewarp 500

  # realtime: simulated time moves with wall-clock (1x)
  python producers/orchestrator_producers.py --mode realtime --sim-start 2023-01-01 --timewarp 1

  # realtime: "every 10 real minutes == ~1 simulated month"
  python producers/orchestrator_producers.py --mode realtime --sim-start 2023-01-01 --month-interval 10 --month-days 30
"""

import argparse, random, uuid, json, math, time
from datetime import datetime, timedelta, date, timezone
from pathlib import Path
import pandas as pd
import numpy as np
from confluent_kafka import Producer

# ========= Helpers =========
def to_iso(dt: datetime) -> str:
    """UTC-aware datetime -> ISO8601 Z"""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def make_trace():
    return uuid.uuid4().hex

def sample_lead_time(city_type_factor: float) -> int:
    """mixed business (exp mean=5) + leisure (normal mean=30)"""
    if random.random() < 0.35 * city_type_factor:
        return max(0, int(random.expovariate(1/5)))
    else:
        return max(1, int(random.gauss(30, 20)))

def seasonality(city_row: dict, sim_date: date) -> float:
    m = sim_date.month
    base = 1.0
    if "peak_months" in city_row and isinstance(city_row["peak_months"], list):
        if m in city_row["peak_months"]:
            base *= 1.4
    if sim_date.weekday() >= 5:
        base *= 1.15
    pop = float(city_row.get("population", 10000) or 10000)
    base *= (1 + math.log1p(pop)/12.0)
    return base

# ========= Load data =========
def load_inputs(base):
    base = Path(base)
    hotels = pd.read_csv(base/"data/curated/hotels_clean.csv", dtype=str)
    airports = pd.read_csv(base/"data/curated/airports_it.csv", dtype=str)
    cities = pd.read_csv(base/"data/curated/cities_it_enriched_cleaned.csv", dtype=str)

    if "population" not in cities.columns:
        cities["population"] = 10000
    cities["population"] = pd.to_numeric(cities["population"], errors="coerce").fillna(10000)

    if "airport_code" in airports.columns and "city_name" in airports.columns:
        airports["iata"] = airports["airport_code"].astype(str)
        iata_to_city = airports.set_index("airport_code")["city_name"].to_dict()
    else:
        iata_to_city = {}

    return hotels, airports, cities, iata_to_city

# ========= Delivery report =========
def _dr_cb(err, msg):
    if err:
        print("Delivery failed:", err)

# ========= Core =========
class Simulator:
    def __init__(self, hotels, airports, cities, iata_map, producer, topics):
        self.hotels = hotels
        self.airports = airports
        self.cities = cities
        self.iata_map = iata_map
        self.producer = producer
        self.topics = topics

        # per-day counters
        self.cnt_weather = 0
        self.cnt_flights = 0
        self.cnt_bookings = 0

        # simulated clock (UTC, set from runner)
        self.sim_now = datetime.now(timezone.utc)

        # Precompute city meta
        self.city_meta = {}
        for _, r in cities.iterrows():
            cid = str(r["city_id"])
            pop = float(r.get("population", 10000) or 10000)
            self.city_meta[cid] = {
                "population": pop,
                "row": r.to_dict(),
                "hotels": hotels[hotels["city_id"] == cid].to_dict("records")
            }

    def set_clock(self, sim_now: datetime):
        if sim_now.tzinfo is None:
            sim_now = sim_now.replace(tzinfo=timezone.utc)
        self.sim_now = sim_now

    def emit(self, topic, key, value):
        payload = json.dumps(value).encode("utf-8")
        keyb = str(key).encode("utf-8")
        try:
            self.producer.produce(topic, payload, key=keyb, on_delivery=_dr_cb)
            self.producer.poll(0)  # trigger callbacks
        except BufferError as e:
            print("Local queue is full, backing off...", e)
            self.producer.poll(0.5)
            self.producer.produce(topic, payload, key=keyb, on_delivery=_dr_cb)

    def gen_weather_for_date(self, sim_date: date):
        for cid, meta in self.city_meta.items():
            row = meta["row"]
            month = sim_date.month
            t_base = 5 + (month/12.0)*20 + random.gauss(0, 3)
            precip = max(0, random.gauss(2 + (12-month)/6.0, 5))
            condition = "clear"
            if precip > 10:
                condition = random.choice(["rain", "storm"])
            elif precip > 2:
                condition = "rain"

            is_peak = False
            if "peak_months" in row and isinstance(row["peak_months"], str):
                try:
                    peaks = [int(x) for x in row["peak_months"].strip("[]").split(",") if x]
                    is_peak = sim_date.month in peaks
                except Exception:
                    is_peak = False

            event_ts = datetime(sim_date.year, sim_date.month, sim_date.day, 0, 0, tzinfo=timezone.utc)
            payload = {
                "schema_version": "v1",
                "event_type": "daily_weather",
                "event_ts": to_iso(event_ts),
                "ingest_ts": to_iso(self.sim_now),
                "trace_id": make_trace(),
                "producer": "faker",
                "city_id": cid,
                "date": sim_date.isoformat(),
                "t_min": round(t_base - random.uniform(2, 6), 1),
                "t_max": round(t_base + random.uniform(2, 6), 1),
                "precip_mm": round(precip, 1),
                "wind_kph": round(max(0, random.gauss(12, 8)), 1),
                "humidity": round(min(1.0, max(0, random.gauss(0.6, 0.15))), 2),
                "condition": condition,
                "is_peak_season": is_peak,
                "holiday_flag": False
            }
            self.emit(self.topics["weather"], f"{cid}|{sim_date.isoformat()}", payload)
            self.cnt_weather += 1

    def gen_flights_for_date(self, sim_date: date):
        for _, ap in self.airports.iterrows():
            iata = ap.get("airport_code")
            city_name = ap.get("city_name")
            if city_name:
                m = self.cities[
                    self.cities["city_name"].fillna("").str.lower().str.contains(str(city_name).lower(), regex=False)
                ]
                if len(m) > 0:
                    city_row = m.iloc[0].to_dict()
                    city_id = str(city_row["city_id"])
                else:
                    city_id = random.choice(list(self.city_meta.keys()))
                    city_row = self.city_meta[city_id]["row"]
            else:
                city_id = random.choice(list(self.city_meta.keys()))
                city_row = self.city_meta[city_id]["row"]

            s = seasonality(city_row, sim_date)
            pop = float(city_row.get("population", 10000))
            expected_arrivals = max(1, int((pop / 2000.0) * s))

            for _ in range(expected_arrivals):
                flight_id = f"FA{random.randint(100, 9999)}"
                seats = random.choice([100, 150, 180, 220])
                load = min(1.0, max(0.3, random.gauss(0.7, 0.12)))
                weather_factor = random.random() * 0.15
                congestion = random.random() * 0.15
                delay = int(random.gauss(5, 10) + (weather_factor + congestion) * 60)
                status = "arrived"
                if random.random() < 0.01 + weather_factor * 0.5:
                    status = "cancelled"
                    delay = None

                sched_dep = datetime(sim_date.year, sim_date.month, sim_date.day,
                                     random.randint(0, 23), random.choice([0, 15, 30, 45]),
                                     tzinfo=timezone.utc)
                sched_arr = sched_dep + timedelta(minutes=random.randint(40, 180))
                event_ts = (sched_arr + timedelta(minutes=(delay or 0)))

                payload = {
                    "schema_version": "v1",
                    "event_type": "flight_arrival",
                    "event_ts": to_iso(event_ts),
                    "ingest_ts": to_iso(self.sim_now),
                    "trace_id": make_trace(),
                    "producer": "faker",
                    "flight_id": flight_id,
                    "airline": random.choice(["ITA", "Ryanair", "easyJet", "WizzAir"]),
                    "service_date": sim_date.isoformat(),
                    "origin_iata": random.choice(list(self.airports["airport_code"].dropna().unique())),
                    "destination_iata": iata,
                    "destination_city_id": city_id,
                    "scheduled_departure": to_iso(sched_dep),
                    "actual_departure": None if status == "cancelled" else to_iso(sched_dep + timedelta(minutes=max(0, delay))),
                    "scheduled_arrival": to_iso(sched_arr),
                    "actual_arrival": None if status == "cancelled" else to_iso(sched_arr + timedelta(minutes=max(0, delay))),
                    "status": status,
                    "delay_min": None if delay is None else int(delay),
                    "load_factor": round(load, 2),
                    "aircraft_type": random.choice(["A320", "A321", "B737", "A319"]),
                    "seats": seats,
                    "weather_factor": round(weather_factor, 3),
                    "airport_congestion": round(congestion, 3)
                }
                self.emit(self.topics["flights"], f"{iata}|{flight_id}", payload)
                self.cnt_flights += 1

    def gen_bookings_for_date(self, sim_date: date):
        for cid, meta in self.city_meta.items():
            s = seasonality(meta["row"], sim_date)
            hotels = meta["hotels"]
            if len(hotels) == 0:
                continue
            base = max(0.5, meta["population"] / 100000.0 * 10.0 * s)
            nb = max(1, int(np.random.poisson(base)))  
            for _ in range(nb):
                hotel = random.choice(hotels)
                lead = sample_lead_time(1.0)
                checkin = sim_date + timedelta(days=lead)
                nights = max(1, int(random.choice([1, 1, 2, 2, 3, 4])))
                adr = max(30.0, float(hotel.get("stars_num") or 3.0) * 30 + random.gauss(0, 20) + 5 * s)
                payload = {
                    "schema_version": "v1",
                    "event_type": "booking_created",
                    "event_ts": to_iso(self.sim_now),   
                    "ingest_ts": to_iso(self.sim_now),  
                    "trace_id": make_trace(),
                    "producer": "faker",
                    "booking_id": f"BKG-{uuid.uuid4().hex[:8]}",
                    "hotel_id": hotel["hotel_id"],
                    "city_id": cid,
                    "city_name": hotel.get("city_name"),
                    "checkin_date": checkin.isoformat(),
                    "checkout_date": (checkin + timedelta(days=nights)).isoformat(),
                    "nights": nights,
                    "guests": random.choice([1, 2, 2, 3]),
                    "rooms": 1,
                    "channel": random.choices(["direct", "ota", "corporate"], weights=[0.4, 0.5, 0.1])[0],
                    "lead_time_days": lead,
                    "adr_proxy": round(adr, 2),
                    "currency": "EUR",
                    "is_refundable": random.random() < 0.7,
                    "status": "active",
                    "cancel_ts": None,
                    "flight_anchor": {
                        "predicted_inbound": int(meta["population"] / 1000.0 * s),
                    }
                }
                if random.random() < 0.005:
                    payload["status"] = "cancelled"
                    payload["cancel_ts"] = to_iso(self.sim_now)
                self.emit(self.topics["bookings"], f"{cid}|{payload['checkin_date']}", payload)
                self.cnt_bookings += 1

# ========= Runner =========
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="/home/tahmast/Projects/Tourism")
    ap.add_argument("--bootstrap-servers", default="localhost:9092")
    ap.add_argument("--mode", choices=["realtime", "timewarp"], default="timewarp")
    ap.add_argument("--rate", type=int, default=1000, help="target msgs/sec when accelerating")
    ap.add_argument("--days", type=int, default=365, help="how many simulated days")
    ap.add_argument("--timewarp", type=float, default=1000.0, help="speedup factor (used in both modes)")
    ap.add_argument("--sim-start", type=str, default="2023-01-01", help="simulation start date (YYYY-MM-DD)")

    # NEW: map "real minutes" -> "one simulated month"
    ap.add_argument("--month-interval", type=float, default=None,
                    help="Real minutes per one simulated month (overrides --timewarp if set). Example: 10 => every 10 real minutes advance ~1 simulated month.")
    ap.add_argument("--month-days", type=float, default=30.0,
                    help="Days considered as one simulated month when computing timewarp (default 30.0; use 30.44 for average).")

    # Light-mode sampling
    ap.add_argument("--sample-cities", type=int, default=0)
    ap.add_argument("--sample-airports", type=int, default=0)
    ap.add_argument("--sample-hotels", type=int, default=0)

    ap.add_argument("--debug", type=int, default=1)
    args = ap.parse_args()

    # ---- compute timewarp from month-interval (if provided) ----
    if args.month_interval and args.month_interval > 0:
        args.timewarp = (args.month_days * 24.0 * 60.0) / args.month_interval
        print(f"[cfg] month-interval={args.month_interval:.2f} min, month-days={args.month_days:.2f} -> timewarp={args.timewarp:.2f}x")

    hotels, airports, cities, iata_map = load_inputs(args.base)

    # sampling & alignment
    if args.sample_cities and args.sample_cities > 0:
        cities = cities.head(args.sample_cities).copy()
    hotels = hotels[hotels["city_id"].isin(cities["city_id"])].copy()
    if args.sample_hotels and args.sample_hotels > 0:
        hotels = hotels.head(args.sample_hotels).copy()
    if args.sample_airports and args.sample_airports > 0:
        airports = airports.head(args.sample_airports).copy()
    if len(hotels) == 0:
        hotels_full = pd.read_csv(Path(args.base)/"data/curated/hotels_clean.csv", dtype=str)
        hotels = hotels_full[hotels_full["city_id"].isin(cities["city_id"])].head(300).copy()

    print("Loaded shapes (post-alignment):", len(hotels), len(airports), len(cities))

    conf = {
        "bootstrap.servers": args.bootstrap_servers,
        "linger.ms": 100,
        "compression.type": "lz4",
        "acks": "1",
        "enable.idempotence": False,
        "statistics.interval.ms": 0,
    }
    if args.debug:
        conf["debug"] = "broker,topic,msg"

    p = Producer(conf)
    topics = {"flights": "flight.events.v1", "bookings": "booking.events.v1", "weather": "weather.events.v1"}
    sim = Simulator(hotels, airports, cities, iata_map, p, topics)

    # Simulation start (date & datetime)
    sim_start_date = date.fromisoformat(args.sim_start)
    sim_start_dt = datetime(sim_start_date.year, sim_start_date.month, sim_start_date.day, 0, 0, tzinfo=timezone.utc)

    if args.mode == "realtime":
        print("Starting realtime loop (ctrl-c to stop)...")
        wall_start = datetime.now(timezone.utc)
        last_emitted_date = None

        while True:
            # simulated now = sim_start + elapsed_real * timewarp
            elapsed = datetime.now(timezone.utc) - wall_start
            sim_now = sim_start_dt + elapsed * max(1.0, float(args.timewarp))
            sim.set_clock(sim_now)
            sim_date = sim_now.date()

            # emit exactly once per simulated day
            if last_emitted_date != sim_date:
                sim.cnt_weather = sim.cnt_flights = sim.cnt_bookings = 0
                sim.gen_weather_for_date(sim_date)
                sim.gen_flights_for_date(sim_date)
                sim.gen_bookings_for_date(sim_date)
                print(f"[{sim_date}] produced: weather={sim.cnt_weather} flights={sim.cnt_flights} bookings={sim.cnt_bookings}")
                p.flush(0)
                last_emitted_date = sim_date

            time.sleep(max(0.02, 1.0 / max(1, args.rate)))

    else:
        days = args.days
        print(f"Simulating {days} days from {sim_start_date} with timewarp={args.timewarp}, rate={args.rate} msgs/sec...")
        for dd in range(days):
            sim_date = sim_start_date + timedelta(days=dd)

            sim.set_clock(datetime(sim_date.year, sim_date.month, sim_date.day, 12, 0, tzinfo=timezone.utc))
            sim.cnt_weather = sim.cnt_flights = sim.cnt_bookings = 0

            sim.gen_weather_for_date(sim_date)
            sim.gen_flights_for_date(sim_date)
            sim.gen_bookings_for_date(sim_date)

            print(f"[{sim_date}] produced: weather={sim.cnt_weather} flights={sim.cnt_flights} bookings={sim.cnt_bookings}")
            p.flush(0)
            time.sleep(0.001)

    p.flush(10)
    print("Done.")

if __name__ == "__main__":
    main()
