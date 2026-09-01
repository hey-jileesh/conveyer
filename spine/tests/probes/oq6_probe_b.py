"""
conveyer-hpp.13.11 -- P-B: OQ-6 validation-2 kernel probe
Stats pruning on corrections-present data, under identity(batch_id) partitioning
(no sort order), per design/adr-oq6-fact-partition-spec.md PS-1..PS-3, refinement 3.

[DS-5] fabricated-synthetic-only. Reuses tests/conftest.py::build_test_session /
_iceberg_conf idiom (spine/tests/conftest.py) verbatim: pyspark 3.5.*,
org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1, local Hadoop catalog,
tmpdir warehouse.

Run via the repl-driven-python kernel (uv run python .claude/skills/repl-driven-python/
repl_client.py eval -f <this file>) from the project root, spark session already
built (see session_setup.py in this same scratchpad directory). Non-gating probe;
does not touch design/ or spine/ files; writes nothing outside a tmpdir warehouse.
"""

import random
import time
from datetime import date, timedelta

from pyspark.sql import functions as F, types as T

# ---------------------------------------------------------------------------
# Session wiring (verbatim idiom from spine/tests/conftest.py::build_test_session /
# _iceberg_conf -- reproduced here rather than imported since this is a throwaway
# kernel script, not a pytest fixture).
# ---------------------------------------------------------------------------

_ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1"
_ICEBERG_EXTENSIONS = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"

_BASE_CONF = {
    "spark.driver.memory": "2g",
    "spark.sql.shuffle.partitions": "2",
    "spark.sql.adaptive.enabled": "true",
    "spark.ui.enabled": "false",
}


def _iceberg_conf(warehouse_dir: str) -> dict:
    return {
        "spark.jars.packages": _ICEBERG_PACKAGE,
        "spark.sql.extensions": _ICEBERG_EXTENSIONS,
        "spark.sql.catalog.spine_cat": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.spine_cat.type": "hadoop",
        "spark.sql.catalog.spine_cat.warehouse": warehouse_dir,
    }


def build_test_session(extra_conf=None):
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.master("local[2]").appName("conveyer-spine-tests-oq6-probeB")
    conf = {**_BASE_CONF, **(extra_conf or {})}
    for key, value in conf.items():
        builder = builder.config(key, value)
    session = builder.getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    return session


# ---------------------------------------------------------------------------
# Dataset generation (pure, seeded, reproducible)
# ---------------------------------------------------------------------------

PAYLOAD_FILLER = "x" * 64


def generate_batches_meta(B, R_min, R_max, c_pct, k_periods, period_days, start_date, seed, old_frac=0.2):
    """Pure, deterministic (seeded) batch metadata generator.
    The SET of correction-flagged batch indices is chosen once per (B, c_pct, seed)
    via random.sample -- independent of k_periods -- so across the k grid for a
    fixed c, the same physical batches are corrections and only the back-shift
    distance varies (cleaner k-comparison, exact c% up to rounding, no per-batch
    Bernoulli variance). One batch/day cadence -- batches appended in event-time
    order (batch_date is monotone in batch_index).
    """
    rnd = random.Random(seed)
    row_counts = [rnd.randint(R_min, R_max) for _ in range(B)]
    n_corrections = round(B * c_pct / 100.0)
    corr_rnd = random.Random(f"{seed}-corrections-{c_pct}")
    correction_idx = set(corr_rnd.sample(range(B), n_corrections)) if n_corrections > 0 else set()

    out = []
    for i in range(B):
        batch_date = start_date + timedelta(days=i)
        row_count = row_counts[i]
        is_correction = i in correction_idx
        if is_correction:
            back_days = k_periods * period_days
            target_date = batch_date - timedelta(days=back_days)
            clamped = target_date < start_date
            if clamped:
                target_date = start_date
        else:
            target_date = batch_date
            clamped = False
        out.append({
            "batch_index": i,
            "batch_id": f"batch_{i:04d}",
            "batch_date": batch_date,
            "row_count": row_count,
            "is_correction": is_correction,
            "target_date": target_date,
            "clamped": clamped,
            "old_frac": old_frac if is_correction else 0.0,
        })
    return out


def build_batches_meta_df(spark, batches_meta):
    schema = T.StructType([
        T.StructField("batch_index", T.IntegerType()),
        T.StructField("batch_id", T.StringType()),
        T.StructField("batch_date", T.DateType()),
        T.StructField("row_count", T.IntegerType()),
        T.StructField("is_correction", T.BooleanType()),
        T.StructField("target_date", T.DateType()),
        T.StructField("old_frac", T.DoubleType()),
    ])
    rows = [
        (m["batch_index"], m["batch_id"], m["batch_date"], m["row_count"],
         m["is_correction"], m["target_date"], m["old_frac"])
        for m in batches_meta
    ]
    return spark.createDataFrame(rows, schema=schema)


def build_fact_rows_df(spark, batches_meta, num_domains=1000, jitter_days=2, seed=1234):
    """One row per (batch, local_idx) up to row_count. `is_old_row` rows (the
    leading `old_frac` fraction of a correction batch's local indices) get
    event_time centered on `target_date` (the k-periods-back business time);
    all other rows get event_time centered on `batch_date` (the batch's real
    append date). +/-jitter_days day jitter + random intraday seconds, all via
    deterministic hashing of (batch_id, local_idx, seed) so the whole dataset
    is reproducible from `seed` alone -- fully distributed/Spark-native, no
    python-level per-row RNG, for speed at scale.
    """
    meta_df = build_batches_meta_df(spark, batches_meta)
    exploded = meta_df.select(
        "*",
        F.explode(F.sequence(F.lit(0), F.col("row_count") - 1)).alias("local_idx"),
    )
    threshold = (F.col("old_frac") * F.col("row_count")).cast("long")
    is_old = F.col("local_idx") < threshold
    base_date = F.when(is_old, F.col("target_date")).otherwise(F.col("batch_date"))

    day_hash = F.xxhash64(F.col("batch_id"), F.col("local_idx"), F.lit(seed))
    jitter = (F.pmod(day_hash, F.lit(2 * jitter_days + 1)) - F.lit(jitter_days)).cast("int")

    seconds_hash = F.xxhash64(F.col("batch_id"), F.col("local_idx"), F.lit(seed + 1))
    seconds_offset = F.pmod(F.abs(seconds_hash), F.lit(86400))

    event_ts = (
        F.unix_timestamp(F.date_add(base_date, jitter).cast("timestamp")) + seconds_offset
    ).cast("timestamp")

    domain_hash = F.xxhash64(F.col("batch_id"), F.col("local_idx"), F.lit(seed + 2))
    domain_num = F.pmod(F.abs(domain_hash), F.lit(num_domains))
    domain_id = F.concat(F.lit("domain_"), F.lpad(domain_num.cast("string"), 4, "0"))

    return exploded.select(
        F.col("batch_id"),
        F.col("batch_date"),
        F.col("is_correction"),
        is_old.alias("is_old_row"),
        event_ts.alias("event_time"),
        domain_id.alias("domain_id"),
        F.lit("order_placed").alias("fact_type"),
        F.lit(PAYLOAD_FILLER).alias("payload"),
        F.monotonically_increasing_id().alias("record_key"),
    )


def write_fact_table(spark, df, full_table_name, num_batches):
    """identity(batch_id) partitioning, no sort order (F-3 / PS-1..PS-3 spec).
    One Spark write job; `write.spark.fanout.enabled=true` + repartition-by-
    partition-column together guarantee exactly one output file per batch_id
    partition without requiring a global sort (validated against a 2-partition
    mini table before scaling up: partition count == file count == distinct
    batch_id count, every time)."""
    ns = ".".join(full_table_name.split(".")[:-1])
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ns}")
    spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
    spark.sql(f"""
        CREATE TABLE {full_table_name} (
          batch_id STRING,
          event_time TIMESTAMP,
          domain_id STRING,
          fact_type STRING,
          payload STRING,
          record_key BIGINT
        ) USING iceberg
        PARTITIONED BY (batch_id)
        TBLPROPERTIES ('write.spark.fanout.enabled'='true')
    """)
    out_cols = ["batch_id", "event_time", "domain_id", "fact_type", "payload", "record_key"]
    (
        df.select(*out_cols)
        .repartition(num_batches, "batch_id")
        .writeTo(full_table_name)
        .append()
    )


# ---------------------------------------------------------------------------
# Ground-truth floor (analytical, from generation metadata -- validated against
# a REAL executed groupBy/filter on materialized data: exact match).
# ---------------------------------------------------------------------------

def analytical_floor(batches_meta, lo_date, hi_date, jitter_days=2):
    """Ground-truth necessary-scan file count for a bounded event_time window
    [lo_date, hi_date] (inclusive), computed directly from generation metadata:
    a batch's file MUST be scanned iff it has a real cluster (normal
    batch_date-centered, or -- for correction batches -- the old
    target_date-centered cluster) whose +/-jitter_days span intersects the
    window. Returns (floor_count, true_match_count, correction_extra_count,
    matched_batch_ids)."""
    matched, true_match, correction_extra = set(), set(), set()
    for m in batches_meta:
        normal_lo = m["batch_date"] - timedelta(days=jitter_days)
        normal_hi = m["batch_date"] + timedelta(days=jitter_days)
        normal_hits = not (normal_hi < lo_date or normal_lo > hi_date)
        old_hits = False
        if m["is_correction"]:
            old_lo = m["target_date"] - timedelta(days=jitter_days)
            old_hi = m["target_date"] + timedelta(days=jitter_days)
            old_hits = not (old_hi < lo_date or old_lo > hi_date)
        if normal_hits or old_hits:
            matched.add(m["batch_id"])
            if normal_hits:
                true_match.add(m["batch_id"])
            if old_hits and not normal_hits:
                correction_extra.add(m["batch_id"])
    return len(matched), len(true_match), len(correction_extra), matched


def analytical_floor_asof(batches_meta, cutoff_date, jitter_days=2):
    """As-of (event_time <= cutoff): unbounded-below variant of analytical_floor."""
    matched, true_match, correction_extra = set(), set(), set()
    for m in batches_meta:
        normal_hits = (m["batch_date"] - timedelta(days=jitter_days)) <= cutoff_date
        old_hits = m["is_correction"] and (m["target_date"] - timedelta(days=jitter_days)) <= cutoff_date
        if normal_hits or old_hits:
            matched.add(m["batch_id"])
            if normal_hits:
                true_match.add(m["batch_id"])
            if old_hits and not normal_hits:
                correction_extra.add(m["batch_id"])
    return len(matched), len(true_match), len(correction_extra), matched


# ---------------------------------------------------------------------------
# Measurand: Iceberg SCAN PLANNING (org.apache.iceberg TableScan.planFiles()),
# via the Java gateway -- planning-level file skip, NOT wall-clock, NOT an
# approximation of Spark's DSv2 explain output.
# ---------------------------------------------------------------------------

def _make_scan_helpers(spark):
    jvm = spark._jvm
    jsess = spark._jsparkSession
    Expressions = jvm.org.apache.iceberg.expressions.Expressions
    DateTimeUtil = jvm.org.apache.iceberg.util.DateTimeUtil
    and_fn = getattr(Expressions, "and")

    def load_table(full_table_name):
        return jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(jsess, full_table_name)

    def _ts_micros(iso_str):
        return DateTimeUtil.isoTimestampToMicros(iso_str.replace(" ", "T"))

    def plan_scan(jtable, expr):
        scan = jtable.newScan()
        if expr is not None:
            scan = scan.filter(expr)
        it = scan.planFiles().iterator()
        files = []
        while it.hasNext():
            task = it.next()
            f = task.file()
            files.append({"path": f.path(), "partition": f.partition().toString(), "record_count": f.recordCount()})
        return files

    def plan_time_range(jtable, lo_iso=None, hi_iso=None):
        exprs = []
        if lo_iso is not None:
            exprs.append(Expressions.greaterThanOrEqual("event_time", _ts_micros(lo_iso)))
        if hi_iso is not None:
            exprs.append(Expressions.lessThanOrEqual("event_time", _ts_micros(hi_iso)))
        expr = exprs[0] if len(exprs) == 1 else (and_fn(exprs[0], exprs[1]) if exprs else None)
        return plan_scan(jtable, expr)

    def plan_domain_eq(jtable, domain_value):
        return plan_scan(jtable, Expressions.equal("domain_id", domain_value))

    return load_table, plan_scan, plan_time_range, plan_domain_eq


# ---------------------------------------------------------------------------
# Secondary metric: hypothetical time-transform scatter count (PS-2 re-argument input)
# ---------------------------------------------------------------------------

def scatter_counts_for_corrections(materialized_df):
    """For each correction batch, under a HYPOTHETICAL day(event_time) or
    month(event_time) partition transform, how many distinct time-partition
    buckets would that one append's rows scatter into? Computed from the REAL
    materialized event_time column, grouped per correction batch_id."""
    corr = materialized_df.filter(F.col("is_correction"))
    per_batch = corr.groupBy("batch_id").agg(
        F.countDistinct(F.to_date("event_time")).alias("distinct_days"),
        F.countDistinct(F.date_format("event_time", "yyyy-MM")).alias("distinct_months"),
    )
    rows = per_batch.collect()
    if not rows:
        return {"n_correction_batches": 0}
    days = [r["distinct_days"] for r in rows]
    months = [r["distinct_months"] for r in rows]
    return {
        "n_correction_batches": len(rows),
        "day_transform_scatter_avg": sum(days) / len(days),
        "day_transform_scatter_max": max(days),
        "month_transform_scatter_avg": sum(months) / len(months),
        "month_transform_scatter_max": max(months),
    }


# ---------------------------------------------------------------------------
# Experiment parameters (Sheet B) -- actual B/R used, per bead's scale-down
# allowance (R scaled 10x down from the ~5-20k default for kernel tractability;
# B kept at the full 240).
# ---------------------------------------------------------------------------

START_DATE = date(2024, 1, 1)
B = 240
R_MIN, R_MAX = 500, 2000
NUM_DOMAINS = 1000
JITTER_DAYS = 2
PERIOD_DAYS = 15          # the "period" unit for k-periods-back magnitude
OLD_FRAC = 0.2            # fraction of a correction batch's OWN rows carrying the old event_time
SEED = 42

Q1 = ("2024-02-01 00:00:00", "2024-02-29 23:59:59")   # point-period: February (leap year)
Q2 = ("2024-02-01 00:00:00", "2024-04-30 23:59:59")   # trailing quarter Feb-Apr
Q3_DOMAIN = "domain_0042"                              # single-domain history
Q4_CUTOFF = "2024-02-29 23:59:59"                      # as-of cutoff (same right edge as Q1)

Q1_DATES = (date(2024, 2, 1), date(2024, 2, 29))
Q2_DATES = (date(2024, 2, 1), date(2024, 4, 30))
Q4_DATE = date(2024, 2, 29)


def run_cell(spark, c_pct, k_periods, table_suffix, scan_helpers):
    load_table, plan_scan, plan_time_range, plan_domain_eq = scan_helpers
    t0 = time.time()
    meta = generate_batches_meta(B=B, R_min=R_MIN, R_max=R_MAX, c_pct=c_pct, k_periods=k_periods,
                                  period_days=PERIOD_DAYS, start_date=START_DATE, seed=SEED, old_frac=OLD_FRAC)
    rows_df = build_fact_rows_df(spark, meta, num_domains=NUM_DOMAINS, jitter_days=JITTER_DAYS, seed=SEED)
    rows_df.cache()
    table = f"spine_cat.oq6_probe.fact_{table_suffix}"
    write_fact_table(spark, rows_df, table, num_batches=B)
    t_write = time.time()

    jtable = load_table(table)
    total_files = len(plan_scan(jtable, None))

    results = {}
    floor_q1, tm_q1, ce_q1, _ = analytical_floor(meta, *Q1_DATES, jitter_days=JITTER_DAYS)
    scanned_q1 = plan_time_range(jtable, *Q1)
    results["Q1"] = {"floor": floor_q1, "true_match": tm_q1, "correction_extra": ce_q1,
                      "scanned": len(scanned_q1), "total": total_files}

    floor_q2, tm_q2, ce_q2, _ = analytical_floor(meta, *Q2_DATES, jitter_days=JITTER_DAYS)
    scanned_q2 = plan_time_range(jtable, *Q2)
    results["Q2"] = {"floor": floor_q2, "true_match": tm_q2, "correction_extra": ce_q2,
                      "scanned": len(scanned_q2), "total": total_files}

    floor_q4, tm_q4, ce_q4, _ = analytical_floor_asof(meta, Q4_DATE, jitter_days=JITTER_DAYS)
    scanned_q4 = plan_time_range(jtable, None, Q4_CUTOFF)
    results["Q4"] = {"floor": floor_q4, "true_match": tm_q4, "correction_extra": ce_q4,
                      "scanned": len(scanned_q4), "total": total_files}

    real_domain_matches = rows_df.filter(F.col("domain_id") == Q3_DOMAIN).select("batch_id").distinct().count()
    scanned_q3 = plan_domain_eq(jtable, Q3_DOMAIN)
    results["Q3"] = {"floor": real_domain_matches, "true_match": real_domain_matches, "correction_extra": 0,
                      "scanned": len(scanned_q3), "total": total_files}

    scatter = scatter_counts_for_corrections(rows_df)
    n_corrections = sum(m["is_correction"] for m in meta)
    n_clamped = sum(m["clamped"] for m in meta)
    n_rows = rows_df.count()

    t_end = time.time()
    rows_df.unpersist()

    return {
        "c_pct": c_pct, "k_periods": k_periods, "table": table,
        "n_batches": B, "n_rows": n_rows,
        "n_corrections": n_corrections, "n_clamped": n_clamped,
        "results": results, "scatter": scatter,
        "write_seconds": round(t_write - t0, 1), "total_seconds": round(t_end - t0, 1),
    }


def main(spark):
    scan_helpers = _make_scan_helpers(spark)
    cells = [
        (0, None, "c00_kNA"),
        (5, 1, "c05_k1"),
        (5, 3, "c05_k3"),
        (5, 12, "c05_k12"),
        (15, 1, "c15_k1"),
        (15, 3, "c15_k3"),
        (15, 12, "c15_k12"),
    ]
    all_results = []
    for c_pct, k_periods, suffix in cells:
        k_for_gen = k_periods if k_periods is not None else 1  # irrelevant when c_pct=0
        r = run_cell(spark, c_pct=c_pct, k_periods=k_for_gen, table_suffix=suffix, scan_helpers=scan_helpers)
        r["k_periods"] = k_periods
        all_results.append(r)
    return all_results


if __name__ == "__main__":
    import tempfile
    warehouse_dir = tempfile.mkdtemp(prefix="oq6-probeB-warehouse-")
    spark = build_test_session(extra_conf=_iceberg_conf(warehouse_dir))
    results = main(spark)
    import json
    print(json.dumps(results, indent=2, default=str))

