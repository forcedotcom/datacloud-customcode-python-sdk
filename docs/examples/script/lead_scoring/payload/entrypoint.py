import itertools
from pathlib import Path
import tempfile
import zipfile

import joblib
import pandas as pd
from pyspark.sql import Row
from pyspark.sql.functions import (
    coalesce,
    col,
    lit,
)

from datacustomcode.client import Client
from datacustomcode.io.writer.base import WriteMode

UNKNOWN = "__unknown__"
MODEL_ARCHIVE = "lead_scorer.zip"
MODEL_MEMBER = "lead_scorer.joblib"

OUTPUT_COLUMNS = [
    "id__c",
    "first_name__c",
    "last_name__c",
    "industry__c",
    "employee_band__c",
    "region__c",
    "source__c",
    "score__c",
]


def load_bundled_model(client):
    archive_path = client.find_file_path(MODEL_ARCHIVE)
    extract_dir = Path(tempfile.mkdtemp(prefix="lead_scorer_"))
    with zipfile.ZipFile(archive_path) as zf:
        zf.extract(MODEL_MEMBER, path=extract_dir)
    return joblib.load(extract_dir / MODEL_MEMBER)


def build_score_grid(spark, bundle):
    """Enumerate every combination of the model's known feature values,
    score them, and return a small Spark DataFrame."""
    pipeline = bundle["pipeline"]
    feature_cols = bundle["feature_cols"]
    domain = bundle["feature_domain"]

    combos = list(itertools.product(*(domain[c] for c in feature_cols)))
    grid_pd = pd.DataFrame(combos, columns=feature_cols)
    grid_pd["score__c"] = pipeline.predict_proba(grid_pd)[:, 1]

    rows = [
        Row(**{c: r[c] for c in feature_cols}, score__c=float(r["score__c"]))
        for r in grid_pd.to_dict(orient="records")
    ]
    return spark.createDataFrame(rows), feature_cols


def main():
    client = Client()

    leads = client.read_dlo("Lead__dll")
    spark = leads.sparkSession

    bundle = load_bundled_model(client)
    grid, feature_cols = build_score_grid(spark, bundle)

    normalized = leads
    for c in feature_cols:
        normalized = normalized.withColumn(c, coalesce(col(c), lit(UNKNOWN)))

    scored = (
        normalized.alias("l")
        .join(grid.alias("g"), feature_cols, "left")
        .withColumn("score__c", coalesce(col("score__c"), lit(0.0)))
    )

    client.write_to_dlo(
        "Lead_Scored__dll", scored.select(*OUTPUT_COLUMNS), WriteMode.OVERWRITE
    )


if __name__ == "__main__":
    main()
