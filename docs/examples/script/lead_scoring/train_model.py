# Train a small lead-scoring classifier and bundle it with the payload.
from __future__ import annotations

import itertools
import pathlib
import random
import zipfile

import joblib
import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

INDUSTRIES = [
    "retail",
    "healthcare",
    "finance",
    "tech",
    "manufacturing",
    "education",
    "government",
    "media",
    "hospitality",
    "transportation",
    "energy",
    "real_estate",
    "telecom",
    "agriculture",
    "other",
]
EMPLOYEE_BANDS = ["1-10", "11-50", "51-200", "201-1000", "1001-5000", "5001+"]
REGIONS = ["NA", "EMEA", "APAC", "LATAM", "ANZ"]
SOURCES = [
    "webform",
    "event",
    "referral",
    "partner",
    "cold_outreach",
    "marketing_campaign",
]

FEATURE_COLS = ["industry__c", "employee_band__c", "region__c", "source__c"]

BAND_SIGNAL = {b: i for i, b in enumerate(EMPLOYEE_BANDS)}
SOURCE_SIGNAL = {
    "referral": 2.0,
    "partner": 1.2,
    "event": 0.8,
    "marketing_campaign": 0.3,
    "webform": 0.0,
    "cold_outreach": -1.0,
}
INDUSTRY_SIGNAL = {
    "tech": 1.2,
    "finance": 1.0,
    "healthcare": 0.7,
    "telecom": 0.5,
    "energy": 0.4,
    "manufacturing": 0.2,
    "retail": 0.0,
    "media": 0.0,
    "real_estate": -0.2,
    "education": -0.3,
    "government": -0.4,
    "hospitality": -0.5,
    "transportation": -0.5,
    "agriculture": -0.7,
    "other": -0.5,
}
REGION_SIGNAL = {"NA": 0.6, "EMEA": 0.4, "APAC": 0.3, "ANZ": 0.2, "LATAM": 0.0}


def synth_rows(n: int, seed: int = 42) -> pd.DataFrame:
    rng = random.Random(seed)
    rows = []
    for _ in range(n):
        row = {
            "industry__c": rng.choice(INDUSTRIES),
            "employee_band__c": rng.choice(EMPLOYEE_BANDS),
            "region__c": rng.choice(REGIONS),
            "source__c": rng.choice(SOURCES),
        }
        logit = (
            INDUSTRY_SIGNAL[row["industry__c"]]
            + 0.4 * BAND_SIGNAL[row["employee_band__c"]]
            + REGION_SIGNAL[row["region__c"]]
            + SOURCE_SIGNAL[row["source__c"]]
            - 1.5
        )
        p = 1.0 / (1.0 + np.exp(-logit))
        row["converted"] = int(rng.random() < p)
        rows.append(row)
    return pd.DataFrame(rows)


def main() -> None:
    df = synth_rows(8000)
    X, y = df[FEATURE_COLS], df["converted"]

    X_tr, X_te, y_tr, y_te = train_test_split(
        X, y, test_size=0.25, random_state=0, stratify=y
    )
    pipe = Pipeline(
        [
            ("ohe", OneHotEncoder(handle_unknown="ignore", sparse_output=False)),
            ("clf", LogisticRegression(max_iter=1000, random_state=0)),
        ]
    )
    pipe.fit(X_tr, y_tr)

    auc = roc_auc_score(y_te, pipe.predict_proba(X_te)[:, 1])
    prevalence = float(y.mean())

    grid_rows = list(itertools.product(INDUSTRIES, EMPLOYEE_BANDS, REGIONS, SOURCES))
    print(
        f"train AUC: {auc:.3f} | prevalence: {prevalence:.3f} | grid: {len(grid_rows)}"
    )

    payload = {
        "pipeline": pipe,
        "feature_cols": FEATURE_COLS,
        "feature_domain": {
            "industry__c": INDUSTRIES,
            "employee_band__c": EMPLOYEE_BANDS,
            "region__c": REGIONS,
            "source__c": SOURCES,
        },
    }

    out_joblib = (
        pathlib.Path(__file__).parent / "payload" / "files" / "lead_scorer.joblib"
    )
    out_joblib.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(payload, out_joblib, compress=3)
    print(f"wrote {out_joblib} ({out_joblib.stat().st_size} bytes)")

    out_zip = out_joblib.with_suffix(".zip")
    with zipfile.ZipFile(out_zip, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.write(out_joblib, arcname=out_joblib.name)
    print(f"wrote {out_zip} ({out_zip.stat().st_size} bytes)")

    out_joblib.unlink()


if __name__ == "__main__":
    main()
