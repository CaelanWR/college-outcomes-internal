#!/usr/bin/env python3
"""Build an expanded precompute school run list.

The server audit is the preferred input because it covers the broader U.S.
school universe. The old NACE match CSVs can still be used locally for a dry
run, but they do not contain every school we care about.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


DEFAULT_TIERS = {"excellent", "strong"}
USABLE_TIERS = {"excellent", "strong", "usable"}
AVOID_TIERS = {"avoid", "watchlist", "thin"}


def _first_existing(df: pd.DataFrame, candidates: list[str]) -> str | None:
    lower_to_col = {str(col).lower(): str(col) for col in df.columns}
    for candidate in candidates:
        if candidate.lower() in lower_to_col:
            return lower_to_col[candidate.lower()]
    return None


def _norm_series(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.strip()


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(path)
    return pd.read_csv(path)


def _standardize_school_frame(df: pd.DataFrame) -> pd.DataFrame:
    unit_col = _first_existing(df, ["unitid", "unit_id", "ipeds_unitid", "matched_unitid"])
    name_col = _first_existing(df, ["ipeds_name", "matched_ipeds_name", "school_name", "name", "institution_name"])
    if unit_col is None or name_col is None:
        raise ValueError("Input must include a unitid column and an IPEDS/school name column.")

    tier_col = _first_existing(df, ["data_capacity_tier", "selection_tier", "capacity_tier"])
    readiness_col = _first_existing(df, ["data_readiness_score", "readiness_num", "capacity_score", "score"])
    match_col = _first_existing(df, ["match_score", "best_match_score"])
    review_col = _first_existing(df, ["needs_match_review", "any_review"])
    total_col = _first_existing(
        df,
        ["bachelor_users_2005_2025", "total_revelio_rows", "revelio_rows", "total_rows"],
    )
    recent_col = _first_existing(
        df,
        ["recent_bachelor_users_2020_2025", "recent_revelio_rows", "recent_rows"],
    )

    out = pd.DataFrame()
    out["unitid"] = pd.to_numeric(df[unit_col], errors="coerce").astype("Int64").astype(str)
    out["unitid"] = out["unitid"].replace("<NA>", "")
    out["ipeds_name"] = _norm_series(df[name_col])
    out["data_capacity_tier"] = _norm_series(df[tier_col]).str.lower() if tier_col else ""
    out["data_readiness_score"] = pd.to_numeric(df[readiness_col], errors="coerce") if readiness_col else pd.NA
    out["match_score"] = pd.to_numeric(df[match_col], errors="coerce") if match_col else pd.NA
    out["needs_match_review"] = _norm_series(df[review_col]).str.lower() if review_col else "no"
    out["total_bachelor_users"] = pd.to_numeric(df[total_col], errors="coerce") if total_col else pd.NA
    out["recent_bachelor_users"] = pd.to_numeric(df[recent_col], errors="coerce") if recent_col else pd.NA

    for col in df.columns:
        if col not in out.columns and col not in {unit_col, name_col}:
            out[f"source_{col}"] = df[col]

    out = out[(out["unitid"] != "") & (out["ipeds_name"] != "")]
    out = out.drop_duplicates("unitid", keep="first")
    return out


def _load_must_include(path: Path) -> pd.DataFrame:
    df = _read_csv(path)
    required = {"match_type", "pattern"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"{path} missing columns: {', '.join(sorted(missing))}")
    df = df.copy()
    df["match_type"] = _norm_series(df["match_type"]).str.lower()
    df["pattern"] = _norm_series(df["pattern"])
    df["reason"] = _norm_series(df["reason"]) if "reason" in df.columns else "must include"
    return df[df["pattern"] != ""]


def _must_include_matches(schools: pd.DataFrame, rules: pd.DataFrame) -> pd.DataFrame:
    names = schools["ipeds_name"].fillna("")
    rows: list[dict] = []
    for _, rule in rules.iterrows():
        pattern = str(rule["pattern"])
        match_type = str(rule["match_type"])
        if match_type == "exact":
            mask = names.str.casefold() == pattern.casefold()
        elif match_type == "contains":
            mask = names.str.contains(pattern, case=False, regex=False, na=False)
        else:
            raise ValueError(f"Unsupported must_include match_type: {match_type}")
        matched = schools.loc[mask, ["unitid", "ipeds_name"]].copy()
        if matched.empty:
            rows.append(
                {
                    "unitid": "",
                    "ipeds_name": pattern,
                    "must_include_reason": str(rule["reason"]),
                    "must_include_pattern": pattern,
                    "must_include_status": "missing_from_input",
                }
            )
            continue
        for _, school in matched.iterrows():
            rows.append(
                {
                    "unitid": school["unitid"],
                    "ipeds_name": school["ipeds_name"],
                    "must_include_reason": str(rule["reason"]),
                    "must_include_pattern": pattern,
                    "must_include_status": "matched",
                }
            )
    if not rows:
        return pd.DataFrame(columns=["unitid", "ipeds_name", "must_include_reason", "must_include_pattern", "must_include_status"])
    out = pd.DataFrame(rows)
    out = out.sort_values(["unitid", "must_include_status", "must_include_pattern"])
    return out.drop_duplicates(["unitid", "ipeds_name"], keep="first")


def _quality_masks(df: pd.DataFrame, high_quality_tiers: set[str], min_score: float, min_must_score: float) -> tuple[pd.Series, pd.Series]:
    tier = df["data_capacity_tier"].fillna("").astype(str).str.lower()
    score = pd.to_numeric(df["data_readiness_score"], errors="coerce")
    match_score = pd.to_numeric(df["match_score"], errors="coerce")
    review = df["needs_match_review"].fillna("").astype(str).str.lower()

    ipeds_matched = df["unitid"].fillna("").astype(str).ne("") & df["ipeds_name"].fillna("").astype(str).ne("")
    clean_match = match_score.isna() | match_score.ge(95)
    not_review = ~review.isin({"yes", "true", "1"})
    not_avoid = ~tier.isin(AVOID_TIERS)

    high_quality = ipeds_matched & clean_match & not_review & (tier.isin(high_quality_tiers) | score.ge(min_score))
    usable_must_include = ipeds_matched & clean_match & (tier.isin(USABLE_TIERS) | score.ge(min_must_score)) & not_avoid
    return high_quality, usable_must_include


def _quality_bin_counts(schools: pd.DataFrame) -> pd.DataFrame:
    df = schools.copy()
    df["quality_bin"] = df["data_capacity_tier"].fillna("").astype(str).str.strip().replace("", "unknown")
    for col in ["high_quality_flag", "must_include_flag", "usable_must_include_flag", "selected_for_expanded_run"]:
        if col not in df.columns:
            df[col] = False
        df[col] = df[col].fillna(False).astype(bool)
    df["data_readiness_score"] = pd.to_numeric(df["data_readiness_score"], errors="coerce")
    df["recent_bachelor_users"] = pd.to_numeric(df["recent_bachelor_users"], errors="coerce")
    df["total_bachelor_users"] = pd.to_numeric(df["total_bachelor_users"], errors="coerce")
    order = {
        "excellent": 0,
        "strong": 1,
        "usable": 2,
        "watchlist": 3,
        "thin": 4,
        "avoid": 5,
        "unknown": 6,
    }
    counts = (
        df.groupby("quality_bin", dropna=False)
        .agg(
            schools=("unitid", "nunique"),
            selected=("selected_for_expanded_run", "sum"),
            high_quality=("high_quality_flag", "sum"),
            must_include=("must_include_flag", "sum"),
            usable_must_include=("usable_must_include_flag", "sum"),
            avg_readiness=("data_readiness_score", "mean"),
            total_bachelor_users=("total_bachelor_users", "sum"),
            recent_bachelor_users=("recent_bachelor_users", "sum"),
        )
        .reset_index()
    )
    counts["sort_order"] = counts["quality_bin"].map(order).fillna(99)
    counts = counts.sort_values(["sort_order", "quality_bin"]).drop(columns=["sort_order"])
    counts["avg_readiness"] = counts["avg_readiness"].round(1)
    return counts


def build_run_list(
    source_path: Path,
    must_include_path: Path,
    current_path: Path | None,
    out_dir: Path,
    target_max: int | None,
    min_score: float,
    min_must_score: float,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    schools = _standardize_school_frame(_read_csv(source_path))
    rules = _load_must_include(must_include_path)
    must_matches = _must_include_matches(schools, rules)
    matched_must = must_matches[must_matches["must_include_status"] == "matched"].copy()

    high_quality, usable_must_include = _quality_masks(schools, DEFAULT_TIERS, min_score, min_must_score)
    schools["must_include_flag"] = schools["unitid"].isin(set(matched_must["unitid"]))
    schools["high_quality_flag"] = high_quality
    schools["usable_must_include_flag"] = usable_must_include & schools["must_include_flag"]
    schools["selected_for_expanded_run"] = schools["high_quality_flag"] | schools["usable_must_include_flag"]

    def selection_reason(row: pd.Series) -> str:
        reasons: list[str] = []
        if bool(row["usable_must_include_flag"]):
            reasons.append("must_include")
        if bool(row["high_quality_flag"]):
            reasons.append("high_quality")
        return "+".join(reasons)

    schools["selection_reason"] = schools.apply(selection_reason, axis=1)

    if current_path and current_path.exists():
        current = _standardize_school_frame(_read_csv(current_path))
        current_units = set(current["unitid"])
        schools.loc[schools["unitid"].isin(current_units), "selected_for_expanded_run"] = True
        schools.loc[schools["unitid"].isin(current_units), "selection_reason"] = schools.loc[
            schools["unitid"].isin(current_units), "selection_reason"
        ].replace("", "current_run")
        schools.loc[
            schools["unitid"].isin(current_units) & schools["selection_reason"].ne("current_run"),
            "selection_reason",
        ] += "+current_run"

    quality_counts = _quality_bin_counts(schools)

    selected = schools[schools["selected_for_expanded_run"]].copy()
    selected["sort_tier"] = selected["data_capacity_tier"].map({"excellent": 0, "strong": 1, "usable": 2}).fillna(3)
    selected["sort_must"] = (~selected["must_include_flag"]).astype(int)
    selected["sort_score"] = pd.to_numeric(selected["data_readiness_score"], errors="coerce").fillna(-1)
    selected["sort_recent"] = pd.to_numeric(selected["recent_bachelor_users"], errors="coerce").fillna(-1)
    selected = selected.sort_values(
        ["sort_must", "sort_tier", "sort_score", "sort_recent", "ipeds_name"],
        ascending=[True, True, False, False, True],
    )
    if target_max is not None and len(selected) > target_max:
        selected = selected.head(target_max).copy()

    selected = selected.drop(columns=[c for c in selected.columns if c.startswith("sort_")])
    selected.insert(0, "run_rank", range(1, len(selected) + 1))

    out_dir.mkdir(parents=True, exist_ok=True)
    selected.to_csv(out_dir / "expanded_school_run_list.csv", index=False)
    quality_counts.to_csv(out_dir / "school_quality_bin_counts.csv", index=False)

    review = must_matches.merge(
        schools[
            [
                "unitid",
                "ipeds_name",
                "data_capacity_tier",
                "data_readiness_score",
                "match_score",
                "needs_match_review",
                "must_include_flag",
                "usable_must_include_flag",
                "selected_for_expanded_run",
                "selection_reason",
            ]
        ],
        on=["unitid", "ipeds_name"],
        how="left",
    )
    review.to_csv(out_dir / "expanded_school_must_include_review.csv", index=False)
    return selected, review, quality_counts


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True, help="Full school capacity audit or matched candidate CSV.")
    parser.add_argument("--must-include", type=Path, default=Path("config/must_include_schools.csv"))
    parser.add_argument("--current", type=Path, default=None, help="Optional current run list to preserve.")
    parser.add_argument("--out-dir", type=Path, default=Path("outputs"))
    parser.add_argument("--target-max", type=int, default=1350, help="Maximum schools to select; use 0 for no cap.")
    parser.add_argument("--min-score", type=float, default=75.0, help="Readiness score floor for non-tiered high quality rows.")
    parser.add_argument("--min-must-score", type=float, default=55.0, help="Readiness score floor for must-include usable rows.")
    args = parser.parse_args()

    target_max = None if args.target_max == 0 else args.target_max
    selected, review, quality_counts = build_run_list(
        source_path=args.source,
        must_include_path=args.must_include,
        current_path=args.current,
        out_dir=args.out_dir,
        target_max=target_max,
        min_score=args.min_score,
        min_must_score=args.min_must_score,
    )

    print("\nSchool quality bins:")
    print(quality_counts.to_string(index=False))
    print()
    print(f"Selected schools: {len(selected):,}")
    print(f"Must-include matched: {(review['must_include_status'] == 'matched').sum():,}")
    print(f"Must-include missing from input: {(review['must_include_status'] == 'missing_from_input').sum():,}")
    if "selected_for_expanded_run" in review.columns:
        selected_review_flag = review["selected_for_expanded_run"].map(lambda value: bool(value) if pd.notna(value) else False)
        missed = review[
            (review["must_include_status"] == "matched")
            & (~selected_review_flag)
        ]
        print(f"Must-include matched but not selected: {len(missed):,}")
        if len(missed):
            print(missed[["unitid", "ipeds_name", "data_capacity_tier", "data_readiness_score", "needs_match_review"]].head(30).to_string(index=False))
    print(f"Wrote: {args.out_dir / 'expanded_school_run_list.csv'}")
    print(f"Wrote: {args.out_dir / 'expanded_school_must_include_review.csv'}")
    print(f"Wrote: {args.out_dir / 'school_quality_bin_counts.csv'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
