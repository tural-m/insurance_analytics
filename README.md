# Insurance Analytics — Snowflake + Python + Power BI

> A full-stack analytics pipeline that surfaces five counter-intuitive pricing findings across 27,364 auto insurance policies — built on Medallion Architecture in Snowflake and visualized in a 6-page Power BI dashboard.

---

## Overview

| | |
|---|---|
| **Domain** | Insurance |
| **Tools** | Python (pandas, numpy) · Snowflake · SQL · Power BI · DAX |
| **Architecture** | Medallion (Bronze → Silver → Gold) |
| **Data** | Synthetic dataset · 27,364 policies · 2025 snapshot |
| **Dashboard** | 6 pages · 18 DAX measures · Snowflake DirectQuery |

---

## Business Problem

An auto insurance portfolio generating $40.6M in premium and $27.9M in claims has a 68.86% overall loss ratio. The question is not whether the portfolio is profitable in aggregate — it's whether the pricing model is allocating risk correctly across segments. This project answers: which customer segments, regions, and vehicle combinations are generating losses disproportionate to their premiums, and why?

---

## Key Findings

1. **Pricing Adequacy Gap** — The Low risk segment (58% of the portfolio) generates only $4.2M in underwriting margin on $20.6M in premium. The Medium risk segment (36% of the portfolio) generates $6.8M on $16.4M — 64% more margin per dollar of premium. The largest segment is the least efficient.

2. **Regional Concentration** — BC (70.96%) and Alberta (70.94%) are the worst performing regions, both above the 65% industry benchmark. The gap is driven by Low risk concentration, not geography-specific claim costs.

3. **Accident Pricing Gradient** — Customers with 0 previous accidents have a 73.38% loss ratio. Customers with 3 accidents have a 50.77% loss ratio. Claim frequency is nearly flat across all groups (20–23%) — the difference is driven entirely by severity, suggesting the premium uplift per accident is not calibrated to the actual claim improvement.

4. **Health Factor Inversion** — Non-smokers with no chronic conditions produce a 77.02% loss ratio. Smokers with chronic conditions produce 53.96%. Average age and driving experience are virtually identical across all four health groups (45–47 years, ~9.8 years experience), ruling out demographics as a confounding factor. Health risk flags are not predictive of auto claims in this dataset.

5. **Vehicle × Accident Interaction** — Minivan with 0 accidents produces a 78.79% loss ratio — the worst single combination. Truck with 3 accidents produces 41.49% — the best. A 37.3 percentage point gap between two combinations that the current pricing model treats as far closer in risk than they actually are.

---

## Architecture

```
Raw CSV
   │
   ▼
Bronze Layer (Snowflake)
   │  Raw ingestion, no transformations
   │  Tables: INSURANCE_RAW
   │
   ▼
Silver Layer (Snowflake)
   │  Python (pandas) — dirty data cleaned
   │  fillna(median) for AGE, standardized categoricals
   │  Output: INSURANCE_CLEAN
   │
   ▼
Gold Detail Layer (Snowflake)
   │  Wide granular table: INSURANCE_DETAIL
   │  27,364 rows · 27 columns
   │  Engineered features: AGEBAND, AGEBANDSORT,
   │  DRIVINGEXPERIENCEBAND, RISKCATEGORYSORT
   │
   ▼
Power BI (DirectQuery → Snowflake GOLD_DETAIL)
   │  18 DAX measures across 2 display folders
   │  Core Metrics · Pricing Analysis
   │  6-page dashboard
```

**Why a dedicated Gold Detail layer?** Keeping a wide, clean, pre-engineered table in Snowflake separates transformation logic from reporting logic. Power BI connects directly to `INSURANCE_DETAIL` via DirectQuery — all DAX measures operate on a single, well-defined grain without requiring Power Query transformations at report time.

---

## Dashboard Pages

| Page | Business Question |
|---|---|
| Portfolio Overview | What does the portfolio look like at a glance? |
| Pricing Adequacy | Is the pricing model allocating risk efficiently? |
| Regional Profitability | Which provinces are underperforming and why? |
| Accident & Health Paradox | Are behavioral and health factors priced correctly? |
| Vehicle & Accident Pricing Gap | Does vehicle type interact with accident history in ways the model ignores? |
| Insights & Recommendations | What should the business do about it? |

---

## DAX Measures

**Core Metrics (11 measures)**

| Measure | Logic |
|---|---|
| `Total Premium` | SUM of annual premium |
| `Total Claims` | SUM of claim amounts |
| `Total Policies` | COUNTROWS |
| `Loss Ratio` | Total Claims / Total Premium |
| `Avg Premium` | AVERAGE of annual premium |
| `Avg Claim Amount` | Total Claims / Total Policies (all rows, including zero-claim) |
| `Avg Claim Severity` | Total Claims / Count of policies with claims > 0 |
| `Claim Frequency` | Count of claimants / Total Policies |
| `Loss Ratio (F × S)` | Validates: Frequency × Severity / Avg Premium = Loss Ratio |
| `Total Premium (M)` | Total Premium / 1,000,000 |
| `Total Claims (M)` | Total Claims / 1,000,000 |

**Pricing Analysis (7 measures)**

| Measure | Logic |
|---|---|
| `Avg Risk Score` | AVERAGE of risk score in filter context |
| `% of Total Policies` | Segment policies / ALL policies |
| `Loss Ratio vs Benchmark` | Loss Ratio − 0.70 portfolio benchmark |
| `Profitability Flag` | Loss Ratio − 0.70 |
| `Worst Combo Loss Ratio` | CALCULATE Loss Ratio → Minivan + 0 accidents |
| `Best Combo Loss Ratio` | CALCULATE Loss Ratio → Truck + 3 accidents |
| `Combo Gap` | Worst − Best combination loss ratio |

---

## Repo Structure

```
insurance-analytics/
│
├── 1-Metadata/
│   └── BronzeLayerMD.sql          # Creates DB, schemas, Bronze tables
│
├── 2-DataProfiling/
│   └── DataProfiling.sql          # Profiles Bronze layer data
│
├── 3-PostBronze/
│   └── (manual CSV upload via Snowflake UI)
│
├── 4-SilverLayer/
│   └── Reframing.sql              # Cleans Bronze → writes to Silver
│
├── 5a-GoldDetailLayer/
│   └── GoldDetail.sql             # Wide granular table (27,364 rows, 27 cols)
│
├── dashboard/
│   └── insurance.pbix             # Power BI Desktop file
│
└── README.md
```

---

## How to Reproduce

**Prerequisites:** Snowflake account · Power BI Desktop · Python 3.x with pandas and numpy

**Step 1 — Snowflake setup**
```sql
-- Run in order:
-- 1. 1-Metadata/BronzeLayerMD.sql
-- 2. Upload raw CSV via Snowflake UI → BRONZE schema
-- 3. 2-DataProfiling/DataProfiling.sql (optional — data profiling)
-- 4. 4-SilverLayer/Reframing.sql
-- 5. 5a-GoldDetailLayer/GoldDetail.sql
```

**Step 2 — Power BI connection**
1. Open `dashboard/insurance.pbix`
2. Transform Data → Data Source Settings
3. Update Snowflake server and database credentials
4. Refresh

---

## Author

Tural Mansimov | [LinkedIn](https://linkedin.com/in/tural-m) 
