"""
Synthetic B2B lead lifecycle data generator.

Simulates Meta Business Messaging Marketing motion — selling messaging APIs
and business tools. Mirrors a Salesforce CRM + marketing automation environment.

Run: python -m src.data_generator
"""
import sqlite3
import os
import sys
import random
from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd
from faker import Faker

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

fake = Faker()
Faker.seed(config.RANDOM_SEED)
np.random.seed(config.RANDOM_SEED)
random.seed(config.RANDOM_SEED)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _weighted_choice(options: dict):
    keys   = list(options.keys())
    weights = list(options.values())
    return random.choices(keys, weights=weights, k=1)[0]

def _rand_date(start: date, end: date) -> date:
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))

def _add_days(d: date, days: float) -> date:
    return d + timedelta(days=int(max(0, days)))

def _quarter(d: date) -> int:
    return (d.month - 1) // 3 + 1

def _seasonality_pipeline(d: date) -> float:
    return config.QUARTERLY_PIPELINE_MULTIPLIER[_quarter(d)]

def _seasonality_close(d: date) -> float:
    return config.QUARTERLY_CLOSE_RATE_MULTIPLIER[_quarter(d)]

# ---------------------------------------------------------------------------
# 1. Campaigns (80 rows)
# ---------------------------------------------------------------------------

def generate_campaigns() -> pd.DataFrame:
    rows = []
    for i in range(1, config.N_CAMPAIGNS + 1):
        ctype   = random.choice(config.CAMPAIGN_TYPES)
        channel = random.choice(config.CHANNELS)
        start   = _rand_date(config.DATA_START_DATE, date(2025, 6, 30))
        dur     = random.randint(30, 120)
        end     = _add_days(start, dur)
        budget  = round(random.uniform(10_000, 500_000), 2)
        rows.append({
            "campaign_id":    f"CAMP_{i:04d}",
            "campaign_name":  f"{start.year}_Q{_quarter(start)}_{channel}_{ctype}_{i}".replace(" ", "_"),
            "campaign_type":  ctype,
            "channel":        channel,
            "start_date":     start.isoformat(),
            "end_date":       end.isoformat(),
            "budget":         budget,
            "target_segment": random.choice(["SMB", "Mid-Market", "Enterprise", "All"]),
            "status":         "Completed" if end < date(2025, 10, 1) else random.choice(["Active", "Paused"]),
        })
    return pd.DataFrame(rows)

# ---------------------------------------------------------------------------
# 2. Accounts (30 000 rows)
# ---------------------------------------------------------------------------

def generate_accounts() -> pd.DataFrame:
    rows = []
    for i in range(1, config.N_ACCOUNTS + 1):
        size     = _weighted_choice(config.COMPANY_SIZES)
        industry = _weighted_choice(config.INDUSTRIES)
        region   = _weighted_choice(config.REGIONS)
        lo, hi   = config.EMPLOYEE_COUNT_RANGES[size]
        emp      = random.randint(lo, hi)
        rev_tier = (
            "Under_1M"     if emp < 50    else
            "1M_10M"       if emp < 500   else
            "10M_100M"     if emp < 5000  else
            "100M_Plus"
        )
        rows.append({
            "account_id":           f"ACC_{i:06d}",
            "company_name":         fake.company(),
            "industry":             industry,
            "company_size":         size,
            "employee_count":       emp,
            "region":               region,
            "country":              fake.country(),
            "annual_revenue_tier":  rev_tier,
            "account_created_date": _rand_date(config.DATA_START_DATE, config.DATA_END_DATE).isoformat(),
            "account_owner":        fake.name(),
            "icp_score":            random.randint(1, 100),
            "salesforce_id":        f"SF_{fake.uuid4()[:12].upper()}",
        })
    return pd.DataFrame(rows)

# ---------------------------------------------------------------------------
# 3. Contacts + lifecycle dates (90 000 rows)
# ---------------------------------------------------------------------------

def generate_contacts(accounts: pd.DataFrame) -> pd.DataFrame:
    rows          = []
    contact_idx   = 0

    # Pre-compute account attributes as dict for fast lookup
    acc_map = accounts.set_index("account_id")[["company_size", "icp_score"]].to_dict("index")

    for _, acc in accounts.iterrows():
        acc_id     = acc["account_id"]
        acc_size   = acc["company_size"]
        acc_icp    = acc["icp_score"]
        n_contacts = np.random.choice([2, 3, 4], p=[0.3, 0.5, 0.2])

        for j in range(n_contacts):
            contact_idx += 1
            cid          = f"CON_{contact_idx:07d}"
            created      = _rand_date(config.DATA_START_DATE, date(2025, 9, 30))

            # ICP score influences progression probability
            icp_boost = (acc_icp - 50) / 200.0   # -0.25 to +0.25

            # Stage progression
            def stage_prob(base):
                return min(0.98, max(0.02, base + icp_boost))

            lead_source = random.choice(config.LEAD_SOURCES)
            ch_mult     = config.CHANNEL_WIN_RATE_MULTIPLIER.get(lead_source, 1.0)
            season_mult = _seasonality_pipeline(created)

            mql_date = opp_date = sql_date = sal_date = closed_date = None

            # MQL
            if random.random() < stage_prob(config.FUNNEL_CONVERSION_RATES["New_Lead_to_MQL"] * season_mult):
                days_to_mql = max(1, int(np.random.normal(
                    config.STAGE_DURATION_PARAMS["New_Lead"]["mean"],
                    config.STAGE_DURATION_PARAMS["New_Lead"]["std"]
                )))
                mql_date = _add_days(created, days_to_mql)
                if mql_date > config.DATA_END_DATE:
                    mql_date = None

            if mql_date:
                # SQL
                if random.random() < stage_prob(config.FUNNEL_CONVERSION_RATES["MQL_to_SQL"]):
                    days = max(1, int(np.random.normal(
                        config.STAGE_DURATION_PARAMS["MQL"]["mean"],
                        config.STAGE_DURATION_PARAMS["MQL"]["std"]
                    )))
                    sql_date = _add_days(mql_date, days)
                    if sql_date > config.DATA_END_DATE:
                        sql_date = None

            if sql_date:
                # SAL
                if random.random() < stage_prob(config.FUNNEL_CONVERSION_RATES["SQL_to_SAL"]):
                    days = max(1, int(np.random.normal(
                        config.STAGE_DURATION_PARAMS["SQL"]["mean"],
                        config.STAGE_DURATION_PARAMS["SQL"]["std"]
                    )))
                    sal_date = _add_days(sql_date, days)
                    if sal_date > config.DATA_END_DATE:
                        sal_date = None

            if sal_date:
                # Opportunity
                if random.random() < stage_prob(config.FUNNEL_CONVERSION_RATES["SAL_to_Opp"]):
                    days = max(1, int(np.random.normal(
                        config.STAGE_DURATION_PARAMS["SAL"]["mean"],
                        config.STAGE_DURATION_PARAMS["SAL"]["std"]
                    )))
                    opp_date = _add_days(sal_date, days)
                    if opp_date > config.DATA_END_DATE:
                        opp_date = None

            if opp_date:
                # Closed
                close_prob = config.FUNNEL_CONVERSION_RATES["Opp_to_Won"] * ch_mult * _seasonality_close(opp_date)
                days = max(1, int(np.random.normal(
                    config.STAGE_DURATION_PARAMS["Opportunity"]["mean"],
                    config.STAGE_DURATION_PARAMS["Opportunity"]["std"]
                )))
                closed_date = _add_days(opp_date, days)
                if closed_date > config.DATA_END_DATE:
                    closed_date = None
                    close_prob  = 0  # still open

            # Determine final status
            if opp_date and closed_date:
                if random.random() < close_prob:
                    status = "Closed_Won"
                else:
                    status = "Closed_Lost"
            elif opp_date:
                status = "Opportunity"
            elif sal_date:
                status = "SAL"
            elif sql_date:
                status = "SQL"
            elif mql_date:
                status = "MQL"
            else:
                status = "New_Lead"

            rows.append({
                "contact_id":        cid,
                "account_id":        acc_id,
                "email":             fake.email(),
                "first_name":        fake.first_name(),
                "last_name":         fake.last_name(),
                "job_title":         fake.job(),
                "job_level":         random.choices(
                    ["C-Suite", "VP", "Director", "Manager", "IC"],
                    weights=[0.05, 0.10, 0.20, 0.35, 0.30]
                )[0],
                "department":        random.choice(["Marketing", "Sales", "IT", "Operations", "Finance", "Product"]),
                "created_date":      created.isoformat(),
                "lead_source":       lead_source,
                "lead_status":       status,
                "mql_date":          mql_date.isoformat()    if mql_date    else None,
                "sql_date":          sql_date.isoformat()    if sql_date    else None,
                "sal_date":          sal_date.isoformat()    if sal_date    else None,
                "opportunity_date":  opp_date.isoformat()    if opp_date    else None,
                "closed_date":       closed_date.isoformat() if closed_date else None,
                "is_primary_contact": (j == 0),
            })

    return pd.DataFrame(rows)

# ---------------------------------------------------------------------------
# 4. Opportunities (from contacts that have opp_date)
# ---------------------------------------------------------------------------

def generate_opportunities(contacts: pd.DataFrame, accounts: pd.DataFrame) -> pd.DataFrame:
    opp_contacts = contacts[contacts["opportunity_date"].notna()].copy()
    acc_size_map = accounts.set_index("account_id")["company_size"].to_dict()
    rows = []
    for i, (_, c) in enumerate(opp_contacts.iterrows(), 1):
        acc_id   = c["account_id"]
        size     = acc_size_map.get(acc_id, "SMB")
        lo, hi   = config.DEAL_SIZE_RANGES[size]
        amount   = round(np.random.lognormal(
            mean=np.log((lo + hi) / 2), sigma=0.4
        ))
        amount   = max(lo, min(hi, amount))
        won      = c["lead_status"] == "Closed_Won"
        opp_date = date.fromisoformat(c["opportunity_date"])
        if c["closed_date"] and isinstance(c["closed_date"], str):
            close_date = date.fromisoformat(c["closed_date"])
        else:
            close_date = _add_days(opp_date, random.randint(20, 90))

        if won:
            stage = "Closed_Won"
            loss_reason = None
        elif c["closed_date"]:
            stage = "Closed_Lost"
            loss_reason = random.choice(["Budget", "Competitor", "Timing", "No_Decision", "Champion_Left"])
        else:
            stage = random.choice(["Discovery", "Proposal", "Negotiation"])
            loss_reason = None

        rows.append({
            "opportunity_id":      f"OPP_{i:06d}",
            "account_id":          acc_id,
            "primary_contact_id":  c["contact_id"],
            "opportunity_name":    f"{fake.company()} - {random.choice(config.PRODUCT_LINES)}",
            "stage":               stage,
            "amount":              amount,
            "created_date":        c["opportunity_date"],
            "close_date":          close_date.isoformat(),
            "days_to_close":       (close_date - opp_date).days,
            "product_line":        random.choice(config.PRODUCT_LINES),
            "win_probability":     round(random.uniform(0.6, 0.95) if won else random.uniform(0.1, 0.5), 2),
            "loss_reason":         loss_reason,
            "closed_won":          won,
        })
    return pd.DataFrame(rows)

# ---------------------------------------------------------------------------
# 5. Touchpoints (800 000 rows)
# ---------------------------------------------------------------------------

def generate_touchpoints(contacts: pd.DataFrame, campaigns: pd.DataFrame) -> pd.DataFrame:
    """
    Generate marketing touchpoints correlated with funnel progression.
    Contacts deeper in the funnel get more touchpoints.
    """
    TOUCHPOINTS_BY_STATUS = {
        "New_Lead":   (1,  3),
        "MQL":        (3,  8),
        "SQL":        (6, 12),
        "SAL":        (8, 15),
        "Opportunity":(12, 20),
        "Closed_Won": (15, 25),
        "Closed_Lost":(8,  18),
        "Disqualified":(2,  5),
        "Recycled":   (3,  8),
    }

    CHANNEL_TOUCHPOINT_MAP = {
        "Paid_Search":        ["ad_impression", "ad_click", "page_visit"],
        "Paid_Social":        ["ad_impression", "ad_click", "form_submit"],
        "Organic_Search":     ["page_visit", "content_download"],
        "Email":              ["email_send", "email_open", "email_click"],
        "Content_Syndication":["content_download", "form_submit"],
        "Direct":             ["page_visit", "demo_request"],
        "Webinar":            ["webinar_register", "webinar_attend"],
        "Events":             ["event_attend", "form_submit"],
        "Partner":            ["partner_intro", "demo_request"],
        "Outbound_Sales":     ["sales_call", "email_send"],
    }

    camp_ids   = campaigns["campaign_id"].tolist()
    camp_names = dict(zip(campaigns["campaign_id"], campaigns["campaign_name"]))
    camp_channels = dict(zip(campaigns["campaign_id"], campaigns["channel"]))

    rows         = []
    tp_idx       = 0
    total_target = config.N_TOUCHPOINTS

    # We'll scale per contact to hit ~800K total
    for _, c in contacts.iterrows():
        status  = c["lead_status"]
        lo, hi  = TOUCHPOINTS_BY_STATUS.get(status, (1, 3))
        n_tp    = random.randint(lo, hi)

        # Determine contact date window
        import pandas as _pd
        def _safe_date(v):
            return None if (v is None or (isinstance(v, float) and _pd.isna(v))) else v

        start_dt  = date.fromisoformat(c["created_date"])
        cd  = _safe_date(c["closed_date"])
        od  = _safe_date(c["opportunity_date"])
        md  = _safe_date(c["mql_date"])
        end_ref   = (
            date.fromisoformat(cd) if cd else
            date.fromisoformat(od) if od else
            date.fromisoformat(md) if md else
            _add_days(start_dt, 60)
        )
        # Add 3% data gap: contacts with no touchpoints before MQL
        if md and random.random() < 0.03:
            n_tp = 0

        tp_dates = sorted([_rand_date(start_dt, end_ref) for _ in range(n_tp)])

        for k, tp_date in enumerate(tp_dates):
            tp_idx += 1
            camp_id = random.choice(camp_ids)
            channel = camp_channels[camp_id]
            tp_type = random.choice(CHANNEL_TOUCHPOINT_MAP.get(channel, config.TOUCHPOINT_TYPES))

            # Determine paid cost (NULL for organic/non-paid)
            if channel in ("Paid_Search", "Paid_Social", "Content_Syndication"):
                cost = round(random.uniform(5, 200), 2)
            else:
                cost = None

            rows.append({
                "touchpoint_id":              f"TP_{tp_idx:09d}",
                "contact_id":                 c["contact_id"],
                "account_id":                 c["account_id"],
                "touchpoint_type":            tp_type,
                "channel":                    channel,
                "campaign_id":                camp_id,
                "campaign_name":              camp_names[camp_id],
                "touchpoint_timestamp":       datetime.combine(tp_date, datetime.min.time()).isoformat(),
                "is_first_touch":             (k == 0),
                "is_last_touch_before_mql":   False,  # set in post-processing
                "is_last_touch_before_opp":   False,
                "content_asset":              _random_content_asset(tp_type),
                "utm_source":                 channel.lower().replace("_", "-"),
                "utm_medium":                 tp_type.lower().replace("_", "-"),
                "utm_campaign":               camp_id,
                "cost":                       cost,
            })

    df = pd.DataFrame(rows)

    # Post-process: flag last touch before MQL and before Opp
    if not df.empty:
        mql_map = contacts.set_index("contact_id")["mql_date"].dropna().to_dict()
        opp_map = contacts.set_index("contact_id")["opportunity_date"].dropna().to_dict()

        for cid, grp in df.groupby("contact_id"):
            if cid in mql_map:
                mql_dt = mql_map[cid]
                before = grp[grp["touchpoint_timestamp"] <= mql_dt]
                if not before.empty:
                    last_idx = before["touchpoint_timestamp"].idxmax()
                    df.loc[last_idx, "is_last_touch_before_mql"] = True

            if cid in opp_map:
                opp_dt = opp_map[cid]
                before = grp[grp["touchpoint_timestamp"] <= opp_dt]
                if not before.empty:
                    last_idx = before["touchpoint_timestamp"].idxmax()
                    df.loc[last_idx, "is_last_touch_before_opp"] = True

    return df

def _random_content_asset(tp_type: str) -> str | None:
    WHITEPAPERS = [
        "Messaging API Getting Started Guide", "WhatsApp for Business ROI Report",
        "B2B Messaging Trends 2025", "Commerce Tools Buyer's Guide",
        "Customer Messaging Best Practices",
    ]
    WEBINARS = [
        "Building on the Messaging API", "WhatsApp Business Platform Deep Dive",
        "Scaling Customer Engagement with AI", "Meta Business Suite Demo Day",
    ]
    if tp_type in ("content_download", "form_submit"):
        return random.choice(WHITEPAPERS)
    elif tp_type in ("webinar_register", "webinar_attend"):
        return random.choice(WEBINARS)
    return None

# ---------------------------------------------------------------------------
# 6. Lead stage history
# ---------------------------------------------------------------------------

def generate_lead_stages(contacts: pd.DataFrame) -> pd.DataFrame:
    """Build a historical log of stage transitions for each contact."""
    stage_order = ["New_Lead", "MQL", "SQL", "SAL", "Opportunity", "Negotiation"]
    date_cols   = {
        "New_Lead":    "created_date",
        "MQL":         "mql_date",
        "SQL":         "sql_date",
        "SAL":         "sal_date",
        "Opportunity": "opportunity_date",
    }
    terminal_map = {
        "Closed_Won":    ("Negotiation", "Won"),
        "Closed_Lost":   ("Negotiation", "Lost"),
        "Disqualified":  ("SQL",         "Disqualified"),
        "Recycled":      ("MQL",         "Recycled"),
    }

    rows   = []
    idx    = 0
    reps   = [fake.name() for _ in range(50)]

    for _, c in contacts.iterrows():
        status    = c["lead_status"]
        prev_date = None

        def _safe_str(v):
            """Return string value or None if NaN/None."""
            if v is None:
                return None
            try:
                import math
                if math.isnan(float(v)):
                    return None
            except (TypeError, ValueError):
                pass
            return str(v) if v else None

        for stage in stage_order:
            col     = date_cols.get(stage)
            col_val = _safe_str(c.get(col)) if col else None
            if col and col_val:
                entered = date.fromisoformat(col_val)
                if prev_date and entered < prev_date:
                    entered = prev_date  # fix occasional overlap

                # Determine exit
                next_stage_idx = stage_order.index(stage) + 1
                if next_stage_idx < len(stage_order):
                    next_col = date_cols.get(stage_order[next_stage_idx])
                    next_val = _safe_str(c.get(next_col)) if next_col else None
                    if next_col and next_val:
                        exited     = date.fromisoformat(next_val)
                        exit_reason = "Converted"
                    else:
                        # Terminal exit
                        if status in terminal_map and terminal_map[status][0] == stage:
                            closed_d   = _safe_str(c.get("closed_date"))
                            exited     = date.fromisoformat(closed_d) if closed_d else _add_days(entered, 30)
                            exit_reason = terminal_map[status][1]
                        else:
                            exited      = None
                            exit_reason = None
                else:
                    exited      = None
                    exit_reason = None

                days_in = (exited - entered).days if exited else None
                idx += 1
                rows.append({
                    "stage_id":     f"STG_{idx:08d}",
                    "contact_id":   c["contact_id"],
                    "account_id":   c["account_id"],
                    "stage_name":   stage,
                    "entered_date": entered.isoformat(),
                    "exited_date":  exited.isoformat() if exited else None,
                    "days_in_stage":days_in,
                    "exit_reason":  exit_reason,
                    "assigned_rep": random.choice(reps),
                })
                prev_date = entered

    return pd.DataFrame(rows)

# ---------------------------------------------------------------------------
# Write to SQLite
# ---------------------------------------------------------------------------

def write_to_db(dfs: dict, db_path: str):
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    for table_name, df in dfs.items():
        df.to_sql(table_name, conn, if_exists="replace", index=False)
        print(f"  ✓ {table_name}: {len(df):,} rows")
    conn.close()

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 60)
    print("B2B Full-Funnel Attribution — Data Generator")
    print("=" * 60)

    print("\n[1/6] Generating campaigns...")
    campaigns = generate_campaigns()

    print("[2/6] Generating accounts...")
    accounts  = generate_accounts()

    print("[3/6] Generating contacts + lifecycle dates...")
    contacts  = generate_contacts(accounts)

    print("[4/6] Generating opportunities...")
    opportunities = generate_opportunities(contacts, accounts)

    print("[5/6] Generating touchpoints (this takes ~60s)...")
    touchpoints = generate_touchpoints(contacts, campaigns)

    print("[6/6] Generating lead stage history...")
    lead_stages = generate_lead_stages(contacts)

    print(f"\nWriting to {config.DB_PATH}...")
    write_to_db({
        "campaigns":    campaigns,
        "accounts":     accounts,
        "contacts":     contacts,
        "opportunities":opportunities,
        "touchpoints":  touchpoints,
        "lead_stages":  lead_stages,
    }, config.DB_PATH)

    print("\nFunnel shape summary:")
    for status, count in contacts["lead_status"].value_counts().items():
        print(f"  {status:20s}: {count:,}")

    print(f"\nTouchpoints: {len(touchpoints):,}")
    print(f"Opportunities: {len(opportunities):,}")
    won  = opportunities["closed_won"].sum()
    lost = (~opportunities["closed_won"] & opportunities["close_date"].notna()).sum()
    print(f"  Closed Won:  {won:,}")
    print(f"  Closed Lost: {lost:,}")
    print("\nData generation complete.")


if __name__ == "__main__":
    main()
