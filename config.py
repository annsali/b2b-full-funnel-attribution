"""
Central configuration for the B2B Full-Funnel Attribution Dashboard.
All funnel stage definitions, attribution weights, and date ranges live here.
"""
from datetime import date, timedelta

# ---------------------------------------------------------------------------
# Date ranges
# ---------------------------------------------------------------------------
DATA_START_DATE = date(2024, 1, 1)
DATA_END_DATE   = date(2025, 12, 31)
RANDOM_SEED     = 42

# ---------------------------------------------------------------------------
# Data volume
# ---------------------------------------------------------------------------
N_ACCOUNTS     = 30_000
N_CONTACTS     = 90_000   # ~3 per account
N_TOUCHPOINTS  = 800_000
N_CAMPAIGNS    = 80

# ---------------------------------------------------------------------------
# Funnel stages (ordered)
# ---------------------------------------------------------------------------
FUNNEL_STAGES = [
    "New_Lead",
    "MQL",
    "SQL",
    "SAL",
    "Opportunity",
    "Negotiation",
    "Closed_Won",
    "Closed_Lost",
    "Disqualified",
    "Recycled",
]

ACTIVE_FUNNEL_STAGES = ["New_Lead", "MQL", "SQL", "SAL", "Opportunity", "Negotiation"]
TERMINAL_STAGES      = ["Closed_Won", "Closed_Lost", "Disqualified"]

# Target funnel shape (rates are stage-to-stage conversion)
FUNNEL_CONVERSION_RATES = {
    "New_Lead_to_MQL":   0.39,   # 39% of leads → MQL
    "MQL_to_SQL":        0.34,   # 34% of MQLs → SQL
    "SQL_to_SAL":        0.67,   # 67% of SQLs → SAL
    "SAL_to_Opp":        0.75,   # 75% of SALs → Opportunity
    "Opp_to_Won":        0.30,   # 30% win rate
}

# Average days in each stage (with std dev for variance)
STAGE_DURATION_PARAMS = {
    "New_Lead":    {"mean": 30,  "std": 20},
    "MQL":         {"mean": 18,  "std": 12},
    "SQL":         {"mean": 7,   "std": 5},
    "SAL":         {"mean": 12,  "std": 8},
    "Opportunity": {"mean": 45,  "std": 25},
    "Negotiation": {"mean": 15,  "std": 10},
}

# ---------------------------------------------------------------------------
# Company attributes
# ---------------------------------------------------------------------------
INDUSTRIES = {
    "Technology":    0.25,
    "Healthcare":    0.15,
    "Finance":       0.15,
    "Retail":        0.12,
    "Manufacturing": 0.10,
    "Media":         0.08,
    "Education":     0.08,
    "Logistics":     0.07,
}

COMPANY_SIZES = {
    "SMB":         0.50,
    "Mid-Market":  0.30,
    "Enterprise":  0.20,
}

EMPLOYEE_COUNT_RANGES = {
    "SMB":         (10,   500),
    "Mid-Market":  (500,  5_000),
    "Enterprise":  (5_000, 100_000),
}

REGIONS = {
    "NA":   0.40,
    "EMEA": 0.30,
    "APAC": 0.20,
    "LATAM":0.10,
}

REVENUE_TIERS = ["Under_1M", "1M_10M", "10M_100M", "100M_Plus"]

# ---------------------------------------------------------------------------
# Lead sources
# ---------------------------------------------------------------------------
LEAD_SOURCES = [
    "Organic_Search", "Paid_Search", "Paid_Social", "Email",
    "Content_Syndication", "Event", "Partner_Referral", "Direct", "Webinar",
]

# Channel-level win rate multipliers (relative to baseline)
CHANNEL_WIN_RATE_MULTIPLIER = {
    "Paid_Search":        0.85,
    "Paid_Social":        0.90,
    "Organic_Search":     1.00,
    "Email":              1.05,
    "Content_Syndication":1.00,
    "Direct":             1.10,
    "Webinar":            1.30,
    "Events":             1.40,
    "Partner":            1.20,
    "Outbound_Sales":     1.10,
}

# ---------------------------------------------------------------------------
# Touchpoint types and channels
# ---------------------------------------------------------------------------
TOUCHPOINT_TYPES = [
    "ad_impression", "ad_click", "email_send", "email_open", "email_click",
    "content_download", "webinar_register", "webinar_attend", "page_visit",
    "form_submit", "demo_request", "trial_start", "sales_call",
    "event_attend", "partner_intro",
]

CHANNELS = [
    "Paid_Search", "Paid_Social", "Organic_Search", "Email",
    "Content_Syndication", "Direct", "Webinar", "Events", "Partner",
    "Outbound_Sales",
]

# ---------------------------------------------------------------------------
# Products
# ---------------------------------------------------------------------------
PRODUCT_LINES = [
    "Messaging_API", "Business_Suite", "WhatsApp_Business",
    "Ads_Platform", "Commerce_Tools",
]

# Deal size ranges by company size
DEAL_SIZE_RANGES = {
    "SMB":        (5_000,   50_000),
    "Mid-Market": (25_000,  150_000),
    "Enterprise": (75_000,  500_000),
}

# ---------------------------------------------------------------------------
# Attribution model settings
# ---------------------------------------------------------------------------
ATTRIBUTION_MODELS = [
    "first_touch", "last_touch", "linear",
    "time_decay", "position_based", "data_driven",
]

TIME_DECAY_HALF_LIFE_DAYS = 7   # configurable

POSITION_BASED_WEIGHTS = {
    "first": 0.40,
    "last":  0.40,
    "middle_total": 0.20,
}

# ---------------------------------------------------------------------------
# Campaign types
# ---------------------------------------------------------------------------
CAMPAIGN_TYPES = [
    "Demand_Gen", "Nurture", "ABM", "Product_Launch",
    "Event", "Webinar", "Content",
]

# ---------------------------------------------------------------------------
# Seasonality: quarterly multipliers for pipeline creation
# ---------------------------------------------------------------------------
QUARTERLY_PIPELINE_MULTIPLIER = {1: 0.90, 2: 1.00, 3: 1.00, 4: 1.30}
QUARTERLY_CLOSE_RATE_MULTIPLIER = {1: 1.15, 2: 1.00, 3: 0.95, 4: 1.05}

# ---------------------------------------------------------------------------
# Reporting thresholds
# ---------------------------------------------------------------------------
BOTTLENECK_THRESHOLD_DAYS = {
    "MQL":         25,
    "SQL":         14,
    "SAL":         20,
    "Opportunity": 60,
}
CONVERSION_RATE_DROP_ALERT_PCT = 0.10  # 10% WoW drop triggers alert

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------
DB_PATH = "data/funnel_data.db"
