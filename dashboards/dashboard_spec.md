# Dashboard Design Specification
## B2B Lead Lifecycle & Full-Funnel Attribution Dashboard

---

## Dashboard 1: Executive Funnel Overview

**Target audience:** VP Marketing, CMO, Revenue Operations leadership
**Refresh cadence:** Daily

### KPI Cards (top row)
| KPI | Metric | Delta |
|-----|--------|-------|
| Total MQLs | Count | MoM % |
| SQLs | Count | MoM % |
| Opportunities | Count | MoM % |
| Closed Won | Count | MoM % |
| Total Revenue | $ | MoM % |
| Win Rate | % | MoM pp |
| Avg Deal Size | $ | MoM % |

**Color encoding:** green = positive delta, red = negative, yellow = flat (±2%)

### Charts

**1A: Funnel Conversion Waterfall**
- Type: Horizontal bar chart (sorted top-to-bottom by stage)
- X-axis: Count of contacts
- Y-axis: Stage labels (New Lead → Closed Won)
- Annotations: Stage-to-stage conversion rate labeled on each bar
- Color: Single gradient (blue → green as funnel narrows)

**1B: Monthly Volume Trend**
- Type: Multi-line chart (4 lines: MQL, SQL, Opp, Won)
- X-axis: Calendar month (YYYY-MM)
- Y-axis: Count
- Color encoding: MQL=blue, SQL=green, Opp=orange, Won=purple
- Reference line: 3-month rolling average for MQL

**1C: Stage Duration Box Plots**
- Type: Box plot (one box per stage)
- Y-axis: Days in stage
- Shows: median, IQR, whiskers (1.5x IQR), outliers
- Bottleneck highlight: stages above threshold colored red

**Filters:** Date range picker, Industry multi-select, Company Size, Region, Product Line

### Calculated Fields (Tableau)
```
MoM Delta % = (SUM([current_month]) - SUM([prior_month])) / SUM([prior_month])
Stage Conversion Rate = SUM([converted_count]) / SUM([stage_entry_count])
Bottleneck Flag = IF [median_days] > [threshold] THEN "BOTTLENECK" ELSE "OK" END
```

---

## Dashboard 2: Attribution & Channel Performance

**Target audience:** Demand Gen Manager, Campaign Manager, Marketing Ops
**Refresh cadence:** Daily

### Charts

**2A: Attributed Revenue by Channel (Primary)**
- Type: Horizontal bar chart, sorted by selected attribution model
- X-axis: Attributed Revenue ($)
- Y-axis: Channel
- Parameter: Attribution Model selector (dropdown: First Touch, Last Touch, Linear, Time Decay, Position-Based, Data-Driven)
- When model changes, bars animate to new values
- Color: Each channel has a fixed color for consistency across dashboards

**2B: ROAS Scatter Plot**
- Type: Bubble scatter chart
- X-axis: Total Spend ($)
- Y-axis: Attributed Revenue ($)
- Bubble size: Conversion volume (# of closed-won contacts)
- Color: Channel
- Reference line: ROAS=1 (break-even, dashed red)
- Annotation: Channels above the line are profitable; below are loss-making

**2C: Campaign Leaderboard**
- Type: Horizontal bar chart, top 15 campaigns
- X-axis: Attributed Revenue ($)
- Color: ROI (diverging color scale: red=negative ROI, green=positive)
- Secondary: Small ROI badge on each bar
- Filter: Campaign Type multi-select

**2D: Model Comparison Table**
- Type: Cross-tab / Highlight table
- Rows: Channels
- Columns: Attribution models
- Values: Attributed Revenue
- Color: Heat encoding within each row (darker = more attribution credit)

**Filters:** Date range, Attribution Model (for 2A/2B), Channel, Campaign Type

### Calculated Fields (Tableau)
```
Selected Model Revenue =
    IF [Selected Model] = "First Touch" THEN [first_touch_rev]
    ELSEIF [Selected Model] = "Time Decay" THEN [time_decay_rev]
    ...
ROAS = [attributed_revenue] / NULLIF([total_spend], 0)
```

---

## Dashboard 3: Lead Velocity & Pipeline Health

**Target audience:** Revenue Ops, Marketing Ops, Sales Leadership
**Refresh cadence:** Daily

### Charts

**3A: Lead Velocity Rate (LVR) Trend**
- Type: Combo chart (bar + line)
- Bars: MQL count per month
- Line (secondary Y): LVR % (MoM growth)
- Reference line: LVR = 0 (horizontal dashed)
- Color zones: Green zone above 0, red zone below 0
- Alert annotation: Flag when LVR drops below -10% (config threshold)

**3B: Pipeline Velocity by Segment**
- Type: Treemap or horizontal bar
- Dimension: Company Size × Industry
- Metric: Pipeline velocity ($pipeline × win_rate × avg_deal) / avg_sales_cycle
- Color: Pipeline velocity (green = fast, red = slow)

**3C: Stage Duration Distribution**
- Type: Box plots (one per stage transition)
- Shows median + IQR + outliers
- Bottleneck highlight: median > threshold → red fill
- Tooltip: "Average time in MQL stage is 18 days vs 25-day bottleneck threshold"

**3D: Marketing Sourced vs. Influenced Pipeline**
- Type: Stacked bar or donut chart
- Dimensions: Marketing Sourced, Marketing Influenced, Sales Only
- Metrics: Pipeline value, Win Rate (dual axis)

**Filters:** Date range, Segment (Industry, Company Size), Product Line

---

## Dashboard 4: Cohort Analysis

**Target audience:** Marketing Strategy, Demand Gen, Lifecycle Marketing
**Refresh cadence:** Weekly

### Charts

**4A: Acquisition Cohort Conversion Heatmap**
- Type: Heatmap
- Rows: Acquisition month (YYYY-MM)
- Columns: Months since acquisition (0–12)
- Values: Cumulative conversion rate to selected stage
- Color scale: White → Blue (0% → 100%)
- Parameter: Stage selector (SQL, Opportunity, Closed Won)
- Annotation: Best-performing cohort highlighted with bold border

**4B: Cohort Conversion Curves (Line Chart)**
- Type: Multi-line, each line = one acquisition cohort
- X-axis: Months since MQL
- Y-axis: Cumulative conversion rate (%)
- Color: Cohort month (sequential palette)
- Highlight: Most recent cohort bolded; comparison cohort selectable

**4C: Channel Cohort Comparison**
- Type: Grouped bar chart
- X-axis: Channel (first-touch)
- Y-axis: Win rate, Revenue per Lead (dual metrics, toggleable)
- Shows which channels produce fastest/highest-value cohorts

**4D: Cohort Revenue Stacked Area**
- Type: Stacked area chart
- X-axis: Calendar month (closed date)
- Y-axis: Revenue ($)
- Color: Acquisition cohort
- Shows how older cohorts continue to contribute revenue

**Filters:** Cohort type (Month/Channel/Segment), Metric (Conversion/Revenue/Retention), Cohort date range

### Calculated Fields (Tableau)
```
Months Since Acquisition = DATEDIFF('month', [cohort_month], [activity_month])
Cumulative Conversion Rate = RUNNING_SUM(SUM([converted])) / FIRST(SUM([cohort_size]))
Payback Period = MIN([months_since]) WHERE [cumulative_revenue] >= [acquisition_cost]
```

---

## Layout Guidelines

### Color System
| Element | Color | Hex |
|---------|-------|-----|
| Primary metric | Meta Blue | #1877F2 |
| Positive delta | Success Green | #42B72A |
| Negative delta | Alert Red | #E74C3C |
| Warning | Orange | #E17055 |
| Neutral | Gray | #636e72 |

### Typography
- Dashboard title: Bold, 18px
- Chart title: Medium, 14px
- KPI value: Bold, 24px
- KPI label: Regular, 11px
- Body text: Regular, 12px

### Layout Grid
Each dashboard uses a 12-column grid:
- KPI cards: 2 columns each (6 cards = full row)
- Primary charts: 6 columns (half-width)
- Full-width trend charts: 12 columns

### Interactivity
- All filter panels are collapsible
- Cross-filtering: clicking a channel bar highlights that channel across all charts
- Hover tooltips show full metric name, value, period, and delta
- Export buttons: CSV download per chart, PDF of full dashboard
