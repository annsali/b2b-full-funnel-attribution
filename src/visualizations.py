"""
All plotting functions for the B2B Attribution Dashboard.

Functions return Plotly figures (interactive HTML) and Matplotlib figures (static).
"""
import os
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots

PALETTE = {
    "primary":   "#1877F2",   # Meta blue
    "secondary": "#42B72A",   # Green
    "accent":    "#E17055",   # Orange
    "neutral":   "#636e72",
    "bg":        "#F8F9FA",
    "channels":  px.colors.qualitative.Set2,
}

# ---------------------------------------------------------------------------
# Funnel charts
# ---------------------------------------------------------------------------

def plot_funnel_waterfall(waterfall: pd.Series) -> go.Figure:
    """Animated funnel waterfall from stage counts."""
    stages = ["total_leads", "total_mql", "total_sql", "total_sal", "total_opp", "total_won"]
    labels = ["New Leads", "MQL", "SQL", "SAL", "Opportunity", "Closed Won"]
    values = [waterfall[s] for s in stages]

    fig = go.Figure(go.Funnel(
        y     = labels,
        x     = values,
        textposition = "inside",
        textinfo     = "value+percent initial",
        marker = dict(
            color = ["#1877F2", "#3498DB", "#2ECC71", "#F39C12", "#E74C3C", "#8E44AD"]
        ),
    ))
    fig.update_layout(title="Full-Funnel Conversion Waterfall", height=500,
                      paper_bgcolor=PALETTE["bg"])
    return fig


def plot_monthly_velocity(monthly_df: pd.DataFrame) -> go.Figure:
    """Line chart: monthly MQL, SQL, Opp, CW volumes."""
    fig = go.Figure()
    colors = {"mqls": PALETTE["primary"], "sqls": PALETTE["secondary"],
              "opps": PALETTE["accent"], "won": PALETTE["neutral"]}
    labels = {"mqls": "MQLs", "sqls": "SQLs", "opps": "Opportunities", "won": "Closed Won"}
    for col, color in colors.items():
        if col not in monthly_df.columns:
            continue
        fig.add_trace(go.Scatter(
            x=monthly_df["month"], y=monthly_df[col],
            name=labels[col], line=dict(color=color, width=2),
            mode="lines+markers",
        ))
    fig.update_layout(
        title="Monthly Funnel Volume Trending",
        xaxis_title="Month", yaxis_title="Count",
        hovermode="x unified", height=400,
        paper_bgcolor=PALETTE["bg"],
    )
    return fig


def plot_stage_duration_box(duration_df: pd.DataFrame) -> go.Figure:
    """Box plots of days in each stage."""
    stages = ["MQL", "SQL", "SAL", "Opportunity", "Negotiation"]
    fig = go.Figure()
    for stage in stages:
        sub = duration_df[duration_df["stage_name"] == stage]["days_in_stage"]
        if sub.empty:
            continue
        fig.add_trace(go.Box(y=sub, name=stage, boxmean=True))
    fig.update_layout(
        title="Stage Duration Distribution (Days)",
        yaxis_title="Days in Stage", height=450,
        paper_bgcolor=PALETTE["bg"],
    )
    return fig


# ---------------------------------------------------------------------------
# Attribution charts
# ---------------------------------------------------------------------------

def plot_attribution_comparison(wide_df: pd.DataFrame) -> go.Figure:
    """Grouped bar: channel × attribution model."""
    model_cols = [c for c in wide_df.columns if c != "channel"]
    fig = go.Figure()
    for i, model in enumerate(model_cols):
        fig.add_trace(go.Bar(
            name=model.replace("_", " ").title(),
            x=wide_df["channel"],
            y=wide_df[model],
        ))
    fig.update_layout(
        barmode="group",
        title="Attributed Revenue by Channel — All Models",
        xaxis_title="Channel", yaxis_title="Attributed Revenue ($)",
        height=500, paper_bgcolor=PALETTE["bg"],
    )
    return fig


def plot_roas_scatter(channel_df: pd.DataFrame) -> go.Figure:
    """Scatter: X = spend, Y = time-decay revenue, bubble = conversions."""
    df = channel_df.copy()
    df = df[df["total_spend"] > 0].copy()
    if df.empty:
        return go.Figure()

    fig = px.scatter(
        df,
        x="total_spend",
        y="time_decay_rev",
        size="time_decay_rev",
        color="channel",
        hover_name="channel",
        text="channel",
        title="ROAS Scatter: Spend vs Attributed Revenue (Time Decay)",
        labels={"total_spend": "Total Spend ($)", "time_decay_rev": "Attributed Revenue ($)"},
    )
    # Add break-even line
    max_val = max(df["total_spend"].max(), df["time_decay_rev"].max())
    fig.add_trace(go.Scatter(
        x=[0, max_val], y=[0, max_val],
        mode="lines", line=dict(color="red", dash="dash"),
        name="Break-even (ROAS=1)", showlegend=True,
    ))
    fig.update_layout(height=500, paper_bgcolor=PALETTE["bg"])
    return fig


# ---------------------------------------------------------------------------
# Lead velocity charts
# ---------------------------------------------------------------------------

def plot_lvr_trend(lvr_df: pd.DataFrame) -> go.Figure:
    """LVR trend: bar for MQL count, line for LVR %."""
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(go.Bar(
        x=lvr_df["month"], y=lvr_df["mqls"],
        name="MQL Count", marker_color=PALETTE["primary"],
    ), secondary_y=False)
    fig.add_trace(go.Scatter(
        x=lvr_df["month"], y=lvr_df.get("mqls_lvr_pct", pd.Series(dtype=float)),
        name="MQL LVR %", line=dict(color=PALETTE["accent"], width=2),
        mode="lines+markers",
    ), secondary_y=True)
    fig.add_hline(y=0, line_dash="dash", line_color="gray", secondary_y=True)
    fig.update_layout(
        title="Lead Velocity Rate (MQL MoM Growth)",
        hovermode="x unified", height=400, paper_bgcolor=PALETTE["bg"],
    )
    fig.update_yaxes(title_text="MQL Count",  secondary_y=False)
    fig.update_yaxes(title_text="LVR %",      secondary_y=True)
    return fig


# ---------------------------------------------------------------------------
# Cohort charts
# ---------------------------------------------------------------------------

def plot_cohort_heatmap(cohort_df: pd.DataFrame, metric: str = "mql_to_won_rate") -> go.Figure:
    """Heatmap: rows = cohort month, columns = cumulative conversion rate."""
    if "cohort_month" not in cohort_df.columns or metric not in cohort_df.columns:
        return go.Figure()
    pivot = cohort_df.pivot_table(
        index="cohort_month", values=metric, aggfunc="first"
    ).reset_index()
    pivot = pivot.sort_values("cohort_month")
    fig = px.bar(
        pivot, x="cohort_month", y=metric,
        color=metric, color_continuous_scale="Blues",
        title=f"Cohort Conversion Rate: {metric}",
        labels={"cohort_month": "Acquisition Month"},
    )
    fig.update_layout(height=400, paper_bgcolor=PALETTE["bg"])
    return fig


def plot_channel_cohort_curves(channel_df: pd.DataFrame) -> go.Figure:
    """Conversion curves by first-touch channel."""
    fig = go.Figure()
    channels = channel_df["channel"].dropna().unique()
    for ch in channels:
        row = channel_df[channel_df["channel"] == ch].iloc[0]
        # Plot a bar showing win rate per channel
        pass

    fig = px.bar(
        channel_df.dropna(subset=["channel"]),
        x="channel", y="won_count",
        color="win_rate",
        color_continuous_scale="Viridis",
        title="Closed Won by First-Touch Channel",
        text="won_count",
    )
    fig.update_layout(height=450, paper_bgcolor=PALETTE["bg"])
    return fig


# ---------------------------------------------------------------------------
# Dashboard builders (complete HTML dashboards)
# ---------------------------------------------------------------------------

def build_executive_dashboard(funnel_df, velocity_df, duration_df, waterfall) -> go.Figure:
    """4-panel executive overview dashboard."""
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=(
            "Full-Funnel Conversion",
            "Monthly Funnel Volume",
            "Stage Duration (Avg Days)",
            "Stage Conversion Rates",
        ),
        specs=[[{"type": "bar"}, {"type": "scatter"}],
               [{"type": "bar"}, {"type": "bar"}]],
    )

    # Panel 1: Funnel bar
    stages = ["New Leads", "MQL", "SQL", "SAL", "Opp", "Won"]
    counts = [waterfall.get(k, 0) for k in
              ["total_leads","total_mql","total_sql","total_sal","total_opp","total_won"]]
    fig.add_trace(go.Bar(x=stages, y=counts, marker_color=PALETTE["primary"],
                         name="Count"), row=1, col=1)

    # Panel 2: Monthly velocity
    if not velocity_df.empty and "mqls" in velocity_df.columns:
        fig.add_trace(go.Scatter(
            x=velocity_df["month"], y=velocity_df["mqls"],
            name="MQLs", line=dict(color=PALETTE["primary"]),
        ), row=1, col=2)
        fig.add_trace(go.Scatter(
            x=velocity_df["month"], y=velocity_df.get("opps", []),
            name="Opps", line=dict(color=PALETTE["accent"]),
        ), row=1, col=2)

    # Panel 3: Avg days per stage
    if not duration_df.empty:
        dur_agg = duration_df.groupby("stage_name")["avg_days"].first().reset_index()
        fig.add_trace(go.Bar(
            x=dur_agg["stage_name"], y=dur_agg["avg_days"],
            marker_color=PALETTE["secondary"], name="Avg Days",
        ), row=2, col=1)

    # Panel 4: Conversion rates waterfall
    rates = [
        config.FUNNEL_CONVERSION_RATES["New_Lead_to_MQL"],
        config.FUNNEL_CONVERSION_RATES["MQL_to_SQL"],
        config.FUNNEL_CONVERSION_RATES["SQL_to_SAL"],
        config.FUNNEL_CONVERSION_RATES["SAL_to_Opp"],
        config.FUNNEL_CONVERSION_RATES["Opp_to_Won"],
    ]
    rate_labels = ["Lead→MQL","MQL→SQL","SQL→SAL","SAL→Opp","Opp→Won"]
    fig.add_trace(go.Bar(
        x=rate_labels, y=[r * 100 for r in rates],
        marker_color=PALETTE["accent"], name="Conv Rate %",
    ), row=2, col=2)

    fig.update_layout(
        title_text="Executive Funnel Overview",
        height=700, showlegend=True,
        paper_bgcolor=PALETTE["bg"],
    )
    return fig


def save_plotly_dashboard(fig: go.Figure, filename: str, output_dir: str = "dashboards"):
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, filename)
    fig.write_html(path, include_plotlyjs="cdn")
    print(f"  ✓ Saved: {path}")
    return path


def save_matplotlib_figure(fig: plt.Figure, filename: str, output_dir: str = "visuals"):
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, filename)
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return path


# ---------------------------------------------------------------------------
# Static matplotlib charts for README
# ---------------------------------------------------------------------------

def static_funnel_chart(waterfall: dict, output_dir: str = "visuals") -> str:
    stages  = ["New Leads", "MQL", "SQL", "SAL", "Opportunity", "Closed Won"]
    keys    = ["total_leads","total_mql","total_sql","total_sal","total_opp","total_won"]
    values  = [waterfall.get(k, 0) for k in keys]
    colors  = ["#1877F2","#3498DB","#2ECC71","#F39C12","#E74C3C","#8E44AD"]

    fig, ax = plt.subplots(figsize=(10, 5))
    bars = ax.barh(stages[::-1], values[::-1], color=colors[::-1])
    for bar, val in zip(bars, values[::-1]):
        ax.text(bar.get_width() * 1.01, bar.get_y() + bar.get_height() / 2,
                f"{val:,}", va="center", fontsize=9)
    ax.set_xlabel("Number of Contacts")
    ax.set_title("B2B Lead Funnel — Volume at Each Stage")
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
    plt.tight_layout()
    return save_matplotlib_figure(fig, "funnel_waterfall.png", output_dir)


def static_attribution_chart(wide_df: pd.DataFrame, output_dir: str = "visuals") -> str:
    model_cols = [c for c in wide_df.columns if c != "channel"]
    x = np.arange(len(wide_df))
    width = 0.12
    fig, ax = plt.subplots(figsize=(14, 6))
    for i, col in enumerate(model_cols):
        ax.bar(x + i * width, wide_df[col] / 1e6, width, label=col.replace("_", " ").title())
    ax.set_xticks(x + width * (len(model_cols) - 1) / 2)
    ax.set_xticklabels(wide_df["channel"], rotation=30, ha="right")
    ax.set_ylabel("Attributed Revenue ($M)")
    ax.set_title("Multi-Touch Attribution: Revenue by Channel × Model")
    ax.legend(loc="upper right", fontsize=8)
    plt.tight_layout()
    return save_matplotlib_figure(fig, "attribution_comparison.png", output_dir)


def static_cohort_heatmap(cohort_df: pd.DataFrame, output_dir: str = "visuals") -> str:
    if cohort_df.empty or "mql_to_won_rate" not in cohort_df.columns:
        fig, ax = plt.subplots()
        ax.text(0.5, 0.5, "No data", ha="center")
        return save_matplotlib_figure(fig, "cohort_heatmap.png", output_dir)
    pivot = cohort_df.set_index("cohort_month")[["mql_to_won_rate"]]
    fig, ax = plt.subplots(figsize=(10, max(4, len(pivot) * 0.35)))
    sns.heatmap(pivot.T, annot=True, fmt=".1%", cmap="Blues", ax=ax, linewidths=0.5)
    ax.set_title("Cohort Win Rates by Acquisition Month")
    plt.tight_layout()
    return save_matplotlib_figure(fig, "cohort_heatmap.png", output_dir)
