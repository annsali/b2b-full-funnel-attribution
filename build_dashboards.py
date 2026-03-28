"""
Build all 4 Plotly interactive dashboards and static visuals for README.
Run: python build_dashboards.py
"""
import os, sqlite3, sys
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import config
from src.funnel_engine       import run_funnel_waterfall, run_monthly_velocity, run_stage_durations
from src.attribution_models  import get_channel_comparison, run_all_models
from src.lead_velocity       import compute_lvr, compute_pipeline_velocity, compute_marketing_pipeline
from src.cohort_analysis     import build_acquisition_cohorts, build_channel_cohorts
from src.revenue_attribution import build_channel_revenue_table, build_campaign_revenue_table, build_content_attribution
from src.visualizations      import save_plotly_dashboard

PALETTE = ["#1877F2","#42B72A","#E17055","#636e72","#6c5ce7","#fdcb6e",
           "#e84393","#00cec9","#fd79a8","#81ecec"]

conn = sqlite3.connect(config.DB_PATH)

# ---------------------------------------------------------------------------
# Load all data
# ---------------------------------------------------------------------------
print("Loading data...")
waterfall    = run_funnel_waterfall(conn).iloc[0]
monthly_vel  = run_monthly_velocity(conn)
stage_dur    = run_stage_durations(conn)
wide_attr    = get_channel_comparison(conn)
all_models   = run_all_models(conn)
lvr_df       = compute_lvr(conn)
pipeline_vel = compute_pipeline_velocity(conn)
mkt_pipe     = compute_marketing_pipeline(conn)
acq_cohorts  = build_acquisition_cohorts(conn)
ch_cohorts   = build_channel_cohorts(conn)
ch_revenue   = build_channel_revenue_table(conn)
camp_revenue = build_campaign_revenue_table(conn)
content_rev  = build_content_attribution(conn)
conn.close()

os.makedirs("dashboards", exist_ok=True)
os.makedirs("visuals", exist_ok=True)

# ---------------------------------------------------------------------------
# Dashboard 1: Executive Funnel Overview
# ---------------------------------------------------------------------------
print("Building Dashboard 1: Executive Funnel Overview...")

stages  = ["New Leads","MQL","SQL","SAL","Opportunity","Closed Won"]
keys    = ["total_leads","total_mql","total_sql","total_sal","total_opp","total_won"]
counts  = [int(waterfall[k]) for k in keys]

fig1 = make_subplots(
    rows=2, cols=2,
    subplot_titles=("Full-Funnel Volume","Monthly Trends","Stage Duration (Avg Days)","Stage Conversion Rates"),
    specs=[[{"type":"bar"},{"type":"scatter"}],[{"type":"bar"},{"type":"bar"}]],
    vertical_spacing=0.18, horizontal_spacing=0.12,
)

# Funnel bars
fig1.add_trace(go.Bar(x=stages, y=counts, marker_color=PALETTE[:6],
                      text=[f"{v:,}" for v in counts], textposition="outside",
                      name="Count"), row=1, col=1)

# Monthly trends
for col_name, color, label in [("mqls","#1877F2","MQLs"),("sqls","#42B72A","SQLs"),
                                ("opps","#E17055","Opps"),("won","#636e72","Won")]:
    if col_name in monthly_vel.columns:
        fig1.add_trace(go.Scatter(
            x=monthly_vel["month"], y=monthly_vel[col_name],
            name=label, line=dict(color=color, width=2), mode="lines+markers",
        ), row=1, col=2)

# Stage duration
if not stage_dur.empty:
    fig1.add_trace(go.Bar(
        x=stage_dur["stage_name"], y=stage_dur["avg_days"],
        marker_color="#6c5ce7", text=stage_dur["avg_days"].round(1),
        textposition="outside", name="Avg Days",
    ), row=2, col=1)

# Conversion rates
rate_labels = ["Lead→MQL","MQL→SQL","SQL→SAL","SAL→Opp","Opp→Won"]
rate_vals   = [waterfall[k]*100 for k in
               ["lead_to_mql","mql_to_sql","sql_to_sal","sal_to_opp","opp_to_won"]]
colors_r    = ["#27ae60" if v >= 50 else "#e67e22" if v >= 25 else "#e74c3c" for v in rate_vals]
fig1.add_trace(go.Bar(
    x=rate_labels, y=rate_vals,
    marker_color=colors_r,
    text=[f"{v:.1f}%" for v in rate_vals], textposition="outside",
    name="Conv Rate",
), row=2, col=2)

fig1.update_layout(
    title_text="<b>Dashboard 1: Executive Funnel Overview</b>",
    title_font_size=18, height=750, showlegend=True,
    paper_bgcolor="#F8F9FA", plot_bgcolor="#FFFFFF",
)
save_plotly_dashboard(fig1, "01_executive_funnel.html")

# ---------------------------------------------------------------------------
# Dashboard 2: Attribution & Channel Performance
# ---------------------------------------------------------------------------
print("Building Dashboard 2: Attribution & Channel Performance...")

model_cols   = [c for c in wide_attr.columns if c not in ("channel",)]
fig2 = make_subplots(
    rows=2, cols=2,
    subplot_titles=("Revenue by Channel (Time Decay)","ROAS Scatter","Top 10 Campaigns","Model Comparison"),
    specs=[[{"type":"bar"},{"type":"scatter"}],[{"type":"bar"},{"type":"heatmap"}]],
    vertical_spacing=0.2, horizontal_spacing=0.12,
)

# Channel revenue
td = all_models[all_models["model"]=="time_decay"].sort_values("attributed_revenue",ascending=False)
fig2.add_trace(go.Bar(
    x=td["channel"], y=td["attributed_revenue"]/1e6,
    marker_color=PALETTE, text=(td["attributed_revenue"]/1e6).round(1),
    texttemplate="%{text}M", textposition="outside", name="Time Decay",
), row=1, col=1)

# ROAS scatter
roas_df = ch_revenue[ch_revenue["total_spend"]>0].copy()
if not roas_df.empty:
    fig2.add_trace(go.Scatter(
        x=roas_df["total_spend"]/1e3, y=roas_df["time_decay_rev"]/1e6,
        mode="markers+text", text=roas_df["channel"],
        textposition="top center",
        marker=dict(size=12, color=PALETTE[:len(roas_df)]),
        name="ROAS", showlegend=False,
    ), row=1, col=2)

# Top campaigns
top_camp = camp_revenue.head(10)
fig2.add_trace(go.Bar(
    y=top_camp["campaign_name"].str[:30], x=top_camp["attributed_revenue"]/1e6,
    orientation="h", marker_color="#1877F2",
    text=(top_camp["attributed_revenue"]/1e6).round(1),
    texttemplate="%{text}M", name="Campaigns",
), row=2, col=1)

# Model heatmap
if not wide_attr.empty:
    z = wide_attr[model_cols].values / 1e6
    fig2.add_trace(go.Heatmap(
        z=z.T, x=wide_attr["channel"],
        y=[c.replace("_"," ").title() for c in model_cols],
        colorscale="Blues", showscale=True,
        text=np.round(z.T,1), texttemplate="%{text}M",
        name="Model Heatmap",
    ), row=2, col=2)

fig2.update_layout(
    title_text="<b>Dashboard 2: Attribution & Channel Performance</b>",
    title_font_size=18, height=850, showlegend=True,
    paper_bgcolor="#F8F9FA", plot_bgcolor="#FFFFFF",
)
save_plotly_dashboard(fig2, "02_attribution_channels.html")

# ---------------------------------------------------------------------------
# Dashboard 3: Lead Velocity & Pipeline Health
# ---------------------------------------------------------------------------
print("Building Dashboard 3: Lead Velocity & Pipeline Health...")

fig3 = make_subplots(
    rows=2, cols=2,
    subplot_titles=("LVR Trend (MQL MoM %)", "Marketing Sourced vs Influenced",
                    "Stage Duration Distribution","Pipeline Velocity by Segment"),
    specs=[[{"secondary_y": True},{"type":"bar"}],
           [{"type":"box"},{"type":"bar"}]],
    vertical_spacing=0.2, horizontal_spacing=0.12,
)

# LVR trend
fig3.add_trace(go.Bar(
    x=lvr_df["month"], y=lvr_df["mqls"],
    name="MQL Count", marker_color="#1877F2", opacity=0.7,
), row=1, col=1, secondary_y=False)
if "mqls_lvr_pct" in lvr_df.columns:
    fig3.add_trace(go.Scatter(
        x=lvr_df["month"], y=lvr_df["mqls_lvr_pct"],
        name="LVR %", line=dict(color="#E17055",width=2), mode="lines+markers",
    ), row=1, col=1, secondary_y=True)

# Sourced vs Influenced
if not mkt_pipe.empty:
    fig3.add_trace(go.Bar(
        x=mkt_pipe["type"], y=mkt_pipe["pipeline_value"]/1e6,
        marker_color=["#1877F2","#42B72A","#636e72"][:len(mkt_pipe)],
        text=(mkt_pipe["pipeline_value"]/1e6).round(1),
        texttemplate="%{text}M", textposition="outside",
        name="Pipeline",
    ), row=1, col=2)

# Stage duration (box approximation with avg/p75)
if not stage_dur.empty:
    for i, row_s in stage_dur.iterrows():
        avg = row_s.get("avg_days",0) or 0
        p75 = row_s.get("p75_days",0) or avg*1.3
        med = row_s.get("median_approx",0) or avg*0.7
        fig3.add_trace(go.Box(
            y=[0, med, avg, p75, p75*1.2],
            name=row_s["stage_name"], showlegend=False,
        ), row=2, col=1)

# Pipeline velocity top segments
top_pv = pipeline_vel.dropna(subset=["pipeline_velocity"]).head(8)
fig3.add_trace(go.Bar(
    x=top_pv["company_size"]+" / "+top_pv["industry"],
    y=top_pv["pipeline_velocity"],
    marker_color="#6c5ce7", name="Velocity",
), row=2, col=2)

fig3.update_layout(
    title_text="<b>Dashboard 3: Lead Velocity & Pipeline Health</b>",
    title_font_size=18, height=800, showlegend=True,
    paper_bgcolor="#F8F9FA", plot_bgcolor="#FFFFFF",
)
save_plotly_dashboard(fig3, "03_lead_velocity.html")

# ---------------------------------------------------------------------------
# Dashboard 4: Cohort Analysis
# ---------------------------------------------------------------------------
print("Building Dashboard 4: Cohort Analysis...")

fig4 = make_subplots(
    rows=2, cols=2,
    subplot_titles=("Cohort Win Rate by Acquisition Month","Channel Cohort Win Rates",
                    "Revenue per Lead by Channel","Cohort Size Over Time"),
    specs=[[{"type":"bar"},{"type":"bar"}],[{"type":"bar"},{"type":"scatter"}]],
    vertical_spacing=0.2, horizontal_spacing=0.12,
)

# Cohort win rates
if not acq_cohorts.empty and "mql_to_won_rate" in acq_cohorts.columns:
    fig4.add_trace(go.Bar(
        x=acq_cohorts["cohort_month"], y=(acq_cohorts["mql_to_won_rate"]*100).round(1),
        marker=dict(color=(acq_cohorts["mql_to_won_rate"]*100),
                    colorscale="Blues", showscale=False),
        text=(acq_cohorts["mql_to_won_rate"]*100).round(1),
        texttemplate="%{text}%", textposition="outside",
        name="Win Rate",
    ), row=1, col=1)

# Channel cohort win rates
if not ch_cohorts.empty:
    ch_sorted = ch_cohorts.sort_values("win_rate", ascending=False).head(10)
    fig4.add_trace(go.Bar(
        x=ch_sorted["channel"], y=(ch_sorted["win_rate"]*100).round(1),
        marker_color=PALETTE[:len(ch_sorted)],
        text=(ch_sorted["win_rate"]*100).round(1),
        texttemplate="%{text}%", textposition="outside",
        name="Channel Win Rate",
    ), row=1, col=2)

# Revenue per lead
if not ch_cohorts.empty:
    ch_rev = ch_cohorts.sort_values("revenue_per_lead", ascending=False).head(10)
    fig4.add_trace(go.Bar(
        x=ch_rev["channel"], y=(ch_rev["revenue_per_lead"]/1e3).round(1),
        marker_color=PALETTE[:len(ch_rev)],
        text=(ch_rev["revenue_per_lead"]/1e3).round(1),
        texttemplate="$%{text}K", textposition="outside",
        name="Rev/Lead",
    ), row=2, col=1)

# Cohort size trending
if not acq_cohorts.empty:
    fig4.add_trace(go.Scatter(
        x=acq_cohorts["cohort_month"], y=acq_cohorts["cohort_size"],
        mode="lines+markers", name="Cohort Size",
        line=dict(color="#1877F2",width=2),
    ), row=2, col=2)

fig4.update_layout(
    title_text="<b>Dashboard 4: Cohort Analysis</b>",
    title_font_size=18, height=800, showlegend=True,
    paper_bgcolor="#F8F9FA", plot_bgcolor="#FFFFFF",
)
save_plotly_dashboard(fig4, "04_cohort_analysis.html")

# ---------------------------------------------------------------------------
# Static Matplotlib charts for README
# ---------------------------------------------------------------------------
print("Building static visuals for README...")

# Funnel waterfall
fig_s1, ax = plt.subplots(figsize=(10,5))
colors_s = ["#1877F2","#3498DB","#2ECC71","#F39C12","#E74C3C","#8E44AD"]
bars = ax.barh(stages[::-1], counts[::-1], color=colors_s[::-1])
for bar, val in zip(bars, counts[::-1]):
    ax.text(bar.get_width()*1.01, bar.get_y()+bar.get_height()/2,
            f"{val:,}", va="center", fontsize=9)
ax.set_xlabel("Contacts"); ax.set_title("B2B Lead Funnel — Volume at Each Stage")
ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x,_: f"{int(x):,}"))
plt.tight_layout()
fig_s1.savefig("visuals/funnel_waterfall.png", dpi=150, bbox_inches="tight")
plt.close(fig_s1)
print("  ✓ visuals/funnel_waterfall.png")

# Attribution comparison
if not wide_attr.empty:
    mcols = [c for c in wide_attr.columns if c != "channel"]
    x = np.arange(len(wide_attr))
    w = 0.12
    fig_s2, ax2 = plt.subplots(figsize=(14,6))
    for i, col in enumerate(mcols):
        ax2.bar(x+i*w, wide_attr[col]/1e6, w, label=col.replace("_"," ").title())
    ax2.set_xticks(x+w*(len(mcols)-1)/2)
    ax2.set_xticklabels(wide_attr["channel"], rotation=30, ha="right")
    ax2.set_ylabel("Attributed Revenue ($M)")
    ax2.set_title("Multi-Touch Attribution: Revenue by Channel × Model")
    ax2.legend(loc="upper right", fontsize=8)
    plt.tight_layout()
    fig_s2.savefig("visuals/attribution_comparison.png", dpi=150, bbox_inches="tight")
    plt.close(fig_s2)
    print("  ✓ visuals/attribution_comparison.png")

# Cohort heatmap
if not acq_cohorts.empty and "mql_to_won_rate" in acq_cohorts.columns:
    pivot = acq_cohorts.set_index("cohort_month")[["mql_to_won_rate"]].copy()
    pivot.columns = ["Win Rate"]
    fig_s3, ax3 = plt.subplots(figsize=(10, max(4, len(pivot)*0.4)))
    sns.heatmap(pivot.T, annot=True, fmt=".1%", cmap="Blues", ax=ax3,
                linewidths=0.5, cbar_kws={"label":"Win Rate"})
    ax3.set_title("Cohort Win Rate by MQL Acquisition Month")
    plt.tight_layout()
    fig_s3.savefig("visuals/cohort_heatmap.png", dpi=150, bbox_inches="tight")
    plt.close(fig_s3)
    print("  ✓ visuals/cohort_heatmap.png")

# LVR trend
if not lvr_df.empty and "mqls" in lvr_df.columns:
    fig_s4, ax4 = plt.subplots(figsize=(12,5))
    ax4b = ax4.twinx()
    ax4.bar(lvr_df["month"], lvr_df["mqls"], color="#1877F2", alpha=0.7, label="MQL Count")
    if "mqls_lvr_pct" in lvr_df.columns:
        ax4b.plot(lvr_df["month"], lvr_df["mqls_lvr_pct"], color="#E17055",
                  linewidth=2, marker="o", label="LVR %")
        ax4b.axhline(y=0, color="gray", linestyle="--", linewidth=0.8)
        ax4b.set_ylabel("LVR %", color="#E17055")
    ax4.set_xlabel("Month"); ax4.set_ylabel("MQL Count", color="#1877F2")
    ax4.set_title("Lead Velocity Rate (MQL Month-over-Month Growth)")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    fig_s4.savefig("visuals/lvr_trend.png", dpi=150, bbox_inches="tight")
    plt.close(fig_s4)
    print("  ✓ visuals/lvr_trend.png")

print("\nAll dashboards and visuals built.")
print("HTML dashboards: dashboards/*.html")
print("Static visuals:  visuals/*.png")
