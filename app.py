import streamlit as st
import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
from pyxlsb import open_workbook
from datetime import date, timedelta
import plotly.graph_objects as go
import plotly.express as px
import tempfile, os
import warnings
warnings.filterwarnings("ignore")

# ─── Page config ─────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Kenaz P&L Dashboard",
    page_icon="🕌",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={"Get Help": None, "Report a bug": None, "About": "One Guardian Brands — Kenaz P&L Dashboard"},
)

# ─── Brand colors ─────────────────────────────────────────────────────────────
GOLD  = "#C9A84C"
CREAM = "#FFFFF8"
DARK  = "#1A1A1A"
CARD  = "#2B2B2B"

st.markdown(f"""
<style>
  [data-testid="stAppViewContainer"] {{ background-color:{DARK}; }}
  [data-testid="stSidebar"] {{ background-color:#222; }}
  .block-container {{ padding-top:1.5rem; }}
  .kpi-card {{ background:{CARD};border:1px solid {GOLD}33;border-radius:10px;
               padding:16px 20px;margin-bottom:8px; }}
  .kpi-label {{ color:#aaa;font-size:11px;letter-spacing:1px;text-transform:uppercase; }}
  .kpi-value {{ color:{GOLD};font-size:26px;font-weight:700;margin:4px 0; }}
  .kpi-sub   {{ color:#777;font-size:12px; }}
  .pnl-table {{ width:100%;border-collapse:collapse;font-size:13px; }}
  .pnl-table th {{ background:{GOLD}22;color:{GOLD};padding:8px 12px;text-align:right;
                   border-bottom:1px solid {GOLD}44; }}
  .pnl-table th:first-child {{ text-align:left; }}
  .pnl-table td {{ padding:7px 12px;text-align:right;border-bottom:1px solid #333;color:#ddd; }}
  .pnl-table td:first-child {{ text-align:left;color:#bbb; }}
  .pnl-table tr.total-row td {{ background:{GOLD}11;font-weight:700;color:{CREAM};
                                 border-top:2px solid {GOLD}44; }}
  .pnl-table tr:hover td {{ background:#2f2f2f; }}
  .positive {{ color:#4caf50!important; }}
  .negative {{ color:#f44336!important; }}
  .percent  {{ color:#aaa!important;font-size:12px; }}
  .header-bar {{ background:{GOLD};padding:12px 20px;border-radius:8px;color:#1A1A1A;
                 font-weight:800;font-size:20px;letter-spacing:1.5px;margin-bottom:20px; }}
  h1,h2,h3 {{ color:{GOLD}!important; }}
  label {{ color:#aaa!important; }}
</style>
""", unsafe_allow_html=True)

# ─── Constants ────────────────────────────────────────────────────────────────
SCOPES = ["https://www.googleapis.com/auth/spreadsheets",
          "https://www.googleapis.com/auth/drive"]

PNL_COLS = [
    "Year","Quarter","Month_name","Month_serial","Channel",
    "MRP Sales","Quantity","Net Sales","COGS","Freight Inward","Wages",
    "Commission","Payment Gateway","Shipping","Others","Ad Spend",
    "Bulk Logistic","Packaging","Warehousing","Rebate",
]

CHANNELS = ["Website","FBA","RK","Meesho","Flipkart","Myntra PPMP"]

# ─── Helpers ──────────────────────────────────────────────────────────────────
def xlsb_to_date(n):
    try:
        return date(1899, 12, 30) + timedelta(days=int(float(n)))
    except Exception:
        return None

def fmt_lacs(v, decimals=2):
    if pd.isna(v) or v == 0:
        return "-"
    return f"Rs{v/100000:,.{decimals}f}L"

def fmt_pct(v):
    if pd.isna(v):
        return "-"
    return f"{v:.1f}%"


# ─── Google Sheets helpers (same pattern as Flipkart dashboard) ───────────────
@st.cache_resource
def get_gsheet_client():
    creds = Credentials.from_service_account_info(
        st.secrets["gcp_service_account"], scopes=SCOPES
    )
    return gspread.authorize(creds)

SHEET_KEY = "10qFitbppdVbNK0w67q1HFK-l7N1uAzHJ0mkyB2XImJQ"

def get_sheet(client):
    return client.open_by_key(SHEET_KEY)

@st.cache_data(ttl=300)
def load_from_gsheet(spreadsheet_name: str = "") -> pd.DataFrame:
    try:
        client = get_gsheet_client()
        ws = get_sheet(client).sheet1
        data = ws.get_all_records()
        if not data:
            return pd.DataFrame(columns=PNL_COLS)
        df = pd.DataFrame(data)
        for col in ["MRP Sales","Quantity","Net Sales","COGS","Freight Inward","Wages",
                    "Commission","Payment Gateway","Shipping","Others","Ad Spend",
                    "Bulk Logistic","Packaging","Warehousing","Rebate"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
        return df
    except Exception as e:
        st.error(f"Sheet load error: {e}")
        return pd.DataFrame(columns=PNL_COLS)

def save_to_gsheet(client, new_df: pd.DataFrame, spreadsheet_name: str = ""):
    sh = get_sheet(client)
    ws = sh.sheet1
    existing = ws.get_all_records()

    new_df = new_df.copy()
    new_df["Month_serial"] = new_df["Month_serial"].astype(str)

    if not existing:
        ws.update([new_df.columns.tolist()] + new_df.astype(str).values.tolist())
        return len(new_df), 0

    ex = pd.DataFrame(existing)
    ex["Month_serial"] = ex["Month_serial"].astype(str)

    # deduplicate on Month_serial + Channel
    ex_keys  = ex["Month_serial"].astype(str) + "_" + ex["Channel"].astype(str)
    new_keys = new_df["Month_serial"].astype(str) + "_" + new_df["Channel"].astype(str)
    truly_new = new_df[~new_keys.isin(ex_keys)]

    if len(truly_new) == 0:
        return 0, len(new_df)

    all_cols = list(dict.fromkeys(ex.columns.tolist() + truly_new.columns.tolist()))
    combined = pd.concat([
        ex.reindex(columns=all_cols, fill_value=""),
        truly_new.reindex(columns=all_cols, fill_value=""),
    ], ignore_index=True)
    combined = combined.sort_values(["Month_serial","Channel"])
    ws.clear()
    ws.update([combined.columns.tolist()] + combined.astype(str).values.tolist())
    return len(truly_new), len(new_df) - len(truly_new)


# ─── XLSB Parser ──────────────────────────────────────────────────────────────
def parse_xlsb(file_bytes: bytes) -> pd.DataFrame:
    with tempfile.NamedTemporaryFile(suffix=".xlsb", delete=False) as tmp:
        tmp.write(file_bytes)
        tmp_path = tmp.name

    try:
        rows = []
        with open_workbook(tmp_path) as wb:
            with wb.get_sheet("For P&L") as ws:
                for row in ws.rows():
                    rows.append([c.v for c in row])
    finally:
        os.unlink(tmp_path)

    headers = rows[4]
    df = pd.DataFrame(rows[5:], columns=headers)

    df = df[pd.to_numeric(df["Sum of Revenue Without Tax"], errors="coerce").notna()]
    df = df[pd.to_numeric(df["Sum of Revenue Without Tax"], errors="coerce") != 0]
    df = df.dropna(subset=["Channel"])
    df = df[df["Channel"].astype(str).isin(CHANNELS)]

    for col in [c for c in df.columns if isinstance(c, str) and c.startswith("Sum of")]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    df["Month_date"]   = df["Month"].apply(xlsb_to_date)
    df["Month_label"]  = df["Month_date"].apply(lambda d: d.strftime("%b-%y") if d else "Unknown")
    df["Month_serial"] = df["Month"].apply(lambda x: str(int(float(x))) if pd.notna(x) else "")

    # handle both formats: old has Years/Quarters cols, new format doesn't
    if "Years (Month)" in df.columns:
        year_col    = df["Years (Month)"].astype(str)
        quarter_col = df["Quarters (Month)"].astype(str)
    else:
        year_col    = df["Month_date"].apply(lambda d: str(d.year) if d else "")
        quarter_col = df["Month_date"].apply(
            lambda d: f"Qtr{((d.month - 1) // 3) + 1}" if d else ""
        )

    return pd.DataFrame({
        "Year":            year_col,
        "Quarter":         quarter_col,
        "Month_name":      df["Month_label"],
        "Month_serial":    df["Month_serial"],
        "Channel":         df["Channel"].astype(str),
        "MRP Sales":       df["Sum of TOTAL MRP"],
        "Quantity":        df["Sum of Qty"],
        "Net Sales":       df["Sum of Revenue Without Tax"],
        "COGS":            df["Sum of COGS"],
        "Freight Inward":  df["Sum of Inward"],
        "Wages":           df["Sum of Wages"],
        "Commission":      df["Sum of commission"],
        "Payment Gateway": df["Sum of Payment Gateway"],
        "Shipping":        df["Sum of Shipping"],
        "Others":          df["Sum of others"],
        "Ad Spend":        df["Sum of Total Spend"],
        "Bulk Logistic":   df["Sum of Bulk Logistic Cost"],
        "Packaging":       df["Sum of Packaging Cost"],
        "Warehousing":     df["Sum of Warehousing Charges"],
        "Rebate":          df["Sum of Rebate"],
    })


# ─── Derived metrics ──────────────────────────────────────────────────────────
def enrich(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in ["Net Sales","COGS","Freight Inward","Wages","Commission",
                "Payment Gateway","Shipping","Others","Ad Spend",
                "Bulk Logistic","Packaging","Warehousing"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    df["GM"]  = df["Net Sales"] - df["COGS"] - df["Freight Inward"] - df["Wages"]
    df["CM1"] = (df["GM"] - df["Commission"] - df["Payment Gateway"]
                 - df["Shipping"] - df["Others"]
                 - df["Bulk Logistic"] - df["Packaging"] - df["Warehousing"])
    df["CM2"] = df["CM1"] - df["Ad Spend"]

    safe = df["Net Sales"].replace(0, np.nan)
    df["COGS%"] = df["COGS"] / safe * 100
    df["GM%"]   = df["GM"]   / safe * 100
    df["CM1%"]  = df["CM1"]  / safe * 100
    df["CM2%"]  = df["CM2"]  / safe * 100

    df["Month_sort"] = df["Month_serial"].apply(
        lambda s: str(int(float(s))).zfill(10) if str(s).strip() not in ["","nan"] else "9999999999"
    )
    return df


# ══════════════════════════════════════════════════════════════════════════════
# SIDEBAR
# ══════════════════════════════════════════════════════════════════════════════
with st.sidebar:
    st.markdown(f"""
    <div style='text-align:center;padding:16px 0 8px 0'>
      <div style='font-size:26px'>🕌</div>
      <div style='font-size:17px;font-weight:800;color:{GOLD}'>Kenaz Perfumes</div>
      <div style='font-size:11px;color:#666;letter-spacing:1px;text-transform:uppercase;margin-top:3px'>
        One Guardian Brands
      </div>
    </div>""", unsafe_allow_html=True)
    st.markdown("---")

    spreadsheet_name = ""  # fixed sheet via key
    st.markdown(f"📋 **Sheet:** [Kenaz_PL_DB](https://docs.google.com/spreadsheets/d/10qFitbppdVbNK0w67q1HFK-l7N1uAzHJ0mkyB2XImJQ)")

    st.markdown("### 📤 Upload P&L File")
    uploaded = st.file_uploader("Kenaz P&L (.xlsb)", type=["xlsb"])

    if uploaded:
        try:
            raw = parse_xlsb(uploaded.read())
            ch_counts = raw["Channel"].value_counts()
            st.success(f"✅ {len(raw):,} rows | {raw['Month_name'].nunique()} months")
            st.info("  \n".join(f"• {ch}: {cnt}" for ch, cnt in ch_counts.items()))

            if st.button("💾 Save to Google Sheets", type="primary"):
                with st.spinner("Saving…"):
                    client = get_gsheet_client()
                    added, dupes = save_to_gsheet(client, raw, spreadsheet_name)
                st.success(f"✅ {added:,} new rows added. {dupes:,} duplicates skipped.")
                st.cache_data.clear()

        except Exception as e:
            st.error(f"Parse error: {e}")

    st.markdown("---")

    df_raw = load_from_gsheet(spreadsheet_name)

    if df_raw.empty:
        st.info("No data yet. Upload a file above.")
        st.stop()

    df_all = enrich(df_raw)

    st.markdown("### 🔍 Filters")
    channels_avail = sorted(df_all["Channel"].unique())
    months_df = (df_all[["Month_name","Month_sort"]]
                 .drop_duplicates().sort_values("Month_sort"))
    months_avail = months_df["Month_name"].tolist()

    sel_channels = st.multiselect("Channels", channels_avail, default=channels_avail)
    sel_months   = st.multiselect("Months",   months_avail,   default=months_avail)

    st.markdown("---")
    view = st.radio("View", ["P&L Summary","Channel Deep-Dive","Month Trend","Channel Mix"])

    st.markdown("---")
    st.markdown("### 🔧 DB Tools")
    if st.button("🗑️ Clear Sheet Cache"):
        st.cache_data.clear()
        st.success("Cache cleared.")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════
st.markdown("<div class='header-bar'>📊 KENAZ PERFUMES — P&L DASHBOARD</div>",
            unsafe_allow_html=True)

df = df_all[df_all["Channel"].isin(sel_channels) & df_all["Month_name"].isin(sel_months)]

if df.empty:
    st.warning("No data matches the selected filters.")
    st.stop()

# ─── KPI strip ────────────────────────────────────────────────────────────────
tot = df[["Net Sales","COGS","GM","CM1","CM2","Ad Spend","Quantity"]].sum()
nsv = tot["Net Sales"]

def kpi(col, label, val, sub=""):
    col.markdown(f"""
    <div class='kpi-card'>
      <div class='kpi-label'>{label}</div>
      <div class='kpi-value'>{val}</div>
      <div class='kpi-sub'>{sub}</div>
    </div>""", unsafe_allow_html=True)

c1,c2,c3,c4,c5,c6 = st.columns(6)
kpi(c1, "Net Sales",    fmt_lacs(nsv),            f"{int(tot['Quantity']):,} units")
kpi(c2, "Gross Margin", fmt_lacs(tot["GM"]),       fmt_pct(tot["GM"]/nsv*100 if nsv else 0)+" of NSV")
kpi(c3, "CM1",          fmt_lacs(tot["CM1"]),      fmt_pct(tot["CM1"]/nsv*100 if nsv else 0)+" of NSV")
kpi(c4, "CM2",          fmt_lacs(tot["CM2"]),      fmt_pct(tot["CM2"]/nsv*100 if nsv else 0)+" of NSV")
kpi(c5, "Ad Spend",     fmt_lacs(tot["Ad Spend"]), fmt_pct(tot["Ad Spend"]/nsv*100 if nsv else 0)+" of NSV")
kpi(c6, "COGS",         fmt_lacs(tot["COGS"]),     fmt_pct(tot["COGS"]/nsv*100 if nsv else 0)+" of NSV")

st.markdown("<br>", unsafe_allow_html=True)

month_order = (df[["Month_name","Month_sort"]]
               .drop_duplicates().sort_values("Month_sort")["Month_name"].tolist())


# ══════════════════════════════════════════════════════════════════════════════
# VIEW: P&L Summary
# ══════════════════════════════════════════════════════════════════════════════
if view == "P&L Summary":
    st.subheader("P&L Summary — Channel-wise")

    channels = sel_channels
    agg = df.groupby("Channel")[["Net Sales","COGS","Freight Inward","Wages","GM",
                                   "Commission","Payment Gateway","Shipping","Others",
                                   "Bulk Logistic","Packaging","Warehousing",
                                   "Ad Spend","CM1","CM2"]].sum()
    total = agg.sum()

    def row_html(label, key, is_total=False, inverse=False):
        cls = "total-row" if is_total else ""
        cells = ""
        for ch in channels:
            v      = agg.loc[ch, key] if ch in agg.index else 0
            nsv_ch = agg.loc[ch, "Net Sales"] if ch in agg.index else 1
            pct    = v / nsv_ch * 100 if nsv_ch else 0
            pos    = (pct >= 0) != inverse
            cc     = "positive" if pos else "negative"
            cells += f"<td><span class='{cc}'>{fmt_lacs(v)}</span><br><span class='percent'>{fmt_pct(pct)}</span></td>"
        tv = total[key]; tn = total["Net Sales"]
        tp = tv / tn * 100 if tn else 0
        tc = "positive" if (tp >= 0) != inverse else "negative"
        cells += f"<td><span class='{tc}'>{fmt_lacs(tv)}</span><br><span class='percent'>{fmt_pct(tp)}</span></td>"
        return f"<tr class='{cls}'><td>{label}</td>{cells}</tr>"

    heads = "".join(f"<th>{ch}</th>" for ch in channels) + "<th>Total</th>"
    st.markdown(f"""
    <table class='pnl-table'>
      <thead><tr><th>Metric (INR Lacs)</th>{heads}</tr></thead>
      <tbody>
        {row_html("Net Sales",            "Net Sales")}
        {row_html("Less: COGS",           "COGS",            inverse=True)}
        {row_html("Less: Freight Inward", "Freight Inward",  inverse=True)}
        {row_html("Less: Wages",          "Wages",           inverse=True)}
        {row_html("Gross Margin",         "GM",              is_total=True)}
        {row_html("Less: Commission",     "Commission",      inverse=True)}
        {row_html("Less: Payment GW",     "Payment Gateway", inverse=True)}
        {row_html("Less: Shipping",       "Shipping",        inverse=True)}
        {row_html("Less: Others",         "Others",          inverse=True)}
        {row_html("Less: Bulk Logistic",  "Bulk Logistic",   inverse=True)}
        {row_html("Less: Packaging",      "Packaging",       inverse=True)}
        {row_html("Less: Warehousing",    "Warehousing",     inverse=True)}
        {row_html("CM1",                  "CM1",             is_total=True)}
        {row_html("Less: Ad Spend",       "Ad Spend",        inverse=True)}
        {row_html("CM2",                  "CM2",             is_total=True)}
      </tbody>
    </table>""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# VIEW: Channel Deep-Dive
# ══════════════════════════════════════════════════════════════════════════════
elif view == "Channel Deep-Dive":
    st.subheader("Channel Deep-Dive")
    ch_sel = st.selectbox("Select Channel", sel_channels)
    dch = df[df["Channel"] == ch_sel]

    pivot = (dch.groupby("Month_name")
               .agg(Net_Sales=("Net Sales","sum"), COGS=("COGS","sum"),
                    GM=("GM","sum"), CM1=("CM1","sum"), CM2=("CM2","sum"),
                    Ad_Spend=("Ad Spend","sum"), Qty=("Quantity","sum"))
               .reindex([m for m in month_order if m in dch["Month_name"].values])
               .reset_index())

    fig = go.Figure()
    fig.add_trace(go.Bar(name="CM2",      x=pivot["Month_name"], y=pivot["CM2"],      marker_color=GOLD))
    fig.add_trace(go.Bar(name="Ad Spend", x=pivot["Month_name"], y=pivot["Ad_Spend"], marker_color="#e67e22"))
    fig.add_trace(go.Scatter(name="Net Sales", x=pivot["Month_name"], y=pivot["Net_Sales"],
                             mode="lines+markers", line=dict(color="#4fc3f7", width=2)))
    fig.update_layout(barmode="stack", template="plotly_dark",
                      plot_bgcolor=DARK, paper_bgcolor=DARK,
                      font=dict(color="#aaa"), legend=dict(orientation="h"),
                      margin=dict(t=30,b=30,l=10,r=10), height=360,
                      xaxis=dict(gridcolor="#333"), yaxis=dict(gridcolor="#333"))
    st.plotly_chart(fig, use_container_width=True)

    pivot_idx = pivot.set_index("Month_name")
    months_here = [m for m in month_order if m in pivot_idx.index]
    metrics = [("Net_Sales","Net Sales"),("COGS","COGS"),("GM","Gross Margin"),
               ("CM1","CM1"),("Ad_Spend","Ad Spend"),("CM2","CM2")]
    heads = "".join(f"<th>{m}</th>" for m in months_here)
    body  = ""
    for key, label in metrics:
        cls = "total-row" if key in ["GM","CM1","CM2"] else ""
        cells = ""
        for m in months_here:
            v     = pivot_idx.loc[m, key] if m in pivot_idx.index else 0
            nsv_m = pivot_idx.loc[m, "Net_Sales"] if m in pivot_idx.index else 1
            pct   = v / nsv_m * 100 if nsv_m else 0
            cells += f"<td>{fmt_lacs(v)}<br><span class='percent'>{fmt_pct(pct)}</span></td>"
        body += f"<tr class='{cls}'><td>{label}</td>{cells}</tr>"

    st.markdown(f"""
    <table class='pnl-table'>
      <thead><tr><th>Metric</th>{heads}</tr></thead>
      <tbody>{body}</tbody>
    </table>""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# VIEW: Month Trend
# ══════════════════════════════════════════════════════════════════════════════
elif view == "Month Trend":
    st.subheader("Monthly Trend — All Channels")

    monthly = (df.groupby(["Month_name","Month_sort"])
                 [["Net Sales","GM","CM1","CM2","Ad Spend"]]
                 .sum().reset_index().sort_values("Month_sort"))

    metric = st.selectbox("Metric", ["Net Sales","GM","CM1","CM2","Ad Spend"])

    fig = go.Figure(go.Bar(
        x=monthly["Month_name"], y=monthly[metric]/100000,
        marker_color=GOLD, name=metric
    ))
    fig.update_layout(template="plotly_dark", plot_bgcolor=DARK, paper_bgcolor=DARK,
                      font=dict(color="#aaa"), yaxis_title="INR Lakhs",
                      margin=dict(t=30,b=30,l=10,r=10), height=380,
                      xaxis=dict(gridcolor="#333"), yaxis=dict(gridcolor="#333"))
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("**By Channel**")
    ch_monthly = (df.groupby(["Channel","Month_name","Month_sort"])[metric]
                    .sum().reset_index().sort_values("Month_sort"))
    fig2 = px.line(ch_monthly, x="Month_name", y=metric, color="Channel",
                   color_discrete_sequence=[GOLD,"#e67e22","#4fc3f7","#81c784","#ce93d8","#ef9a9a"],
                   category_orders={"Month_name": month_order})
    fig2.update_layout(template="plotly_dark", plot_bgcolor=DARK, paper_bgcolor=DARK,
                       font=dict(color="#aaa"), margin=dict(t=20,b=30,l=10,r=10), height=350,
                       xaxis=dict(gridcolor="#333"), yaxis=dict(gridcolor="#333"),
                       legend=dict(orientation="h"))
    st.plotly_chart(fig2, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# VIEW: Channel Mix
# ══════════════════════════════════════════════════════════════════════════════
elif view == "Channel Mix":
    st.subheader("Channel Mix")
    ch_agg = df.groupby("Channel")[["Net Sales","CM1","CM2","Ad Spend","Quantity"]].sum().reset_index()
    COLORS = [GOLD,"#e67e22","#4fc3f7","#81c784","#ce93d8","#ef9a9a"]

    col1, col2 = st.columns(2)
    with col1:
        fig = px.pie(ch_agg, names="Channel", values="Net Sales", title="Revenue Split",
                     color_discrete_sequence=COLORS)
        fig.update_layout(template="plotly_dark", paper_bgcolor=DARK,
                          font=dict(color="#aaa"), margin=dict(t=40,b=10), height=340)
        st.plotly_chart(fig, use_container_width=True)
    with col2:
        fig = px.pie(ch_agg, names="Channel", values="CM2", title="CM2 Split",
                     color_discrete_sequence=COLORS)
        fig.update_layout(template="plotly_dark", paper_bgcolor=DARK,
                          font=dict(color="#aaa"), margin=dict(t=40,b=10), height=340)
        st.plotly_chart(fig, use_container_width=True)

    ch_agg["CM1%"] = ch_agg["CM1"] / ch_agg["Net Sales"] * 100
    ch_agg["CM2%"] = ch_agg["CM2"] / ch_agg["Net Sales"] * 100
    ch_agg = ch_agg.sort_values("Net Sales", ascending=True)

    fig3 = go.Figure()
    fig3.add_trace(go.Bar(name="CM1%", x=ch_agg["CM1%"], y=ch_agg["Channel"],
                          orientation="h", marker_color=GOLD))
    fig3.add_trace(go.Bar(name="CM2%", x=ch_agg["CM2%"], y=ch_agg["Channel"],
                          orientation="h", marker_color="#e67e22"))
    fig3.update_layout(barmode="group", template="plotly_dark",
                       plot_bgcolor=DARK, paper_bgcolor=DARK,
                       font=dict(color="#aaa"), xaxis_title="% of Net Sales",
                       margin=dict(t=20,b=30,l=10,r=10), height=320,
                       legend=dict(orientation="h"))
    st.plotly_chart(fig3, use_container_width=True)


# ─── Footer ───────────────────────────────────────────────────────────────────
st.markdown(f"""
<hr style='border-color:#333;margin-top:40px'>
<div style='text-align:center;color:#555;font-size:11px'>
  Kenaz Perfumes · One Guardian Brands · FY 25-26 P&amp;L Analytics
</div>""", unsafe_allow_html=True)
