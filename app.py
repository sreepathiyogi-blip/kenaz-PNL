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

st.set_page_config(
    page_title="Kenaz P&L Dashboard", page_icon="🕌", layout="wide",
    initial_sidebar_state="expanded",
    menu_items={"Get Help": None, "Report a bug": None, "About": "One Guardian Brands — Kenaz P&L Dashboard"},
)

GOLD  = "#C9A84C"
CREAM = "#FFFFF8"
DARK  = "#1A1A1A"
CARD  = "#2B2B2B"

st.markdown(f"""
<style>
  [data-testid="stAppViewContainer"] {{ background-color:{DARK}; }}
  [data-testid="stSidebar"] {{ background-color:#222; }}
  .block-container {{ padding-top:1.5rem; }}
  .kpi-card {{ background:{CARD};border:1px solid {GOLD}33;border-radius:10px;padding:16px 20px;margin-bottom:8px; }}
  .kpi-label {{ color:#aaa;font-size:11px;letter-spacing:1px;text-transform:uppercase; }}
  .kpi-value {{ color:{GOLD};font-size:26px;font-weight:700;margin:4px 0; }}
  .kpi-sub   {{ color:#777;font-size:12px; }}
  .pnl-table {{ width:100%;border-collapse:collapse;font-size:12.5px; }}
  .pnl-table th {{ background:{GOLD}22;color:{GOLD};padding:8px 12px;text-align:right;
                   border-bottom:2px solid {GOLD}55;white-space:nowrap; }}
  .pnl-table th:first-child {{ text-align:left;min-width:220px; }}
  .pnl-table td {{ padding:6px 12px;text-align:right;border-bottom:1px solid #2a2a2a;color:#ddd;white-space:nowrap; }}
  .pnl-table td:first-child {{ text-align:left;color:#bbb; }}
  .pnl-table tr.total-row td {{ background:{GOLD}18;font-weight:700;color:{CREAM};border-top:2px solid {GOLD}55;border-bottom:2px solid {GOLD}33; }}
  .pnl-table tr.pct-row td {{ color:#888;font-size:11.5px;border-bottom:1px solid #222; }}
  .pnl-table tr.pct-row td:first-child {{ color:#666; }}
  .pnl-table tr.section-gap td {{ height:6px;background:{DARK};border:none; }}
  .pnl-table tr:not(.pct-row):not(.total-row):not(.section-gap):hover td {{ background:#252525; }}
  .positive {{ color:#4caf50!important; }}
  .negative {{ color:#e57373!important; }}
  .header-bar {{ background:{GOLD};padding:12px 20px;border-radius:8px;color:#1A1A1A;
                 font-weight:800;font-size:20px;letter-spacing:1.5px;margin-bottom:20px; }}
  h1,h2,h3 {{ color:{GOLD}!important; }}
  label {{ color:#aaa!important; }}
  .stRadio label {{ color:#ccc!important; }}
</style>
""", unsafe_allow_html=True)

# ─── Constants ────────────────────────────────────────────────────────────────
SCOPES     = ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
SHEET_KEY  = "10qFitbppdVbNK0w67q1HFK-l7N1uAzHJ0mkyB2XImJQ"
CHANNELS   = ["Website","Amazon","Meesho","Flipkart","Myntra PPMP"]
EAN_MAP    = {
    8906188065836: "Triumph",      8906188065799: "Gentleman",
    8906188065928: "Oud Ameer",    8906188065775: "Untamed",
    8906188065867: "Fortuna",      8906188065850: "La Beaute",
    8906188065881: "Twilight",     8906188065980: "Bahiyaa Bayda",
    8904512100307: "Female Gift Set", 8904512100291: "Male Gift Set",
}
SKU_NUM_COLS = ["Revenue Without Tax","Qty","COGS","TOTAL MRP","Inward","Wages",
                "commission","Payment Gateway","Shipping","Bulk Logistic Cost",
                "Packaging Cost","Warehousing Charges","Rebate","others","Total Spend"]
PNL_COLS   = ["Month_serial","Month_name","Channel","MRP Sales","Quantity","Net Sales",
               "COGS","Freight Inward","Wages","Commission","Payment Gateway","Shipping",
               "Others","Ad Spend","Bulk Logistic","Packaging","Warehousing","Rebate"]

# ─── Helpers ──────────────────────────────────────────────────────────────────
def xlsb_to_date(n):
    try: return date(1899,12,30) + timedelta(days=int(float(n)))
    except: return None

def L(v):
    """Format as Lakhs with 2 decimals"""
    if pd.isna(v) or v == 0: return "-"
    return f"{v/100000:,.2f}"

def Lbold(v):
    neg = v < 0
    s = L(abs(v)) if not pd.isna(v) else "-"
    if s == "-": return "-"
    return f"({s})" if neg else s

def P(v):
    """Format as percentage"""
    if pd.isna(v): return "-"
    return f"{v:.1f}%"

def INR(v):
    if pd.isna(v) or v == 0: return "-"
    return f"&#8377;{v:,.0f}"

def color_val(v, inverse=False):
    if pd.isna(v) or v == 0: return "#aaa"
    pos = v > 0
    if inverse: pos = not pos
    return "#4caf50" if pos else "#e57373"

# ─── Google Sheets ─────────────────────────────────────────────────────────────
@st.cache_resource
def get_gsheet_client():
    creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=SCOPES)
    return gspread.authorize(creds)

def get_sheet(client):
    return client.open_by_key(SHEET_KEY)

@st.cache_data(ttl=300)
def load_from_gsheet(_dummy="") -> pd.DataFrame:
    try:
        client = get_gsheet_client()
        ws = get_sheet(client).sheet1
        data = ws.get_all_records()
        if not data: return pd.DataFrame(columns=PNL_COLS)
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

def save_to_gsheet(client, new_df: pd.DataFrame):
    sh  = get_sheet(client)
    ws  = sh.sheet1
    existing = ws.get_all_records()
    new_df = new_df.copy()
    new_df["Month_serial"] = new_df["Month_serial"].astype(str)
    NUM_COLS = ["MRP Sales","Quantity","Net Sales","COGS","Freight Inward","Wages",
                "Commission","Payment Gateway","Shipping","Others","Ad Spend",
                "Bulk Logistic","Packaging","Warehousing","Rebate"]

    def df_to_rows(df):
        out = df.copy()
        for c in NUM_COLS:
            if c in out.columns:
                out[c] = pd.to_numeric(out[c], errors="coerce").fillna(0).round(4)
        return [out.columns.tolist()] + out.astype(str).values.tolist()

    if not existing:
        ws.update(df_to_rows(new_df))
        return len(new_df), 0
    ex = pd.DataFrame(existing)
    ex["Month_serial"] = ex["Month_serial"].astype(str)
    ex_keys  = ex["Month_serial"]  + "_" + ex["Channel"].astype(str)
    new_keys = new_df["Month_serial"] + "_" + new_df["Channel"].astype(str)
    truly_new = new_df[~new_keys.isin(ex_keys)]
    if len(truly_new) == 0: return 0, len(new_df)
    all_cols = list(dict.fromkeys(ex.columns.tolist() + truly_new.columns.tolist()))
    combined = pd.concat([ex.reindex(columns=all_cols, fill_value=""),
                          truly_new.reindex(columns=all_cols, fill_value="")], ignore_index=True)
    combined = combined.sort_values(["Month_serial","Channel"])
    ws.clear()
    ws.update(df_to_rows(combined))
    return len(truly_new), len(new_df) - len(truly_new)

# ─── XLSB Parser ──────────────────────────────────────────────────────────────
def parse_xlsb(file_bytes: bytes) -> pd.DataFrame:
    with tempfile.NamedTemporaryFile(suffix=".xlsb", delete=False) as tmp:
        tmp.write(file_bytes); tmp_path = tmp.name
    try:
        rows = []
        with open_workbook(tmp_path) as wb:
            with wb.get_sheet("For P&L") as ws:
                for row in ws.rows(): rows.append([c.v for c in row])
    finally:
        os.unlink(tmp_path)

    headers = rows[4]
    df = pd.DataFrame(rows[5:], columns=headers)
    df = df[pd.to_numeric(df["Sum of Revenue Without Tax"], errors="coerce").notna()]
    df = df[pd.to_numeric(df["Sum of Revenue Without Tax"], errors="coerce") != 0]
    df = df.dropna(subset=["Channel"])
    df = df[df["Channel"].astype(str).isin(["Website","FBA","RK","Meesho","Flipkart","Myntra PPMP"])]

    for col in [c for c in df.columns if isinstance(c,str) and c.startswith("Sum of")]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # combine FBA + RK → Amazon
    df["Channel"] = df["Channel"].apply(lambda x: "Amazon" if x in ["FBA","RK"] else x)

    df["Month_date"]   = df["Month"].apply(xlsb_to_date)
    df["Month_name"]   = df["Month_date"].apply(lambda d: d.strftime("%b-%y") if d else "Unknown")
    df["Month_serial"] = df["Month"].apply(lambda x: str(int(float(x))) if pd.notna(x) else "")

    if "Years (Month)" in df.columns:
        year_col    = df["Years (Month)"].astype(str)
        quarter_col = df["Quarters (Month)"].astype(str)
    else:
        year_col    = df["Month_date"].apply(lambda d: str(d.year) if d else "")
        quarter_col = df["Month_date"].apply(lambda d: f"Qtr{((d.month-1)//3)+1}" if d else "")

    grp = df.groupby(["Month_serial","Month_name","Channel"])[
        ["Sum of TOTAL MRP","Sum of Qty","Sum of Revenue Without Tax","Sum of COGS",
         "Sum of Inward","Sum of Wages","Sum of commission","Sum of Payment Gateway",
         "Sum of Shipping","Sum of others","Sum of Total Spend","Sum of Bulk Logistic Cost",
         "Sum of Packaging Cost","Sum of Warehousing Charges","Sum of Rebate"]
    ].sum().reset_index()

    return pd.DataFrame({
        "Month_serial":    grp["Month_serial"],
        "Month_name":      grp["Month_name"],
        "Channel":         grp["Channel"],
        "MRP Sales":       grp["Sum of TOTAL MRP"],
        "Quantity":        grp["Sum of Qty"],
        "Net Sales":       grp["Sum of Revenue Without Tax"],
        "COGS":            grp["Sum of COGS"],
        "Freight Inward":  grp["Sum of Inward"],
        "Wages":           grp["Sum of Wages"],
        "Commission":      grp["Sum of commission"],
        "Payment Gateway": grp["Sum of Payment Gateway"],
        "Shipping":        grp["Sum of Shipping"],
        "Others":          grp["Sum of others"],
        "Ad Spend":        grp["Sum of Total Spend"],
        "Bulk Logistic":   grp["Sum of Bulk Logistic Cost"],
        "Packaging":       grp["Sum of Packaging Cost"],
        "Warehousing":     grp["Sum of Warehousing Charges"],
        "Rebate":          grp["Sum of Rebate"],
    })

# ─── SKU Parser ───────────────────────────────────────────────────────────────
def parse_sku_data(file_bytes: bytes) -> pd.DataFrame:
    """Parse Data sheet → product-level P&L."""
    with tempfile.NamedTemporaryFile(suffix=".xlsb", delete=False) as tmp:
        tmp.write(file_bytes); tmp_path = tmp.name
    try:
        rows = []
        with open_workbook(tmp_path) as wb:
            with wb.get_sheet("Data") as ws:
                for row in ws.rows(): rows.append([c.v for c in row])
    finally:
        os.unlink(tmp_path)

    headers = rows[1]
    df = pd.DataFrame(rows[2:], columns=headers)

    df["Month_label"] = df["Month"].apply(
        lambda n: (date(1899,12,30)+timedelta(days=int(float(n)))).strftime("%b-%y")
        if pd.notna(n) else None)
    df["EAN"]   = pd.to_numeric(df["New SKU"], errors="coerce")
    df["Model"] = df["EAN"].apply(lambda x: EAN_MAP.get(int(x),"Other") if pd.notna(x) else "Unknown")
    df["Channel"] = df["Channel"].apply(lambda x: "Amazon" if x in ["FBA","RK"] else str(x))

    for c in SKU_NUM_COLS:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    df = df.dropna(subset=["Month_label"])

    # split sales vs returns for RTO%
    sales_df   = df[df["Sale / Return"] == "Sale"]
    returns_df = df[df["Sale / Return"] == "Return"]

    grp = sales_df.groupby(["Model","EAN","Month_label","Month","Channel"])[SKU_NUM_COLS].sum().reset_index()
    ret_grp = returns_df.groupby(["Model","EAN","Month_label","Month","Channel"])[
        ["Revenue Without Tax"]].sum().reset_index().rename(columns={"Revenue Without Tax":"Return Amount"})

    grp = grp.merge(ret_grp, on=["Model","EAN","Month_label","Month","Channel"], how="left")
    grp["Return Amount"] = grp["Return Amount"].fillna(0)

    grp["Month_sort"] = grp["Month"].apply(
        lambda s: str(int(float(s))).zfill(10) if pd.notna(s) else "9999999999")

    # RTO% per model+month+channel already computed above
    # ─── also read Data sheet for overall RTO% (used in main P&L) ───────────

    # rename to match main P&L col names
    grp = grp.rename(columns={
        "Revenue Without Tax": "Net Sales",
        "Qty":                 "Quantity",
        "TOTAL MRP":           "MRP Sales",
        "Inward":              "Freight Inward",
        "Wages":               "Wages",
        "commission":          "Commission",
        "Payment Gateway":     "Payment Gateway",
        "Shipping":            "Shipping",
        "Bulk Logistic Cost":  "Bulk Logistic",
        "Packaging Cost":      "Packaging",
        "Warehousing Charges": "Warehousing",
        "others":              "Others",
        "Total Spend":         "Ad Spend",
    })
    # RTO% = Return Amount / Gross Sales (Net Sales here is gross sales only)
    grp["RTO%"] = (grp["Return Amount"] / grp["Net Sales"].replace(0, np.nan) * 100).fillna(0)
    return grp

# ─── Enrich ───────────────────────────────────────────────────────────────────
def enrich(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in ["Net Sales","COGS","Freight Inward","Wages","Commission","Payment Gateway",
                "Shipping","Others","Ad Spend","Bulk Logistic","Packaging","Warehousing","Rebate","MRP Sales"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    df["Material Margin"] = df["Net Sales"] - df["COGS"]
    df["Freight+Wages"]   = df["Freight Inward"] + df["Wages"]
    df["GM"]  = df["Material Margin"] - df["Freight+Wages"]
    df["CnL"] = (df["Commission"] + df["Payment Gateway"] + df["Shipping"] +
                 df["Bulk Logistic"] + df["Packaging"] + df["Warehousing"] +
                 df["Rebate"] + df["Others"])
    df["CM1"] = df["GM"] - df["CnL"]
    df["CM2"] = df["CM1"] - df["Ad Spend"]

    safe = df["Net Sales"].replace(0, np.nan)
    df["Discount%"]       = (1 - df["Net Sales"] / df["MRP Sales"].replace(0,np.nan)) * 100
    df["COGS%"]           = df["COGS"]            / safe * 100
    df["MatMargin%"]      = df["Material Margin"] / safe * 100
    df["FreightWages%"]   = df["Freight+Wages"]   / safe * 100
    df["GM%"]             = df["GM"]              / safe * 100
    df["CnL%"]            = df["CnL"]             / safe * 100
    df["CM1%"]            = df["CM1"]             / safe * 100
    df["ACOS%"]           = df["Ad Spend"]        / safe * 100
    df["CM2%"]            = df["CM2"]             / safe * 100
    df["ASP"]             = df["Net Sales"]        / df["Quantity"].replace(0, np.nan)

    df["Month_sort"] = df["Month_serial"].apply(
        lambda s: str(int(float(s))).zfill(10) if str(s).strip() not in ["","nan"] else "9999999999"
    )
    return df

# ─── P&L Table Builder ────────────────────────────────────────────────────────
def build_pnl_table(df, months, channels):
    """Build the full P&L table HTML exactly like the format requested."""

    METRICS = ["MRP Sales","Quantity","Net Sales","COGS","Freight Inward","Wages",
               "Commission","Payment Gateway","Shipping","Bulk Logistic","Packaging",
               "Warehousing","Rebate","Others","Ad Spend"]

    # aggregate by month — use simple groupby sum (pandas 3 safe)
    grp = df.groupby(["Month_name","Month_sort"])[METRICS].sum().reset_index()

    # reindex to selected months in order
    grp = grp[grp["Month_name"].isin(months)].copy()
    month_order = grp.sort_values("Month_sort")["Month_name"].tolist()

    # build lookup dict: month -> {metric -> value}
    lookup = {}
    for _, row in grp.iterrows():
        lookup[row["Month_name"]] = {m: float(row[m]) for m in METRICS}

    def v(metric, m):
        return lookup.get(m, {}).get(metric, 0)

    def nsv(m): return v("Net Sales", m) or np.nan
    def mrp(m): return v("MRP Sales", m) or np.nan

    # derived per month
    def mat_margin(m): return v("Net Sales",m) - v("COGS",m)
    def fw(m):         return v("Freight Inward",m) + v("Wages",m)
    def gm(m):         return mat_margin(m) - fw(m)
    def cnl(m):        return (v("Commission",m) + v("Payment Gateway",m) + v("Shipping",m) +
                               v("Bulk Logistic",m) + v("Packaging",m) + v("Warehousing",m) +
                               v("Rebate",m) + v("Others",m))
    def cm1(m):        return gm(m) - cnl(m)
    def cm2(m):        return cm1(m) - v("Ad Spend",m)

    # total column — sum across all months in lookup
    tot = {metric: sum(lookup.get(m,{}).get(metric,0) for m in month_order) for metric in METRICS}
    def T(fn): return sum(fn(m) for m in month_order)

    months_cols = month_order  # display columns

    def th(label):
        heads = "".join(f"<th>{m}</th>" for m in months_cols)
        return f"<thead><tr><th>{label}</th>{heads}<th>Total</th></tr></thead>"

    def data_row(label, vals_fn, total_fn, fmt_fn=L, cls="", color_fn=None, inverse=False):
        cells = ""
        for m in months_cols:
            val = vals_fn(m)
            s   = fmt_fn(val)
            if color_fn:
                c = color_val(val, inverse)
                cells += f"<td style='color:{c}'>{s}</td>"
            else:
                cells += f"<td>{s}</td>"
        tv = total_fn()
        ts = fmt_fn(tv)
        if color_fn:
            tc = color_val(tv, inverse)
            return f"<tr class='{cls}'><td>{label}</td>{cells}<td style='color:{tc}'>{ts}</td></tr>"
        return f"<tr class='{cls}'><td>{label}</td>{cells}<td>{ts}</td></tr>"

    def pct_row(label, pct_fn, total_pct_fn, cls="pct-row", inverse=False):
        cells = ""
        for m in months_cols:
            val = pct_fn(m)
            c   = color_val(val, inverse)
            cells += f"<td style='color:{c}'>{P(val)}</td>"
        tv = total_pct_fn()
        tc = color_val(tv, inverse)
        return f"<tr class='{cls}'><td>{label}</td>{cells}<td style='color:{tc}'>{P(tv)}</td></tr>"

    def gap():
        n = len(months_cols) + 2
        return f"<tr class='section-gap'>{'<td></td>'*n}</tr>"

    tot_ns  = tot.get("Net Sales",0)
    tot_mrp = tot.get("MRP Sales",0)

    rows_html = ""

    # MRP Sales
    rows_html += data_row("MRP Sales", lambda m: v("MRP Sales",m)/100000, lambda: tot_mrp/100000)
    # Quantity
    rows_html += data_row("Quantity",  lambda m: v("Quantity",m), lambda: tot.get("Quantity",0),
                          fmt_fn=lambda x: f"{int(x):,}" if not pd.isna(x) and x!=0 else "-")
    # ASP
    rows_html += data_row("ASP", lambda m: v("Net Sales",m)/v("Quantity",m) if v("Quantity",m) else np.nan,
                          lambda: tot_ns/tot.get("Quantity",1) if tot.get("Quantity") else np.nan,
                          fmt_fn=lambda x: f"&#8377;{x:,.0f}" if not pd.isna(x) else "-")
    # Discount %
    rows_html += pct_row("Discount %",
                         lambda m: (1 - v("Net Sales",m)/mrp(m))*100 if mrp(m) else np.nan,
                         lambda: (1 - tot_ns/tot_mrp)*100 if tot_mrp else np.nan,
                         inverse=True)

    rows_html += gap()

    # Net Sales
    rows_html += data_row("Net Sales", lambda m: v("Net Sales",m)/100000, lambda: tot_ns/100000,
                          cls="total-row")

    rows_html += gap()

    # COGS
    rows_html += data_row("Less: COGS", lambda m: v("COGS",m)/100000, lambda: tot.get("COGS",0)/100000,
                          color_fn=True, inverse=True)
    rows_html += pct_row("COGS %",
                         lambda m: v("COGS",m)/nsv(m)*100,
                         lambda: tot.get("COGS",0)/tot_ns*100 if tot_ns else np.nan,
                         inverse=True)

    rows_html += gap()

    # Material Margin
    rows_html += data_row("Material Margins", lambda m: mat_margin(m)/100000,
                          lambda: T(mat_margin)/100000, cls="total-row", color_fn=True)
    rows_html += pct_row("Material Margins (%)",
                         lambda m: mat_margin(m)/nsv(m)*100,
                         lambda: T(mat_margin)/tot_ns*100 if tot_ns else np.nan)

    rows_html += gap()

    # Freight Inward + Wages
    rows_html += data_row("Less: Freight Inwards", lambda m: v("Freight Inward",m)/100000,
                          lambda: tot.get("Freight Inward",0)/100000, color_fn=True, inverse=True)
    rows_html += data_row("Less: Wages - Fixed", lambda m: v("Wages",m)/100000,
                          lambda: tot.get("Wages",0)/100000, color_fn=True, inverse=True)
    rows_html += data_row("Freight Inwards & Wages Total", lambda m: fw(m)/100000,
                          lambda: T(fw)/100000, color_fn=True, inverse=True)
    rows_html += pct_row("Inward %age",
                         lambda m: fw(m)/nsv(m)*100,
                         lambda: T(fw)/tot_ns*100 if tot_ns else np.nan,
                         inverse=True)

    rows_html += gap()

    # Gross Margin
    rows_html += data_row("Gross Margins", lambda m: gm(m)/100000,
                          lambda: T(gm)/100000, cls="total-row", color_fn=True)
    rows_html += pct_row("Gross Margins (%)",
                         lambda m: gm(m)/nsv(m)*100,
                         lambda: T(gm)/tot_ns*100 if tot_ns else np.nan)

    rows_html += gap()

    # C&L breakdown
    rows_html += data_row("Less: Commission Expense",        lambda m: v("Commission",m)/100000,     lambda: tot.get("Commission",0)/100000,     color_fn=True, inverse=True)
    rows_html += data_row("Less: Payment Gateway Charges",   lambda m: v("Payment Gateway",m)/100000,lambda: tot.get("Payment Gateway",0)/100000,color_fn=True, inverse=True)
    rows_html += data_row("Less: Shipping Charges",          lambda m: v("Shipping",m)/100000,       lambda: tot.get("Shipping",0)/100000,       color_fn=True, inverse=True)
    rows_html += data_row("Less: Bulk Logistic Cost",        lambda m: v("Bulk Logistic",m)/100000,  lambda: tot.get("Bulk Logistic",0)/100000,  color_fn=True, inverse=True)
    rows_html += data_row("Less: Packaging Cost",            lambda m: v("Packaging",m)/100000,      lambda: tot.get("Packaging",0)/100000,      color_fn=True, inverse=True)
    rows_html += data_row("Less: Warehousing Charges",       lambda m: v("Warehousing",m)/100000,    lambda: tot.get("Warehousing",0)/100000,    color_fn=True, inverse=True)
    rows_html += data_row("Less: Rebate",                    lambda m: v("Rebate",m)/100000,         lambda: tot.get("Rebate",0)/100000,         color_fn=True, inverse=True)
    rows_html += data_row("Less: Others",                    lambda m: v("Others",m)/100000,         lambda: tot.get("Others",0)/100000,         color_fn=True, inverse=True)
    rows_html += data_row("Commission & Logistics Total",    lambda m: cnl(m)/100000,                lambda: T(cnl)/100000,                      color_fn=True, inverse=True)
    rows_html += pct_row("Commission & Logistics %",
                         lambda m: cnl(m)/nsv(m)*100,
                         lambda: T(cnl)/tot_ns*100 if tot_ns else np.nan,
                         inverse=True)

    rows_html += gap()

    # CM1
    rows_html += data_row("CM1", lambda m: cm1(m)/100000, lambda: T(cm1)/100000,
                          cls="total-row", color_fn=True,
                          fmt_fn=lambda x: Lbold(x) if not pd.isna(x) else "-")
    rows_html += pct_row("CM1 (%)",
                         lambda m: cm1(m)/nsv(m)*100,
                         lambda: T(cm1)/tot_ns*100 if tot_ns else np.nan)

    rows_html += gap()

    # Ad Spend / ACOS
    rows_html += data_row("Less: Performance Marketing", lambda m: v("Ad Spend",m)/100000,
                          lambda: tot.get("Ad Spend",0)/100000, color_fn=True, inverse=True)
    rows_html += pct_row("ACOS (%)",
                         lambda m: v("Ad Spend",m)/nsv(m)*100,
                         lambda: tot.get("Ad Spend",0)/tot_ns*100 if tot_ns else np.nan,
                         inverse=True)

    rows_html += gap()

    # CM2
    rows_html += data_row("CM2", lambda m: cm2(m)/100000, lambda: T(cm2)/100000,
                          cls="total-row", color_fn=True,
                          fmt_fn=lambda x: Lbold(x) if not pd.isna(x) else "-")
    rows_html += pct_row("CM2 (%)",
                         lambda m: cm2(m)/nsv(m)*100,
                         lambda: T(cm2)/tot_ns*100 if tot_ns else np.nan)

    return f"""
    <div style='overflow-x:auto'>
    <table class='pnl-table'>
      {th("Particulars (INR Lacs)")}
      <tbody>{rows_html}</tbody>
    </table>
    </div>"""

# ══════════════════════════════════════════════════════════════════════════════
# SIDEBAR
# ══════════════════════════════════════════════════════════════════════════════
with st.sidebar:
    st.markdown(f"""
    <div style='text-align:center;padding:16px 0 8px 0'>
      <div style='font-size:26px'>🕌</div>
      <div style='font-size:17px;font-weight:800;color:{GOLD}'>Kenaz Perfumes</div>
      <div style='font-size:11px;color:#666;letter-spacing:1px;text-transform:uppercase;margin-top:3px'>One Guardian Brands</div>
    </div>""", unsafe_allow_html=True)
    st.markdown("---")

    st.markdown(f"📋 **Sheet:** [Kenaz_PL_DB](https://docs.google.com/spreadsheets/d/{SHEET_KEY})")

    st.markdown("### 📤 Upload P&L File")
    uploaded = st.file_uploader("Kenaz P&L (.xlsb)", type=["xlsb"])

    if uploaded:
        try:
            file_bytes = uploaded.read()
            st.session_state["last_upload_bytes"] = file_bytes

            raw = parse_xlsb(file_bytes)
            ch_counts = raw["Channel"].value_counts()
            st.success(f"✅ {len(raw):,} rows | {raw['Month_name'].nunique()} months")
            st.info("  \n".join(f"• {ch}: {cnt}" for ch, cnt in ch_counts.items()))
            if st.button("💾 Save to Google Sheets", type="primary"):
                with st.spinner("Saving…"):
                    client = get_gsheet_client()
                    added, dupes = save_to_gsheet(client, raw)
                st.success(f"✅ {added:,} new rows. {dupes:,} dupes skipped.")
                st.cache_data.clear()
        except Exception as e:
            st.error(f"Parse error: {e}")

    st.markdown("---")
    df_raw = load_from_gsheet()

    if df_raw.empty:
        st.info("No data yet. Upload a file above.")
        st.stop()

    df_all = enrich(df_raw)

    with st.expander("🔍 Debug: Raw sheet data", expanded=False):
        st.write(f"Rows in sheet: {len(df_raw)}")
        st.write(f"Columns: {list(df_raw.columns)}")
        if not df_raw.empty:
            st.write("Net Sales sample:", df_raw["Net Sales"].head(5).tolist() if "Net Sales" in df_raw.columns else "col missing")
            st.write("Net Sales dtype:", str(df_raw["Net Sales"].dtype) if "Net Sales" in df_raw.columns else "col missing")
            st.dataframe(df_raw.head(10))

    st.markdown("### 🔍 Filters")
    channels_avail = sorted(df_all["Channel"].unique())
    months_df = df_all[["Month_name","Month_sort"]].drop_duplicates().sort_values("Month_sort")
    months_avail = months_df["Month_name"].tolist()

    sel_channels = st.multiselect("Channels", channels_avail, default=channels_avail)
    sel_months   = st.multiselect("Months",   months_avail,   default=months_avail)

    st.markdown("---")
    view = st.radio("View", ["P&L Summary","Product P&L","Channel Deep-Dive","Month Trend","Channel Mix"])

    st.markdown("---")
    if st.button("🗑️ Clear Cache"):
        st.cache_data.clear()
        st.success("Cache cleared.")
    if st.button("🔥 Clear Sheet & Re-upload", type="secondary"):
        with st.spinner("Clearing sheet..."):
            try:
                client = get_gsheet_client()
                ws = get_sheet(client).sheet1
                ws.clear()
                st.cache_data.clear()
                st.success("Sheet cleared. Now re-upload your file.")
            except Exception as e:
                st.error(f"Error: {e}")

# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════
st.markdown("<div class='header-bar'>📊 KENAZ PERFUMES — P&L DASHBOARD</div>", unsafe_allow_html=True)

df = df_all[df_all["Channel"].isin(sel_channels) & df_all["Month_name"].isin(sel_months)]
if df.empty:
    st.warning("No data for selected filters.")
    st.stop()

# KPI strip
tot = df[["Net Sales","COGS","GM","CM1","CM2","Ad Spend","Quantity"]].sum()
nsv = tot["Net Sales"]

def kpi(col, label, val, sub=""):
    col.markdown(f"""<div class='kpi-card'>
      <div class='kpi-label'>{label}</div>
      <div class='kpi-value'>{val}</div>
      <div class='kpi-sub'>{sub}</div></div>""", unsafe_allow_html=True)

c1,c2,c3,c4,c5,c6 = st.columns(6)
kpi(c1,"Net Sales",   f"&#8377;{nsv/100000:,.1f}L",  f"{int(tot['Quantity']):,} units")
kpi(c2,"Gross Margin",f"&#8377;{tot['GM']/100000:,.1f}L", P(tot['GM']/nsv*100 if nsv else 0)+" of NSV")
kpi(c3,"CM1",         f"&#8377;{tot['CM1']/100000:,.1f}L",P(tot['CM1']/nsv*100 if nsv else 0)+" of NSV")
kpi(c4,"CM2",         f"&#8377;{tot['CM2']/100000:,.1f}L",P(tot['CM2']/nsv*100 if nsv else 0)+" of NSV")
kpi(c5,"Ad Spend",    f"&#8377;{tot['Ad Spend']/100000:,.1f}L",P(tot['Ad Spend']/nsv*100 if nsv else 0)+" ACOS")
kpi(c6,"COGS",        f"&#8377;{tot['COGS']/100000:,.1f}L",P(tot['COGS']/nsv*100 if nsv else 0)+" of NSV")

st.markdown("<br>", unsafe_allow_html=True)

month_order = df[["Month_name","Month_sort"]].drop_duplicates().sort_values("Month_sort")["Month_name"].tolist()

# ══════════════════════════════════════════════════════════════════════════════
if view == "P&L Summary":
    # channel selector tabs
    tab_options = ["Total"] + sel_channels
    selected_tab = st.radio("View by", tab_options, horizontal=True, label_visibility="collapsed")

    if selected_tab == "Total":
        df_view = df
    else:
        df_view = df[df["Channel"] == selected_tab]

    st.markdown(build_pnl_table(df_view, sel_months, sel_channels), unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
elif view == "Product P&L":
    st.subheader("Product-wise P&L")

    if "last_upload_bytes" not in st.session_state:
        st.info("Upload and save a file first to see product-level P&L.")
        st.stop()

    with st.spinner("Parsing product data..."):
        sku_df = parse_sku_data(st.session_state["last_upload_bytes"])

    # filters
    all_models = sorted(sku_df["Model"].unique())
    all_months_sku = (sku_df[["Month_label","Month_sort"]]
                      .drop_duplicates().sort_values("Month_sort")["Month_label"].tolist())
    target_months = {'Oct-25','Nov-25','Dec-25','Jan-26','Feb-26','Mar-26','Apr-26'}
    avail_months = [m for m in all_months_sku if m in target_months]

    col_f1, col_f2, col_f3 = st.columns([2,2,2])
    with col_f1:
        sel_models = st.multiselect("Products", all_models, default=all_models, key="sku_models")
    with col_f2:
        sel_sku_months = st.multiselect("Months", avail_months, default=avail_months, key="sku_months")
    with col_f3:
        sel_sku_ch = st.multiselect("Channels", CHANNELS, default=CHANNELS, key="sku_ch")

    sku_f = sku_df[
        sku_df["Model"].isin(sel_models) &
        sku_df["Month_label"].isin(sel_sku_months) &
        sku_df["Channel"].isin(sel_sku_ch)
    ]

    if sku_f.empty:
        st.warning("No data for selected filters.")
        st.stop()

    # ── Month-wise product summary table ─────────────────────────────────────
    month_ord = (sku_f[["Month_label","Month_sort"]].drop_duplicates()
                 .sort_values("Month_sort")["Month_label"].tolist())

    grp = sku_f.groupby(["Model","Month_label","Month_sort"])[
        ["Net Sales","Quantity","MRP Sales","COGS","Freight Inward","Wages",
         "Commission","Payment Gateway","Shipping","Bulk Logistic",
         "Packaging","Warehousing","Others","Ad Spend"]
    ].sum().reset_index()

    def sku_metrics(g):
        ns = g["Net Sales"].sum()
        cogs = g["COGS"].sum()
        fw = (g["Freight Inward"] + g["Wages"]).sum()
        gm = ns - cogs - fw
        cnl = (g["Commission"]+g["Payment Gateway"]+g["Shipping"]+
               g["Bulk Logistic"]+g["Packaging"]+g["Warehousing"]+g["Others"]).sum()
        cm1 = gm - cnl
        ads = g["Ad Spend"].sum()
        cm2 = cm1 - ads
        qty = g["Quantity"].sum()
        mrp = g["MRP Sales"].sum()
        return pd.Series({
            "Net Sales": ns, "COGS": cogs, "GM": gm, "CnL": cnl,
            "CM1": cm1, "Ad Spend": ads, "CM2": cm2,
            "Qty": qty, "MRP Sales": mrp,
        })

    summary = grp.groupby(["Model","Month_label","Month_sort"]).apply(sku_metrics).reset_index()
    summary["GM%"]  = summary["GM"]  / summary["Net Sales"].replace(0,np.nan) * 100
    summary["CM1%"] = summary["CM1"] / summary["Net Sales"].replace(0,np.nan) * 100
    summary["CM2%"] = summary["CM2"] / summary["Net Sales"].replace(0,np.nan) * 100

    # Pivot: model × month for Net Sales
    st.markdown("#### Net Sales by Product (INR Lacs)")
    ns_pivot = summary.pivot_table(index="Model", columns="Month_label",
                                    values="Net Sales", aggfunc="sum").reindex(columns=month_ord)
    ns_pivot["Total"] = ns_pivot.sum(axis=1)
    ns_pivot = ns_pivot.sort_values("Total", ascending=False)

    def style_lacs(v):
        try:
            f = float(v)
            return f"{f/100000:,.2f}" if f != 0 else "-"
        except: return str(v)

    heads = "".join(f"<th>{m}</th>" for m in month_ord) + "<th>Total</th>"
    body = ""
    for model in ns_pivot.index:
        cells = ""
        for m in month_ord:
            v = ns_pivot.loc[model, m] if m in ns_pivot.columns else 0
            cells += f"<td>{style_lacs(v)}</td>"
        tot_v = ns_pivot.loc[model, "Total"]
        body += f"<tr><td><b>{model}</b></td>{cells}<td><b>{style_lacs(tot_v)}</b></td></tr>"
    # totals row
    tot_cells = ""
    for m in month_ord:
        tot_cells += f"<td><b>{style_lacs(ns_pivot[m].sum())}</b></td>"
    tot_cells += f"<td><b>{style_lacs(ns_pivot['Total'].sum())}</b></td>"
    body += f"<tr class='total-row'><td>Total</td>{tot_cells}</tr>"

    st.markdown(f"""
    <div style='overflow-x:auto'>
    <table class='pnl-table'>
      <thead><tr><th>Product</th>{heads}</tr></thead>
      <tbody>{body}</tbody>
    </table></div>""", unsafe_allow_html=True)

    # ── Full P&L per product (select one) ────────────────────────────────────
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown("#### Product Deep-Dive P&L")
    sel_prod = st.selectbox("Select Product", sorted(sku_f["Model"].unique()), key="sku_prod")
    prod_df = sku_f[sku_f["Model"] == sel_prod]

    prod_monthly = prod_df.groupby(["Month_label","Month_sort"])[
        ["Net Sales","Quantity","MRP Sales","COGS","Freight Inward","Wages",
         "Commission","Payment Gateway","Shipping","Bulk Logistic",
         "Packaging","Warehousing","Others","Ad Spend"]
    ].sum().reset_index().sort_values("Month_sort")
    prod_monthly = prod_monthly.set_index("Month_label")
    prod_months = [m for m in month_ord if m in prod_monthly.index]

    def pv(col, m): return prod_monthly.loc[m, col] if m in prod_monthly.index else 0
    def mat_m(m):   return pv("Net Sales",m) - pv("COGS",m)
    def fw_m(m):    return pv("Freight Inward",m) + pv("Wages",m)
    def gm_m(m):    return mat_m(m) - fw_m(m)
    def cnl_m(m):   return (pv("Commission",m)+pv("Payment Gateway",m)+pv("Shipping",m)+
                            pv("Bulk Logistic",m)+pv("Packaging",m)+pv("Warehousing",m)+pv("Others",m))
    def cm1_m(m):   return gm_m(m) - cnl_m(m)
    def cm2_m(m):   return cm1_m(m) - pv("Ad Spend",m)

    def prow(label, fn, pct_fn=None, is_total=False, inverse=False):
        cls = "total-row" if is_total else ""
        cells = ""
        for m in prod_months:
            v = fn(m)
            ns = pv("Net Sales",m) or np.nan
            s = Lbold(v) if is_total else L(v)
            c = color_val(v, inverse)
            cells += f"<td style='color:{c}'>{s}</td>"
            if pct_fn:
                pass
        tv = sum(fn(m) for m in prod_months)
        ts = Lbold(tv) if is_total else L(tv)
        tc = color_val(tv, inverse)
        row = f"<tr class='{cls}'><td>{label}</td>{cells}<td style='color:{tc}'>{ts}</td></tr>"

        if pct_fn:
            pcells = ""
            tot_ns_p = sum(pv("Net Sales",m) for m in prod_months) or np.nan
            for m in prod_months:
                ns_m = pv("Net Sales",m) or np.nan
                pv_ = pct_fn(m, ns_m)
                pc = color_val(pv_, inverse)
                pcells += f"<td style='color:{pc}'>{P(pv_)}</td>"
            tot_pv = pct_fn(None, tot_ns_p) if pct_fn else np.nan
            tpc = color_val(tot_pv, inverse)
            row += f"<tr class='pct-row'><td></td>{pcells}<td style='color:{tpc}'>{P(tot_pv)}</td></tr>"
        return row

    def safe_pct(v, ns): return v/ns*100 if ns and not pd.isna(ns) else np.nan

    ph = "".join(f"<th>{m}</th>" for m in prod_months) + "<th>Total</th>"
    pb = ""

    pb += prow("MRP Sales",     lambda m: pv("MRP Sales",m)/100000)
    pb += prow("Quantity",      lambda m: pv("Quantity",m),
               fmt_fn=lambda x: f"{int(x):,}" if x else "-")
    pb += f"<tr class='section-gap'>{'<td></td>'*(len(prod_months)+2)}</tr>"
    pb += prow("Net Sales",     lambda m: pv("Net Sales",m)/100000, is_total=True)

    pb += f"<tr class='section-gap'>{'<td></td>'*(len(prod_months)+2)}</tr>"
    pb += prow("Less: COGS",    lambda m: pv("COGS",m)/100000, inverse=True,
               pct_fn=lambda m,ns: safe_pct(pv("COGS",m if m else prod_months[-1])/1 if m else
                                   sum(pv("COGS",mx) for mx in prod_months), ns))
    pb += prow("Material Margins", lambda m: mat_m(m)/100000, is_total=True,
               pct_fn=lambda m,ns: safe_pct(mat_m(m) if m else sum(mat_m(mx) for mx in prod_months), ns))
    pb += f"<tr class='section-gap'>{'<td></td>'*(len(prod_months)+2)}</tr>"
    pb += prow("Less: Freight+Wages", lambda m: fw_m(m)/100000, inverse=True,
               pct_fn=lambda m,ns: safe_pct(fw_m(m) if m else sum(fw_m(mx) for mx in prod_months), ns))
    pb += prow("Gross Margin",  lambda m: gm_m(m)/100000,  is_total=True,
               pct_fn=lambda m,ns: safe_pct(gm_m(m) if m else sum(gm_m(mx) for mx in prod_months), ns))
    pb += f"<tr class='section-gap'>{'<td></td>'*(len(prod_months)+2)}</tr>"
    pb += prow("Less: Commission",    lambda m: pv("Commission",m)/100000, inverse=True)
    pb += prow("Less: Payment GW",    lambda m: pv("Payment Gateway",m)/100000, inverse=True)
    pb += prow("Less: Shipping",      lambda m: pv("Shipping",m)/100000, inverse=True)
    pb += prow("Less: Bulk Logistic", lambda m: pv("Bulk Logistic",m)/100000, inverse=True)
    pb += prow("Less: Packaging",     lambda m: pv("Packaging",m)/100000, inverse=True)
    pb += prow("Less: Warehousing",   lambda m: pv("Warehousing",m)/100000, inverse=True)
    pb += prow("Less: Others",        lambda m: pv("Others",m)/100000, inverse=True)
    pb += prow("C&L Total",    lambda m: cnl_m(m)/100000, inverse=True,
               pct_fn=lambda m,ns: safe_pct(cnl_m(m) if m else sum(cnl_m(mx) for mx in prod_months), ns))
    pb += prow("CM1",          lambda m: cm1_m(m)/100000, is_total=True,
               pct_fn=lambda m,ns: safe_pct(cm1_m(m) if m else sum(cm1_m(mx) for mx in prod_months), ns))
    pb += f"<tr class='section-gap'>{'<td></td>'*(len(prod_months)+2)}</tr>"
    pb += prow("Less: Ad Spend", lambda m: pv("Ad Spend",m)/100000, inverse=True,
               pct_fn=lambda m,ns: safe_pct(pv("Ad Spend",m) if m else sum(pv("Ad Spend",mx) for mx in prod_months), ns))
    pb += prow("CM2",            lambda m: cm2_m(m)/100000, is_total=True,
               pct_fn=lambda m,ns: safe_pct(cm2_m(m) if m else sum(cm2_m(mx) for mx in prod_months), ns))

    st.markdown(f"""
    <div style='overflow-x:auto'>
    <table class='pnl-table'>
      <thead><tr><th>{sel_prod} (INR Lacs)</th>{ph}</tr></thead>
      <tbody>{pb}</tbody>
    </table></div>""", unsafe_allow_html=True)

    # ── Bar chart: CM2 trend by product ──────────────────────────────────────
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown("#### CM2 Trend by Product")
    cm2_data = summary[summary["Month_label"].isin(prod_months)].copy()
    fig = px.bar(cm2_data.sort_values("Month_sort"), x="Month_label", y="CM2",
                 color="Model", barmode="group",
                 color_discrete_sequence=[GOLD,"#e67e22","#4fc3f7","#81c784","#ce93d8",
                                          "#ef9a9a","#fff176","#b0bec5","#80cbc4","#ffcc80"])
    fig.update_layout(template="plotly_dark", plot_bgcolor=DARK, paper_bgcolor=DARK,
                      font=dict(color="#aaa"), yaxis_title="INR", legend=dict(orientation="h"),
                      margin=dict(t=20,b=30,l=10,r=10), height=380,
                      xaxis=dict(gridcolor="#333",categoryorder="array",categoryarray=month_ord),
                      yaxis=dict(gridcolor="#333"))
    st.plotly_chart(fig, use_container_width=True)

# ══════════════════════════════════════════════════════════════════════════════
elif view == "Channel Deep-Dive":
    st.subheader("Channel Deep-Dive")
    ch_sel = st.selectbox("Channel", sel_channels)
    dch = df[df["Channel"] == ch_sel]

    pivot = (dch.groupby("Month_name")
               .agg(Net_Sales=("Net Sales","sum"), GM=("GM","sum"),
                    CM1=("CM1","sum"), CM2=("CM2","sum"), Ad_Spend=("Ad Spend","sum"))
               .reindex([m for m in month_order if m in dch["Month_name"].values])
               .reset_index())

    fig = go.Figure()
    fig.add_trace(go.Bar(name="CM2",      x=pivot["Month_name"], y=pivot["CM2"]/100000,      marker_color=GOLD))
    fig.add_trace(go.Bar(name="Ad Spend", x=pivot["Month_name"], y=pivot["Ad_Spend"]/100000, marker_color="#e67e22"))
    fig.add_trace(go.Scatter(name="Net Sales", x=pivot["Month_name"], y=pivot["Net_Sales"]/100000,
                             mode="lines+markers", line=dict(color="#4fc3f7", width=2)))
    fig.update_layout(barmode="stack", template="plotly_dark", plot_bgcolor=DARK, paper_bgcolor=DARK,
                      font=dict(color="#aaa"), yaxis_title="INR Lakhs", legend=dict(orientation="h"),
                      margin=dict(t=30,b=30,l=10,r=10), height=360,
                      xaxis=dict(gridcolor="#333"), yaxis=dict(gridcolor="#333"))
    st.plotly_chart(fig, use_container_width=True)
    st.markdown(build_pnl_table(dch, sel_months, [ch_sel]), unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
elif view == "Month Trend":
    st.subheader("Monthly Trend")
    metric = st.selectbox("Metric", ["Net Sales","GM","CM1","CM2","Ad Spend","COGS"])
    monthly = df.groupby(["Month_name","Month_sort"])[metric].sum().reset_index().sort_values("Month_sort")

    fig = go.Figure(go.Bar(x=monthly["Month_name"], y=monthly[metric]/100000, marker_color=GOLD, name=metric))
    fig.update_layout(template="plotly_dark", plot_bgcolor=DARK, paper_bgcolor=DARK,
                      font=dict(color="#aaa"), yaxis_title="INR Lakhs",
                      margin=dict(t=30,b=30,l=10,r=10), height=380,
                      xaxis=dict(gridcolor="#333"), yaxis=dict(gridcolor="#333"))
    st.plotly_chart(fig, use_container_width=True)

    ch_monthly = df.groupby(["Channel","Month_name","Month_sort"])[metric].sum().reset_index().sort_values("Month_sort")
    fig2 = px.line(ch_monthly, x="Month_name", y=metric, color="Channel",
                   color_discrete_sequence=[GOLD,"#e67e22","#4fc3f7","#81c784","#ce93d8"],
                   category_orders={"Month_name": month_order})
    fig2.update_layout(template="plotly_dark", plot_bgcolor=DARK, paper_bgcolor=DARK,
                       font=dict(color="#aaa"), margin=dict(t=20,b=30,l=10,r=10), height=350,
                       xaxis=dict(gridcolor="#333"), yaxis=dict(gridcolor="#333"),
                       legend=dict(orientation="h"))
    st.plotly_chart(fig2, use_container_width=True)

# ══════════════════════════════════════════════════════════════════════════════
elif view == "Channel Mix":
    st.subheader("Channel Mix")
    ch_agg = df.groupby("Channel")[["Net Sales","CM1","CM2","Ad Spend","Quantity"]].sum().reset_index()
    COLORS = [GOLD,"#e67e22","#4fc3f7","#81c784","#ce93d8"]

    col1, col2 = st.columns(2)
    with col1:
        fig = px.pie(ch_agg, names="Channel", values="Net Sales", title="Revenue Split", color_discrete_sequence=COLORS)
        fig.update_layout(template="plotly_dark", paper_bgcolor=DARK, font=dict(color="#aaa"), margin=dict(t=40,b=10), height=340)
        st.plotly_chart(fig, use_container_width=True)
    with col2:
        fig = px.pie(ch_agg, names="Channel", values="CM2", title="CM2 Split", color_discrete_sequence=COLORS)
        fig.update_layout(template="plotly_dark", paper_bgcolor=DARK, font=dict(color="#aaa"), margin=dict(t=40,b=10), height=340)
        st.plotly_chart(fig, use_container_width=True)

    ch_agg["CM1%"] = ch_agg["CM1"]/ch_agg["Net Sales"]*100
    ch_agg["CM2%"] = ch_agg["CM2"]/ch_agg["Net Sales"]*100
    ch_agg = ch_agg.sort_values("Net Sales", ascending=True)
    fig3 = go.Figure()
    fig3.add_trace(go.Bar(name="CM1%", x=ch_agg["CM1%"], y=ch_agg["Channel"], orientation="h", marker_color=GOLD))
    fig3.add_trace(go.Bar(name="CM2%", x=ch_agg["CM2%"], y=ch_agg["Channel"], orientation="h", marker_color="#e67e22"))
    fig3.update_layout(barmode="group", template="plotly_dark", plot_bgcolor=DARK, paper_bgcolor=DARK,
                       font=dict(color="#aaa"), xaxis_title="% of Net Sales",
                       margin=dict(t=20,b=30,l=10,r=10), height=320, legend=dict(orientation="h"))
    st.plotly_chart(fig3, use_container_width=True)

st.markdown(f"""<hr style='border-color:#333;margin-top:40px'>
<div style='text-align:center;color:#555;font-size:11px'>Kenaz Perfumes · One Guardian Brands · FY 25-26 P&amp;L Analytics</div>""",
unsafe_allow_html=True)
