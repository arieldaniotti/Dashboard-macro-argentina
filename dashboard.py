import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import plotly.express as px
import plotly.graph_objects as go
import requests
import json
from datetime import timedelta

# -----------------------------------------------------------
# CONFIG + CSS
# -----------------------------------------------------------
st.set_page_config(page_title="Terminal Macro", layout="wide", initial_sidebar_state="collapsed")

st.markdown("""
<style>
.stApp { background-color: #07090f; color: #e2e8f0; font-family: sans-serif; }
.section-title {
    font-size: 20px; color: #38bdf8; font-weight: 800;
    text-transform: uppercase; border-bottom: 1px solid #1e293b;
    padding-bottom: 8px; margin: 20px 0;
}
.subsection-title {
    font-size: 14px; color: #94a3b8; font-weight: 600;
    text-transform: uppercase; margin: 12px 0 8px 0;
    letter-spacing: 0.5px;
}
.metric-card {
    background-color: #0b0e18; border: 1px solid #1e293b;
    border-radius: 8px; padding: 15px;
}
.m-title { font-size: 13px; color: #94a3b8; font-weight: bold; text-transform: uppercase; }
.m-val { font-size: 24px; font-weight: 700; font-family: monospace; margin: 8px 0; color: #f8fafc; }
.m-sub { font-size: 12px; color: #64748b; margin-top: 4px; }
.m-deltas {
    display: flex; justify-content: space-between;
    font-size: 12px; border-top: 1px solid #1e293b;
    padding-top: 8px; font-weight: 600;
}
.d-good { color: #10b981; }
.d-bad { color: #f87171; }
.d-flat { color: #64748b; }
.ai-box {
    background-color: #0a1525; border: 1px solid #1a3050;
    border-radius: 8px; padding: 20px;
}
.ai-line { font-size: 14px; color: #cbd5e1; line-height: 1.6; margin-bottom: 8px; }
.ai-label {
    color: #38bdf8; font-weight: bold; font-size: 12px;
    text-transform: uppercase; margin-right: 6px;
}
.news-card {
    background-color: #0b0e18; border-left: 3px solid #38bdf8;
    border-radius: 6px; padding: 12px 14px; margin-bottom: 10px;
}
.news-medio {
    font-size: 11px; color: #38bdf8; text-transform: uppercase;
    font-weight: bold; letter-spacing: 0.5px;
}
.news-title { font-size: 15px; color: #f8fafc; font-weight: 600; margin: 4px 0; }
.news-title a { color: #f8fafc; text-decoration: none; }
.news-title a:hover { color: #38bdf8; }
.news-why { font-size: 13px; color: #94a3b8; }
.table-vr {
    width: 100%; border-collapse: collapse; font-size: 14px;
    background: #0b0e18; border-radius: 8px; overflow: hidden;
}
.table-vr th {
    background: #1e293b; color: #94a3b8; font-weight: 600;
    text-transform: uppercase; font-size: 11px; letter-spacing: 0.5px;
    padding: 10px 12px; text-align: left;
}
.table-vr td {
    padding: 10px 12px; border-top: 1px solid #1e293b;
    color: #e2e8f0; font-variant-numeric: tabular-nums;
}
.spread-pill {
    display: inline-block; padding: 3px 10px; border-radius: 12px;
    font-weight: 600; font-size: 13px;
}
.spread-good { background: rgba(16, 185, 129, 0.15); color: #10b981; }
.spread-bad { background: rgba(248, 113, 113, 0.15); color: #f87171; }
.spread-flat { background: rgba(100, 116, 139, 0.15); color: #94a3b8; }
.eval-tag { font-size: 12px; font-weight: 500; padding: 2px 8px; border-radius: 4px; }
.eval-good-strong { color: #10b981; }
.eval-good-mild { color: #34d399; }
.eval-neutral { color: #94a3b8; }
.eval-bad-mild { color: #fca5a5; }
.eval-bad-strong { color: #f87171; }
.cost-card {
    background-color: #0b0e18; border: 1px solid #1e293b;
    border-radius: 8px; padding: 14px;
}
.cost-title { font-size: 12px; color: #94a3b8; text-transform: uppercase; font-weight: 600; }
.cost-val { font-size: 22px; font-weight: 700; font-family: monospace; color: #f8fafc; margin: 6px 0; }
.cost-delta { font-size: 11px; font-weight: 600; display: flex; justify-content: space-between; }
.mini-card {
    background-color: #0b0e18; border: 1px solid #1e293b;
    border-radius: 8px; padding: 12px;
}
.mini-label { font-size: 11px; color: #94a3b8; text-transform: uppercase; font-weight: 600; }
.mini-val { font-size: 20px; font-weight: 700; font-family: monospace; color: #f8fafc; margin: 4px 0; }
.mini-sub { font-size: 11px; color: #64748b; }
</style>
""", unsafe_allow_html=True)


# -----------------------------------------------------------
# DATA LOAD
# -----------------------------------------------------------
@st.cache_data(ttl=600)
def load_all():
    scopes = ["https://www.googleapis.com/auth/spreadsheets",
              "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scopes)
    sh = gspread.authorize(creds).open("Dashboard Macro")

    def read(n):
        try:
            data = sh.worksheet(n).get_all_values()
            if len(data) <= 1:
                return pd.DataFrame()
            df = pd.DataFrame(data[1:], columns=data[0])
            if "fecha" in df.columns:
                df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
            return df
        except Exception:
            return pd.DataFrame()

    return read("DB_Insights"), read("DB_Historico"), read("DB_Macro"), read("DB_Noticias")


df_ai, df_hist, df_macro, df_news = load_all()


# -----------------------------------------------------------
# HELPERS
# -----------------------------------------------------------
def get_insight(campo, default=""):
    if df_ai.empty or campo not in df_ai.columns:
        return default
    try:
        val = str(df_ai[campo].iloc[-1]).strip()
        return val if val and val.lower() != "nan" else default
    except Exception:
        return default


def get_insight_float(campo, default=None):
    raw = get_insight(campo, "")
    if not raw:
        return default
    try:
        return float(raw)
    except Exception:
        return default


def get_json(campo, default=None):
    raw = get_insight(campo, "")
    if not raw:
        return default if default is not None else []
    try:
        return json.loads(raw)
    except Exception:
        return default if default is not None else []


@st.cache_data(ttl=3600)
def get_fear_greed():
    try:
        r = requests.get("https://api.alternative.me/fng/", timeout=5).json()
        return r["data"][0]["value"], r["data"][0]["value_classification"]
    except Exception:
        return "N/A", "-"


fng_val, fng_class = get_fear_greed()


def fmt_num(v, decimals=2):
    try:
        s = f"{v:,.{decimals}f}".replace(",", "X").replace(".", ",").replace("X", ".")
        if decimals and s.endswith("," + "0" * decimals):
            s = s[:-(decimals + 1)]
        return s
    except Exception:
        return str(v)


def fmt_abbrev(v, prefix=""):
    """Abreviado estilo $2.5K, $2.5M, $2.5B para etiquetas en barras."""
    if v is None:
        return "N/D"
    try:
        v = float(v)
        abs_v = abs(v)
        sign = "-" if v < 0 else ""
        if abs_v >= 1e9:
            return f"{prefix}{sign}{abs_v/1e9:.1f}B"
        if abs_v >= 1e6:
            return f"{prefix}{sign}{abs_v/1e6:.1f}M"
        if abs_v >= 1e3:
            return f"{prefix}{sign}{abs_v/1e3:.1f}K"
        if abs_v >= 1:
            return f"{prefix}{sign}{abs_v:.1f}"
        return f"{prefix}{sign}{abs_v:.2f}"
    except Exception:
        return str(v)


def color_class(delta, is_inverted=False):
    if delta is None:
        return "d-flat"
    if abs(delta) < 0.01:
        return "d-flat"
    if is_inverted:
        return "d-bad" if delta > 0 else "d-good"
    return "d-good" if delta > 0 else "d-bad"


def fmt_delta(v, unit="%"):
    if v is None:
        return "N/D"
    return f"{v:+.1f}{unit}"


def etiqueta_inversion(spread):
    if spread > 10: return ("Ganancia real fuerte", "eval-good-strong")
    elif spread > 3: return ("Supera inflación USD", "eval-good-mild")
    elif spread > -3: return ("Neutral", "eval-neutral")
    elif spread > -10: return ("Pierde poder de compra", "eval-bad-mild")
    else: return ("Pérdida real fuerte", "eval-bad-strong")


def etiqueta_financiamiento(spread):
    if spread < -10: return ("Licuación fuerte", "eval-good-strong")
    elif spread < -3: return ("Licuación moderada", "eval-good-mild")
    elif spread < 3: return ("Costo neutro", "eval-neutral")
    elif spread < 10: return ("Deuda cara en USD", "eval-bad-mild")
    else: return ("Deuda muy cara", "eval-bad-strong")


# -----------------------------------------------------------
# KPI RENDER (Resumen)
# -----------------------------------------------------------
def render_kpi(title, col, prefix="", suffix="", is_inverted=False, mode="ratio"):
    try:
        if col not in df_hist.columns:
            st.markdown(
                f'<div class="metric-card" style="border-color:#7f1d1d;">'
                f'<div class="m-title" style="color:#fca5a5;">{title}</div>'
                f'<div class="m-sub">Faltan datos</div></div>',
                unsafe_allow_html=True,
            )
            return

        df = df_hist[["fecha", col]].copy()
        df[col] = pd.to_numeric(df[col], errors="coerce")
        df = df.dropna(subset=[col]).drop_duplicates(subset=["fecha"]).sort_values("fecha")

        if df.empty:
            st.markdown(
                f'<div class="metric-card" style="border-color:#7f1d1d;">'
                f'<div class="m-title" style="color:#fca5a5;">{title}</div>'
                f'<div class="m-sub">Sin registros</div></div>',
                unsafe_allow_html=True,
            )
            return

        val = df[col].iloc[-1]

        # FIX 1D: último registro distinto al actual (penúltimo de la serie)
        val_prev = df[col].iloc[-2] if len(df) >= 2 else None

        def get_ant(days):
            target = df["fecha"].iloc[-1] - timedelta(days=days)
            sub = df[df["fecha"] <= target]
            return sub[col].iloc[-1] if not sub.empty else None

        ant_1m = get_ant(30)
        ant_1a = get_ant(365)

        def safe_pct(new, old):
            if old is None or old == 0:
                return None
            return ((new / old) - 1) * 100

        def safe_diff(new, old):
            if old is None:
                return None
            return new - old

        if mode == "ratio":
            d1 = safe_pct(val, val_prev)
            m1 = safe_pct(val, ant_1m)
            y1 = safe_pct(val, ant_1a)
            unit = "%"
        elif mode == "points":
            d1 = safe_diff(val, val_prev)
            m1 = safe_diff(val, ant_1m)
            y1 = safe_diff(val, ant_1a)
            unit = ""
        elif mode == "pp":
            d1 = safe_diff(val, val_prev)
            m1 = safe_diff(val, ant_1m)
            y1 = safe_diff(val, ant_1a)
            unit = "pp"
        else:
            d1 = m1 = y1 = None; unit = ""

        if mode == "points":
            val_str = f"{int(val):,}".replace(",", ".")
        else:
            val_str = fmt_num(val, 2)

        st.markdown(
            f'<div class="metric-card">'
            f'<div class="m-title">{title}</div>'
            f'<div class="m-val">{prefix}{val_str}{suffix}</div>'
            f'<div class="m-deltas">'
            f'<span>1D: <span class="{color_class(d1, is_inverted)}">{fmt_delta(d1, unit)}</span></span>'
            f'<span>1M: <span class="{color_class(m1, is_inverted)}">{fmt_delta(m1, unit)}</span></span>'
            f'<span>1A: <span class="{color_class(y1, is_inverted)}">{fmt_delta(y1, unit)}</span></span>'
            f'</div></div>',
            unsafe_allow_html=True,
        )
    except Exception as e:
        st.markdown(
            f'<div class="metric-card" style="border-color:#7f1d1d;">'
            f'<div class="m-title" style="color:#fca5a5;">{title}</div>'
            f'<div class="m-sub">Error: {str(e)[:50]}</div></div>',
            unsafe_allow_html=True,
        )


# -----------------------------------------------------------
# MACRO CARD Argentina
# -----------------------------------------------------------
def macro_card_integrated(label, valor_str, subtexto, delta_text, delta_color,
                          serie_valores, serie_fechas, color_hex, age_days=None):
    age_html = ""
    if age_days is not None and age_days > 45:
        age_html = f'<div style="font-size:10px; color:#64748b; margin-top:3px;">Dato de hace {age_days}d</div>'

    st.markdown(
        f'<div style="background: linear-gradient(135deg, #0b0e18 0%, #0f1420 100%);'
        f' border: 1px solid #1e293b; border-radius: 10px 10px 0 0;'
        f' padding: 14px 16px 8px 16px; border-bottom: none;">'
        f'<div style="font-size:11px; color:#94a3b8; text-transform:uppercase; '
        f'font-weight:600; letter-spacing:0.5px;">{label}</div>'
        f'<div style="font-size:28px; font-weight:700; font-family:monospace; '
        f'margin:4px 0; color:#f8fafc;">{valor_str}</div>'
        f'<div style="font-size:12px; color:#94a3b8;">{subtexto}</div>'
        f'<div style="font-size:12px; font-weight:600; color:{delta_color}; margin-top:4px;">{delta_text}</div>'
        f'{age_html}'
        f'</div>',
        unsafe_allow_html=True,
    )

    if serie_valores and len(serie_valores) >= 2:
        fig = go.Figure()
        rgb = tuple(int(color_hex.lstrip('#')[i:i+2], 16) for i in (0, 2, 4))
        fig.add_trace(go.Scatter(
            x=list(range(len(serie_valores))),
            y=serie_valores,
            mode="lines",
            line=dict(color=color_hex, width=2.5),
            fill="tozeroy",
            fillcolor=f"rgba{rgb + (0.15,)}",
            hovertext=[f"{f}: {v}" for f, v in zip(serie_fechas, serie_valores)] if serie_fechas else None,
            hoverinfo="text",
        ))
        y_min = min(serie_valores); y_max = max(serie_valores)
        y_range = y_max - y_min
        y_lower = y_min - y_range * 0.1 if y_range > 0 else y_min - 1
        y_upper = y_max + y_range * 0.1 if y_range > 0 else y_max + 1
        fig.update_layout(
            margin=dict(l=0, r=0, t=0, b=0),
            paper_bgcolor="#0f1420", plot_bgcolor="#0f1420",
            xaxis=dict(visible=False, fixedrange=True, range=[-0.5, len(serie_valores) - 0.5]),
            yaxis=dict(visible=False, fixedrange=True, range=[y_lower, y_upper]),
            height=90, showlegend=False,
        )
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
    else:
        st.markdown(
            '<div style="background:#0f1420; border:1px solid #1e293b; '
            'border-top:none; border-radius:0 0 10px 10px; padding:30px; '
            'text-align:center; color:#64748b; font-size:12px;">'
            'Sin serie histórica disponible</div>',
            unsafe_allow_html=True,
        )


def render_ipc_card():
    mes = get_insight_float("ipc_mes")
    yoy = get_insight_float("ipc_yoy")
    accel = get_insight_float("ipc_accel_pp")
    serie = get_json("ipc_serie_json", {})

    if mes is None:
        st.markdown(
            '<div class="metric-card" style="border-color:#7f1d1d;">'
            '<div class="m-title" style="color:#fca5a5;">Inflación (IPC)</div>'
            '<div class="m-sub">Sin datos</div></div>',
            unsafe_allow_html=True,
        )
        return

    valor_str = f"{mes:.2f}%"
    subtexto = f"Interanual: <b style='color:#f8fafc'>{yoy:.1f}%</b>" if yoy is not None else ""

    if accel is not None:
        if accel > 0:
            delta_text = f"↑ Aceleró {accel:.2f}pp vs mes previo"
            delta_color = "#f87171"
        elif accel < 0:
            delta_text = f"↓ Desaceleró {abs(accel):.2f}pp vs mes previo"
            delta_color = "#10b981"
        else:
            delta_text = "= Sin cambios vs mes previo"
            delta_color = "#94a3b8"
    else:
        delta_text = ""; delta_color = "#94a3b8"

    macro_card_integrated(
        "Inflación (IPC mensual)",
        valor_str, subtexto, delta_text, delta_color,
        serie.get("valores", []), serie.get("fechas", []),
        "#f87171",
    )


def render_emae_card():
    val = get_insight_float("emae_val")
    yoy = get_insight_float("emae_yoy")
    age = get_insight_float("emae_age_days")
    serie = get_json("emae_serie_json", {})

    if val is None:
        st.markdown(
            '<div class="metric-card" style="border-color:#7f1d1d;">'
            '<div class="m-title" style="color:#fca5a5;">Actividad (EMAE)</div>'
            '<div class="m-sub">Sin datos</div></div>',
            unsafe_allow_html=True,
        )
        return

    valor_str = f"{val:.1f} pts"

    if yoy is not None:
        if yoy > 0:
            delta_text = f"↑ +{yoy:.1f}% interanual"
            delta_color = "#10b981"
        elif yoy < 0:
            delta_text = f"↓ {yoy:.1f}% interanual"
            delta_color = "#f87171"
        else:
            delta_text = "= Sin variación interanual"
            delta_color = "#94a3b8"
        subtexto = "Índice base 2004=100"
    else:
        delta_text = ""; delta_color = "#94a3b8"; subtexto = ""

    macro_card_integrated(
        "Actividad económica (EMAE)",
        valor_str, subtexto, delta_text, delta_color,
        serie.get("valores", []), serie.get("fechas", []),
        "#378ADD", age_days=int(age) if age is not None else None,
    )


def render_salario_real_card():
    yoy = get_insight_float("salario_real_yoy")
    age = get_insight_float("salario_real_age_days")
    fuente = get_insight("salario_fuente", "RIPTE")
    serie = get_json("salario_real_serie_json", {})

    if yoy is None:
        st.markdown(
            '<div class="metric-card" style="border-color:#7f1d1d;">'
            '<div class="m-title" style="color:#fca5a5;">Salario Real</div>'
            '<div class="m-sub">Sin datos - chequear serie INDEC</div></div>',
            unsafe_allow_html=True,
        )
        return

    if yoy > 0:
        valor_str = f"+{yoy:.1f}%"
        delta_text = "Salarios le ganaron a inflación"
        delta_color = "#10b981"
    elif yoy < 0:
        valor_str = f"{yoy:.1f}%"
        delta_text = "Salarios perdieron poder de compra"
        delta_color = "#f87171"
    else:
        valor_str = "0.0%"
        delta_text = "Sin variación real"
        delta_color = "#94a3b8"

    subtexto = f"Variación interanual ({fuente})"

    macro_card_integrated(
        "Salario real",
        valor_str, subtexto, delta_text, delta_color,
        serie.get("valores", []), serie.get("fechas", []),
        "#10b981", age_days=int(age) if age is not None else None,
    )


# -----------------------------------------------------------
# TABLA VALOR REAL
# -----------------------------------------------------------
def tabla_valor_real(items_dict, bench, is_credit=False):
    rows = []
    for nombre, ret in items_dict.items():
        if ret is None:
            continue
        try:
            ret = float(ret)
        except Exception:
            continue
        spread = ret - bench

        if is_credit:
            etiqueta, eval_class = etiqueta_financiamiento(spread)
            good = spread < 0
        else:
            etiqueta, eval_class = etiqueta_inversion(spread)
            good = spread > 0

        if abs(spread) < 0.5:
            pill_class = "spread-flat"
        elif good:
            pill_class = "spread-good"
        else:
            pill_class = "spread-bad"

        rows.append({
            "nombre": nombre,
            "ret": ret,
            "spread": spread,
            "pill_class": pill_class,
            "etiqueta": etiqueta,
            "eval_class": eval_class,
        })

    if not rows:
        st.info("Sin datos disponibles")
        return

    if is_credit:
        rows.sort(key=lambda x: x["spread"])
    else:
        rows.sort(key=lambda x: -x["spread"])

    filas_html = ""
    for r in rows:
        filas_html += (
            f'<tr>'
            f'<td>{r["nombre"]}</td>'
            f'<td style="text-align:right;">{r["ret"]:+.2f}%</td>'
            f'<td style="text-align:right; color:#94a3b8;">{bench:+.2f}%</td>'
            f'<td style="text-align:right;"><span class="spread-pill {r["pill_class"]}">{r["spread"]:+.2f}pp</span></td>'
            f'<td><span class="eval-tag {r["eval_class"]}">{r["etiqueta"]}</span></td>'
            f'</tr>'
        )

    col_label = "Costo USD" if is_credit else "Retorno USD"
    html = (
        f'<table class="table-vr">'
        f'<thead><tr>'
        f'<th>{"Deuda" if is_credit else "Activo"}</th>'
        f'<th style="text-align:right;">{col_label}</th>'
        f'<th style="text-align:right;">Benchmark</th>'
        f'<th style="text-align:right;">Spread</th>'
        f'<th>Evaluación</th>'
        f'</tr></thead>'
        f'<tbody>{filas_html}</tbody>'
        f'</table>'
    )
    st.markdown(html, unsafe_allow_html=True)


# -----------------------------------------------------------
# PORTFOLIO CARD
# -----------------------------------------------------------
def render_portfolio_card(nombre, data):
    if not data:
        st.markdown(
            f'<div class="metric-card" style="border-color:#7f1d1d;">'
            f'<div class="m-title" style="color:#fca5a5;">{nombre}</div>'
            f'<div class="m-sub">Sin datos</div></div>',
            unsafe_allow_html=True,
        )
        return

    val = data.get("val", 0)
    m1 = data.get("m1")
    a1 = data.get("a1")

    val_str = fmt_num(val, 2)
    m1_class = color_class(m1)
    a1_class = color_class(a1)

    st.markdown(
        f'<div class="metric-card">'
        f'<div class="m-title">{nombre}</div>'
        f'<div class="m-val">USD {val_str}</div>'
        f'<div class="m-deltas">'
        f'<span>1M: <span class="{m1_class}">{fmt_delta(m1)}</span></span>'
        f'<span>1A: <span class="{a1_class}">{fmt_delta(a1)}</span></span>'
        f'</div></div>',
        unsafe_allow_html=True,
    )


# -----------------------------------------------------------
# MINI CARD (for Inmobiliario KPIs + Expectativas)
# -----------------------------------------------------------
def render_mini_card(label, valor, sub, yoy_pct=None, unit="USD", valor_is_money=True):
    """Tarjeta compacta con valor + variación interanual opcional."""
    if valor is None:
        val_str = "N/D"
    elif valor_is_money:
        val_str = f"{unit} {valor}" if isinstance(valor, str) else f"{unit} {fmt_num(valor, 0) if valor >= 100 else fmt_num(valor, 2)}"
    else:
        val_str = f"{valor}"

    yoy_html = ""
    if yoy_pct is not None:
        yoy_class = color_class(yoy_pct)
        yoy_html = f'<div style="font-size:12px; font-weight:600; margin-top:4px;"><span class="{yoy_class}">YoY: {yoy_pct:+.1f}%</span></div>'

    st.markdown(
        f'<div class="mini-card">'
        f'<div class="mini-label">{label}</div>'
        f'<div class="mini-val">{val_str}</div>'
        f'<div class="mini-sub">{sub}</div>'
        f'{yoy_html}'
        f'</div>',
        unsafe_allow_html=True,
    )


def render_cost_card(titulo, actual, hace_1m, hace_1a, unit="USD"):
    if actual is None:
        return

    try:
        delta_1m = ((actual / hace_1m) - 1) * 100 if hace_1m else None
        delta_1a = ((actual / hace_1a) - 1) * 100 if hace_1a else None
    except Exception:
        delta_1m = delta_1a = None

    c1m = color_class(delta_1m, is_inverted=True)
    c1a = color_class(delta_1a, is_inverted=True)

    val_str = fmt_num(actual, 2) if actual < 1000 else f"{int(actual):,}".replace(",", ".")

    st.markdown(
        f'<div class="cost-card">'
        f'<div class="cost-title">{titulo}</div>'
        f'<div class="cost-val">{unit} {val_str}</div>'
        f'<div class="cost-delta">'
        f'<span>1M: <span class="{c1m}">{fmt_delta(delta_1m)}</span></span>'
        f'<span>1A: <span class="{c1a}">{fmt_delta(delta_1a)}</span></span>'
        f'</div></div>',
        unsafe_allow_html=True,
    )


# -----------------------------------------------------------
# BARRA HORIZONTAL con etiquetas (para Macro Global, Vencimientos)
# -----------------------------------------------------------
def bar_horizontal(labels, values, title="", unit="", color="#38bdf8",
                   height=240, value_prefix="", abbrev=False):
    """
    Gráfico de barras horizontal con etiquetas visibles al final de cada barra.
    """
    # Etiquetas
    if abbrev:
        text_labels = [fmt_abbrev(v, value_prefix) for v in values]
    else:
        text_labels = [f"{value_prefix}{v:.1f}{unit}" if v is not None else "N/D" for v in values]

    # Convertir None a 0 para el eje, pero marcar con hover
    values_plot = [v if v is not None else 0 for v in values]

    fig = go.Figure()
    fig.add_trace(go.Bar(
        y=labels,
        x=values_plot,
        orientation="h",
        marker=dict(color=color),
        text=text_labels,
        textposition="outside",  # etiqueta fuera de la barra
        textfont=dict(color="#f8fafc", size=13, family="monospace"),
        hovertemplate="%{y}: %{text}<extra></extra>",
        cliponaxis=False,
    ))

    # Calcular rango con margen para las etiquetas
    max_val = max([v for v in values if v is not None] or [1])
    min_val = min([v for v in values if v is not None] or [0])
    margin = (max_val - min_val) * 0.25 if max_val != min_val else max_val * 0.25
    if min_val >= 0:
        x_range = [0, max_val + margin]
    else:
        x_range = [min_val - margin * 0.5, max_val + margin]

    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=0, r=40, t=20 if title else 5, b=5),
        height=height,
        title=dict(text=title, font=dict(size=13, color="#94a3b8")) if title else None,
        xaxis=dict(range=x_range, showgrid=False, zeroline=False, showticklabels=False),
        yaxis=dict(tickfont=dict(color="#e2e8f0", size=12)),
        showlegend=False,
    )
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})


# -----------------------------------------------------------
# TABS
# -----------------------------------------------------------
st.title("📊 Dashboard Económico Financiero")
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "📌 Resumen", "🌎 Macro Global", "🇦🇷 Argentina",
    "🔮 Expectativas", "🏗️ Inmobiliario", "💼 Portafolio",
])

# ===========================================================
# TAB 1 - RESUMEN
# ===========================================================
with tab1:
    st.markdown('<div class="section-title">🌐 MUNDO</div>', unsafe_allow_html=True)
    c1, c2, c3, c4 = st.columns(4)
    with c1: render_kpi("S&P 500", "SP500", mode="ratio")
    with c2: render_kpi("Brent", "Brent", prefix="USD ", mode="ratio")
    with c3: render_kpi("Bitcoin", "BTC", prefix="USD ", mode="ratio")
    with c4: render_kpi("Oro", "Oro", prefix="USD ", mode="ratio")

    st.markdown('<div class="section-title">🇦🇷 ARGENTINA</div>', unsafe_allow_html=True)
    c5, c6, c7, c8 = st.columns(4)
    with c5: render_kpi("Merval", "Merval", mode="ratio")
    with c6: render_kpi("Riesgo País", "Riesgo_Pais", suffix=" bps", is_inverted=True, mode="points")
    with c7: render_kpi("Dólar Oficial", "USD_Oficial", prefix="$", is_inverted=True, mode="ratio")
    with c8: render_kpi("Brecha CCL", "Brecha_CCL", suffix="%", is_inverted=True, mode="pp")

    st.markdown("<br>", unsafe_allow_html=True)
    col_fg, col_ia = st.columns([1, 3])

    with col_fg:
        color_fg = "#f87171" if "Fear" in fng_class else "#10b981" if "Greed" in fng_class else "#f59e0b"
        st.markdown(
            f'<div class="metric-card" style="text-align:center; height:100%; '
            f'display:flex; flex-direction:column; justify-content:center;">'
            f'<div class="m-title" style="margin-bottom:10px;">Cripto Fear & Greed</div>'
            f'<div class="m-val" style="font-size:32px; color:{color_fg};">{fng_val}</div>'
            f'<div style="color:{color_fg}; font-weight:bold;">{fng_class.upper()}</div>'
            f'</div>',
            unsafe_allow_html=True,
        )

    with col_ia:
        mundo = get_insight("mundo", "Sin datos")
        argentina = get_insight("argentina", "Sin datos")
        a_mirar = get_insight("a_mirar", "Sin datos")

        st.markdown(
            f'<div class="ai-box">'
            f'<div style="color:#38bdf8;font-weight:bold;margin-bottom:12px;font-size:14px;">'
            f'🤖 FLASH MARKET</div>'
            f'<div class="ai-line"><span class="ai-label">🌐 Mundo:</span>{mundo}</div>'
            f'<div class="ai-line"><span class="ai-label">🇦🇷 Argentina:</span>{argentina}</div>'
            f'<div class="ai-line"><span class="ai-label">👀 A mirar:</span>{a_mirar}</div>'
            f'</div>',
            unsafe_allow_html=True,
        )

    destacadas = get_json("destacadas_json", [])
    if destacadas:
        st.markdown('<div class="section-title">📰 TOP 4 NOTICIAS DEL DÍA</div>', unsafe_allow_html=True)
        for n in destacadas[:4]:
            titular = n.get("titular", "")
            medio = n.get("medio", "")
            url = n.get("url", "#")
            por_que = n.get("por_que_importa", "")
            st.markdown(
                f'<div class="news-card">'
                f'<div class="news-medio">{medio}</div>'
                f'<div class="news-title"><a href="{url}" target="_blank">{titular}</a></div>'
                f'<div class="news-why">{por_que}</div>'
                f'</div>',
                unsafe_allow_html=True,
            )


# ===========================================================
# TAB 2 - MACRO GLOBAL (4 barras horizontales por variable)
# ===========================================================
with tab2:
    st.markdown('<div class="section-title">🌎 COMPARATIVA REGIONAL</div>', unsafe_allow_html=True)

    macro_global = get_json("macro_global_json", {})

    if macro_global and any(macro_global.values()):
        # Orden de países fijo
        paises_orden = ["argentina", "brasil", "chile", "eeuu"]
        labels = ["🇦🇷 Argentina", "🇧🇷 Brasil", "🇨🇱 Chile", "🇺🇸 EEUU"]

        # === FILA 1: Tasa PM + Inflación ===
        col_a, col_b = st.columns(2)

        with col_a:
            st.markdown('<div class="subsection-title">Tasa política monetaria (%)</div>', unsafe_allow_html=True)
            values = [macro_global.get(p, {}).get("tasa_pm") for p in paises_orden]
            bar_horizontal(labels, values, unit="%", color="#38bdf8", height=240, abbrev=False)

        with col_b:
            st.markdown('<div class="subsection-title">Inflación interanual (%)</div>', unsafe_allow_html=True)
            values = [macro_global.get(p, {}).get("inflacion_yoy") for p in paises_orden]
            bar_horizontal(labels, values, unit="%", color="#f87171", height=240, abbrev=False)

        # === FILA 2: CDS + Bono 10Y ===
        col_c, col_d = st.columns(2)

        with col_c:
            st.markdown('<div class="subsection-title">Riesgo soberano (CDS 5Y, bps)</div>', unsafe_allow_html=True)
            values = [macro_global.get(p, {}).get("cds_5y") for p in paises_orden]
            bar_horizontal(labels, values, unit="bps", color="#f59e0b", height=240, abbrev=True, value_prefix="")

        with col_d:
            st.markdown('<div class="subsection-title">Rendimiento bono 10Y (%)</div>', unsafe_allow_html=True)
            values = [macro_global.get(p, {}).get("bono_10y") for p in paises_orden]
            bar_horizontal(labels, values, unit="%", color="#10b981", height=240, abbrev=False)

        # === ANÁLISIS LLM ===
        analisis = get_insight("analisis_global", "")
        if analisis:
            st.markdown(
                f'<div class="ai-box" style="margin-top:20px;">'
                f'<div style="color:#38bdf8;font-weight:bold;margin-bottom:8px;font-size:13px;">'
                f'🌎 LECTURA REGIONAL</div>'
                f'<div style="font-size:14px;color:#cbd5e1;line-height:1.7;">{analisis}</div>'
                f'</div>',
                unsafe_allow_html=True,
            )
        else:
            st.info("Análisis del LLM pendiente (se completa tras primera corrida del pipeline V20).")
    else:
        st.info("Sin datos todavía. La próxima corrida del pipeline va a poblar esta solapa.")


# ===========================================================
# TAB 3 - ARGENTINA (ex-AR Estrategia)
# ===========================================================
with tab3:
    st.markdown('<div class="section-title">🇦🇷 SEMÁFORO MACROECONÓMICO</div>', unsafe_allow_html=True)

    col_a, col_b, col_c = st.columns(3)
    with col_a: render_ipc_card()
    with col_b: render_emae_card()
    with col_c: render_salario_real_card()

    lectura = get_insight("lectura_macro", "")
    if lectura:
        st.markdown(
            f'<div class="ai-box" style="margin-top:16px;">'
            f'<div style="color:#38bdf8;font-weight:bold;margin-bottom:8px;font-size:13px;">'
            f'📖 LECTURA TRANSVERSAL</div>'
            f'<div style="font-size:14px;color:#cbd5e1;line-height:1.7;">{lectura}</div>'
            f'</div>',
            unsafe_allow_html=True,
        )

    st.markdown('<div class="section-title">🔍 ANÁLISIS DE VALOR REAL (BASE USD)</div>', unsafe_allow_html=True)

    intervalo = st.radio("Intervalo:", ["Mensual", "Anual"], horizontal=True, label_visibility="collapsed")

    if intervalo == "Mensual":
        bench = get_insight_float("bench_1m", default=0.0)
        rendimientos = get_json("valor_real_1m_json", {})
        financiamiento = get_json("financiamiento_1m_json", {})
        analisis_vr = get_insight("analisis_vr_1m", "")
    else:
        bench = get_insight_float("bench_1a", default=0.0)
        rendimientos = get_json("valor_real_1a_json", {})
        financiamiento = get_json("financiamiento_1a_json", {})
        analisis_vr = get_insight("analisis_vr_1a", "")

    col_inv, col_fin = st.columns(2)
    with col_inv:
        st.subheader("💰 Inversiones en USD")
        st.caption(f"Benchmark dólares quietos: {bench:+.2f}%")
        if rendimientos:
            tabla_valor_real(rendimientos, bench, is_credit=False)
        else:
            st.info("Sin datos de rendimientos todavía.")

    with col_fin:
        st.subheader("💳 Costo de financiamiento en USD")
        st.caption(f"Benchmark dólares quietos: {bench:+.2f}%")
        if financiamiento:
            tabla_valor_real(financiamiento, bench, is_credit=True)
        else:
            st.info("Sin datos de financiamiento todavía.")

    if analisis_vr:
        label_periodo = "ESTE MES" if intervalo == "Mensual" else "EN LOS ÚLTIMOS 12 MESES"
        st.markdown(
            f'<div class="ai-box" style="margin-top:20px;">'
            f'<div style="color:#38bdf8;font-weight:bold;margin-bottom:10px;font-size:13px;">'
            f'💡 QUÉ PASÓ {label_periodo}</div>'
            f'<div style="font-size:14px;color:#cbd5e1;line-height:1.7;">{analisis_vr}</div>'
            f'</div>',
            unsafe_allow_html=True,
        )


# ===========================================================
# TAB 4 - EXPECTATIVAS
# ===========================================================
with tab4:
    st.markdown('<div class="section-title">🔮 EXPECTATIVAS DE MERCADO</div>', unsafe_allow_html=True)

    # === 4 TARJETAS CLAVE ===
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        ratio_fut = get_insight_float("ratio_dolar_12m_spot")
        color_r = "#f87171" if ratio_fut and ratio_fut > 1.3 else "#10b981" if ratio_fut and ratio_fut < 1.1 else "#f59e0b"
        st.markdown(
            f'<div class="metric-card">'
            f'<div class="m-title">Dólar futuro 12m / spot</div>'
            f'<div class="m-val" style="color:{color_r};">{ratio_fut if ratio_fut else "N/D"}x</div>'
            f'<div class="m-sub">Ratio vs dólar oficial hoy</div></div>',
            unsafe_allow_html=True,
        )

    with col2:
        dev_impl = get_insight_float("dev_anualizada_implicita")
        st.markdown(
            f'<div class="metric-card">'
            f'<div class="m-title">Devaluación implícita 12m</div>'
            f'<div class="m-val">{dev_impl if dev_impl else "N/D"}%</div>'
            f'<div class="m-sub">Anualizada ROFEX</div></div>',
            unsafe_allow_html=True,
        )

    with col3:
        rem = get_json("rem_json", {})
        rem_inf = rem.get("inflacion_12m") if isinstance(rem, dict) else None
        st.markdown(
            f'<div class="metric-card">'
            f'<div class="m-title">REM Inflación 12m</div>'
            f'<div class="m-val">{rem_inf if rem_inf else "N/D"}%</div>'
            f'<div class="m-sub">Relevamiento BCRA</div></div>',
            unsafe_allow_html=True,
        )

    with col4:
        tasa_real = get_insight_float("tasa_real_esperada")
        color_tr = "#10b981" if tasa_real and tasa_real > 0 else "#f87171"
        st.markdown(
            f'<div class="metric-card">'
            f'<div class="m-title">Tasa real esperada</div>'
            f'<div class="m-val" style="color:{color_tr};">{tasa_real if tasa_real else "N/D"}%</div>'
            f'<div class="m-sub">Plazo fijo vs inflación</div></div>',
            unsafe_allow_html=True,
        )

    # === GRÁFICOS LADO A LADO ===
    col_fut, col_venc = st.columns(2)

    with col_fut:
        st.markdown('<div class="subsection-title">Curva futuros de dólar (ROFEX)</div>', unsafe_allow_html=True)
        futuros = get_json("rofex_futuros_json", [])
        if futuros:
            labels = [str(f.get("vencimiento", "")) for f in futuros[:6]]
            values = [float(f.get("precio", 0)) for f in futuros[:6]]
            bar_horizontal(labels, values, unit="", color="#38bdf8", height=280, abbrev=True, value_prefix="$")
        else:
            st.info("Sin datos de futuros todavía.")

    with col_venc:
        st.markdown('<div class="subsection-title">Próximos vencimientos deuda (USD MM)</div>', unsafe_allow_html=True)
        venc = get_json("vencimientos_deuda_json", [])
        if venc:
            # Tomamos los próximos 8 meses (no 24) para que el gráfico sea legible
            venc_top = venc[:8]
            labels = [v["mes"] for v in venc_top]
            values = [v["monto_usd_mm"] for v in venc_top]
            # Multiplicamos por 1e6 solo para la abreviación estética
            values_display = [v * 1_000_000 for v in values]
            text_labels = [fmt_abbrev(v * 1_000_000, "$") for v in values]
            fig = go.Figure()
            fig.add_trace(go.Bar(
                y=labels, x=values,
                orientation="h",
                marker=dict(color="#f87171"),
                text=text_labels,
                textposition="outside",
                textfont=dict(color="#f8fafc", size=12, family="monospace"),
                hovertemplate="%{y}: USD %{x:,.0f}M<extra></extra>",
                cliponaxis=False,
            ))
            max_v = max(values) if values else 1
            fig.update_layout(
                template="plotly_dark",
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                margin=dict(l=0, r=60, t=5, b=5),
                height=280,
                xaxis=dict(range=[0, max_v * 1.3], showgrid=False, zeroline=False, showticklabels=False),
                yaxis=dict(tickfont=dict(color="#e2e8f0", size=12), autorange="reversed"),
                showlegend=False,
            )
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
        else:
            st.info("Sin datos de vencimientos todavía.")

    # === ANÁLISIS LLM ===
    analisis_exp = get_insight("analisis_expectativas", "")
    if analisis_exp:
        st.markdown(
            f'<div class="ai-box" style="margin-top:20px;">'
            f'<div style="color:#38bdf8;font-weight:bold;margin-bottom:8px;font-size:13px;">'
            f'🔮 LECTURA DE EXPECTATIVAS</div>'
            f'<div style="font-size:14px;color:#cbd5e1;line-height:1.7;">{analisis_exp}</div>'
            f'</div>',
            unsafe_allow_html=True,
        )
    else:
        st.info("Análisis del LLM pendiente.")


# ===========================================================
# TAB 5 - INMOBILIARIO
# ===========================================================
with tab5:
    st.markdown('<div class="section-title">🏗️ MERCADO INMOBILIARIO CABA</div>', unsafe_allow_html=True)

    m2_actual = get_json("m2_actual_json", {})
    escrit_caba = get_json("escrituras_caba_json", {})
    escrit_cba = get_json("escrituras_cba_json", {})
    costos_const = get_json("costos_construccion_json", {})
    creditos_hipot = get_json("creditos_hipot_json", {})
    anios_recupero = get_insight_float("anios_recupero_alquiler")
    anios_recupero_yoy = get_insight_float("anios_recupero_yoy")
    m2_venta_yoy = get_insight_float("m2_venta_yoy")
    m2_const_yoy = get_insight_float("m2_const_yoy")

    # === 4 TARJETAS KPI ARRIBA CON YOY ===
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        v = m2_actual.get("venta_m2_usd") if isinstance(m2_actual, dict) else None
        render_mini_card("Venta m² usado", v, "CABA - promedio", yoy_pct=m2_venta_yoy, unit="USD")
    with col2:
        c = m2_actual.get("construccion_m2_usd") if isinstance(m2_actual, dict) else None
        render_mini_card("Costo construcción m²", c, "CABA - reposición", yoy_pct=m2_const_yoy, unit="USD")
    with col3:
        render_mini_card("Años de recupero", anios_recupero, "Alquiler vs precio venta",
                         yoy_pct=anios_recupero_yoy, unit="", valor_is_money=False)
    with col4:
        escrit_val = escrit_caba.get("actual") if isinstance(escrit_caba, dict) else None
        escrit_yoy = escrit_caba.get("yoy_pct") if isinstance(escrit_caba, dict) else None
        render_mini_card("Escrituras CABA", escrit_val, "Último mes", yoy_pct=escrit_yoy,
                         unit="", valor_is_money=False)

    # === ESCRITURAS CABA + CBA CON 6M Y 12M ===
    st.markdown('<div class="subsection-title">Escrituras mensuales - evolución</div>', unsafe_allow_html=True)
    col_caba, col_cba = st.columns(2)

    def escrit_card(label, data):
        if not data:
            st.info(f"Sin datos de {label}")
            return
        actual = data.get("actual")
        s6m = data.get("s6m_pct")
        yoy = data.get("yoy_pct")
        c6 = color_class(s6m)
        cy = color_class(yoy)

        st.markdown(
            f'<div class="metric-card">'
            f'<div class="m-title">Escrituras {label}</div>'
            f'<div class="m-val">{actual if actual else "N/D"}</div>'
            f'<div class="m-sub">Operaciones último mes</div>'
            f'<div class="m-deltas">'
            f'<span>vs 6M: <span class="{c6}">{fmt_delta(s6m)}</span></span>'
            f'<span>vs 12M: <span class="{cy}">{fmt_delta(yoy)}</span></span>'
            f'</div></div>',
            unsafe_allow_html=True,
        )

    with col_caba:
        escrit_card("CABA", escrit_caba)
    with col_cba:
        escrit_card("Córdoba", escrit_cba)

    # === COSTOS CONSTRUCCIÓN ===
    st.markdown('<div class="subsection-title">Costos de construcción (USD, variación)</div>', unsafe_allow_html=True)
    if costos_const:
        cols = st.columns(3)
        items = [
            ("Cemento (bolsa 50kg)", "cemento_bolsa_50kg", "USD"),
            ("Acero (tonelada)", "acero_tonelada", "USD"),
            ("Mano de obra (jornal)", "mano_obra_jornal", "USD"),
        ]
        for i, (titulo, key, unit) in enumerate(items):
            with cols[i]:
                d = costos_const.get(key, {})
                render_cost_card(
                    titulo,
                    d.get("actual_usd"),
                    d.get("hace_1m_usd"),
                    d.get("hace_1a_usd"),
                    unit=unit,
                )

    # === CRÉDITOS HIPOTECARIOS OTORGADOS vs IPC (gráfico único) ===
    st.markdown('<div class="subsection-title">Créditos hipotecarios otorgados vs inflación</div>', unsafe_allow_html=True)
    if creditos_hipot and "fechas" in creditos_hipot:
        fig = go.Figure()
        # Valores en millones, etiquetas abreviadas
        valores_mm = creditos_hipot.get("otorgados_mm", [])
        # Convertir a unidades reales para abreviación (millones)
        valores_reales = [v * 1_000_000 for v in valores_mm]
        text_labels = [fmt_abbrev(v, "$") for v in valores_reales]

        fig.add_trace(go.Bar(
            x=creditos_hipot["fechas"],
            y=valores_mm,
            name="Créditos otorgados (ARS MM)",
            marker=dict(color="#38bdf8"),
            text=text_labels,
            textposition="outside",
            textfont=dict(color="#f8fafc", size=10),
            yaxis="y",
            hovertemplate="%{x}: %{text}<extra></extra>",
        ))
        fig.add_trace(go.Scatter(
            x=creditos_hipot["fechas"],
            y=creditos_hipot.get("ipc_mensual", []),
            name="IPC mensual %",
            line=dict(color="#f87171", width=2.5),
            mode="lines+markers",
            yaxis="y2",
        ))
        fig.update_layout(
            template="plotly_dark",
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            margin=dict(l=0, r=0, t=20, b=0), height=320,
            xaxis_title="",
            yaxis=dict(title="Créditos ARS MM"),
            yaxis2=dict(title="IPC %", overlaying="y", side="right"),
            legend=dict(orientation="h", y=1.1),
        )
        st.plotly_chart(fig, use_container_width=True)

    # === ANÁLISIS LLM ===
    analisis_inmo = get_insight("analisis_inmo", "")
    if analisis_inmo:
        st.markdown(
            f'<div class="ai-box" style="margin-top:20px;">'
            f'<div style="color:#38bdf8;font-weight:bold;margin-bottom:8px;font-size:13px;">'
            f'🏗️ LECTURA Y RECOMENDACIÓN</div>'
            f'<div style="font-size:14px;color:#cbd5e1;line-height:1.7;">{analisis_inmo}</div>'
            f'</div>',
            unsafe_allow_html=True,
        )
    else:
        st.info("Análisis del LLM pendiente.")

    st.caption("💡 Fuentes: Reporte Inmobiliario, Colegio de Escribanos (CABA/Córdoba), CAC, BCRA.")


# ===========================================================
# TAB 6 - PORTFOLIO
# ===========================================================
with tab6:
    st.markdown('<div class="section-title">💼 PORTAFOLIO DIVERSIFICADO</div>', unsafe_allow_html=True)

    portfolio = get_json("portfolio_json", {})

    if portfolio:
        nombres = list(portfolio.keys())
        if len(nombres) >= 5:
            cols1 = st.columns(5)
            for i in range(5):
                with cols1[i]:
                    if i < len(nombres):
                        render_portfolio_card(nombres[i], portfolio[nombres[i]])

            st.markdown("<br>", unsafe_allow_html=True)
            cols2 = st.columns(5)
            for i in range(5, min(10, len(nombres))):
                with cols2[i - 5]:
                    render_portfolio_card(nombres[i], portfolio[nombres[i]])
        else:
            cols = st.columns(len(nombres))
            for i, nombre in enumerate(nombres):
                with cols[i]:
                    render_portfolio_card(nombre, portfolio[nombre])

        st.markdown("<br>", unsafe_allow_html=True)
        st.markdown('<div class="subsection-title">Performance acumulada 12m - Portfolio vs S&P 500</div>', unsafe_allow_html=True)

        try:
            portfolio_cols = ["NVDA", "MELI", "MSFT", "GOOGL", "VIST", "YPF", "PAMP", "GGAL_ADR", "META", "BTC"]
            cols_present = [c for c in portfolio_cols if c in df_hist.columns]
            sp_present = "SP500" in df_hist.columns

            if cols_present and sp_present:
                df_perf = df_hist[["fecha"] + cols_present + ["SP500"]].copy()
                df_perf["fecha"] = pd.to_datetime(df_perf["fecha"], errors="coerce")
                for c in cols_present + ["SP500"]:
                    df_perf[c] = pd.to_numeric(df_perf[c], errors="coerce")
                df_perf = df_perf.dropna(subset=cols_present, how="all").sort_values("fecha")
                df_perf = df_perf.ffill().dropna(subset=cols_present + ["SP500"])

                if not df_perf.empty:
                    base = df_perf.iloc[0]
                    for c in cols_present + ["SP500"]:
                        df_perf[c + "_idx"] = df_perf[c] / base[c] * 100

                    df_perf["PORTFOLIO"] = df_perf[[c + "_idx" for c in cols_present]].mean(axis=1)

                    fig = go.Figure()
                    fig.add_trace(go.Scatter(
                        x=df_perf["fecha"], y=df_perf["PORTFOLIO"],
                        mode="lines", name="Portfolio (equiponderado)",
                        line=dict(color="#38bdf8", width=3),
                    ))
                    fig.add_trace(go.Scatter(
                        x=df_perf["fecha"], y=df_perf["SP500_idx"],
                        mode="lines", name="S&P 500",
                        line=dict(color="#94a3b8", width=2, dash="dot"),
                    ))
                    fig.update_layout(
                        template="plotly_dark",
                        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                        margin=dict(l=0, r=0, t=20, b=0), height=320,
                        legend=dict(orientation="h", y=1.1),
                        yaxis_title="Base 100",
                    )
                    st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            st.caption(f"No se pudo graficar performance: {e}")

        analisis_port = get_insight("analisis_portfolio", "")
        if analisis_port:
            st.markdown(
                f'<div class="ai-box" style="margin-top:20px;">'
                f'<div style="color:#38bdf8;font-weight:bold;margin-bottom:8px;font-size:13px;">'
                f'💼 PERSPECTIVA SECTORIAL</div>'
                f'<div style="font-size:14px;color:#cbd5e1;line-height:1.7;">{analisis_port}</div>'
                f'</div>',
                unsafe_allow_html=True,
            )
        else:
            st.info("Análisis del LLM pendiente.")
    else:
        st.info("Esperando datos del portfolio.")
