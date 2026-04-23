import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import plotly.graph_objects as go
import requests
import json
import os
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
.news-medio { font-size: 11px; color: #38bdf8; text-transform: uppercase; font-weight: bold; letter-spacing: 0.5px; }
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
.spread-pill { display: inline-block; padding: 3px 10px; border-radius: 12px; font-weight: 600; font-size: 13px; }
.spread-good { background: rgba(16, 185, 129, 0.15); color: #10b981; }
.spread-bad { background: rgba(248, 113, 113, 0.15); color: #f87171; }
.spread-flat { background: rgba(100, 116, 139, 0.15); color: #94a3b8; }
.eval-tag { font-size: 12px; font-weight: 500; padding: 2px 8px; border-radius: 4px; }
.eval-good-strong { color: #10b981; }
.eval-good-mild { color: #34d399; }
.eval-neutral { color: #94a3b8; }
.eval-bad-mild { color: #fca5a5; }
.eval-bad-strong { color: #f87171; }
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
    sheet_name = st.secrets.get("SHEET_NAME", "Dashboard Macro")
    sh = gspread.authorize(creds).open(sheet_name)

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

    return read("DB_Insights"), read("DB_Historico"), read("DB_Noticias")


df_ai, df_hist, df_news = load_all()


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


def fmt_abbrev_money(v, prefix="$"):
    """Solo para montos grandes tipo vencimientos: $4.5MM, $1.2B"""
    if v is None:
        return "N/D"
    try:
        v = float(v)
        abs_v = abs(v)
        sign = "-" if v < 0 else ""
        if abs_v >= 1000:
            return f"{prefix}{sign}{abs_v/1000:.1f}B"
        if abs_v >= 1:
            return f"{prefix}{sign}{abs_v:.1f}MM"
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
        # FIX 1D: penúltimo valor
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
    serie = get_json("salario_real_serie_json", {})

    if yoy is None:
        st.markdown(
            '<div class="metric-card" style="border-color:#7f1d1d;">'
            '<div class="m-title" style="color:#fca5a5;">Salario Real</div>'
            '<div class="m-sub">Sin datos</div></div>',
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

    # Subtítulo explicativo de metodología
    subtexto = "Índice Salarios INDEC deflactado por IPC (12m)"

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
            "nombre": nombre, "ret": ret, "spread": spread,
            "pill_class": pill_class, "etiqueta": etiqueta, "eval_class": eval_class,
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
# BARRA HORIZONTAL (Macro Global)
# -----------------------------------------------------------
def bar_horizontal_etiq(labels, values, color="#38bdf8", height=220, unit="%", decimals=2):
    """Barras horizontales con etiquetas claras al final."""
    text_labels = [f"{v:+.{decimals}f}{unit}" if v is not None else "N/D" for v in values]
    values_plot = [v if v is not None else 0 for v in values]

    fig = go.Figure()
    fig.add_trace(go.Bar(
        y=labels, x=values_plot,
        orientation="h",
        marker=dict(color=color),
        text=text_labels, textposition="outside",
        textfont=dict(color="#f8fafc", size=13, family="monospace"),
        hovertemplate="%{y}: %{text}<extra></extra>",
        cliponaxis=False,
    ))

    nonnull = [v for v in values if v is not None]
    if nonnull:
        max_val = max(nonnull); min_val = min(nonnull)
        margin = max(abs(max_val - min_val) * 0.3, abs(max_val) * 0.2, 1)
        if min_val >= 0:
            x_range = [0, max_val + margin]
        else:
            x_range = [min_val - margin * 0.3, max_val + margin]
    else:
        x_range = [0, 1]

    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=0, r=60, t=5, b=5),
        height=height,
        xaxis=dict(range=x_range, showgrid=False, zeroline=False, showticklabels=False),
        yaxis=dict(tickfont=dict(color="#e2e8f0", size=12), autorange="reversed"),
        showlegend=False,
    )
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})


# -----------------------------------------------------------
# BARRA VERTICAL (Expectativas - ROFEX y vencimientos)
# -----------------------------------------------------------
def bar_vertical(labels, values, color="#38bdf8", height=280, value_format="number", prefix=""):
    """
    Barras verticales con etiquetas ARRIBA de cada barra.
    value_format: "number" (completo con separador miles) o "abbrev" (K/MM/B).
    """
    if value_format == "abbrev":
        text_labels = [fmt_abbrev_money(v, prefix) for v in values]
    else:  # number completo
        text_labels = [f"{prefix}{v:,.0f}".replace(",", ".") if v is not None else "N/D" for v in values]

    values_plot = [v if v is not None else 0 for v in values]

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=labels, y=values_plot,
        marker=dict(color=color),
        text=text_labels, textposition="outside",
        textfont=dict(color="#f8fafc", size=12, family="monospace"),
        hovertemplate="%{x}: %{text}<extra></extra>",
        cliponaxis=False,
    ))

    max_val = max([v for v in values if v is not None] or [1])
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=0, r=0, t=30, b=30),
        height=height,
        xaxis=dict(showgrid=False, tickfont=dict(color="#e2e8f0", size=11)),
        yaxis=dict(
            range=[0, max_val * 1.20],
            showgrid=True, gridcolor="#1e293b",
            showticklabels=False,  # sacamos ticks porque ya hay etiquetas
            zeroline=False,
        ),
        showlegend=False,
    )
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})


# -----------------------------------------------------------
# TABS (solo 4)
# -----------------------------------------------------------
st.title("📊 Dashboard Económico Financiero")
tab1, tab2, tab3, tab4 = st.tabs([
    "📌 Resumen", "🌎 Macro Global", "🇦🇷 Argentina", "🔮 Expectativas",
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
# TAB 2 - MACRO GLOBAL
# ===========================================================
with tab2:
    st.markdown('<div class="section-title">🌎 COMPARATIVA REGIONAL</div>', unsafe_allow_html=True)

    macro_global = get_json("macro_global_json", {})

    if macro_global and any(macro_global.values()):
        paises_orden = ["argentina", "brasil", "chile", "eeuu"]
        labels = ["🇦🇷 Argentina", "🇧🇷 Brasil", "🇨🇱 Chile", "🇺🇸 EEUU"]

        # Fila 1: Inflación + Riesgo País
        col_a, col_b = st.columns(2)
        with col_a:
            st.markdown('<div class="subsection-title">Inflación interanual (%)</div>', unsafe_allow_html=True)
            values = [macro_global.get(p, {}).get("inflacion_yoy") for p in paises_orden]
            bar_horizontal_etiq(labels, values, color="#f87171", height=220, unit="%", decimals=1)
        with col_b:
            st.markdown('<div class="subsection-title">Riesgo soberano (bps)</div>', unsafe_allow_html=True)
            values = [macro_global.get(p, {}).get("riesgo_pais") for p in paises_orden]
            # Etiquetas sin decimales para bps
            text_labels = [f"{int(v)}bps" if v is not None else "N/D" for v in values]
            values_plot = [v if v is not None else 0 for v in values]
            fig = go.Figure()
            fig.add_trace(go.Bar(
                y=labels, x=values_plot, orientation="h",
                marker=dict(color="#f59e0b"),
                text=text_labels, textposition="outside",
                textfont=dict(color="#f8fafc", size=13, family="monospace"),
                cliponaxis=False,
            ))
            max_v = max([v for v in values if v is not None] or [1])
            fig.update_layout(
                template="plotly_dark",
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                margin=dict(l=0, r=80, t=5, b=5), height=220,
                xaxis=dict(range=[0, max_v * 1.3], showgrid=False, zeroline=False, showticklabels=False),
                yaxis=dict(tickfont=dict(color="#e2e8f0", size=12), autorange="reversed"),
                showlegend=False,
            )
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

        # Fila 2: Tasa Real (full width, porque es la métrica clave)
        st.markdown('<div class="subsection-title">Tasa real anual (nominal − inflación)</div>', unsafe_allow_html=True)
        values = [macro_global.get(p, {}).get("tasa_real") for p in paises_orden]
        bar_horizontal_etiq(labels, values, color="#10b981", height=200, unit="%", decimals=2)

        st.caption("💡 Tasa real positiva = capital real ganando poder de compra. Argentina usa Plazo Fijo 30d como tasa de referencia; Brasil y Chile usan su tasa de política monetaria.")

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
            st.info("Análisis del LLM pendiente.")
    else:
        st.info("Sin datos todavía. La próxima corrida del pipeline V21 va a poblar esta solapa.")


# ===========================================================
# TAB 3 - ARGENTINA
# ===========================================================
with tab3:
    st.markdown('<div class="section-title">🇦🇷 SEMÁFORO MACROECONÓMICO</div>', unsafe_allow_html=True)

    col_a, col_b, col_c = st.columns(3)
    with col_a: render_ipc_card()
    with col_b: render_emae_card()
    with col_c: render_salario_real_card()

    st.caption(
        "💡 **Salario real**: base 100 = índice del mes hace 12 meses. "
        "Valor >0% indica que los salarios crecieron más que la inflación acumulada. "
        "Fuente: Índice de Salarios Nivel General (INDEC) deflactado por IPC."
    )

    lectura = get_insight("lectura_macro", "")
    if lectura and "insuficiente" not in lectura.lower():
        st.markdown(
            f'<div class="ai-box" style="margin-top:16px;">'
            f'<div style="color:#38bdf8;font-weight:bold;margin-bottom:8px;font-size:13px;">'
            f'📖 LECTURA TRANSVERSAL</div>'
            f'<div style="font-size:14px;color:#cbd5e1;line-height:1.7;">{lectura}</div>'
            f'</div>',
            unsafe_allow_html=True,
        )
    elif lectura:
        # Mensaje honesto cuando hay info pero LLM no aplica
        st.info(f"ℹ️ {lectura}")

    st.markdown('<div class="section-title">🔍 ANÁLISIS DE VALOR REAL (BASE USD)</div>', unsafe_allow_html=True)

    intervalo = st.radio("Intervalo:", ["Mensual", "Anual"], horizontal=True, label_visibility="collapsed")

    if intervalo == "Mensual":
        bench = get_insight_float("bench_1m", default=0.0) or 0.0
        rendimientos = get_json("valor_real_1m_json", {})
        financiamiento = get_json("financiamiento_1m_json", {})
        analisis_vr = get_insight("analisis_vr_1m", "")
    else:
        bench = get_insight_float("bench_1a", default=0.0) or 0.0
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

    # === FILA 1 DE TARJETAS: 4 KPIs ===
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        dev_impl = get_insight_float("dev_anualizada_implicita")
        ratio_fut = get_insight_float("ratio_dolar_12m_spot")
        if dev_impl is not None:
            # Una sola tarjeta: "Dólar futuro" con % y ratio entre paréntesis
            val_str = f"{dev_impl:+.1f}%"
            ratio_str = f"(ratio {ratio_fut:.2f}x)" if ratio_fut else ""
            color_v = "#f87171" if dev_impl > 15 else "#10b981" if dev_impl < 5 else "#f59e0b"
        else:
            val_str = "N/D"
            ratio_str = ""
            color_v = "#94a3b8"
        st.markdown(
            f'<div class="metric-card">'
            f'<div class="m-title">Dólar futuro 12m</div>'
            f'<div class="m-val" style="color:{color_v};">{val_str}</div>'
            f'<div class="m-sub">Devaluación implícita {ratio_str}</div></div>',
            unsafe_allow_html=True,
        )

    with col2:
        rem = get_json("rem_json", {})
        rem_inf = rem.get("inflacion_12m") if isinstance(rem, dict) else None
        try:
            rem_inf_num = float(rem_inf) if rem_inf is not None else None
        except Exception:
            rem_inf_num = None
        val_str = f"{rem_inf_num:.1f}%" if rem_inf_num is not None else "N/D"
        st.markdown(
            f'<div class="metric-card">'
            f'<div class="m-title">REM Inflación 12m</div>'
            f'<div class="m-val">{val_str}</div>'
            f'<div class="m-sub">Mediana analistas BCRA</div></div>',
            unsafe_allow_html=True,
        )

    with col3:
        tasa_real = get_insight_float("tasa_real_esperada")
        color_tr = "#10b981" if tasa_real and tasa_real > 0 else "#f87171"
        val_str = f"{tasa_real:+.2f}%" if tasa_real is not None else "N/D"
        st.markdown(
            f'<div class="metric-card">'
            f'<div class="m-title">Tasa real esperada</div>'
            f'<div class="m-val" style="color:{color_tr};">{val_str}</div>'
            f'<div class="m-sub">Plazo fijo vs inflación REM</div></div>',
            unsafe_allow_html=True,
        )

    with col4:
        # Reservas
        reservas = get_insight_float("reservas_actual_usd_mm")
        r_1m = get_insight_float("reservas_delta_1m")
        r_1a = get_insight_float("reservas_delta_1a")
        val_str = fmt_abbrev_money(reservas, "USD ") if reservas else "N/D"
        c1m = color_class(r_1m)
        c1a = color_class(r_1a)
        st.markdown(
            f'<div class="metric-card">'
            f'<div class="m-title">Reservas BCRA</div>'
            f'<div class="m-val">{val_str}</div>'
            f'<div class="m-deltas">'
            f'<span>1M: <span class="{c1m}">{fmt_delta(r_1m)}</span></span>'
            f'<span>1A: <span class="{c1a}">{fmt_delta(r_1a)}</span></span>'
            f'</div></div>',
            unsafe_allow_html=True,
        )

    # === FILA 2: GRÁFICOS LADO A LADO ===
    col_fut, col_venc = st.columns(2)

    with col_fut:
        st.markdown('<div class="subsection-title">Curva futuros de dólar (ROFEX)</div>', unsafe_allow_html=True)
        futuros = get_json("rofex_futuros_json", [])
        if futuros:
            labels = [str(f.get("vencimiento", "")) for f in futuros[:8]]
            values = [float(f.get("precio", 0)) for f in futuros[:8]]
            # Números completos (no abreviados) para curva
            bar_vertical(labels, values, color="#38bdf8", height=280,
                        value_format="number", prefix="$")
        else:
            st.info("Curva de futuros: esperando credenciales ROFEX o dato REM.")

    with col_venc:
        st.markdown('<div class="subsection-title">Vencimientos deuda soberana (USD)</div>', unsafe_allow_html=True)
        venc = get_json("vencimientos_deuda_json", [])
        if venc:
            venc_top = venc[:8]
            labels = [v["mes"] for v in venc_top]
            values = [v["monto_usd_mm"] for v in venc_top]
            # Abreviados para vencimientos
            bar_vertical(labels, values, color="#f87171", height=280,
                        value_format="abbrev", prefix="$")
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
