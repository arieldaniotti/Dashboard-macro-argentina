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
/* Colores tenues estilo rendimiento.co */
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
.eval-tag {
    font-size: 12px; font-weight: 500;
    padding: 2px 8px; border-radius: 4px;
}
.eval-good-strong { color: #10b981; }
.eval-good-mild { color: #34d399; }
.eval-neutral { color: #94a3b8; }
.eval-bad-mild { color: #fca5a5; }
.eval-bad-strong { color: #f87171; }
.country-card {
    background: linear-gradient(135deg, #0b0e18 0%, #0f1420 100%);
    border: 1px solid #1e293b; border-radius: 8px;
    padding: 14px; text-align: center;
}
.country-flag { font-size: 24px; margin-bottom: 4px; }
.country-name { font-size: 11px; color: #94a3b8; text-transform: uppercase; font-weight: 600; }
.country-val { font-size: 22px; font-weight: 700; font-family: monospace; margin: 4px 0; color: #f8fafc; }
.country-unit { font-size: 11px; color: #64748b; }
.cost-card {
    background-color: #0b0e18; border: 1px solid #1e293b;
    border-radius: 8px; padding: 14px;
}
.cost-title { font-size: 12px; color: #94a3b8; text-transform: uppercase; font-weight: 600; }
.cost-val { font-size: 22px; font-weight: 700; font-family: monospace; color: #f8fafc; margin: 6px 0; }
.cost-delta { font-size: 11px; font-weight: 600; display: flex; justify-content: space-between; }
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


def color_class(delta, is_inverted=False):
    if delta is None:
        return "d-flat"
    if abs(delta) < 0.01:
        return "d-flat"
    if is_inverted:
        return "d-bad" if delta > 0 else "d-good"
    return "d-good" if delta > 0 else "d-bad"


def fmt_delta(v, unit="%"):
    """Formatea un delta con unidad, o muestra N/D si es None."""
    if v is None:
        return "N/D"
    return f"{v:+.1f}{unit}"


# -----------------------------------------------------------
# ETIQUETAS SEMÁNTICAS
# -----------------------------------------------------------
def etiqueta_inversion(spread):
    if spread > 10: return ("Ganancia real fuerte", "eval-good-strong")
    elif spread > 3: return ("Supera inflación USD", "eval-good-mild")
    elif spread > -3: return ("Neutral", "eval-neutral")
    elif spread > -10: return ("Pierde poder de compra", "eval-bad-mild")
    else: return ("Pérdida real fuerte", "eval-bad-strong")


def etiqueta_financiamiento(spread):
    """
    Para financiamiento: el spread es (costo_usd - bench).
    Si es NEGATIVO significa licuación (ganás tomando la deuda).
    Si es POSITIVO significa costo real alto en USD.
    """
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
        last_date = df["fecha"].iloc[-1]

        def get_ant(days):
            """Devuelve None si no hay dato previo válido."""
            sub = df[df["fecha"] <= (last_date - timedelta(days=days))]
            if sub.empty:
                return None
            return sub[col].iloc[-1]

        df_prev = df[df["fecha"] < last_date]
        val_1d = df_prev[col].iloc[-1] if not df_prev.empty else None
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
            d1 = safe_pct(val, val_1d)
            m1 = safe_pct(val, ant_1m)
            y1 = safe_pct(val, ant_1a)
            unit = "%"
        elif mode == "points":
            d1 = safe_diff(val, val_1d)
            m1 = safe_diff(val, ant_1m)
            y1 = safe_diff(val, ant_1a)
            unit = ""
        elif mode == "pp":
            d1 = safe_diff(val, val_1d)
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
# MACRO CARD: usa SIEMPRE línea con área (unificado para Argentina)
# -----------------------------------------------------------
def macro_card_integrated(label, valor_str, subtexto, delta_text, delta_color,
                          serie_valores, serie_fechas, color_hex, age_days=None):
    """Card unificada con gráfico de LÍNEA + área tenue. Tipo único para Argentina."""
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
        # LÍNEA con área (mismo tipo para las 3 tarjetas Argentina)
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

        y_min = min(serie_valores)
        y_max = max(serie_valores)
        y_range = y_max - y_min
        y_lower = y_min - y_range * 0.1 if y_range > 0 else y_min - 1
        y_upper = y_max + y_range * 0.1 if y_range > 0 else y_max + 1

        fig.update_layout(
            margin=dict(l=0, r=0, t=0, b=0),
            paper_bgcolor="#0f1420",
            plot_bgcolor="#0f1420",
            xaxis=dict(visible=False, fixedrange=True, range=[-0.5, len(serie_valores) - 0.5]),
            yaxis=dict(visible=False, fixedrange=True, range=[y_lower, y_upper]),
            height=90,
            showlegend=False,
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

    subtexto = "Variación interanual (base 100)"

    macro_card_integrated(
        "Salario real (RIPTE)",
        valor_str, subtexto, delta_text, delta_color,
        serie.get("valores", []), serie.get("fechas", []),
        "#10b981", age_days=int(age) if age is not None else None,
    )


# -----------------------------------------------------------
# TABLA DE VALOR REAL
# -----------------------------------------------------------
def tabla_valor_real(items_dict, bench, is_credit=False):
    rows = []
    for nombre, ret in items_dict.items():
        if ret is None:
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
    """Tarjeta simple con precio actual + delta 1M + delta 1A en USD."""
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
# COUNTRY CARD (Macro Global)
# -----------------------------------------------------------
def render_country_metric(flag, country, value, unit=""):
    """Tarjeta por país para comparación macro global."""
    if value is None:
        val_str = "N/D"
    elif unit == "bps":
        val_str = f"{int(value):,}".replace(",", ".")
    else:
        val_str = f"{value:.2f}"

    st.markdown(
        f'<div class="country-card">'
        f'<div class="country-flag">{flag}</div>'
        f'<div class="country-name">{country}</div>'
        f'<div class="country-val">{val_str}</div>'
        f'<div class="country-unit">{unit}</div>'
        f'</div>',
        unsafe_allow_html=True,
    )


# -----------------------------------------------------------
# COST CARD (Inmobiliario)
# -----------------------------------------------------------
def render_cost_card(titulo, actual, hace_1m, hace_1a, unit="USD"):
    """Tarjeta con costo actual + delta vs mes y año en colores."""
    if actual is None:
        return

    try:
        delta_1m = ((actual / hace_1m) - 1) * 100 if hace_1m else None
        delta_1a = ((actual / hace_1a) - 1) * 100 if hace_1a else None
    except Exception:
        delta_1m = delta_1a = None

    c1m = color_class(delta_1m, is_inverted=True)  # costo bajando es bueno
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
# TABS
# -----------------------------------------------------------
st.title("📊 Dashboard Económico Financiero")
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "📌 Resumen", "🌎 Macro Global", "🇦🇷 AR Estrategia",
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
# TAB 2 - MACRO GLOBAL (comparación 4 países)
# ===========================================================
with tab2:
    st.markdown('<div class="section-title">🌎 COMPARATIVA REGIONAL</div>', unsafe_allow_html=True)

    macro_global = get_json("macro_global_json", {})

    if macro_global:
        flags = {"argentina": "🇦🇷", "chile": "🇨🇱", "brasil": "🇧🇷", "eeuu": "🇺🇸"}
        nombres = {"argentina": "Argentina", "chile": "Chile", "brasil": "Brasil", "eeuu": "EEUU"}

        # === TASA DE POLÍTICA MONETARIA ===
        st.markdown('<div class="subsection-title">Tasa de política monetaria</div>', unsafe_allow_html=True)
        cols = st.columns(4)
        for i, pais in enumerate(["argentina", "chile", "brasil", "eeuu"]):
            with cols[i]:
                v = macro_global.get(pais, {}).get("tasa_pm")
                render_country_metric(flags[pais], nombres[pais], v, "% TNA")

        # === INFLACIÓN YoY ===
        st.markdown('<div class="subsection-title">Inflación interanual</div>', unsafe_allow_html=True)
        cols = st.columns(4)
        for i, pais in enumerate(["argentina", "chile", "brasil", "eeuu"]):
            with cols[i]:
                v = macro_global.get(pais, {}).get("inflacion_yoy")
                render_country_metric(flags[pais], nombres[pais], v, "% YoY")

        # === CDS / RIESGO ===
        st.markdown('<div class="subsection-title">Riesgo soberano (spread/CDS 5Y)</div>', unsafe_allow_html=True)
        cols = st.columns(4)
        for i, pais in enumerate(["argentina", "chile", "brasil", "eeuu"]):
            with cols[i]:
                v = macro_global.get(pais, {}).get("cds_5y")
                render_country_metric(flags[pais], nombres[pais], v, "bps")

        # === BONO 10Y ===
        st.markdown('<div class="subsection-title">Rendimiento bono 10Y</div>', unsafe_allow_html=True)
        cols = st.columns(4)
        for i, pais in enumerate(["argentina", "chile", "brasil", "eeuu"]):
            with cols[i]:
                v = macro_global.get(pais, {}).get("bono_10y")
                render_country_metric(flags[pais], nombres[pais], v, "%")

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
        st.info("Esperando datos de macro global (se completa tras primera corrida del pipeline V19).")


# ===========================================================
# TAB 3 - ARGENTINA
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

    # Métricas clave arriba
    col1, col2, col3, col4 = st.columns(4)
    rem = get_json("rem_json", {})

    with col1:
        rem_inf = rem.get("inflacion_12m") if isinstance(rem, dict) else None
        st.markdown(
            f'<div class="metric-card">'
            f'<div class="m-title">REM Inflación 12m</div>'
            f'<div class="m-val">{rem_inf if rem_inf else "N/D"}%</div>'
            f'<div class="m-sub">Relevamiento BCRA</div></div>',
            unsafe_allow_html=True,
        )

    with col2:
        inf_impl = get_insight_float("inflacion_implicita_12m")
        st.markdown(
            f'<div class="metric-card">'
            f'<div class="m-title">Inflación implícita 12m</div>'
            f'<div class="m-val">{inf_impl if inf_impl else "N/D"}%</div>'
            f'<div class="m-sub">Bonos CER vs tasa fija</div></div>',
            unsafe_allow_html=True,
        )

    with col3:
        tasa_real = get_insight_float("tasa_real_esperada")
        color_tr = "#10b981" if tasa_real and tasa_real > 0 else "#f87171"
        st.markdown(
            f'<div class="metric-card">'
            f'<div class="m-title">Tasa real esperada</div>'
            f'<div class="m-val" style="color:{color_tr};">{tasa_real if tasa_real else "N/D"}%</div>'
            f'<div class="m-sub">Plazo fijo vs inflación</div></div>',
            unsafe_allow_html=True,
        )

    with col4:
        tasa_pf = get_insight_float("tasa_plazo_fijo")
        st.markdown(
            f'<div class="metric-card">'
            f'<div class="m-title">Plazo fijo 30d</div>'
            f'<div class="m-val">{tasa_pf if tasa_pf else "N/D"}%</div>'
            f'<div class="m-sub">TNA BCRA</div></div>',
            unsafe_allow_html=True,
        )

    # === CURVA DE FUTUROS ROFEX ===
    st.markdown('<div class="subsection-title">Curva de futuros de dólar (ROFEX)</div>', unsafe_allow_html=True)
    futuros = get_json("rofex_futuros_json", [])
    if futuros:
        df_fut = pd.DataFrame(futuros)
        if "vencimiento" in df_fut.columns and "precio" in df_fut.columns:
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=df_fut["vencimiento"],
                y=df_fut["precio"],
                mode="lines+markers",
                line=dict(color="#38bdf8", width=3),
                marker=dict(size=10, color="#38bdf8"),
                hovertemplate="%{x}: $%{y:.2f}<extra></extra>",
            ))
            fig.update_layout(
                template="plotly_dark",
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                margin=dict(l=0, r=0, t=20, b=0),
                height=280,
                xaxis_title="Vencimiento", yaxis_title="Precio USD/ARS",
            )
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Sin datos de futuros todavía.")

    # === VENCIMIENTOS DE DEUDA ===
    st.markdown('<div class="subsection-title">Vencimientos de deuda soberana (USD MM)</div>', unsafe_allow_html=True)
    venc = get_json("vencimientos_deuda_json", [])
    if venc:
        df_v = pd.DataFrame(venc)
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=df_v["mes"],
            y=df_v["monto_usd_mm"],
            marker=dict(color="#f87171"),
            hovertemplate="%{x}: USD %{y:,.0f}M<extra></extra>",
        ))
        fig.update_layout(
            template="plotly_dark",
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            margin=dict(l=0, r=0, t=20, b=0),
            height=280,
            xaxis_title="", yaxis_title="USD Millones",
        )
        st.plotly_chart(fig, use_container_width=True)

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


# ===========================================================
# TAB 5 - INMOBILIARIO
# ===========================================================
with tab5:
    st.markdown('<div class="section-title">🏗️ MERCADO INMOBILIARIO CABA</div>', unsafe_allow_html=True)

    m2_actual = get_json("m2_actual_json", {})
    m2_series = get_json("m2_series_json", {})
    escrit_caba = get_json("escrituras_caba_json", {})
    escrit_cba = get_json("escrituras_cba_json", {})
    costos_const = get_json("costos_construccion_json", {})
    creditos_hipot = get_json("creditos_hipot_json", {})
    anios_recupero = get_insight_float("anios_recupero_alquiler")

    # === KPIs ARRIBA ===
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        v = m2_actual.get("venta_m2_usd") if isinstance(m2_actual, dict) else None
        st.markdown(
            f'<div class="metric-card">'
            f'<div class="m-title">Venta m² usado</div>'
            f'<div class="m-val">USD {v if v else "N/D"}</div>'
            f'<div class="m-sub">CABA - promedio</div></div>',
            unsafe_allow_html=True,
        )
    with col2:
        c = m2_actual.get("construccion_m2_usd") if isinstance(m2_actual, dict) else None
        st.markdown(
            f'<div class="metric-card">'
            f'<div class="m-title">Costo construcción m²</div>'
            f'<div class="m-val">USD {c if c else "N/D"}</div>'
            f'<div class="m-sub">CABA - reposición</div></div>',
            unsafe_allow_html=True,
        )
    with col3:
        st.markdown(
            f'<div class="metric-card">'
            f'<div class="m-title">Años de recupero</div>'
            f'<div class="m-val">{anios_recupero if anios_recupero else "N/D"}</div>'
            f'<div class="m-sub">Alquiler vs precio venta</div></div>',
            unsafe_allow_html=True,
        )
    with col4:
        escrit_val = escrit_caba.get("ultimo") if isinstance(escrit_caba, dict) else None
        st.markdown(
            f'<div class="metric-card">'
            f'<div class="m-title">Escrituras/mes CABA</div>'
            f'<div class="m-val">{escrit_val if escrit_val else "N/D"}</div>'
            f'<div class="m-sub">Colegio Escribanos</div></div>',
            unsafe_allow_html=True,
        )

    # === GRÁFICO M² USD ===
    st.markdown('<div class="subsection-title">Evolución precio m² en USD - Venta vs Construcción</div>', unsafe_allow_html=True)
    if m2_series and "venta" in m2_series and "construccion" in m2_series:
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=m2_series["venta"]["fechas"],
            y=m2_series["venta"]["valores"],
            mode="lines+markers", name="Venta USD/m²",
            line=dict(color="#38bdf8", width=2.5),
        ))
        fig.add_trace(go.Scatter(
            x=m2_series["construccion"]["fechas"],
            y=m2_series["construccion"]["valores"],
            mode="lines+markers", name="Costo construcción USD/m²",
            line=dict(color="#f59e0b", width=2.5),
        ))
        fig.update_layout(
            template="plotly_dark",
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            margin=dict(l=0, r=0, t=20, b=0), height=300,
            legend=dict(orientation="h", y=1.1),
            xaxis_title="", yaxis_title="USD/m²",
        )
        st.plotly_chart(fig, use_container_width=True)

    # === COSTOS CONSTRUCCIÓN ===
    st.markdown('<div class="subsection-title">Costos de construcción (en USD, variación)</div>', unsafe_allow_html=True)
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

    # === CRÉDITOS HIPOTECARIOS vs IPC ===
    st.markdown('<div class="subsection-title">Créditos hipotecarios vs inflación</div>', unsafe_allow_html=True)
    if creditos_hipot and "fechas" in creditos_hipot:
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=creditos_hipot["fechas"],
            y=creditos_hipot["creditos_mm"],
            name="Stock hipotecarios (AR$ MM)",
            marker=dict(color="#38bdf8"),
            yaxis="y",
        ))
        fig.add_trace(go.Scatter(
            x=creditos_hipot["fechas"],
            y=creditos_hipot["ipc_mensual"],
            name="IPC mensual %",
            line=dict(color="#f87171", width=2.5),
            yaxis="y2",
        ))
        fig.update_layout(
            template="plotly_dark",
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            margin=dict(l=0, r=0, t=20, b=0), height=300,
            xaxis_title="",
            yaxis=dict(title="Stock créditos"),
            yaxis2=dict(title="IPC %", overlaying="y", side="right"),
            legend=dict(orientation="h", y=1.1),
        )
        st.plotly_chart(fig, use_container_width=True)

    # === ESCRITURAS ===
    if escrit_caba and escrit_cba:
        st.markdown('<div class="subsection-title">Escrituras mensuales - CABA vs Córdoba</div>', unsafe_allow_html=True)
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=escrit_caba.get("fechas", []),
            y=escrit_caba.get("valores", []),
            mode="lines+markers", name="CABA",
            line=dict(color="#38bdf8", width=2.5),
        ))
        fig.add_trace(go.Scatter(
            x=escrit_cba.get("fechas", []),
            y=escrit_cba.get("valores", []),
            mode="lines+markers", name="Córdoba",
            line=dict(color="#10b981", width=2.5),
        ))
        fig.update_layout(
            template="plotly_dark",
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            margin=dict(l=0, r=0, t=20, b=0), height=280,
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

    st.caption("💡 Fuentes: Reporte Inmobiliario, Colegio de Escribanos (CABA/Córdoba), CAC, BCRA. Datos de scraping con fallbacks.")


# ===========================================================
# TAB 6 - PORTFOLIO
# ===========================================================
with tab6:
    st.markdown('<div class="section-title">💼 PORTAFOLIO DIVERSIFICADO</div>', unsafe_allow_html=True)

    portfolio = get_json("portfolio_json", {})

    if portfolio:
        # Tarjetas en 2 filas de 5
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

        # === PERFORMANCE ACUMULADA PORTFOLIO vs S&P 500 ===
        st.markdown("<br>", unsafe_allow_html=True)
        st.markdown('<div class="subsection-title">Performance acumulada 12m - Portfolio equiponderado vs S&P 500</div>', unsafe_allow_html=True)

        # Calculamos performance usando df_hist
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
                    # Normalizamos a base 100
                    base = df_perf.iloc[0]
                    for c in cols_present + ["SP500"]:
                        df_perf[c + "_idx"] = df_perf[c] / base[c] * 100

                    # Portfolio equiponderado
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

        # === ANÁLISIS LLM ===
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
        st.info("Esperando datos del portfolio (se completan tras primera corrida del pipeline V19).")
