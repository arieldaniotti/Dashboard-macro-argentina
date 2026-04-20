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
    .d-bad { color: #ef4444; }
    .d-flat { color: #64748b; }
    .ai-box {
        background-color: #0a1525; border: 1px solid #1a3050;
        border-radius: 8px; padding: 20px; height: 100%;
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
    .macro-card {
        background: linear-gradient(135deg, #0b0e18 0%, #0f1420 100%);
        border: 1px solid #1e293b; border-radius: 10px;
        padding: 18px; min-height: 120px;
    }
    .macro-label { font-size: 12px; color: #94a3b8; text-transform: uppercase; font-weight: 600; letter-spacing: 0.5px; }
    .macro-val { font-size: 32px; font-weight: 700; font-family: monospace; margin: 6px 0; color: #f8fafc; }
    .macro-sub { font-size: 13px; color: #94a3b8; }
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
# HELPERS TO ACCESS DB_Insights
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


# -----------------------------------------------------------
# KPI RENDER - 3 MODOS
# -----------------------------------------------------------
def fmt_num(v, decimals=2):
    """Formato argentino: 1.234.567,89"""
    try:
        s = f"{v:,.{decimals}f}".replace(",", "X").replace(".", ",").replace("X", ".")
        if decimals and s.endswith("," + "0" * decimals):
            s = s[:-(decimals + 1)]
        return s
    except Exception:
        return str(v)


def color_class(delta, is_inverted=False):
    """
    is_inverted=True significa que subir es malo (riesgo país, dólar, brecha).
    """
    if abs(delta) < 0.01:
        return "d-flat"
    if is_inverted:
        return "d-bad" if delta > 0 else "d-good"
    return "d-good" if delta > 0 else "d-bad"


def render_kpi(title, col, prefix="", suffix="", is_inverted=False, mode="ratio"):
    """
    mode:
      - 'ratio': variación porcentual típica (precios). Muestra 1D/1M/1A en %.
      - 'points': diferencia absoluta (riesgo país). Muestra 1D/1M/1A en bps.
      - 'pp': diferencia en puntos porcentuales (brecha CCL, % en general).
              El valor es un %, pero las variaciones se muestran en pp.
    """
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
                f'<div class="m-sub">Sin registros válidos</div></div>',
                unsafe_allow_html=True,
            )
            return

        val = df[col].iloc[-1]
        last_date = df["fecha"].iloc[-1]

        def get_ant(days):
            sub = df[df["fecha"] <= (last_date - timedelta(days=days))]
            return sub[col].iloc[-1] if not sub.empty else df[col].iloc[0]

        df_prev_day = df[df["fecha"] < last_date]
        val_1d = df_prev_day[col].iloc[-1] if not df_prev_day.empty else val
        ant_1m = get_ant(30)
        ant_1a = get_ant(365)

        # Compute deltas according to mode
        if mode == "ratio":
            d1 = ((val / val_1d) - 1) * 100 if val_1d else 0
            m1 = ((val / ant_1m) - 1) * 100 if ant_1m else 0
            y1 = ((val / ant_1a) - 1) * 100 if ant_1a else 0
            unit_d, unit_m, unit_y = "%", "%", "%"
        elif mode == "points":
            d1 = val - val_1d
            m1 = val - ant_1m
            y1 = val - ant_1a
            unit_d, unit_m, unit_y = "bps", "bps", "bps"
        elif mode == "pp":
            d1 = val - val_1d
            m1 = val - ant_1m
            y1 = val - ant_1a
            unit_d, unit_m, unit_y = "pp", "pp", "pp"
        else:
            d1 = m1 = y1 = 0
            unit_d = unit_m = unit_y = ""

        # Format number (value shown)
        if mode == "points":
            val_str = f"{int(val):,}".replace(",", ".")
        else:
            val_str = fmt_num(val, 2)

        def fmt_delta(v, u):
            return f"{v:+.1f}{u}"

        st.markdown(
            f'<div class="metric-card">'
            f'<div class="m-title">{title}</div>'
            f'<div class="m-val">{prefix}{val_str}{suffix}</div>'
            f'<div class="m-deltas">'
            f'<span>1D: <span class="{color_class(d1, is_inverted)}">{fmt_delta(d1, unit_d)}</span></span>'
            f'<span>1M: <span class="{color_class(m1, is_inverted)}">{fmt_delta(m1, unit_m)}</span></span>'
            f'<span>1A: <span class="{color_class(y1, is_inverted)}">{fmt_delta(y1, unit_y)}</span></span>'
            f'</div></div>',
            unsafe_allow_html=True,
        )
    except Exception as e:
        st.markdown(
            f'<div class="metric-card" style="border-color:#7f1d1d;">'
            f'<div class="m-title" style="color:#fca5a5;">{title}</div>'
            f'<div class="m-sub">Error: {str(e)[:60]}</div></div>',
            unsafe_allow_html=True,
        )


# -----------------------------------------------------------
# MACRO CARD (IPC, EMAE, RIPTE) - render especial
# -----------------------------------------------------------
def render_macro_ipc():
    """Card de IPC: usa ipc_mes / ipc_yoy / ipc_accel_pp del pipeline."""
    mes = get_insight_float("ipc_mes")
    yoy = get_insight_float("ipc_yoy")
    accel = get_insight_float("ipc_accel_pp")

    if mes is None:
        st.markdown(
            '<div class="macro-card" style="border-color:#7f1d1d;">'
            '<div class="macro-label" style="color:#fca5a5;">Inflación (IPC)</div>'
            '<div class="macro-sub">Sin datos</div></div>',
            unsafe_allow_html=True,
        )
        return

    accel_html = ""
    if accel is not None:
        color = "#ef4444" if accel > 0 else "#10b981" if accel < 0 else "#64748b"
        flecha = "↑" if accel > 0 else "↓" if accel < 0 else "="
        texto = "Aceleró" if accel > 0 else "Desaceleró" if accel < 0 else "Sin cambios"
        accel_html = f'<span style="color:{color};">{flecha} {texto} {abs(accel):.2f}pp vs mes previo</span>'

    yoy_html = f"Interanual: <b>{yoy:.1f}%</b>" if yoy is not None else ""

    st.markdown(
        f'<div class="macro-card">'
        f'<div class="macro-label">Inflación (IPC último mes)</div>'
        f'<div class="macro-val">{mes:.2f}%</div>'
        f'<div class="macro-sub">{yoy_html}</div>'
        f'<div class="macro-sub" style="margin-top:6px;">{accel_html}</div>'
        f'</div>',
        unsafe_allow_html=True,
    )


def render_macro_generic(title, col, prefix="", suffix=""):
    """EMAE / RIPTE / cualquier serie macro genérica que publica con delay."""
    if col not in df_hist.columns:
        st.markdown(
            f'<div class="macro-card" style="border-color:#7f1d1d;">'
            f'<div class="macro-label" style="color:#fca5a5;">{title}</div>'
            f'<div class="macro-sub">Sin datos</div></div>',
            unsafe_allow_html=True,
        )
        return

    df = df_hist[["fecha", col]].copy()
    df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=[col]).drop_duplicates(subset=["fecha"]).sort_values("fecha")

    if df.empty:
        st.markdown(
            f'<div class="macro-card" style="border-color:#7f1d1d;">'
            f'<div class="macro-label" style="color:#fca5a5;">{title}</div>'
            f'<div class="macro-sub">Sin datos</div></div>',
            unsafe_allow_html=True,
        )
        return

    val = df[col].iloc[-1]
    last = df["fecha"].iloc[-1]
    age_days = (pd.Timestamp.now() - last).days

    df_1m = df[df["fecha"] <= (last - timedelta(days=30))]
    ant_1m = df_1m[col].iloc[-1] if not df_1m.empty else None

    df_1a = df[df["fecha"] <= (last - timedelta(days=365))]
    ant_1a = df_1a[col].iloc[-1] if not df_1a.empty else None

    m1 = ((val / ant_1m) - 1) * 100 if ant_1m else None
    y1 = ((val / ant_1a) - 1) * 100 if ant_1a else None

    def delta_html(d, suffix="%"):
        if d is None:
            return '<span style="color:#64748b;">—</span>'
        color = "#10b981" if d > 0 else "#ef4444" if d < 0 else "#64748b"
        return f'<span style="color:{color}; font-weight:600;">{d:+.1f}{suffix}</span>'

    val_fmt = fmt_num(val, 2)
    fecha_str = last.strftime("%m/%Y") + (f" (dato de hace {age_days}d)" if age_days > 45 else "")

    st.markdown(
        f'<div class="macro-card">'
        f'<div class="macro-label">{title}</div>'
        f'<div class="macro-val">{prefix}{val_fmt}{suffix}</div>'
        f'<div class="macro-sub">Último dato: {fecha_str}</div>'
        f'<div class="macro-sub" style="margin-top:8px;">'
        f'1M: {delta_html(m1)} · 1A: {delta_html(y1)}'
        f'</div></div>',
        unsafe_allow_html=True,
    )


# -----------------------------------------------------------
# VALOR REAL DOT PLOT (reemplaza las barras feas)
# -----------------------------------------------------------
def dot_plot_valor_real(data_dict, bench, titulo, is_credit=False):
    """
    data_dict: {"Activo": retorno_usd, ...}
    bench: benchmark %
    is_credit: para financiamiento, la interpretación se invierte
               (costo por encima del benchmark = malo → rojo).
    """
    # Filtrar Nones
    items = [(k, v) for k, v in data_dict.items() if v is not None]
    if not items:
        st.warning(f"Sin datos para {titulo}")
        return

    # Ordenar por rendimiento (asc) para que mejor quede arriba en horizontal
    items = sorted(items, key=lambda x: x[1])
    labels = [k for k, _ in items]
    values = [v for _, v in items]

    # Color según distancia al benchmark
    colors = []
    for v in values:
        if is_credit:
            # En crédito: v < bench es bueno (me cuesta menos que dólares quietos)
            good = v < bench
        else:
            good = v > bench
        colors.append("#10b981" if good else "#ef4444")

    fig = go.Figure()

    # Línea de distancia desde benchmark al punto
    for i, (lbl, v, c) in enumerate(zip(labels, values, colors)):
        fig.add_trace(go.Scatter(
            x=[bench, v],
            y=[lbl, lbl],
            mode="lines",
            line=dict(color=c, width=4),
            showlegend=False,
            hoverinfo="skip",
        ))

    # Puntos con los valores
    fig.add_trace(go.Scatter(
        x=values,
        y=labels,
        mode="markers+text",
        marker=dict(size=14, color=colors, line=dict(color="#ffffff", width=1.5)),
        text=[f"{v:+.1f}%" for v in values],
        textposition="middle right",
        textfont=dict(color="#f8fafc", size=12),
        showlegend=False,
        hovertemplate="<b>%{y}</b><br>Retorno USD: %{x:.2f}%<extra></extra>",
    ))

    # Línea vertical del benchmark
    fig.add_vline(
        x=bench,
        line=dict(color="#fbbf24", width=2, dash="dash"),
        annotation=dict(
            text=f"Benchmark: {bench:+.2f}%",
            showarrow=False,
            xanchor="left",
            yanchor="bottom",
            font=dict(color="#fbbf24", size=11),
            bgcolor="rgba(7, 9, 15, 0.8)",
        ),
        annotation_position="top",
    )

    # Padding del eje X
    all_x = values + [bench]
    x_min = min(all_x) - abs(max(all_x) - min(all_x)) * 0.15 - 2
    x_max = max(all_x) + abs(max(all_x) - min(all_x)) * 0.25 + 2

    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=10, r=50, t=20, b=30),
        height=max(250, 50 * len(labels) + 60),
        xaxis=dict(
            showgrid=True, gridcolor="rgba(30, 41, 59, 0.5)",
            zeroline=True, zerolinecolor="#334155", zerolinewidth=1,
            ticksuffix="%", range=[x_min, x_max],
        ),
        yaxis=dict(showgrid=False, tickfont=dict(size=13)),
    )

    st.plotly_chart(fig, use_container_width=True)


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
        color_fg = "#ef4444" if "Fear" in fng_class else "#10b981" if "Greed" in fng_class else "#f59e0b"
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

    # Noticias destacadas
    destacadas = get_json("destacadas_json", [])
    if destacadas:
        st.markdown('<div class="section-title">📰 NOTICIAS DESTACADAS</div>', unsafe_allow_html=True)
        for n in destacadas[:5]:
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
# TAB 2 - MACRO GLOBAL (placeholder, trabajo futuro)
# ===========================================================
with tab2:
    st.info("Pendiente: integración con FRED para tasas Fed/SELIC, yield curve, y CDS de países emergentes. Por ahora funciona si ya tenés la hoja DB_Macro cargada.")

    if not df_macro.empty:
        col1, col2 = st.columns(2)
        with col1:
            tasas_cols = [c for c in ["FEDFUNDS", "Tasa_SELIC_Brasil"] if c in df_macro.columns]
            if tasas_cols:
                st.markdown("**Tasas de política monetaria**")
                for c in tasas_cols:
                    df_macro[c] = pd.to_numeric(df_macro[c], errors="coerce")
                fig = px.line(df_macro, x="fecha", y=tasas_cols, template="plotly_dark",
                              color_discrete_sequence=["#38bdf8", "#34d399"])
                fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                                  margin=dict(l=0, r=0, t=30, b=0), legend_title="")
                st.plotly_chart(fig, use_container_width=True)

        with col2:
            if "T10Y2Y" in df_macro.columns:
                st.markdown("**Yield curve 10Y-2Y**")
                df_macro["T10Y2Y"] = pd.to_numeric(df_macro["T10Y2Y"], errors="coerce")
                fig = px.area(df_macro, x="fecha", y="T10Y2Y", template="plotly_dark",
                              color_discrete_sequence=["#f59e0b"])
                fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                                  margin=dict(l=0, r=0, t=30, b=0))
                st.plotly_chart(fig, use_container_width=True)


# ===========================================================
# TAB 3 - ARGENTINA
# ===========================================================
with tab3:
    st.markdown('<div class="section-title">🇦🇷 SEMÁFORO MACROECONÓMICO</div>', unsafe_allow_html=True)
    c1, c2, c3 = st.columns(3)
    with c1: render_macro_ipc()
    with c2: render_macro_generic("Actividad (EMAE)", "EMAE", suffix=" pts")
    with c3: render_macro_generic("Salario (RIPTE)", "RIPTE", prefix="$")

    st.markdown('<div class="section-title">🔍 ANÁLISIS DE VALOR REAL (BASE USD)</div>', unsafe_allow_html=True)

    intervalo = st.radio("Intervalo:", ["Mensual", "Anual"], horizontal=True, label_visibility="collapsed")

    # Benchmark del intervalo elegido
    bench_col = "bench_1m" if intervalo == "Mensual" else "bench_1a"
    bench = get_insight_float(bench_col, default=-4.3)

    # Rendimientos precalculados en el pipeline
    if intervalo == "Mensual":
        rendimientos = get_json("valor_real_1m_json", {})
    else:
        rendimientos = get_json("valor_real_1a_json", {})

    # Placeholders para activos sin datos propios (los fijos quedan claramente marcados)
    # TODO: reemplazar por datos reales cuando tengamos APIs
    if intervalo == "Mensual":
        rendimientos["m2 CABA (est.)"] = 1.2
        rendimientos["Plazo Fijo (est.)"] = -4.0
        rendimientos["Dólares quietos"] = 0.0
    else:
        rendimientos["m2 CABA (est.)"] = 5.5
        rendimientos["Plazo Fijo (est.)"] = -25.0
        rendimientos["Dólares quietos"] = 0.0

    # Costos de financiamiento (placeholders - TODO API BCRA)
    if intervalo == "Mensual":
        financiamiento = {
            "Adelanto Cta Cte (est.)": 15.2,
            "Tarjeta (est.)": 8.4,
            "Préstamo Personal (est.)": 5.1,
            "Hipotecario UVA (est.)": 2.0,
            "SGR Cheques (est.)": -4.5,
        }
    else:
        financiamiento = {
            "Adelanto Cta Cte (est.)": 85.0,
            "Tarjeta (est.)": 60.0,
            "Préstamo Personal (est.)": 45.0,
            "Hipotecario UVA (est.)": 12.0,
            "SGR Cheques (est.)": -10.0,
        }

    col_inv, col_fin = st.columns(2)
    with col_inv:
        st.subheader("💰 Inversiones en USD")
        st.caption(f"Benchmark: {bench:+.2f}% · Verde = supera el benchmark (ganó poder de compra)")
        dot_plot_valor_real(rendimientos, bench, "Inversiones", is_credit=False)

    with col_fin:
        st.subheader("💳 Costo de financiamiento en USD")
        st.caption(f"Benchmark: {bench:+.2f}% · Verde = costo menor al benchmark (el crédito fue negocio)")
        dot_plot_valor_real(financiamiento, bench, "Financiamiento", is_credit=True)

    # Comentario del LLM sobre el gráfico
    analisis = get_insight("analisis_valor_real", "")
    if analisis:
        st.markdown(
            f'<div class="ai-box" style="margin-top:20px;">'
            f'<div style="color:#38bdf8;font-weight:bold;margin-bottom:10px;font-size:14px;">'
            f'💡 QUÉ PASÓ ESTE {"MES" if intervalo == "Mensual" else "AÑO"}</div>'
            f'<div style="font-size:15px;color:#cbd5e1;line-height:1.7;">{analisis}</div>'
            f'</div>',
            unsafe_allow_html=True,
        )

    st.caption("💡 Los valores marcados con (est.) son estimaciones. Pendiente de conectar APIs de BCRA (tasas de préstamos y plazo fijo) y Reporte Inmobiliario (m2).")


# ===========================================================
# TABS 4, 5, 6 - placeholders
# ===========================================================
with tab4:
    st.info("Módulo de Expectativas (curvas de futuros Rofex, REM BCRA) en desarrollo.")

with tab5:
    st.info("Módulo Inmobiliario en desarrollo.")

with tab6:
    st.info("Módulo de Portafolio en desarrollo.")
