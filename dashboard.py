import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import plotly.express as px
import requests

# ==========================================
# 1. CONFIGURACIÓN Y ESTILOS (Look Bloomberg)
# ==========================================
st.set_page_config(page_title="Dashboard Macro", layout="wide", initial_sidebar_state="collapsed")

st.markdown("""
    <style>
    .stApp { background-color: #07090f; color: #e2e8f0; font-family: sans-serif; }
    
    /* Tarjetas de Resumen */
    .metric-card { background-color: #0b0e18; border: 1px solid #1e293b; border-radius: 8px; padding: 20px; box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1); }
    .m-title { font-size: 16px; color: #cbd5e1; font-weight: 800; text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 10px; }
    .m-val { font-size: 28px; color: #f8fafc; font-weight: 700; font-family: 'Courier New', monospace; margin-bottom: 15px; }
    
    /* Porcentajes y variaciones */
    .m-deltas { display: flex; justify-content: space-between; font-size: 18px; font-weight: 800; padding-top: 12px; border-top: 1px solid #1e293b; }
    
    /* Lógica de colores financieros */
    .d-up-good { color: #10b981; }   /* Verde: Sube activo normal (ej. S&P) */
    .d-down-bad { color: #ef4444; }  /* Rojo: Baja activo normal (ej. S&P) */
    .d-up-bad { color: #ef4444; }    /* Rojo: Sube riesgo (ej. Riesgo País, Brecha) */
    .d-down-good { color: #10b981; } /* Verde: Baja riesgo (ej. Riesgo País, Brecha) */
    .d-flat { color: #64748b; }      /* Gris: Sin cambios */
    
    .d-label { color: #64748b; font-size: 13px; margin-right: 8px; font-weight: 600; }
    
    /* Caja IA */
    .ai-box { background-color: #0a1525; border: 1px solid #1a3050; border-radius: 8px; padding: 24px; height: 100%; margin-top: 0; }
    .ai-title { color: #38bdf8; font-size: 13px; font-weight: bold; text-transform: uppercase; margin-bottom: 15px; display: flex; align-items: center; gap: 8px;}
    .ai-text { color: #cbd5e1; font-size: 15px; line-height: 1.7; }
    </style>
""", unsafe_allow_html=True)

# ==========================================
# 2. CONEXIÓN A LA BASE DE DATOS Y APIS
# ==========================================
@st.cache_data(ttl=600)
def load_data():
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scopes)
    client = gspread.authorize(creds)
    sh = client.open("Dashboard Macro")
    
    def safe_read(sheet_name):
        try:
            ws = sh.worksheet(sheet_name)
            data = ws.get_all_values()
            if len(data) > 1:
                df = pd.DataFrame(data[1:], columns=data[0])
                df = df.loc[:, df.columns != '']
                df = df.loc[:, ~df.columns.duplicated()]
                
                # Formateo de fechas y eliminación de duplicados para cálculos limpios
                if 'fecha' in df.columns:
                    df['fecha'] = pd.to_datetime(df['fecha'], errors='coerce', format='mixed', dayfirst=True)
                    df = df.dropna(subset=['fecha']).drop_duplicates(subset=['fecha'], keep='last').sort_values('fecha')
                
                return df
            return pd.DataFrame()
        except: return pd.DataFrame()

    df_ai = safe_read("DB_Insights")
    df_macro = safe_read("DB_Macro")
    df_hist = safe_read("DB_Historico")
        
    return df_ai, df_macro, df_hist

df_insights, df_macro, df_hist = load_data()

@st.cache_data(ttl=3600)
def get_fear_and_greed():
    try:
        r = requests.get("https://api.alternative.me/fng/", timeout=5).json()
        return r['data'][0]['value'], r['data'][0]['value_classification']
    except: return "N/A", "Desconocido"

fng_val, fng_class = get_fear_and_greed()

# ==========================================
# 3. LÓGICA FINANCIERA DE VARIACIONES
# ==========================================
st.title("📊 Dashboard Económico Financiero")
st.caption("Actualización diaria automática. Diseño y analítica propietaria.")

def get_kpi_advanced(col_name):
    try:
        if col_name not in df_hist.columns: return "N/A", 0, 0, False
        
        df = df_hist[['fecha', col_name]].copy()
        df[col_name] = pd.to_numeric(df[col_name], errors='coerce')
        df = df.dropna(subset=[col_name]).sort_values('fecha')
        
        if df.empty: return "N/A", 0, 0, False
        
        actual = df[col_name].iloc[-1]
        prev_1d = df[col_name].iloc[-2] if len(df) > 1 else actual
        
        hace_1m = df['fecha'].iloc[-1] - pd.Timedelta(days=30)
        m1_val = df.loc[(df['fecha'] - hace_1m).abs().idxmin(), col_name]

        # Identificamos si es una métrica en puntos (donde suba = malo)
        is_points = col_name in ['Riesgo_Pais', 'Brecha_CCL']
        
        if is_points:
            d1_delta = actual - prev_1d
            m1_delta = actual - m1_val
        else:
            d1_delta = ((actual / prev_1d) - 1) * 100 if prev_1d else 0
            m1_delta = ((actual / m1_val) - 1) * 100 if m1_val else 0
            
        val_str = f"{actual:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        if val_str.endswith(",00"): val_str = val_str[:-3]
        
        return val_str, d1_delta, m1_delta, is_points
    except Exception as e: 
        return "N/A", 0, 0, False

def render_card_advanced(title, col_name, prefix="", suffix=""):
    val, d1, m1, is_points = get_kpi_advanced(col_name)
    
    inverted = col_name in ['Riesgo_Pais', 'Brecha_CCL']
    
    def get_style(delta, is_inv):
        if val == "N/A": return "N/A", "d-flat"
        if abs(delta) < 0.005: return "▬ 0.00", "d-flat"
        
        symbol = "▲" if delta > 0 else "▼"
        # Mostramos 'pp' para brecha, 'bps' para riesgo país, '%' para el resto
        label = f"{abs(delta):.2f}" + (" pp" if (is_points and col_name == 'Brecha_CCL') else " bps" if is_points else "%")
        
        if is_inv:
            color = "d-up-bad" if delta > 0 else "d-down-good"
        else:
            color = "d-up-good" if delta > 0 else "d-down-bad"
            
        return f"{symbol} {label}", color

    d1_label, d1_class = get_style(d1, inverted)
    m1_label, m1_class = get_style(m1, inverted)

    st.markdown(f"""
    <div class="metric-card">
        <div class="m-title">{title}</div>
        <div class="m-val">{prefix}{val}{suffix}</div>
        <div class="m-deltas">
            <span><span class="d-label">1D</span><span class="{d1_class}">{d1_label}</span></span>
            <span><span class="d-label">1M</span><span class="{m1_class}">{m1_label}</span></span>
        </div>
    </div>
    """, unsafe_allow_html=True)

def aplicar_estilo_bloomberg(fig):
    fig.update_layout(
        xaxis_title="", yaxis_title="", legend_title="",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(l=0, r=0, t=30, b=0), hovermode="x unified"
    )
    fig.update_traces(line=dict(width=2.5))
    return fig

# ==========================================
# 4. SOLAPAS (TABS)
# ==========================================
tab1, tab2, tab3, tab4, tab5 = st.tabs(["📌 Resumen", "🌎 Macro Global", "🇦🇷 Argentina", "🏗️ Inmobiliario", "💼 Portafolio"])

# --- TAB 1: RESUMEN ---
with tab1:
    st.subheader("Panorama de Mercado")
    
    c1, c2, c3, c4 = st.columns(4)
    with c1: render_card_advanced("S&P 500", "SP500")
    with c2: render_card_advanced("Merval", "Merval")
    with c3: render_card_advanced("Oro", "Oro", prefix="USD ")
    with c4: render_card_advanced("Brent", "Brent", prefix="USD ")
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    c5, c6, c7, c8 = st.columns(4)
    with c5: render_card_advanced("Dólar Oficial", "USD_Oficial", prefix="$")
    with c6: render_card_advanced("Riesgo País", "Riesgo_Pais", suffix=" bps")
    with c7: render_card_advanced("Brecha CCL", "Brecha_CCL", suffix="%")
    with c8: render_card_advanced("Bitcoin", "BTC", prefix="USD ")

    st.markdown("<br>", unsafe_allow_html=True)
    col_fg, col_ia = st.columns([1, 3])
    
    with col_fg:
        color_fg = "#ef4444" if "Fear" in fng_class else "#10b981" if "Greed" in fng_class else "#f59e0b"
        st.markdown(f"""
        <div class="metric-card" style="text-align: center; height: 100%; display: flex; flex-direction: column; justify-content: center; padding: 24px;">
            <div class="m-title" style="margin-bottom: 15px;">Índice Miedo y Codicia</div>
            <div class="m-val" style="font-size: 38px; color: {color_fg};">{fng_val}</div>
            <div style="color: {color_fg}; font-weight: bold; font-size: 14px;">{fng_class.upper()}</div>
        </div>
        """, unsafe_allow_html=True)

    with col_ia:
        if not df_insights.empty:
            texto_ia = str(df_insights['Analisis_LLM'].iloc[-1]).replace(chr(10), '<br>')
            st.markdown(f"""
                <div class="ai-box">
                    <div class="ai-title">🤖 Insight IA — Análisis de Cierre</div>
                    <div class="ai-text">{texto_ia}</div>
                </div>
            """, unsafe_allow_html=True)

# --- TAB 2: MACRO GLOBAL ---
with tab2:
    st.subheader("Contexto Internacional y Tasas")
    col1, col2 = st.columns(2)
    if not df_macro.empty:
        with col1:
            st.markdown("**Evolución Tasa FED vs SELIC (Brasil)**")
            tasas_cols = [c for c in ['FEDFUNDS', 'Tasa_SELIC_Brasil'] if c in df_macro.columns]
            if tasas_cols:
                for c in tasas_cols: df_macro[c] = pd.to_numeric(df_macro[c], errors='coerce')
                fig_tasas = px.line(df_macro, x='fecha', y=tasas_cols, template='plotly_dark', color_discrete_sequence=['#38bdf8', '#34d399'])
                st.plotly_chart(aplicar_estilo_bloomberg(fig_tasas), use_container_width=True)
            else: st.info("Datos no disponibles.")
                
        with col2:
            st.markdown("**Yield Curve (10Y - 2Y)**")
            if 'T10Y2Y' in df_macro.columns:
                df_macro['T10Y2Y'] = pd.to_numeric(df_macro['T10Y2Y'], errors='coerce')
                fig_yield = px.area(df_macro, x='fecha', y='T10Y2Y', template='plotly_dark', color_discrete_sequence=['#f59e0b'])
                fig_yield.update_traces(fillcolor='rgba(245, 158, 11, 0.2)', line=dict(width=2)) 
                st.plotly_chart(aplicar_estilo_bloomberg(fig_yield), use_container_width=True)
            else: st.info("Datos no disponibles.")

# --- TAB 3: ARGENTINA ---
with tab3:
    st.subheader("Variables Monetarias Locales")
    if not df_hist.empty:
        st.markdown("**Dólar Oficial vs Blue vs CCL**")
        usd_cols = [c for c in ['USD_Oficial', 'USD_Blue', 'CCL'] if c in df_hist.columns]
        if usd_cols:
            for c in usd_cols: df_hist[c] = pd.to_numeric(df_hist[c], errors='coerce')
            fig_usd = px.line(df_hist, x='fecha', y=usd_cols, template='plotly_dark', color_discrete_sequence=['#94a3b8', '#38bdf8', '#34d399'])
            st.plotly_chart(aplicar_estilo_bloomberg(fig_usd), use_container_width=True)
        else: st.info("Datos no disponibles.")

# --- TAB 4: INMOBILIARIO ---
with tab4:
    st.subheader("Mercado Inmobiliario")
    st.info("💡 Espacio reservado para el módulo de Real Estate.")

# --- TAB 5: PORTAFOLIO ---
with tab5:
    st.subheader("Portafolio de Inversión")
    st.info("💡 Espacio reservado para Asset Allocation.")
