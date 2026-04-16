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
    
    /* Nuevas Tarjetas de Resumen */
    .metric-card { background-color: #0b0e18; border: 1px solid #1e293b; border-radius: 8px; padding: 16px; box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1); }
    .m-title { font-size: 13px; color: #94a3b8; font-weight: 700; text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 8px; }
    .m-val { font-size: 24px; color: #f8fafc; font-weight: 700; font-family: 'Courier New', monospace; margin-bottom: 12px; }
    .m-deltas { display: flex; justify-content: space-between; font-size: 13px; font-weight: 600; padding-top: 10px; border-top: 1px solid #1e293b; }
    .d-up { color: #10b981; } /* Verde Esmeralda */
    .d-down { color: #ef4444; } /* Rojo */
    .d-flat { color: #64748b; } /* Gris */
    .d-label { color: #475569; font-size: 11px; margin-right: 6px; }
    
    /* Caja de Inteligencia Artificial */
    .ai-box { background-color: #0a1525; border: 1px solid #1a3050; border-radius: 8px; padding: 24px; margin-top: 30px; }
    .ai-title { color: #38bdf8; font-size: 13px; font-weight: bold; text-transform: uppercase; margin-bottom: 15px; display: flex; align-items: center; gap: 8px;}
    .ai-text { color: #cbd5e1; font-size: 15px; line-height: 1.7; }
    </style>
""", unsafe_allow_html=True)

# ==========================================
# 2. CONEXIÓN A LA BASE DE DATOS Y APIS
# ==========================================
@st.cache_data(ttl=3600)
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
                return df
            return pd.DataFrame()
        except: return pd.DataFrame()

    df_ai = safe_read("DB_Insights")
    df_macro = safe_read("DB_Macro")
    df_hist = safe_read("DB_Historico")
    
    if not df_macro.empty and 'fecha' in df_macro.columns:
        df_macro['fecha'] = pd.to_datetime(df_macro['fecha'], format='%d/%m/%Y', errors='coerce')
    if not df_hist.empty and 'fecha' in df_hist.columns:
        df_hist['fecha'] = pd.to_datetime(df_hist['fecha'], errors='coerce')
        
    return df_ai, df_macro, df_hist

df_insights, df_macro, df_hist = load_data()

# Fetch del Índice Fear & Greed en vivo (Crypto proxy para el mercado global)
@st.cache_data(ttl=3600)
def get_fear_and_greed():
    try:
        r = requests.get("https://api.alternative.me/fng/", timeout=5).json()
        return r['data'][0]['value'], r['data'][0]['value_classification']
    except: return "N/A", "Desconocido"

fng_val, fng_class = get_fear_and_greed()

# ==========================================
# 3. FUNCIONES DE CÁLCULO Y RENDERIZADO VISUAL
# ==========================================
st.title("📊 Dashboard Económico Financiero")
st.caption("Actualización diaria automática. Diseño y analítica propietaria.")

# Función maestra que extrae el valor actual, delta diario y delta mensual desde la historia bruta
# Función maestra que extrae el valor actual, delta diario y mensual
def get_kpi(col_name):
    try:
        # Verificamos que la columna exista
        if col_name not in df_hist.columns: return "N/A", 0, 0
        
        # Aislamos las columnas que necesitamos
        df = df_hist[['fecha', col_name]].copy()
        
        # Convertimos texto a número y ELIMINAMOS las filas fantasmas/vacías
        df[col_name] = pd.to_numeric(df[col_name], errors='coerce')
        df = df.dropna(subset=['fecha', col_name]).sort_values('fecha')
        
        if df.empty: return "N/A", 0, 0
        
        # Último valor y día anterior
        actual = df[col_name].iloc[-1]
        d1 = df[col_name].iloc[-2] if len(df)>1 else actual
        
        # Valor de hace 1 mes cronológico
        hace_1m = df['fecha'].iloc[-1] - pd.Timedelta(days=30)
        idx_1m = (df['fecha'] - hace_1m).abs().idxmin()
        m1 = df.loc[idx_1m, col_name]
        
        # Porcentajes
        d1_pct = ((actual/d1)-1)*100 if d1 and d1 != 0 else 0
        m1_pct = ((actual/m1)-1)*100 if m1 and m1 != 0 else 0
        
        # Formateo (ej. 1234.56 -> 1.234,56)
        val_str = f"{actual:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        if val_str.endswith(",00"): val_str = val_str[:-3]
        
        return val_str, d1_pct, m1_pct
    except Exception as e: 
        return "N/A", 0, 0

# Función HTML para dibujar la tarjeta
def render_card(title, val, d1, d1m, prefix="", suffix=""):
    def f_delta(v):
        try:
            if v > 0: return f"▲ {v:.2f}%", "d-up"
            if v < 0: return f"▼ {abs(v):.2f}%", "d-down"
            return "▬ 0.00%", "d-flat"
        except: return "N/A", "d-flat"
    
    d1_s, d1_c = f_delta(d1)
    d1m_s, d1m_c = f_delta(d1m)
    
    return f"""
    <div class="metric-card">
        <div class="m-title">{title}</div>
        <div class="m-val">{prefix}{val}{suffix}</div>
        <div class="m-deltas">
            <span><span class="d-label">1D</span><span class="{d1_c}">{d1_s}</span></span>
            <span><span class="d-label">1M</span><span class="{d1m_c}">{d1m_s}</span></span>
        </div>
    </div>
    """

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

# --- TAB 1: RESUMEN (DISEÑO NUEVO) ---
with tab1:
    st.subheader("Panorama de Mercado")
    
    # Extraemos todos los datos matemáticos
    sp_v, sp_d1, sp_d1m = get_kpi('SP500')
    merv_v, merv_d1, merv_d1m = get_kpi('Merval')
    ofi_v, ofi_d1, ofi_d1m = get_kpi('USD_Oficial')
    oro_v, oro_d1, oro_d1m = get_kpi('Oro')
    
    brent_v, brent_d1, brent_d1m = get_kpi('Brent')
    btc_v, btc_d1, btc_d1m = get_kpi('BTC')
    rp_v, rp_d1, rp_d1m = get_kpi('Riesgo_Pais')
    brecha_v, brecha_d1, brecha_d1m = get_kpi('Brecha_CCL')

    # Fila 1: Global y Merval
    col1, col2, col3, col4 = st.columns(4)
    with col1: st.markdown(render_card("S&P 500", sp_v, sp_d1, sp_d1m), unsafe_allow_html=True)
    with col2: st.markdown(render_card("Merval", merv_v, merv_d1, merv_d1m), unsafe_allow_html=True)
    with col3: st.markdown(render_card("Oro", oro_v, oro_d1, oro_d1m, prefix="USD "), unsafe_allow_html=True)
    with col4: st.markdown(render_card("Brent", brent_v, brent_d1, brent_d1m, prefix="USD "), unsafe_allow_html=True)
    
    st.markdown("<br>", unsafe_allow_html=True) # Espaciador
    
    # Fila 2: Argentina y Cripto
    col5, col6, col7, col8 = st.columns(4)
    with col5: st.markdown(render_card("Dólar Oficial", ofi_v, ofi_d1, ofi_d1m, prefix="$"), unsafe_allow_html=True)
    with col6: st.markdown(render_card("Riesgo País", rp_v, rp_d1, rp_d1m, suffix=" bps"), unsafe_allow_html=True)
    with col7: st.markdown(render_card("Brecha CCL", brecha_v, brecha_d1, brecha_d1m, suffix="%"), unsafe_allow_html=True)
    with col8: st.markdown(render_card("Bitcoin", btc_v, btc_d1, btc_d1m, prefix="USD "), unsafe_allow_html=True)

    # Fila 3: Fear & Greed y Análisis IA al final
    st.markdown("<br>", unsafe_allow_html=True)
    
    col_fg, col_ia = st.columns([1, 3]) # El IA ocupa más espacio
    
    with col_fg:
        color_fg = "#ef4444" if "Fear" in fng_class else "#10b981" if "Greed" in fng_class else "#f59e0b"
        st.markdown(f"""
        <div class="metric-card" style="text-align: center; height: 100%; display: flex; flex-direction: column; justify-content: center;">
            <div class="m-title" style="margin-bottom: 15px;">Índice Miedo y Codicia</div>
            <div class="m-val" style="font-size: 38px; color: {color_fg};">{fng_val}</div>
            <div style="color: {color_fg}; font-weight: bold; font-size: 14px;">{fng_class.upper()}</div>
        </div>
        """, unsafe_allow_html=True)

    with col_ia:
        if not df_insights.empty:
            texto_ia = df_insights['Analisis_LLM'].iloc[-1].replace(chr(10), '<br>')
            st.markdown(f"""
                <div class="ai-box" style="margin-top: 0; height: 100%;">
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
