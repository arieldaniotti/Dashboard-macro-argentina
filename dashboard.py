import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import plotly.express as px

# ==========================================
# 1. CONFIGURACIÓN Y ESTILOS (Look Bloomberg)
# ==========================================
st.set_page_config(page_title="Dashboard Macro", layout="wide", initial_sidebar_state="collapsed")

st.markdown("""
    <style>
    .stApp { background-color: #07090f; color: #e2e8f0; font-family: sans-serif; }
    .metric-container { background-color: #0b0e18; border: 1px solid #141928; border-radius: 7px; padding: 15px; text-align: center; }
    .metric-label { font-size: 11px; color: #94a3b8; text-transform: uppercase; letter-spacing: 0.05em; }
    .metric-value { font-size: 24px; font-weight: 600; color: #f1f5f9; font-family: monospace; }
    .metric-delta-up { color: #34d399; font-size: 13px; }
    .metric-delta-down { color: #f87171; font-size: 13px; }
    .ai-box { background-color: #0a1525; border: 1px solid #1a3050; border-radius: 7px; padding: 20px; margin-bottom: 20px; }
    .ai-title { color: #38bdf8; font-size: 12px; font-weight: bold; text-transform: uppercase; margin-bottom: 10px; }
    .ai-text { color: #cbd5e1; font-size: 14px; line-height: 1.6; }
    </style>
""", unsafe_allow_html=True)

# ==========================================
# 2. CONEXIÓN A LA BASE DE DATOS
# ==========================================
@st.cache_data(ttl=3600)
def load_data():
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scopes)
    client = gspread.authorize(creds)
    sh = client.open("Dashboard Macro")
    
    # Leemos todas las pestañas
    df_res = pd.DataFrame(sh.worksheet("DB_Resumen").get_all_records())
    df_ai = pd.DataFrame(sh.worksheet("DB_Insights").get_all_records())
    df_macro = pd.DataFrame(sh.worksheet("DB_Macro").get_all_records())
    df_hist = pd.DataFrame(sh.worksheet("DB_Historico").get_all_records())
    
    return df_res, df_ai, df_macro, df_hist

df_resumen, df_insights, df_macro, df_hist = load_data()

# ==========================================
# 3. INTERFAZ VISUAL Y NAVEGACIÓN
# ==========================================
st.title("📊 Dashboard Económico Financiero")
st.caption("Actualización diaria mediante pipeline automático.")

# Creamos las 5 solapas
tab1, tab2, tab3, tab4, tab5 = st.tabs(["📌 Resumen", "🌎 Macro Global", "🇦🇷 Argentina", "🏗️ Inmobiliario", "💼 Portafolio"])

# Función para tarjetas de métricas
def render_metric(label, value, delta):
    color_class = "metric-delta-up" if float(delta) >= 0 else "metric-delta-down"
    arrow = "▲" if float(delta) >= 0 else "▼"
    return f"""
    <div class="metric-container">
        <div class="metric-label">{label}</div>
        <div class="metric-value">{value}</div>
        <div class="{color_class}">{arrow} {abs(float(delta))}%</div>
    </div>
    """

def get_val(df, metrica):
    try:
        row = df[df['Metrica'] == metrica]
        return row['Valor_Actual'].values[0], row['Delta_1D_%'].values[0]
    except:
        return "N/A", 0

# ------------------------------------------
# TAB 1: RESUMEN Y FLASH IA
# ------------------------------------------
with tab1:
    if not df_insights.empty:
        texto_ia = df_insights['Analisis_LLM'].iloc[-1]
        st.markdown(f"""
            <div class="ai-box">
                <div class="ai-title">🤖 Análisis IA — Flash del Mercado</div>
                <div class="ai-text">{texto_ia.replace(chr(10), '<br>')}</div>
            </div>
        """, unsafe_allow_html=True)

    st.subheader("Mercados")
    col1, col2, col3, col4, col5 = st.columns(5)
    
    sp_v, sp_d = get_val(df_resumen, 'SP500')
    oro_v, oro_d = get_val(df_resumen, 'Oro')
    btc_v, btc_d = get_val(df_resumen, 'BTC')
    rp_v, rp_d = get_val(df_resumen, 'Riesgo_Pais')
    brecha_v, brecha_d = get_val(df_resumen, 'Brecha_CCL')

    with col1: st.markdown(render_metric("S&P 500", f"{sp_v}", sp_d), unsafe_allow_html=True)
    with col2: st.markdown(render_metric("Oro", f"USD {oro_v}", oro_d), unsafe_allow_html=True)
    with col3: st.markdown(render_metric("Bitcoin", f"USD {btc_v}", btc_d), unsafe_allow_html=True)
    with col4: st.markdown(render_metric("Riesgo País", f"{rp_v}", rp_d), unsafe_allow_html=True)
    with col5: st.markdown(render_metric("Brecha Cambiaria", f"{brecha_v}%", brecha_d), unsafe_allow_html=True)

# ------------------------------------------
# TAB 2: MACRO GLOBAL
# ------------------------------------------
with tab2:
    st.subheader("Contexto Internacional y Tasas")
    col1, col2 = st.columns(2)
    
    if not df_macro.empty:
        with col1:
            st.markdown("**Evolución Tasa FED vs SELIC (Brasil)**")
            fig_tasas = px.line(df_macro, x='fecha', y=['FEDFUNDS', 'Tasa_SELIC_Brasil'], 
                                template='plotly_dark', color_discrete_sequence=['#38bdf8', '#34d399'])
            fig_tasas.update_layout(legend_title_text='Tasa')
            st.plotly_chart(fig_tasas, use_container_width=True)
            
        with col2:
            st.markdown("**Yield Curve (10Y - 2Y)**")
            fig_yield = px.area(df_macro, x='fecha', y='T10Y2Y', template='plotly_dark', color_discrete_sequence=['#f59e0b'])
            st.plotly_chart(fig_yield, use_container_width=True)

# ------------------------------------------
# TAB 3: ARGENTINA
# ------------------------------------------
with tab3:
    st.subheader("Variables Monetarias Locales")
    if not df_hist.empty:
        st.markdown("**Dólar Oficial vs Blue vs CCL**")
        fig_usd = px.line(df_hist, x='fecha', y=['USD_Oficial', 'USD_Blue', 'CCL'], 
                          template='plotly_dark', color_discrete_sequence=['#94a3b8', '#38bdf8', '#34d399'])
        fig_usd.update_layout(legend_title_text='Tipo de Cambio')
        st.plotly_chart(fig_usd, use_container_width=True)

# ------------------------------------------
# TAB 4: INMOBILIARIO
# ------------------------------------------
with tab4:
    st.subheader("Mercado Inmobiliario")
    st.info("💡 Espacio reservado para el módulo de Real Estate. Aquí conectaremos los datos de Costo de Construcción y Valor del M2.")

# ------------------------------------------
# TAB 5: PORTAFOLIO
# ------------------------------------------
with tab5:
    st.subheader("Portafolio de Inversión")
    st.info("💡 Espacio reservado para Asset Allocation, TIR y métricas de riesgo personalizadas.")
