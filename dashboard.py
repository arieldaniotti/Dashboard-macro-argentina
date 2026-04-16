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
# 2. CONEXIÓN A LA BASE DE DATOS Y LIMPIEZA
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

    df_res = safe_read("DB_Resumen")
    df_ai = safe_read("DB_Insights")
    df_macro = safe_read("DB_Macro")
    df_hist = safe_read("DB_Historico")
    
    # 🌟 PULIDO: Convertimos explícitamente la columna fecha a formato "DateTime" 
    # para que Plotly dibuje el eje X de forma inteligente y no amontone las letras.
    if not df_macro.empty and 'fecha' in df_macro.columns:
        df_macro['fecha'] = pd.to_datetime(df_macro['fecha'], format='%d/%m/%Y', errors='coerce')
    if not df_hist.empty and 'fecha' in df_hist.columns:
        df_hist['fecha'] = pd.to_datetime(df_hist['fecha'], errors='coerce')
        
    return df_res, df_ai, df_macro, df_hist

df_resumen, df_insights, df_macro, df_hist = load_data()

# ==========================================
# 3. INTERFAZ VISUAL Y NAVEGACIÓN
# ==========================================
st.title("📊 Dashboard Económico Financiero")
st.caption("Actualización diaria mediante pipeline automático.")

tab1, tab2, tab3, tab4, tab5 = st.tabs(["📌 Resumen", "🌎 Macro Global", "🇦🇷 Argentina", "🏗️ Inmobiliario", "💼 Portafolio"])

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
        # Formateo de números grandes (agregamos coma de miles)
        val = float(row['Valor_Actual'].values[0])
        val_str = f"{val:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        # Limpieza para que si es 528.0 bps, quede bien
        if val_str.endswith(",00"): val_str = val_str[:-3]
        return val_str, row['Delta_1D_%'].values[0]
    except: return "N/A", 0

# 🌟 PULIDO: Función maestra para tunear todos los gráficos iguales
def aplicar_estilo_bloomberg(fig):
    fig.update_layout(
        xaxis_title="", # Saca la palabra "fecha"
        yaxis_title="", # Saca la palabra "value"
        legend_title="", # Saca la palabra "variable"
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1), # Leyenda arriba horizontal
        margin=dict(l=0, r=0, t=30, b=0), # Quita márgenes muertos
        hovermode="x unified" # Línea vertical interactiva al pasar el mouse
    )
    fig.update_traces(line=dict(width=2.5)) # Hace las líneas más gruesas
    return fig

# --- TAB 1: RESUMEN ---
with tab1:
    if not df_insights.empty:
        texto_ia = df_insights['Analisis_LLM'].iloc[-1]
        st.markdown(f'<div class="ai-box"><div class="ai-title">🤖 Análisis IA — Flash del Mercado</div><div class="ai-text">{texto_ia.replace(chr(10), "<br>")}</div></div>', unsafe_allow_html=True)

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
    with col4: st.markdown(render_metric("Riesgo País", f"{rp_v} bps", rp_d), unsafe_allow_html=True)
    with col5: st.markdown(render_metric("Brecha Cambiaria", f"{brecha_v}%", brecha_d), unsafe_allow_html=True)

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
            else:
                st.info("Datos de tasas no disponibles temporalmente.")
                
        with col2:
            st.markdown("**Yield Curve (10Y - 2Y)**")
            if 'T10Y2Y' in df_macro.columns:
                df_macro['T10Y2Y'] = pd.to_numeric(df_macro['T10Y2Y'], errors='coerce')
                fig_yield = px.area(df_macro, x='fecha', y='T10Y2Y', template='plotly_dark', color_discrete_sequence=['#f59e0b'])
                # En gráficos de área rellenamos con opacidad
                fig_yield.update_traces(fillcolor='rgba(245, 158, 11, 0.2)', line=dict(width=2)) 
                st.plotly_chart(aplicar_estilo_bloomberg(fig_yield), use_container_width=True)
            else:
                st.info("Datos de Yield Curve no disponibles temporalmente.")

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
        else:
            st.info("Datos de dólares no disponibles temporalmente.")

# --- TAB 4: INMOBILIARIO ---
with tab4:
    st.subheader("Mercado Inmobiliario")
    st.info("💡 Espacio reservado para el módulo de Real Estate. Aquí cruzaremos costo de construcción vs M2.")

# --- TAB 5: PORTAFOLIO ---
with tab5:
    st.subheader("Portafolio de Inversión")
    st.info("💡 Espacio reservado para Asset Allocation y seguimiento de cartera personal.")
