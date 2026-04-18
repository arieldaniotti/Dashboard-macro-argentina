import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import plotly.express as px
import plotly.graph_objects as go
import requests
from datetime import timedelta

# 1. ESTILOS Y RESPONSIVE
st.set_page_config(page_title="Terminal Macro", layout="wide", initial_sidebar_state="collapsed")
st.markdown("""
    <style>
    .stApp { background-color: #07090f; color: #e2e8f0; font-family: sans-serif; }
    .section-title { font-size: 20px; color: #38bdf8; font-weight: 800; text-transform: uppercase; border-bottom: 1px solid #1e293b; padding-bottom: 8px; margin: 20px 0;}
    .metric-card { background-color: #0b0e18; border: 1px solid #1e293b; border-radius: 8px; padding: 15px; }
    .m-title { font-size: 14px; color: #94a3b8; font-weight: bold; text-transform: uppercase; }
    .m-val { font-size: 24px; font-weight: 700; font-family: monospace; margin: 10px 0; }
    .m-deltas { display: flex; justify-content: space-between; font-size: 12px; border-top: 1px solid #1e293b; padding-top: 8px; }
    .d-up-good { color: #10b981; } .d-down-bad { color: #ef4444; }
    .d-up-bad { color: #ef4444; } .d-down-good { color: #10b981; }
    
    @media (max-width: 768px) {
        .m-val { font-size: 18px; }
        .section-title { font-size: 16px; }
    }
    </style>
""", unsafe_allow_html=True)

# 2. CARGA DE DATOS
@st.cache_data(ttl=600)
def load_all():
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scopes)
    client = gspread.authorize(creds)
    sh = client.open("Dashboard Macro")
    
    def read(n):
        data = sh.worksheet(n).get_all_values()
        df = pd.DataFrame(data[1:], columns=data[0])
        if 'fecha' in df.columns: df['fecha'] = pd.to_datetime(df['fecha'])
        return df

    return read("DB_Insights"), read("DB_Historico")

df_ai, df_hist = load_all()

def render_card_3d(title, col, prefix="", suffix=""):
    try:
        df = df_hist[['fecha', col]].dropna()
        df[col] = pd.to_numeric(df[col])
        actual = df[col].iloc[-1]
        
        # Variación 1M
        hace_1m = df['fecha'].iloc[-1] - timedelta(days=30)
        val_1m = df[df['fecha'] <= hace_1m].iloc[-1][col]
        delta_1m = ((actual / val_1m) - 1) * 100
        
        color = "d-up-bad" if (delta_1m > 0 and col in ['Riesgo_Pais', 'Brecha_CCL']) else "d-up-good"
        sym = "▲" if delta_1m > 0 else "▼"

        st.markdown(f"""
        <div class="metric-card">
            <div class="m-title">{title}</div>
            <div class="m-val">{prefix}{actual:,.0f}{suffix}</div>
            <div class="m-deltas">
                <span class="d-label">MENSUAL</span>
                <span class="{color}">{sym} {abs(delta_1m):.1f}%</span>
            </div>
        </div>
        """, unsafe_allow_html=True)
    except: st.error(f"Error en {col}")

# 3. INTERFAZ
tab1, tab2, tab3, tab4 = st.tabs(["📌 Resumen", "🌎 Macro", "🇦🇷 Estrategia", "🔮 Expectativas"])

with tab1:
    st.markdown('<div class="section-title">Monitor de Mercado</div>', unsafe_allow_html=True)
    c1, c2, c3, c4 = st.columns(4)
    with c1: render_card_3d("S&P 500", "SP500")
    with c2: render_card_3d("Merval", "Merval")
    with c3: render_card_3d("Riesgo País", "Riesgo_Pais", suffix=" bps")
    with c4: render_card_3d("Brecha CCL", "Brecha_CCL", suffix="%")
    
    if not df_ai.empty:
        st.markdown(f'<div class="ai-box"><div class="ai-title">🤖 FLASH MARKET</div><div class="ai-text">{df_ai["Analisis_LLM"].iloc[-1]}</div></div>', unsafe_allow_html=True)

with tab3:
    st.markdown('<div class="section-title">🔍 Análisis de Valor Real (Base USD)</div>', unsafe_allow_html=True)
    
    # --- CÁLCULO DEL BENCHMARK ---
    # Simulamos el punto de equilibrio (Inflación en USD)
    # En el futuro esto vendrá de (IPC / Devaluación)
    benchmark_val = -4.3  # El país se abarató 4.3% (ejemplo usuario)

    col_inv, col_fin = st.columns(2)
    
    with col_inv:
        st.subheader("💰 Inversiones vs Encarecimiento")
        df_inv = pd.DataFrame({
            "Activo": ["Merval", "AL30", "S&P 500", "Dólares sin invertir", "Plazo Fijo"],
            "Retorno": [25.4, 18.2, 8.5, 0.0, -8.4] # Retorno nominal en USD
        }).sort_values("Retorno")
        
        # Color: Verde si supera al Benchmark (línea punteada)
        colores = ['#10b981' if v > benchmark_val else '#ef4444' for v in df_inv["Retorno"]]
        
        fig = go.Figure(go.Bar(x=df_inv["Retorno"], y=df_inv["Activo"], orientation='h', marker_color=colores))
        fig.add_vline(x=benchmark_val, line_dash="dash", line_color="#cbd5e1", 
                     annotation_text=f"Benchmark: {benchmark_val}%", annotation_position="top")
        fig.update_layout(template='plotly_dark', margin=dict(l=0, r=10, t=20, b=0), height=300)
        st.plotly_chart(fig, use_container_width=True)

    with col_fin:
        st.subheader("💳 Costo de Financiamiento Real")
        df_fin = pd.DataFrame({
            "Línea": ["Adelanto Cta Cte", "Tarjeta", "Préstamo Personal", "Hipotecario UVA", "SGR (Cheques)"],
            "Costo": [15.2, 8.4, 5.1, 2.0, -4.5]
        }).sort_values("Costo")
        
        # Color: Verde si es menor al Benchmark (se licúa)
        colores_f = ['#ef4444' if v > benchmark_val else '#10b981' for v in df_fin["Costo"]]
        
        fig_f = go.Figure(go.Bar(x=df_fin["Costo"], y=df_fin["Línea"], orientation='h', marker_color=colores_f))
        fig_f.add_vline(x=benchmark_val, line_dash="dash", line_color="#cbd5e1", 
                       annotation_text=f"Benchmark: {benchmark_val}%", annotation_position="top")
        fig_f.update_layout(template='plotly_dark', margin=dict(l=0, r=10, t=20, b=0), height=300)
        st.plotly_chart(fig_f, use_container_width=True)
