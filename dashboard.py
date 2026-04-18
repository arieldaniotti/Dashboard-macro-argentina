import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import plotly.graph_objects as go
from datetime import timedelta

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
    .ai-box { background-color: #0a1525; border: 1px solid #1a3050; border-radius: 8px; padding: 20px; margin-top: 20px; }
    .ai-text { color: #cbd5e1; font-size: 15px; line-height: 1.6; }
    </style>
""", unsafe_allow_html=True)

@st.cache_data(ttl=600)
def load_all():
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scopes)
    sh = gspread.authorize(creds).open("Dashboard Macro")
    def read(n):
        data = sh.worksheet(n).get_all_values()
        df = pd.DataFrame(data[1:], columns=data[0])
        if 'fecha' in df.columns: df['fecha'] = pd.to_datetime(df['fecha'], errors='coerce')
        return df
    return read("DB_Insights"), read("DB_Historico")

df_ai, df_hist = load_all()

def render_kpi(title, col, suffix=""):
    df = df_hist[['fecha', col]].dropna()
    df[col] = pd.to_numeric(df[col])
    val = df[col].iloc[-1]
    ant = df[df['fecha'] <= (df['fecha'].iloc[-1] - timedelta(days=30))].iloc[-1][col]
    delta = ((val/ant)-1)*100
    color = "d-up-bad" if (delta > 0 and col in ['Riesgo_Pais', 'Brecha_CCL']) else "d-up-good"
    st.markdown(f'<div class="metric-card"><div class="m-title">{title}</div><div class="m-val">{val:,.0f}{suffix}</div><div class="m-deltas"><span>1M</span><span class="{color}">{"▲" if delta>0 else "▼"} {abs(delta):.1f}%</span></div></div>', unsafe_allow_html=True)

tab1, tab2, tab3 = st.tabs(["📌 Resumen", "🌎 Macro", "🇦🇷 AR Estrategia"])

with tab1:
    st.markdown('<div class="section-title">Monitor en Tiempo Real</div>', unsafe_allow_html=True)
    c1, c2, c3, c4 = st.columns(4)
    with c1: render_kpi("S&P 500", "SP500")
    with c2: render_kpi("Merval", "Merval")
    with c3: render_kpi("Riesgo País", "Riesgo_Pais", suffix=" bps")
    with c4: render_kpi("Brecha CCL", "Brecha_CCL", suffix="%")
    if not df_ai.empty:
        st.markdown(f'<div class="ai-box"><div style="color:#38bdf8;font-weight:bold;margin-bottom:10px;">🤖 FLASH MARKET</div><div class="ai-text">{df_ai["Analisis_LLM"].iloc[-1].split("💡 EL DATO REAL:")[0]}</div></div>', unsafe_allow_html=True)

with tab3:
    st.markdown('<div class="section-title">🔍 ANÁLISIS DE VALOR REAL (BASE USD)</div>', unsafe_allow_html=True)
    
    intervalo = st.radio("Seleccionar Intervalo:", ["Mensual", "Anual"], horizontal=True)
    bench = float(df_ai['Bench_1M' if intervalo=="Mensual" else 'Bench_1A'].iloc[-1])

    col_inv, col_fin = st.columns(2)
    
    with col_inv:
        st.subheader("💰 Inversiones medidas en USD")
        # Datos Reales y Simulados (m2 estimado)
        df_inv = pd.DataFrame({
            "Activo": ["Merval", "AL30", "S&P 500", "Dólares sin invertir", "m2 Venta (CABA)", "Plazo Fijo"],
            "Retorno": [25.4, 18.2, 8.5, 0.0, 3.2, -8.4] if intervalo=="Mensual" else [140.0, 95.0, 22.0, 0.0, 5.5, -25.0]
        })
        df_inv["Neta"] = df_inv["Retorno"] - bench
        df_inv = df_inv.sort_values("Neta")
        fig = go.Figure(go.Bar(x=df_inv["Neta"], y=df_inv["Activo"], orientation='h', marker_color=['#10b981' if v > 0 else '#ef4444' for v in df_inv["Neta"]], text=[f"{v:+.1f}%" for v in df_inv["Neta"]], textposition='outside'))
        fig.add_vline(x=0, line_width=2, line_color="#94a3b8", annotation_text=" EMPATE (USD Quietos)")
        fig.update_layout(template='plotly_dark', margin=dict(l=0, r=40, t=10, b=0), height=350)
        st.plotly_chart(fig, use_container_width=True)

    with col_fin:
        st.subheader("💳 Costo de financiamiento en USD")
        df_fin = pd.DataFrame({
            "Línea": ["Adelanto Cta Cte", "Tarjeta", "Préstamo Personal", "Hipotecario UVA", "SGR (Cheques)"],
            "Costo": [15.2, 8.4, 5.1, 2.0, -4.5] if intervalo=="Mensual" else [85.0, 60.0, 45.0, 12.0, -10.0]
        })
        df_fin["Neta"] = df_fin["Costo"] - bench
        df_fin = df_fin.sort_values("Neta")
        fig_f = go.Figure(go.Bar(x=df_fin["Neta"], y=df_fin["Línea"], orientation='h', marker_color=['#ef4444' if v > 0 else '#10b981' for v in df_fin["Neta"]], text=[f"{v:+.1f}%" for v in df_fin["Neta"]], textposition='outside'))
        fig_f.add_vline(x=0, line_width=2, line_color="#94a3b8", annotation_text=" EMPATE")
        fig_f.update_layout(template='plotly_dark', margin=dict(l=0, r=40, t=10, b=0), height=350)
        st.plotly_chart(fig_f, use_container_width=True)

    if not df_ai.empty and "💡 EL DATO REAL:" in df_ai["Analisis_LLM"].iloc[-1]:
        st.markdown(f'<div class="ai-box"><div style="color:#38bdf8;font-weight:bold;margin-bottom:10px;">💡 LA EXPLICACIÓN:</div><div class="ai-text">{df_ai["Analisis_LLM"].iloc[-1].split("💡 EL DATO REAL:")[1]}</div></div>', unsafe_allow_html=True)
