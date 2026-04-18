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
    .d-up-bad { color: #ef4444; } .d-down-good { color: #10b981; }
    .ai-box { background-color: #0a1525; border: 1px solid #1a3050; border-radius: 8px; padding: 20px; margin-top: 20px; }
    .ai-text { color: #cbd5e1; font-size: 15px; line-height: 1.6; }
    </style>
""", unsafe_allow_html=True)

@st.cache_data(ttl=600)
def load_all():
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    try:
        creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scopes)
        client = gspread.authorize(creds)
        sh = client.open("Dashboard Macro")
        def read(n):
            data = sh.worksheet(n).get_all_values()
            df = pd.DataFrame(data[1:], columns=data[0])
            if 'fecha' in df.columns: df['fecha'] = pd.to_datetime(df['fecha'], errors='coerce')
            return df
        return read("DB_Insights"), read("DB_Historico")
    except: return pd.DataFrame(), pd.DataFrame()

df_ai, df_hist = load_all()

tab1, tab2, tab3 = st.tabs(["📌 Resumen", "🌎 Macro", "🇦🇷 AR Estrategia"])

with tab1:
    st.info("Pestaña Resumen. Ve a AR Estrategia para ver los nuevos gráficos de Valor Real.")

with tab3:
    st.markdown('<div class="section-title">🔍 Análisis de Valor Real (Base USD)</div>', unsafe_allow_html=True)
    
    # Extraemos el benchmark calculado por el robot (o usamos -4.3 si no está listo)
    try:
        benchmark_val = float(df_ai['Benchmark_USD'].iloc[-1])
    except:
        benchmark_val = -4.3

    col_inv, col_fin = st.columns(2)
    
    # 1. INVERSIONES
    with col_inv:
        st.markdown(f"### 💰 Inversiones — exceso sobre USD quietos")
        
        # Datos Nominales vs USD
        df_inv = pd.DataFrame({
            "Activo": ["Merval", "AL30", "S&P 500", "Dólares sin invertir", "Plazo Fijo"],
            "Retorno_Nominal": [25.4, 18.2, 8.5, 0.0, -8.4] 
        })
        
        # El 0% nominal ahora es el "Benchmark" invertido. 
        # Ganancia real = Retorno Nominal - Benchmark
        df_inv["Exceso_Real"] = df_inv["Retorno_Nominal"] - benchmark_val
        df_inv = df_inv.sort_values("Exceso_Real", ascending=True)
        
        colores_inv = ['#10b981' if v > 0 else '#ef4444' for v in df_inv["Exceso_Real"]]
        
        fig = go.Figure(go.Bar(
            x=df_inv["Exceso_Real"], y=df_inv["Activo"], orientation='h', marker_color=colores_inv,
            text=[f"{v:+.1f}%" for v in df_inv["Exceso_Real"]], textposition='auto', textfont=dict(color='white', weight='bold')
        ))
        # La línea del Cero es el EMPATE (Dólares quietos)
        fig.add_vline(x=0, line_width=2, line_color="#cbd5e1", line_dash="dash")
        fig.update_layout(template='plotly_dark', margin=dict(l=0, r=0, t=30, b=0), height=350, xaxis=dict(showgrid=False), yaxis=dict(showgrid=False))
        st.plotly_chart(fig, use_container_width=True)

    # 2. FINANCIAMIENTO
    with col_fin:
        st.markdown(f"### 💳 Financiamiento — costo sobre USD quietos")
        
        df_fin = pd.DataFrame({
            "Línea": ["Adelanto Cta Cte", "Tarjeta", "Préstamo Personal", "Hipotecario UVA", "SGR (Cheques)"],
            "Costo_Nominal": [15.2, 8.4, 5.1, 2.0, -4.5]
        })
        
        df_fin["Exceso_Costo"] = df_fin["Costo_Nominal"] - benchmark_val
        df_fin = df_fin.sort_values("Exceso_Costo", ascending=True)
        
        # Rojo si es costo caro (>0), Verde si se licúa (<0)
        colores_f = ['#ef4444' if v > 0 else '#10b981' for v in df_fin["Exceso_Costo"]]
        
        fig_f = go.Figure(go.Bar(
            x=df_fin["Exceso_Costo"], y=df_fin["Línea"], orientation='h', marker_color=colores_f,
            text=[f"{v:+.1f}%" for v in df_fin["Exceso_Costo"]], textposition='auto', textfont=dict(color='white', weight='bold')
        ))
        fig_f.add_vline(x=0, line_width=2, line_color="#cbd5e1", line_dash="dash")
        fig_f.update_layout(template='plotly_dark', margin=dict(l=0, r=0, t=30, b=0), height=350, xaxis=dict(showgrid=False), yaxis=dict(showgrid=False))
        st.plotly_chart(fig_f, use_container_width=True)

    # 3. EXPLICACIÓN LLM ("Doña Rosa")
    if not df_ai.empty:
        texto_completo = str(df_ai["Analisis_LLM"].iloc[-1])
        # Filtramos solo la parte de "EL DATO REAL" para mostrarla acá abajo
        if "💡 EL DATO REAL:" in texto_completo:
            explicacion_dona_rosa = texto_completo.split("💡 EL DATO REAL:")[1].strip()
            st.markdown(f"""
                <div class="ai-box">
                    <div style="color: #38bdf8; font-weight: bold; margin-bottom: 10px;">💡 LA EXPLICACIÓN DEL GRÁFICO (El Dato Real):</div>
                    <div class="ai-text">{explicacion_dona_rosa}</div>
                </div>
            """, unsafe_allow_html=True)
