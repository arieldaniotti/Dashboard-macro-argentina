import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import plotly.express as px
import plotly.graph_objects as go
import requests
from datetime import timedelta

# ==========================================
# 1. CONFIGURACIÓN Y ESTILOS
# ==========================================
st.set_page_config(page_title="Terminal Macro", layout="wide", initial_sidebar_state="collapsed")
st.markdown("""
    <style>
    .stApp { background-color: #07090f; color: #e2e8f0; font-family: sans-serif; }
    .section-title { font-size: 20px; color: #38bdf8; font-weight: 800; text-transform: uppercase; border-bottom: 1px solid #1e293b; padding-bottom: 8px; margin: 20px 0;}
    .metric-card { background-color: #0b0e18; border: 1px solid #1e293b; border-radius: 8px; padding: 15px; box-shadow: 0 4px 6px -1px rgba(0,0,0,0.1); }
    .m-title { font-size: 14px; color: #cbd5e1; font-weight: bold; text-transform: uppercase; }
    .m-val { font-size: 24px; font-weight: 700; font-family: 'Courier New', monospace; margin: 10px 0; color: #f8fafc;}
    .m-deltas { display: flex; justify-content: space-between; font-size: 13px; border-top: 1px solid #1e293b; padding-top: 8px; font-weight: 600;}
    .d-up-good { color: #10b981; } .d-down-bad { color: #ef4444; }
    .d-up-bad { color: #ef4444; } .d-down-good { color: #10b981; } .d-flat { color: #64748b; }
    .d-label { color: #64748b; font-size: 11px; margin-right: 4px; }
    .ai-box { background-color: #0a1525; border: 1px solid #1a3050; border-radius: 8px; padding: 20px; height: 100%;}
    .ai-title { color: #38bdf8; font-size: 14px; font-weight: bold; text-transform: uppercase; margin-bottom: 15px;}
    .ai-text { color: #cbd5e1; font-size: 15px; line-height: 1.6; }
    </style>
""", unsafe_allow_html=True)

# ==========================================
# 2. FUNCIONES Y CONEXIÓN
# ==========================================
@st.cache_data(ttl=600)
def load_all():
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scopes)
    sh = gspread.authorize(creds).open("Dashboard Macro")
    def read(n):
        try:
            data = sh.worksheet(n).get_all_values()
            df = pd.DataFrame(data[1:], columns=data[0])
            if 'fecha' in df.columns: df['fecha'] = pd.to_datetime(df['fecha'], errors='coerce')
            return df
        except: return pd.DataFrame()
    return read("DB_Insights"), read("DB_Historico")

df_ai, df_hist = load_all()

@st.cache_data(ttl=3600)
def get_fear_greed():
    try: 
        r = requests.get("https://api.alternative.me/fng/", timeout=5).json()
        return r['data'][0]['value'], r['data'][0]['value_classification']
    except: return "N/A", "-"

fng_val, fng_class = get_fear_greed()

def aplicar_estilo_bloomberg(fig):
    fig.update_layout(
        xaxis_title="", yaxis_title="", legend_title="",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(l=0, r=0, t=30, b=0), hovermode="x unified",
        paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
        xaxis=dict(showgrid=True, gridcolor='#1e293b'),
        yaxis=dict(showgrid=True, gridcolor='#1e293b')
    )
    fig.update_traces(line=dict(width=2.5))
    return fig

def render_kpi(title, col, prefix="", suffix="", is_inverted=False):
    try:
        df = df_hist[['fecha', col]].dropna()
        df[col] = pd.to_numeric(df[col])
        val = df[col].iloc[-1]
        
        val_1d = df[col].iloc[-2] if len(df)>1 else val
        hace_1m = df['fecha'].iloc[-1] - timedelta(days=30)
        ant_1m_df = df[df['fecha'] <= hace_1m]
        val_1m = ant_1m_df.iloc[-1][col] if not ant_1m_df.empty else df[col].iloc[0]
        hace_1a = df['fecha'].iloc[-1] - timedelta(days=365)
        ant_1a_df = df[df['fecha'] <= hace_1a]
        val_1a = ant_1a_df.iloc[-1][col] if not ant_1a_df.empty else df[col].iloc[0]

        is_points = col in ['Riesgo_Pais', 'Brecha_CCL']
        unidad = "bps" if col == "Riesgo_Pais" else "%" if is_points else "%"
        
        if is_points:
            d1, m1, y1 = val - val_1d, val - val_1m, val - val_1a
        else:
            d1 = ((val/val_1d)-1)*100 if val_1d else 0
            m1 = ((val/val_1m)-1)*100 if val_1m else 0
            y1 = ((val/val_1a)-1)*100 if val_1a else 0

        def get_c(delta):
            if abs(delta) < 0.05: return f"▬ 0.0{unidad}", "d-flat"
            sym = "▲" if delta > 0 else "▼"
            lbl = f"{abs(delta):.1f}{unidad}"
            if is_inverted: clr = "d-up-bad" if delta > 0 else "d-down-good"
            else: clr = "d-up-good" if delta > 0 else "d-down-bad"
            return f"{sym} {lbl}", clr

        d1_l, d1_c = get_c(d1)
        m1_l, m1_c = get_c(m1)
        y1_l, y1_c = get_c(y1)
        
        fmt_val = f"{val:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        if fmt_val.endswith(",00"): fmt_val = fmt_val[:-3]

        st.markdown(f"""
        <div class="metric-card">
            <div class="m-title">{title}</div>
            <div class="m-val">{prefix}{fmt_val}{suffix}</div>
            <div class="m-deltas">
                <span><span class="d-label">1D</span><span class="{d1_c}">{d1_l}</span></span>
                <span><span class="d-label">1M</span><span class="{m1_c}">{m1_l}</span></span>
                <span><span class="d-label">1A</span><span class="{y1_c}">{y1_l}</span></span>
            </div>
        </div>
        """, unsafe_allow_html=True)
    except: st.error(f"Error {col}")

# ==========================================
# 3. INTERFAZ TABS
# ==========================================
st.title("📊 Dashboard Económico Financiero")
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(["📌 Resumen", "🌎 Macro Global", "🇦🇷 AR Estrategia", "🔮 Expectativas", "🏗️ Inmobiliario", "💼 Portafolio"])

# --- TAB 1: RESUMEN ---
with tab1:
    st.markdown('<div class="section-title">🌐 MUNDO</div>', unsafe_allow_html=True)
    c1, c2, c3, c4 = st.columns(4)
    with c1: render_kpi("S&P 500", "SP500")
    with c2: render_kpi("Brent", "Brent", prefix="USD ")
    with c3: render_kpi("Bitcoin", "BTC", prefix="USD ")
    with c4: render_kpi("Oro", "Oro", prefix="USD ")
    
    st.markdown('<div class="section-title">🇦🇷 ARGENTINA</div>', unsafe_allow_html=True)
    c5, c6, c7, c8 = st.columns(4)
    with c5: render_kpi("Merval", "Merval")
    with c6: render_kpi("Riesgo País", "Riesgo_Pais", suffix=" bps", is_inverted=True)
    with c7: render_kpi("Dólar Oficial", "USD_Oficial", prefix="$", is_inverted=True)
    with c8: render_kpi("Brecha CCL", "Brecha_CCL", suffix="%", is_inverted=True)

    st.markdown("<br>", unsafe_allow_html=True)
    col_fg, col_ia = st.columns([1, 3])
    
    with col_fg:
        color_fg = "#ef4444" if "Fear" in fng_class else "#10b981" if "Greed" in fng_class else "#f59e0b"
        st.markdown(f"""
        <div class="metric-card" style="text-align: center; height: 100%; display: flex; flex-direction: column; justify-content: center;">
            <div class="m-title" style="margin-bottom: 10px;">Cripto Fear & Greed</div>
            <div class="m-val" style="font-size: 32px; color: {color_fg};">{fng_val}</div>
            <div style="color: {color_fg}; font-weight: bold;">{fng_class.upper()}</div>
        </div>
        """, unsafe_allow_html=True)

    with col_ia:
        if not df_ai.empty:
            txt_full = str(df_ai["Analisis_LLM"].iloc[-1]).replace(chr(10), '<br>')
            flash_txt = txt_full.split("💡 EL DATO REAL:")[0] if "💡 EL DATO REAL:" in txt_full else txt_full
            st.markdown(f'<div class="ai-box"><div class="ai-title">🤖 FLASH MARKET</div><div class="ai-text">{flash_txt}</div></div>', unsafe_allow_html=True)

# --- TAB 2: MACRO GLOBAL ---
with tab2:
    st.subheader("Contexto Internacional y Tasas")
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**Evolución Tasas (Demo)**")
        # ¡Acá está el fix! Cambié freq='M' por freq='ME'
        df_mock = pd.DataFrame({'fecha': pd.date_range(start='1/1/2023', periods=12, freq='ME'), 'FED': [4,4.5,4.7,5,5.2,5.5,5.5,5.5,5.5,5.5,5.5,5.5]})
        fig_tasas = px.line(df_mock, x='fecha', y='FED', template='plotly_dark', color_discrete_sequence=['#38bdf8'])
        st.plotly_chart(aplicar_estilo_bloomberg(fig_tasas), use_container_width=True)

# --- TAB 3: ARGENTINA ESTRATEGIA ---
with tab3:
    st.markdown('<div class="section-title">🇦🇷 MACROECONOMÍA</div>', unsafe_allow_html=True)
    c1, c2, c3 = st.columns(3)
    val_ipc = pd.to_numeric(df_hist['IPC'], errors='coerce').iloc[-1] if 'IPC' in df_hist else 0
    val_emae = pd.to_numeric(df_hist['EMAE'], errors='coerce').iloc[-1] if 'EMAE' in df_hist else 0
    val_ripte = pd.to_numeric(df_hist['RIPTE'], errors='coerce').iloc[-1] if 'RIPTE' in df_hist else 0
    
    with c1: st.markdown(f'<div class="metric-card"><div class="m-title">Inflación (IPC Últ. Mes)</div><div class="m-val">{val_ipc:.1f}%</div></div>', unsafe_allow_html=True)
    with c2: st.markdown(f'<div class="metric-card"><div class="m-title">Actividad (EMAE)</div><div class="m-val">{val_emae:,.0f} pts</div></div>', unsafe_allow_html=True)
    with c3: st.markdown(f'<div class="metric-card"><div class="m-title">Salario Prom. (RIPTE)</div><div class="m-val">${val_ripte:,.0f}</div></div>', unsafe_allow_html=True)

    st.markdown("<br><hr>", unsafe_allow_html=True)
    st.markdown('<div class="section-title">🔍 Análisis de Valor Real en Dólares</div>', unsafe_allow_html=True)
    
    intervalo = st.radio("Seleccionar Intervalo:", ["Mensual", "Anual"], horizontal=True)
    dias = 30 if intervalo == "Mensual" else 365
    
    try: bench_val = float(df_ai['Bench_1M' if intervalo == "Mensual" else 'Bench_1A'].iloc[-1])
    except: bench_val = -4.3 if intervalo == "Mensual" else 15.0

    def calc_retorno_usd(columna, es_pesos=False):
        try:
            df = df_hist[['fecha', columna, 'CCL']].dropna()
            if df.empty: return 0.0
            val_hoy = pd.to_numeric(df[columna]).iloc[-1]
            ccl_hoy = pd.to_numeric(df['CCL']).iloc[-1]
            
            df_ant = df[df['fecha'] <= (df['fecha'].iloc[-1] - timedelta(days=dias))]
            if df_ant.empty:
                val_ant = pd.to_numeric(df[columna]).iloc[0]
                ccl_ant = pd.to_numeric(df['CCL']).iloc[0]
            else:
                val_ant = pd.to_numeric(df_ant[columna]).iloc[-1]
                ccl_ant = pd.to_numeric(df_ant['CCL']).iloc[-1]

            usd_hoy = val_hoy / ccl_hoy if es_pesos else val_hoy
            usd_ant = val_ant / ccl_ant if es_pesos else val_ant
            return ((usd_hoy / usd_ant) - 1) * 100
        except: return 0.0

    ret_merval = calc_retorno_usd("Merval", es_pesos=True)
    ret_al30 = calc_retorno_usd("AL30", es_pesos=True)
    ret_sp500 = calc_retorno_usd("SP500", es_pesos=False)

    col_inv, col_fin = st.columns(2)
    
    with col_inv:
        st.subheader("💰 Inversiones medidas en USD")
        df_inv = pd.DataFrame({
            "Activo": ["Merval", "AL30", "S&P 500", "Dólares sin invertir", "m2 Venta (CABA)", "Plazo Fijo"],
            "Retorno": [ret_merval, ret_al30, ret_sp500, 0.0, 1.2 if intervalo=="Mensual" else 5.5, -4.0 if intervalo=="Mensual" else -25.0]
        })
        df_inv["Neta"] = df_inv["Retorno"] - bench_val
        df_inv = df_inv.sort_values("Neta")
        
        colores_inv = ['#10b981' if v > 0 else '#ef4444' for v in df_inv["Neta"]]
        fig = go.Figure(go.Bar(x=df_inv["Neta"], y=df_inv["Activo"], orientation='h', marker_color=colores_inv, text=[f"{v:+.1f}%" for v in df_inv["Neta"]], textposition='outside', textfont=dict(color='white')))
        fig.add_vline(x=0, line_width=2, line_color="#cbd5e1", line_dash="dash", annotation_text=" PUNTO EQUILIBRIO")
        fig.update_layout(template='plotly_dark', margin=dict(l=0, r=40, t=10, b=0), height=350, xaxis=dict(showgrid=False), yaxis=dict(showgrid=False))
        st.plotly_chart(fig, use_container_width=True)

    with col_fin:
        st.subheader("💳 Costo de financiamiento en USD")
        df_fin = pd.DataFrame({
            "Línea": ["Adelanto Cta Cte", "Tarjeta", "Préstamo Personal", "Hipotecario UVA", "SGR (Cheques)"],
            "Costo": [15.2, 8.4, 5.1, 2.0, -4.5] if intervalo=="Mensual" else [85.0, 60.0, 45.0, 12.0, -10.0]
        })
        df_fin["Neta"] = df_fin["Costo"] - bench_val
        df_fin = df_fin.sort_values("Neta")
        
        colores_fin = ['#ef4444' if v > 0 else '#10b981' for v in df_fin["Neta"]]
        fig_f = go.Figure(go.Bar(x=df_fin["Neta"], y=df_fin["Línea"], orientation='h', marker_color=colores_fin, text=[f"{v:+.1f}%" for v in df_fin["Neta"]], textposition='outside', textfont=dict(color='white')))
        fig_f.add_vline(x=0, line_width=2, line_color="#cbd5e1", line_dash="dash", annotation_text=" PUNTO EQUILIBRIO")
        fig_f.update_layout(template='plotly_dark', margin=dict(l=0, r=40, t=10, b=0), height=350, xaxis=dict(showgrid=False), yaxis=dict(showgrid=False))
        st.plotly_chart(fig_f, use_container_width=True)

    if not df_ai.empty and "💡 EL DATO REAL:" in str(df_ai["Analisis_LLM"].iloc[-1]):
        txt_dona_rosa = str(df_ai["Analisis_LLM"].iloc[-1]).split("💡 EL DATO REAL:")[1].strip()
        st.markdown(f'<div class="ai-box" style="margin-top:20px;"><div class="ai-title">💡 LA EXPLICACIÓN (DOÑA ROSA)</div><div class="ai-text">{txt_dona_rosa}</div></div>', unsafe_allow_html=True)

with tab4: st.info("Módulo de Expectativas en desarrollo.")
with tab5: st.info("Módulo de Real Estate en desarrollo.")
with tab6: st.info("Módulo de Portafolio en desarrollo.")
