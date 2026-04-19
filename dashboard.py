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
    .m-title { font-size: 13px; color: #94a3b8; font-weight: bold; text-transform: uppercase; }
    .m-val { font-size: 24px; font-weight: 700; font-family: monospace; margin: 8px 0; color: #f8fafc;}
    .m-deltas { display: flex; justify-content: space-between; font-size: 12px; border-top: 1px solid #1e293b; padding-top: 8px; font-weight: 600;}
    .d-up-good { color: #10b981; } .d-down-bad { color: #ef4444; }
    .d-up-bad { color: #ef4444; } .d-down-good { color: #10b981; } .d-flat { color: #64748b; }
    .ai-box { background-color: #0a1525; border: 1px solid #1a3050; border-radius: 8px; padding: 20px; height: 100%;}
    </style>
""", unsafe_allow_html=True)

# ==========================================
# 2. CONEXIÓN A DATOS
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
    return read("DB_Insights"), read("DB_Historico"), read("DB_Macro")

df_ai, df_hist, df_macro = load_all()

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

# --- FUNCIÓN BLINDADA ANTI-FERIADOS ---
def render_kpi(title, col, prefix="", suffix="", is_inverted=False, is_macro=False):
    try:
        if col not in df_hist.columns:
            st.error(f"No hay datos de {col}")
            return
            
        df = df_hist[['fecha', col]].dropna()
        if df.empty:
            st.error(f"Tabla vacía para {col}")
            return
            
        df[col] = pd.to_numeric(df[col], errors='coerce')
        val = df[col].iloc[-1]
        last_date = df['fecha'].iloc[-1]
        
        # Filtros de fecha seguros
        df_1m = df[df['fecha'] <= (last_date - timedelta(days=30))]
        ant_1m = df_1m.iloc[-1][col] if not df_1m.empty else df[col].iloc[0]
        
        df_1a = df[df['fecha'] <= (last_date - timedelta(days=365))]
        ant_1a = df_1a.iloc[-1][col] if not df_1a.empty else df[col].iloc[0]
        
        is_points = col in ['Riesgo_Pais', 'Brecha_CCL']
        
        if is_points:
            m1 = val - ant_1m
            y1 = val - ant_1a
        else:
            m1 = ((val/ant_1m)-1)*100 if ant_1m else 0
            y1 = ((val/ant_1a)-1)*100 if ant_1a else 0

        def get_clr(d):
            if abs(d) < 0.05: return "d-flat"
            if is_inverted: return "d-up-bad" if d > 0 else "d-down-good"
            return "d-up-good" if d > 0 else "d-down-bad"

        fmt_m = f"{m1:+.1f}bps" if is_points and col == 'Riesgo_Pais' else f"{m1:+.1f}%"
        fmt_y = f"{y1:+.1f}bps" if is_points and col == 'Riesgo_Pais' else f"{y1:+.1f}%"
        
        val_str = f"{val:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        if val_str.endswith(",00"): val_str = val_str[:-3]

        if is_macro:
            st.markdown(f'<div class="metric-card"><div class="m-title">{title}</div><div class="m-val">{prefix}{val_str}{suffix}</div><div class="m-deltas"><span>1M: <span class="{get_clr(m1)}">{fmt_m}</span></span><span>1A: <span class="{get_clr(y1)}">{fmt_y}</span></span></div></div>', unsafe_allow_html=True)
        else:
            val_1d = df[col].iloc[-2] if len(df)>1 else val
            d1 = (val - val_1d) if is_points else ((val/val_1d)-1)*100 if val_1d else 0
            fmt_d = f"{d1:+.1f}bps" if is_points and col == 'Riesgo_Pais' else f"{d1:+.1f}%"
            st.markdown(f'<div class="metric-card"><div class="m-title">{title}</div><div class="m-val">{prefix}{val_str}{suffix}</div><div class="m-deltas"><span>1D: <span class="{get_clr(d1)}">{fmt_d}</span></span><span>1M: <span class="{get_clr(m1)}">{fmt_m}</span></span><span>1A: <span class="{get_clr(y1)}">{fmt_y}</span></span></div></div>', unsafe_allow_html=True)
    except: st.error(f"Error renderizando {col}")

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
            flash_txt = txt_full.split("💡 EL DATO REAL:")[0].replace("---", "") if "💡 EL DATO REAL:" in txt_full else txt_full
            st.markdown(f'<div class="ai-box"><div style="color:#38bdf8;font-weight:bold;margin-bottom:10px;font-size:14px;">🤖 FLASH MARKET</div><div style="font-size:14px;color:#cbd5e1;line-height:1.6;">{flash_txt}</div></div>', unsafe_allow_html=True)

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
        with col2:
            st.markdown("**Yield Curve (10Y - 2Y)**")
            if 'T10Y2Y' in df_macro.columns:
                df_macro['T10Y2Y'] = pd.to_numeric(df_macro['T10Y2Y'], errors='coerce')
                fig_yield = px.area(df_macro, x='fecha', y='T10Y2Y', template='plotly_dark', color_discrete_sequence=['#f59e0b'])
                fig_yield.update_traces(fillcolor='rgba(245, 158, 11, 0.2)', line=dict(width=2)) 
                st.plotly_chart(aplicar_estilo_bloomberg(fig_yield), use_container_width=True)

# --- TAB 3: ARGENTINA ESTRATEGIA ---
with tab3:
    st.markdown('<div class="section-title">🇦🇷 SEMÁFORO MACROECONÓMICO</div>', unsafe_allow_html=True)
    c1, c2, c3 = st.columns(3)
    with c1: render_kpi("Inflación (IPC Últ. Mes)", "IPC", suffix="%", is_macro=True)
    with c2: render_kpi("Actividad (EMAE)", "EMAE", suffix=" pts", is_macro=True)
    with c3: render_kpi("Salario (RIPTE)", "RIPTE", prefix="$", is_macro=True)

    st.markdown('<div class="section-title">🔍 ANÁLISIS DE VALOR REAL (BASE USD)</div>', unsafe_allow_html=True)
    intervalo = st.radio("Seleccionar Intervalo:", ["Mensual", "Anual"], horizontal=True)
    bench = float(df_ai['Bench_1M' if intervalo=="Mensual" else 'Bench_1A'].iloc[-1]) if not df_ai.empty else -4.3

    # --- CÁLCULO BLINDADO DE RETORNO REAL ---
    def get_real_ret(col, es_pesos=False):
        try:
            df = df_hist[['fecha', col, 'CCL']].dropna()
            if df.empty: return 0.0
            
            v_h = pd.to_numeric(df[col]).iloc[-1]
            ccl_h = pd.to_numeric(df['CCL']).iloc[-1]
            last_date = df['fecha'].iloc[-1]
            
            # Buscamos la fecha límite y agarramos el dato, si está vacío vamos al dato más viejo.
            df_a = df[df['fecha'] <= (last_date - timedelta(days=30 if intervalo=="Mensual" else 365))]
            
            if df_a.empty:
                v_a = pd.to_numeric(df[col]).iloc[0]
                ccl_a = pd.to_numeric(df['CCL']).iloc[0]
            else:
                v_a = pd.to_numeric(df_a[col]).iloc[-1]
                ccl_a = pd.to_numeric(df_a['CCL']).iloc[-1]
                
            usd_h = v_h/ccl_h if es_pesos else v_h
            usd_a = v_a/ccl_a if es_pesos else v_a
            
            return ((usd_h/usd_a)-1)*100
        except: return 0.0

    r_merval, r_al30, r_sp500 = get_real_ret("Merval", True), get_real_ret("AL30", True), get_real_ret("SP500", False)

    col_inv, col_fin = st.columns(2)
    with col_inv:
        st.subheader("💰 Inversiones medidas en USD")
        df_i = pd.DataFrame({
            "Activo": ["Merval", "AL30", "S&P 500", "Dólares sin invertir", "m2 Venta (CABA)", "Plazo Fijo"],
            "Ret": [r_merval, r_al30, r_sp500, 0.0, 1.2 if intervalo=="Mensual" else 5.5, -4.0 if intervalo=="Mensual" else -25.0]
        })
        df_i["Neta"] = df_i["Ret"] - bench
        df_i = df_i.sort_values("Neta")
        fig = go.Figure(go.Bar(x=df_i["Neta"], y=df_i["Activo"], orientation='h', marker_color=['#10b981' if v > 0 else '#ef4444' for v in df_i["Neta"]], text=[f"{v:+.1f}%" for v in df_i["Neta"]], textposition='outside', textfont=dict(color='white')))
        fig.add_vline(x=0, line_width=2, line_color="#cbd5e1", line_dash="dash", annotation_text=" PUNTO EQUILIBRIO")
        fig.update_layout(template='plotly_dark', margin=dict(l=0, r=40, t=10, b=0), height=350, xaxis=dict(showgrid=False), paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
        st.plotly_chart(fig, use_container_width=True)

    with col_fin:
        st.subheader("💳 Costo de financiamiento en USD")
        df_f = pd.DataFrame({
            "Línea": ["Adelanto Cta Cte", "Tarjeta", "Préstamo Personal", "Hipotecario UVA", "SGR (Cheques)"],
            "Costo": [15.2, 8.4, 5.1, 2.0, -4.5] if intervalo=="Mensual" else [85.0, 60.0, 45.0, 12.0, -10.0]
        })
        df_f["Neta"] = df_f["Costo"] - bench
        df_f = df_f.sort_values("Neta")
        fig_f = go.Figure(go.Bar(x=df_f["Neta"], y=df_f["Línea"], orientation='h', marker_color=['#ef4444' if v > 0 else '#10b981' for v in df_f["Neta"]], text=[f"{v:+.1f}%" for v in df_f["Neta"]], textposition='outside', textfont=dict(color='white')))
        fig_f.add_vline(x=0, line_width=2, line_color="#cbd5e1", line_dash="dash", annotation_text=" PUNTO EQUILIBRIO")
        fig_f.update_layout(template='plotly_dark', margin=dict(l=0, r=40, t=10, b=0), height=350, xaxis=dict(showgrid=False), paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
        st.plotly_chart(fig_f, use_container_width=True)

    if not df_ai.empty and "💡 EL DATO REAL:" in str(df_ai["Analisis_LLM"].iloc[-1]):
        txt_dona = str(df_ai["Analisis_LLM"].iloc[-1]).split("💡 EL DATO REAL:")[1].strip()
        st.markdown(f'<div class="ai-box" style="margin-top:20px;"><div style="color:#38bdf8;font-weight:bold;margin-bottom:10px;font-size:14px;">💡 LA EXPLICACIÓN (DOÑA ROSA)</div><div style="font-size:15px;color:#cbd5e1;line-height:1.6;">{txt_dona}</div></div>', unsafe_allow_html=True)

# --- TAB 4: EXPECTATIVAS ---
with tab4:
    st.subheader("Curvas de Futuros y Expectativas (REM)")
    st.caption("Datos implícitos Matba Rofex.")
    col1, col2, col3, col4 = st.columns(4)
    def render_future_card(contrato, precio, tna, delta_precio):
        color = "color: #10b981;" if delta_precio < 0 else "color: #ef4444;"
        st.markdown(f"""
        <div class="metric-card" style="padding: 15px;">
            <div style="font-size: 14px; color: #94a3b8; font-weight: bold; text-transform: uppercase;">{contrato}</div>
            <div style="font-size: 24px; color: #f8fafc; font-weight: bold; font-family: monospace; margin: 8px 0;">$ {precio}</div>
            <div style="display: flex; justify-content: space-between; font-size: 14px; border-top: 1px solid #1e293b; padding-top: 8px;">
                <span style="color: #38bdf8; font-weight: bold;">TNA: {tna}%</span>
                <span style="{color} font-weight: bold;">{delta_precio}% 1D</span>
            </div>
        </div>
        """, unsafe_allow_html=True)

    with col1: render_future_card("Fin Mes Actual", "1.415,50", "45.2", 0.15)
    with col2: render_future_card("Fin Próximo Mes", "1.468,20", "48.5", -0.05)
    with col3: render_future_card("Diciembre", "1.750,00", "52.1", 1.20)
    
    st.markdown("<br>### Inflación Esperada (REM BCRA)", unsafe_allow_html=True)
    col5, col6, col7, col8 = st.columns(4)
    with col5: render_future_card("IPC Mes Próximo", "4.5", "-", -0.2)
    with col6: render_future_card("IPC 12 Meses", "65.0", "-", -2.5)

with tab5: st.info("Módulo Inmobiliario en desarrollo.")
with tab6: st.info("Módulo de Portafolio en desarrollo.")
