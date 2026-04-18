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
st.set_page_config(page_title="Dashboard Macro", layout="wide", initial_sidebar_state="collapsed")

st.markdown("""
    <style>
    .stApp { background-color: #07090f; color: #e2e8f0; font-family: sans-serif; }
    
    .section-title { font-size: 22px; color: #38bdf8; font-weight: 800; text-transform: uppercase; letter-spacing: 0.1em; margin-top: 30px; border-bottom: 1px solid #1e293b; padding-bottom: 10px; margin-bottom: 20px;}
    
    .metric-card { background-color: #0b0e18; border: 1px solid #1e293b; border-radius: 8px; padding: 20px; box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1); }
    .m-title { font-size: 16px; color: #cbd5e1; font-weight: 800; text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 10px; }
    .m-val { font-size: 26px; color: #f8fafc; font-weight: 700; font-family: 'Courier New', monospace; margin-bottom: 15px; }
    
    .m-deltas { display: flex; justify-content: space-between; font-size: 14px; font-weight: 800; padding-top: 12px; border-top: 1px solid #1e293b; }
    
    .d-up-good { color: #10b981; }   
    .d-down-bad { color: #ef4444; }  
    .d-up-bad { color: #ef4444; }    
    .d-down-good { color: #10b981; } 
    .d-flat { color: #64748b; }      
    .d-label { color: #64748b; font-size: 11px; margin-right: 4px; font-weight: 600; }
    
    .ai-box { background-color: #0a1525; border: 1px solid #1a3050; border-radius: 8px; padding: 20px; height: 100%; margin-top: 0; }
    .ai-title { color: #38bdf8; font-size: 13px; font-weight: bold; text-transform: uppercase; margin-bottom: 15px; display: flex; align-items: center; gap: 8px;}
    .ai-text { color: #cbd5e1; font-size: 14px; line-height: 1.6; }

    /* ==================================================== */
    /* MODO CELULAR (Se activa solo en pantallas chicas)    */
    /* ==================================================== */
    @media (max-width: 768px) {
        .metric-card { padding: 12px; }
        .m-title { font-size: 13px; margin-bottom: 5px; }
        .m-val { font-size: 20px; margin-bottom: 10px; }
        .m-deltas { font-size: 12px; padding-top: 8px; }
        .d-label { font-size: 9px; }
        .section-title { font-size: 18px; margin-top: 15px; margin-bottom: 10px; }
        .ai-box { padding: 15px; }
        .ai-text { font-size: 13px; }
        .block-container { padding-top: 1.5rem; padding-bottom: 1rem; }
    }
    </style>
""", unsafe_allow_html=True)

# ==========================================
# 2. CONEXIÓN A LA BASE DE DATOS
# ==========================================
@st.cache_data(ttl=600)
def fetch_database_data_v3():
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
                if 'fecha' in df.columns:
                    df['fecha'] = pd.to_datetime(df['fecha'], errors='coerce')
                    df = df.dropna(subset=['fecha']).drop_duplicates(subset=['fecha'], keep='last').sort_values('fecha')
                return df
            return pd.DataFrame()
        except: return pd.DataFrame()

    return safe_read("DB_Insights"), safe_read("DB_Macro"), safe_read("DB_Historico")

df_insights, df_macro, df_hist = fetch_database_data_v3()

@st.cache_data(ttl=3600)
def get_fear_and_greed():
    try:
        r = requests.get("https://api.alternative.me/fng/", timeout=5).json()
        return r['data'][0]['value'], r['data'][0]['value_classification']
    except: return "N/A", "Desconocido"

fng_val, fng_class = get_fear_and_greed()

# ==========================================
# 3. LÓGICA MATEMÁTICA Y GRÁFICOS
# ==========================================
def aplicar_estilo_bloomberg(fig):
    fig.update_layout(
        xaxis_title="", yaxis_title="", legend_title="",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(l=0, r=0, t=30, b=0), hovermode="x unified"
    )
    fig.update_traces(line=dict(width=2.5))
    return fig

def get_kpi_3d(col_name):
    try:
        if col_name not in df_hist.columns: return "N/A", 0, 0, 0, False
        
        df = df_hist[['fecha', col_name]].copy()
        df[col_name] = pd.to_numeric(df[col_name], errors='coerce')
        df = df.dropna(subset=[col_name]).sort_values('fecha')
        if df.empty: return "N/A", 0, 0, 0, False
        
        actual = df[col_name].iloc[-1]
        prev_1d = df[col_name].iloc[-2] if len(df) > 1 else actual
        
        def get_past_val(days_ago):
            target_date = df['fecha'].iloc[-1] - timedelta(days=days_ago)
            past_df = df[df['fecha'] <= target_date]
            return past_df.iloc[-1][col_name] if not past_df.empty else df[col_name].iloc[0]

        m1_val = get_past_val(30)
        y1_val = get_past_val(365)
            
        is_points = col_name in ['Riesgo_Pais', 'Brecha_CCL']
        
        if is_points:
            d1_delta = actual - prev_1d
            m1_delta = actual - m1_val
            y1_delta = actual - y1_val
        else:
            d1_delta = ((actual / prev_1d) - 1) * 100 if prev_1d else 0
            m1_delta = ((actual / m1_val) - 1) * 100 if m1_val else 0
            y1_delta = ((actual / y1_val) - 1) * 100 if y1_val else 0
            
        val_str = f"{actual:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        if val_str.endswith(",00"): val_str = val_str[:-3]
        
        return val_str, d1_delta, m1_delta, y1_delta, is_points
    except: return "N/A", 0, 0, 0, False

def render_card_3d(title, col_name, prefix="", suffix=""):
    val, d1, m1, y1, is_points = get_kpi_3d(col_name)
    inverted = col_name in ['Riesgo_Pais', 'Brecha_CCL']
    
    def get_style(delta, is_inv):
        if val == "N/A": return "N/A", "d-flat"
        unidad = "pp" if (is_points and col_name == 'Brecha_CCL') else "bps" if is_points else "%"
        
        if abs(delta) < 0.005: return f"▬ 0.00{unidad}", "d-flat"
        
        symbol = "▲" if delta > 0 else "▼"
        label = f"{abs(delta):.1f}{unidad}"
        
        if is_inv: color = "d-up-bad" if delta > 0 else "d-down-good"
        else: color = "d-up-good" if delta > 0 else "d-down-bad"
            
        return f"{symbol} {label}", color

    d1_l, d1_c = get_style(d1, inverted)
    m1_l, m1_c = get_style(m1, inverted)
    y1_l, y1_c = get_style(y1, inverted)

    st.markdown(f"""
    <div class="metric-card">
        <div class="m-title">{title}</div>
        <div class="m-val">{prefix}{val}{suffix}</div>
        <div class="m-deltas">
            <span><span class="d-label">1D</span><span class="{d1_c}">{d1_l}</span></span>
            <span><span class="d-label">1M</span><span class="{m1_c}">{m1_l}</span></span>
            <span><span class="d-label">1A</span><span class="{y1_c}">{y1_l}</span></span>
        </div>
    </div>
    """, unsafe_allow_html=True)

# ==========================================
# 4. SOLAPAS (TABS)
# ==========================================
st.title("📊 Dashboard Económico Financiero")
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(["📌 Resumen", "🌎 Macro Global", "🇦🇷 Argentina", "🔮 Expectativas", "🏗️ Inmobiliario", "💼 Portafolio"])

with tab1:
    st.markdown('<div class="section-title">🌐 MUNDO</div>', unsafe_allow_html=True)
    c1, c2, c3, c4 = st.columns(4)
    with c1: render_card_3d("S&P 500", "SP500")
    with c2: render_card_3d("Brent", "Brent", prefix="USD ")
    with c3: render_card_3d("Bitcoin", "BTC", prefix="USD ")
    with c4: render_card_3d("Oro", "Oro", prefix="USD ")
    
    st.markdown('<div class="section-title">🇦🇷 ARGENTINA</div>', unsafe_allow_html=True)
    c5, c6, c7, c8 = st.columns(4)
    with c5: render_card_3d("Merval", "Merval")
    with c6: render_card_3d("Riesgo País", "Riesgo_Pais", suffix=" bps")
    with c7: render_card_3d("Dólar Oficial", "USD_Oficial", prefix="$")
    with c8: render_card_3d("Brecha CCL", "Brecha_CCL", suffix="%")

    st.markdown("<br>", unsafe_allow_html=True)
    col_fg, col_ia = st.columns([1, 3])
    
    with col_fg:
        color_fg = "#ef4444" if "Fear" in fng_class else "#10b981" if "Greed" in fng_class else "#f59e0b"
        st.markdown(f"""
        <div class="metric-card" style="text-align: center; height: 100%; display: flex; flex-direction: column; justify-content: center; padding: 20px;">
            <div class="m-title" style="margin-bottom: 10px;">Cripto Fear & Greed</div>
            <div class="m-val" style="font-size: 32px; color: {color_fg};">{fng_val}</div>
            <div style="color: {color_fg}; font-weight: bold; font-size: 13px;">{fng_class.upper()}</div>
        </div>
        """, unsafe_allow_html=True)

    with col_ia:
        if not df_insights.empty:
            texto_ia = str(df_insights['Analisis_LLM'].iloc[-1]).replace(chr(10), '<br>')
            st.markdown(f"""
                <div class="ai-box">
                    <div class="ai-title">🤖 FLASH MARKET</div>
                    <div class="ai-text">{texto_ia}</div>
                </div>
            """, unsafe_allow_html=True)

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

# --- TAB 3: ARGENTINA (Visión Estratégica) ---
with tab3:
    st.markdown('<div class="section-title">📊 ESTRATEGIA Y MACROECONOMÍA</div>', unsafe_allow_html=True)
    
    # Bloque 1: Macro Superior (Contexto)
    c1, c2, c3 = st.columns(3)
    with c1: render_card_3d("Inflación (IPC)", "IPC", suffix="%")
    with c2: render_card_3d("Actividad (EMAE)", "EMAE", suffix=" pts")
    with c3: render_card_3d("Salario Real (RIPTE)", "RIPTE", prefix="$")

    st.markdown("<br><hr>", unsafe_allow_html=True)
    
    # Bloque 2: Gráficos de Valor Real (Estilo Institucional)
    st.markdown("### 🔍 Análisis de Valor Real en USD")
    st.caption("Rendimientos y costos netos tras descontar la inflación en dólares (Dólar Constante).")
    
    col_inv, col_fin = st.columns(2)

    # 1. GRÁFICO DE INVERSIONES
    with col_inv:
        st.markdown("**💰 Inversiones vs Inflación USD (Últimos 12M)**")
        
        # Datos simulados para la maqueta visual
        df_inv = pd.DataFrame({
            "Activo": ["Merval", "AL30", "S&P 500", "Lecap", "m2 Venta", "Plazo Fijo"],
            "Retorno_Real_USD": [25.4, 18.2, 8.5, 2.1, -1.5, -8.4]
        }).sort_values("Retorno_Real_USD", ascending=True) 
        
        colores_inv = ['#10b981' if val > 0 else '#ef4444' for val in df_inv["Retorno_Real_USD"]]
        
        fig_inv = go.Figure(go.Bar(
            x=df_inv["Retorno_Real_USD"], 
            y=df_inv["Activo"], 
            orientation='h',
            marker_color=colores_inv,
            text=[f"{val}%" for val in df_inv["Retorno_Real_USD"]],
            textposition='outside',
            textfont=dict(color='#cbd5e1', size=12)
        ))
        
        fig_inv.update_layout(
            template='plotly_dark',
            paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
            margin=dict(l=0, r=40, t=20, b=0),
            xaxis=dict(showgrid=True, gridcolor='#1e293b', zeroline=True, zerolinecolor='#94a3b8', zerolinewidth=2),
            yaxis=dict(showgrid=False)
        )
        fig_inv.add_vline(x=0, line_width=2, line_dash="dash", line_color="#cbd5e1", annotation_text=" Inflación USD (0%)", annotation_position="top right")
        
        st.plotly_chart(fig_inv, use_container_width=True)

    # 2. GRÁFICO DE FINANCIAMIENTO
    with col_fin:
        st.markdown("**💳 Costo de Financiamiento Real en USD**")
        
        # Datos simulados para la maqueta visual
        df_fin = pd.DataFrame({
            "Línea de Crédito": ["Adelanto Cta Cte", "Tarjeta Crédito", "Préstamo Personal", "Hipotecario UVA", "Prendario", "Desc. Cheques"],
            "Costo_Real_USD": [15.2, 8.4, 5.1, 2.0, -1.2, -4.5]
        }).sort_values("Costo_Real_USD", ascending=True)
        
        colores_fin = ['#ef4444' if val > 0 else '#10b981' for val in df_fin["Costo_Real_USD"]]
        
        fig_fin = go.Figure(go.Bar(
            x=df_fin["Costo_Real_USD"], 
            y=df_fin["Línea de Crédito"], 
            orientation='h',
            marker_color=colores_fin,
            text=[f"{val}%" for val in df_fin["Costo_Real_USD"]],
            textposition='outside',
            textfont=dict(color='#cbd5e1', size=12)
        ))
        
        fig_fin.update_layout(
            template='plotly_dark',
            paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
            margin=dict(l=0, r=40, t=20, b=0),
            xaxis=dict(showgrid=True, gridcolor='#1e293b', zeroline=True, zerolinecolor='#94a3b8', zerolinewidth=2),
            yaxis=dict(showgrid=False)
        )
        fig_fin.add_vline(x=0, line_width=2, line_dash="dash", line_color="#cbd5e1", annotation_text=" Inflación USD (0%)", annotation_position="top right")
        
        st.plotly_chart(fig_fin, use_container_width=True)

with tab4:
    st.subheader("Curvas de Futuros y Expectativas (REM)")
    st.caption("Maqueta visual: Datos fijos de demostración. Sin gráficos, foco en tasas implícitas.")
    
    st.markdown("### Dólar Futuro (Matba Rofex)")
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

with tab5:
    st.subheader("Mercado Inmobiliario")
    st.info("💡 Espacio reservado para el módulo de Real Estate.")

with tab6:
    st.subheader("Portafolio de Inversión")
    st.info("💡 Espacio reservado para Asset Allocation.")
