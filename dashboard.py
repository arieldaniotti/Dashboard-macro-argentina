
import streamlit as st
import pandas as pd
import gspread
from google.oauth2 import service_account
import plotly.express as px
from datetime import datetime

st.set_page_config(
    page_title="Dashboard Macro Argentina",
    page_icon="📊",
    layout="wide"
)

st.title("Dashboard Macro Argentina")
st.caption(f"Última actualización: {datetime.today().strftime('%d/%m/%Y')}")

# Autenticación via Secrets de Streamlit
@st.cache_data(ttl=3600)
def cargar_datos():
    credenciales = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"],
        scopes=[
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    gc   = gspread.authorize(credenciales)
    sh   = gc.open("Dashboard Macro")
    data = sh.sheet1.get_all_records()
    return pd.DataFrame(data)

df = cargar_datos()

# ---- MÉTRICAS GLOBALES ----
st.subheader("Mercados globales")
col1, col2, col3, col4, col5 = st.columns(5)

ultimo   = df.iloc[-1]
anterior = df.iloc[-2] if len(df) > 1 else ultimo

col1.metric("S&P 500",    f"{ultimo['SP500']:,.0f}",    f"{ultimo['SP500']-anterior['SP500']:+.0f}")
col2.metric("Nasdaq 100", f"{ultimo['Nasdaq']:,.0f}",   f"{ultimo['Nasdaq']-anterior['Nasdaq']:+.0f}")
col3.metric("Oro",        f"USD {ultimo['Oro']:,.0f}",  f"{ultimo['Oro']-anterior['Oro']:+.0f}")
col4.metric("Brent",      f"USD {ultimo['Brent']:.1f}", f"{ultimo['Brent']-anterior['Brent']:+.1f}")
col5.metric("Bitcoin",    f"USD {ultimo['BTC']:,.0f}",  f"{ultimo['BTC']-anterior['BTC']:+.0f}")

st.divider()

# ---- ARGENTINA ----
st.subheader("Argentina — tipos de cambio")
col1, col2, col3, col4 = st.columns(4)

col1.metric("USD Oficial", f"$ {ultimo['USD_Oficial']:,.0f}")
col2.metric("USD Blue",    f"$ {ultimo['USD_Blue']:,.0f}")
col3.metric("CCL",         f"$ {ultimo['CCL']:,.0f}")
col4.metric("Brecha CCL",  f"{ultimo['Brecha_CCL']:.1f}%")

st.divider()

# ---- MACRO USA ----
st.subheader("Macro USA")
col1, col2, col3, col4 = st.columns(4)

col1.metric("Inflación USA", f"{ultimo['Inflacion_USA']:.2f}%")
col2.metric("Tasa Fed",      f"{ultimo['Tasa_Fed']:.2f}%")
col3.metric("Tasa real",     f"{ultimo['Tasa_Real']:.2f}%")
col4.metric("Yield curve",   f"{ultimo['Yield_Curve']:.2f}%")

st.divider()

# ---- GRÁFICOS ----
st.subheader("Evolución histórica")
col1, col2 = st.columns(2)

with col1:
    fig = px.line(df, x="Fecha", y="SP500",
                  title="S&P 500",
                  color_discrete_sequence=["#185FA5"])
    fig.update_layout(showlegend=False, height=300)
    st.plotly_chart(fig, use_container_width=True)

with col2:
    fig = px.line(df, x="Fecha", y=["USD_Oficial","USD_Blue","CCL"],
                  title="Tipos de cambio ARS",
                  color_discrete_sequence=["#185FA5","#E24B4A","#639922"])
    fig.update_layout(height=300)
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# ---- PORTAFOLIO ----
st.subheader("Portafolio — precios ADR (USD)")
portafolio = pd.DataFrame({
    "Activo":     ["MELI","NVDA","MSFT","GOOGL","YPF","VIST","PAM","GGAL"],
    "Precio USD": [
        ultimo["MELI"], ultimo["NVDA"], ultimo["MSFT"], ultimo["GOOGL"],
        ultimo["YPF"],  ultimo["VIST"], ultimo["PAM"],  ultimo["GGAL"]
    ]
})
st.dataframe(portafolio, use_container_width=True, hide_index=True)
