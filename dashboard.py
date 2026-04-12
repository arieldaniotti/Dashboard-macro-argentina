
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

@st.cache_data(ttl=3600)
def cargar_datos():
    credenciales = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"],
        scopes=[
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    gc    = gspread.authorize(credenciales)
    sh    = gc.open("Dashboard Macro")
    datos = sh.sheet1.get_all_values()
    df    = pd.DataFrame(datos[1:], columns=datos[0])
    for col in df.columns:
        if col not in ["Fecha", "Hora"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df

def safe_metric(label, col, df, prefijo="", sufijo="", decimales=2):
    try:
        ultimo   = float(df.iloc[-1][col])
        anterior = float(df.iloc[-2][col]) if len(df) > 1 else ultimo
        delta    = ultimo - anterior
        if decimales == 0:
            st.metric(label, f"{prefijo}{ultimo:,.0f}{sufijo}", f"{delta:+,.0f}")
        else:
            st.metric(label, f"{prefijo}{ultimo:,.{decimales}f}{sufijo}", f"{delta:+,.{decimales}f}")
    except:
        st.metric(label, "N/D", None)

df = cargar_datos()

# ---- MERCADOS GLOBALES ----
st.subheader("Mercados globales")
c1, c2, c3, c4, c5 = st.columns(5)
with c1: safe_metric("S&P 500",    "SP500",  df, decimales=0)
with c2: safe_metric("Nasdaq 100", "Nasdaq", df, decimales=0)
with c3: safe_metric("Oro",        "Oro",    df, "USD ", decimales=0)
with c4: safe_metric("Brent",      "Brent",  df, "USD ", decimales=1)
with c5: safe_metric("Bitcoin",    "BTC",    df, "USD ", decimales=0)

st.divider()

# ---- ARGENTINA ----
st.subheader("Argentina — tipos de cambio")
c1, c2, c3, c4 = st.columns(4)
with c1: safe_metric("USD Oficial", "USD_Oficial", df, "$ ", decimales=0)
with c2: safe_metric("USD Blue",    "USD_Blue",    df, "$ ", decimales=0)
with c3: safe_metric("CCL",         "CCL",         df, "$ ", decimales=0)
with c4: safe_metric("Brecha CCL",  "Brecha_CCL",  df, sufijo="%", decimales=1)

st.divider()

# ---- MACRO USA ----
st.subheader("Macro USA")
c1, c2, c3, c4 = st.columns(4)
with c1: safe_metric("Inflación USA", "Inflacion_USA", df, sufijo="%")
with c2: safe_metric("Tasa Fed",      "Tasa_Fed",      df, sufijo="%")
with c3: safe_metric("Tasa real",     "Tasa_Real",     df, sufijo="%")
with c4: safe_metric("Yield curve",   "Yield_Curve",   df, sufijo="%")

st.divider()

# ---- GRÁFICOS ----
st.subheader("Evolución histórica")
c1, c2 = st.columns(2)

with c1:
    if "SP500" in df.columns:
        fig = px.line(df, x="Fecha", y="SP500",
                      title="S&P 500",
                      color_discrete_sequence=["#185FA5"])
        fig.update_layout(showlegend=False, height=300)
        st.plotly_chart(fig, use_container_width=True)

with c2:
    cols = [c for c in ["USD_Oficial","USD_Blue","CCL"] if c in df.columns]
    if cols:
        fig = px.line(df, x="Fecha", y=cols,
                      title="Tipos de cambio ARS",
                      color_discrete_sequence=["#185FA5","#E24B4A","#639922"])
        fig.update_layout(height=300)
        st.plotly_chart(fig, use_container_width=True)

st.divider()

# ---- PORTAFOLIO ----
st.subheader("Portafolio — precios ADR (USD)")
activos = ["MELI","NVDA","MSFT","GOOGL","YPF","VIST","PAM","GGAL"]
precios = []
for a in activos:
    try:
        precios.append(round(float(df.iloc[-1][a]), 2))
    except:
        precios.append(None)

st.dataframe(
    pd.DataFrame({"Activo": activos, "Precio USD": precios}),
    use_container_width=True,
    hide_index=True
)
