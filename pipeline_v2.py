import os
import json
import requests
import yfinance as yf
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta
import numpy as np
import time

# ==========================================
# 1. CONFIGURACIÓN Y SEGURIDAD (GitHub Secrets)
# ==========================================
print("🚀 Iniciando Pipeline Automatizado V4.1...")

# Traemos las claves desde la bóveda de GitHub (Secrets)
FRED_API_KEY   = os.environ.get("FRED_API_KEY")
CHILE_USER     = os.environ.get("CHILE_USER")
CHILE_PASS     = os.environ.get("CHILE_PASS")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")

# Conexión a Google Sheets usando tu secreto GCLOUD_SERVICE_ACCOUNT
try:
    gcp_json = os.environ.get("GCLOUD_SERVICE_ACCOUNT")
    creds_dict = json.loads(gcp_json)
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    gc = gspread.authorize(creds)
    # Abrimos el archivo por su nombre (Asegurate que el robot esté invitado como editor)
    sh = gc.open("Dashboard Macro")
except Exception as e:
    raise RuntimeError(f"❌ Error crítico de autenticación con Google Cloud: {e}")

hoy          = datetime.today()
hace_1a      = hoy - timedelta(days=365)
fecha_inicio = hace_1a.strftime("%Y-%m-%d")

# ==========================================
# 2. CONEXIÓN A LA BASE DE DATOS (VERSIÓN ROBUSTA)
# ==========================================
@st.cache_data(ttl=3600)
def load_data():
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scopes)
    client = gspread.authorize(creds)
    sh = client.open("Dashboard Macro")
    
    # Función auxiliar para lectura segura (evita crashes por columnas vacías)
    def safe_read(sheet_name):
        try:
            ws = sh.worksheet(sheet_name)
            data = ws.get_all_values()
            if len(data) > 1:
                return pd.DataFrame(data[1:], columns=data[0])
            return pd.DataFrame()
        except Exception as e:
            st.error(f"Error leyendo {sheet_name}: {e}")
            return pd.DataFrame()

    # Leemos todas las pestañas de forma segura
    df_res = safe_read("DB_Resumen")
    df_ai = safe_read("DB_Insights")
    df_macro = safe_read("DB_Macro")
    df_hist = safe_read("DB_Historico")
    
    return df_res, df_ai, df_macro, df_hist

df_resumen, df_insights, df_macro, df_hist = load_data()

# ==========================================
# 3. PROCESO DE DATOS
# ==========================================
print("📥 Descargando datos (Arg, Global, Macro)...")

# Argentina
df_blue = pd.DataFrame(fetch_api_data("https://api.argentinadatos.com/v1/cotizaciones/dolares/blue")).rename(columns={"venta":"USD_Blue"})
df_oficial = pd.DataFrame(fetch_api_data("https://api.argentinadatos.com/v1/cotizaciones/dolares/oficial")).rename(columns={"venta":"USD_Oficial"})
df_rp = pd.DataFrame(fetch_api_data("https://api.argentinadatos.com/v1/finanzas/indices/riesgo-pais")).rename(columns={"valor":"Riesgo_Pais"})

# Limpieza básica
for df in [df_blue, df_oficial, df_rp]:
    if not df.empty: df["fecha"] = pd.to_datetime(df["fecha"])

df_arg = df_oficial[["fecha", "USD_Oficial"]].merge(df_blue[["fecha", "USD_Blue"]], on="fecha", how="outer")
df_arg = df_arg.merge(df_rp[["fecha", "Riesgo_Pais"]], on="fecha", how="left")
df_arg = df_arg[df_arg["fecha"] >= fecha_inicio]

# Mercados Yahoo
tickers = {"SP500":"^GSPC", "Merval":"^MERV", "Oro":"GC=F", "Brent":"BZ=F", "BTC":"BTC-USD", "GGAL_ADR":"GGAL", "GGAL_LOC":"GGAL.BA"}
df_mercado = pd.DataFrame()
for col, ticker in tickers.items():
    t = yf.download(ticker, start=fecha_inicio, progress=False, auto_adjust=True)
    if not t.empty:
        df_mercado[col] = t["Close"]

df_mercado = df_mercado.reset_index().rename(columns={"Date":"fecha"})
df_mercado["fecha"] = pd.to_datetime(df_mercado["fecha"]).dt.tz_localize(None)

# Macro Regional
df_fed = get_fred_series('FEDFUNDS', FRED_API_KEY, fecha_inicio)
df_yield = get_fred_series('T10Y2Y', FRED_API_KEY, fecha_inicio)
df_selic = get_bcb_data("11", "Tasa_SELIC_Brasil")
df_tpm_chile = get_chile_data("F073.TCO.PRE.Z.D", "Tasa_TPM_Chile", CHILE_USER, CHILE_PASS, fecha_inicio)

# Unificación
df_final = df_mercado.merge(df_arg, on="fecha", how="outer").sort_values("fecha")
for df_m in [df_fed, df_yield, df_selic, df_tpm_chile]:
    if not df_m.empty: df_final = df_final.merge(df_m, on='fecha', how='left')

df_final = df_final.ffill()

# Ingeniería de variables
df_final["CCL"] = (df_final["GGAL_LOC"] / (df_final["GGAL_ADR"] / 10)).round(2)
df_final["Brecha_CCL"] = (((df_final["CCL"] / df_final["USD_Oficial"]) - 1) * 100).round(2)
df_final = df_final.dropna(subset=["SP500"]).round(2)

# Deltas para Resumen
def get_metrics(df):
    results = []
    cols = ["SP500", "Merval", "Oro", "Brent", "BTC", "USD_Blue", "CCL", "Brecha_CCL", "Riesgo_Pais"]
    for c in cols:
        if c in df.columns:
            actual = df[c].iloc[-1]
            d1 = ((actual / df[c].iloc[-2]) - 1) * 100
            m1 = ((actual / df[c].iloc[abs(df["fecha"] - (hoy - timedelta(days=30))).idxmin()]) - 1) * 100
            results.append([c, actual, round(d1,2), round(m1,2)])
    return pd.DataFrame(results, columns=["Metrica", "Valor_Actual", "Delta_1D_%", "Delta_1M_%"])

df_resumen = get_metrics(df_final)

# ==========================================
# 4. CARGA A GOOGLE SHEETS
# ==========================================
print("📤 Actualizando Google Sheets...")
def write_ws(sh, name, df):
    try:
        ws = sh.worksheet(name)
    except:
        ws = sh.add_worksheet(title=name, rows="1000", cols="20")
    ws.clear()
    ws.update([df.columns.values.tolist()] + df.astype(str).replace('nan', '').values.tolist())
    time.sleep(2)

write_ws(sh, "DB_Historico", df_final)
write_ws(sh, "DB_Resumen", df_resumen)

# ==========================================
# 5. CEREBRO IA (Gemini)
# ==========================================
print("🧠 Generando análisis IA...")
prompt = f"""
Sos un analista financiero senior. Hoy es {hoy.strftime('%d/%m/%Y')}.
Datos clave: S&P 500 {df_final['SP500'].iloc[-1]}, Riesgo País {df_final['Riesgo_Pais'].iloc[-1]}, Brecha {df_final['Brecha_CCL'].iloc[-1]}%.
Redactá un flash de mercado de 3 párrafos cortos (Global, Local, Clave hoy). Estilo Bloomberg, sin markdown.
"""
url_ai = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-flash-latest:generateContent?key={GEMINI_API_KEY}"
try:
    res = requests.post(url_ai, json={"contents": [{"parts": [{"text": prompt}]}]}, timeout=20)
    texto_ia = res.json()['candidates'][0]['content']['parts'][0]['text']
    df_ai = pd.DataFrame([["Fecha", "Analisis_LLM"], [hoy.strftime('%d/%m/%Y'), texto_ia]])
    write_ws(sh, "DB_Insights", df_ai)
    print("✅ IA Finalizada.")
except Exception as e:
    print(f"⚠️ Error en IA: {e}")

print("🏁 Pipeline completado con éxito.")
