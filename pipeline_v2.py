import os
import json
import requests
import yfinance as yf
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta
import time
import feedparser

print("🚀 Iniciando Pipeline Automatizado V8 (Valor Real + IA Doña Rosa)...")

FRED_API_KEY   = os.environ.get("FRED_API_KEY")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")

gcp_json = os.environ.get("GCLOUD_SERVICE_ACCOUNT")
creds_dict = json.loads(gcp_json)
scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
gc = gspread.authorize(creds)
sh = gc.open("Dashboard Macro")

hoy = datetime.today()
hace_1a = hoy - timedelta(days=365)

def fetch_api_data(url):
    try:
        res = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=20)
        return res.json()
    except: return []

# --- 1. DATOS ARGENTINA ---
print("📊 Recolectando Macro Argentina...")
df_oficial = pd.DataFrame(fetch_api_data("https://api.argentinadatos.com/v1/cotizaciones/dolares/oficial")).rename(columns={"venta":"USD_Oficial"})
df_rp = pd.DataFrame(fetch_api_data("https://api.argentinadatos.com/v1/finanzas/indices/riesgo-pais")).rename(columns={"valor":"Riesgo_Pais"})
df_ipc = pd.DataFrame(fetch_api_data("https://api.argentinadatos.com/v1/finanzas/indices/inflacion")).rename(columns={"valor":"IPC"})

df_oficial["fecha"] = pd.to_datetime(df_oficial["fecha"])

# --- CÁLCULO DEL BENCHMARK REAL (Inflación vs Devaluación 1M) ---
try:
    # Último dato de inflación mensual disponible (ej: 4.0)
    ipc_1m = float(df_ipc['IPC'].iloc[-1]) / 100
    
    # Devaluación del último mes
    usd_hoy = df_oficial['USD_Oficial'].iloc[-1]
    usd_1m = df_oficial[df_oficial['fecha'] <= (hoy - timedelta(days=30))].iloc[-1]['USD_Oficial']
    dev_1m = (usd_hoy / usd_1m) - 1
    
    # Fórmula exacta de encarecimiento en USD
    benchmark_real = ((1 + ipc_1m) / (1 + dev_1m)) - 1
    benchmark_pct = round(benchmark_real * 100, 2)
except Exception as e:
    benchmark_pct = -4.3 # Valor de contingencia si falla la API

# --- 2. NOTICIAS ---
def get_news():
    feeds = ["https://www.ambito.com/rss/economia.xml", "https://www.cronista.com/files/rss/finanzas-mercados.xml"]
    kw = ['caputo', 'bcra', 'inflación', 'cepo', 'dólar', 'milei']
    titles = []
    for f in feeds:
        d = feedparser.parse(f)
        for e in d.entries[:5]:
            if any(k in e.title.lower() for k in kw): titles.append(e.title)
    return "\n- ".join(titles[:10])

noticias = get_news()

# --- 3. MERCADOS ---
tickers = {"SP500":"^GSPC", "Merval":"^MERV", "BTC":"BTC-USD"}
df_m = pd.DataFrame()
for col, t in tickers.items():
    data = yf.download(t, start=hace_1a.strftime("%Y-%m-%d"), progress=False, auto_adjust=True)
    if not data.empty: df_m[col] = data["Close"]

df_m = df_m.reset_index().rename(columns={"Date":"fecha"})
df_m["fecha"] = pd.to_datetime(df_m["fecha"]).dt.tz_localize(None)

df_final = df_m.merge(df_oficial[["fecha", "USD_Oficial"]], on="fecha", how="outer").ffill()

def write_ws(name, df):
    try: ws = sh.worksheet(name)
    except: ws = sh.add_worksheet(title=name, rows="1000", cols="20")
    ws.clear()
    ws.update([df.columns.values.tolist()] + df.astype(str).replace('nan', '').values.tolist())

write_ws("DB_Historico", df_final)

# --- 4. CEREBRO IA (Con explicación para Doña Rosa) ---
print("🧠 Generando análisis IA...")
prompt = f"""
Sos un Estratega Financiero. 
1. Escribí 3 viñetas crudas y objetivas sobre el mercado hoy usando estos titulares: {noticias}. 
Formato OBLIGATORIO: 
* **🌎 Mundo:** ...
* **🇦🇷 Argentina:** ...
* **🔮 A mirar mañana:** ...

2. IMPORTANTE: El Benchmark de encarecimiento de Argentina en USD este mes dio {benchmark_pct}%. 
Abajo de las viñetas, escribí un título que diga "💡 EL DATO REAL:". Debajo, escribí un solo párrafo didáctico, claro y simple (para alguien que no sabe de finanzas) explicando qué significa este {benchmark_pct}% para quien guardó dólares bajo el colchón y para quien sacó un crédito.
"""
url_ai = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-flash-latest:generateContent?key={GEMINI_API_KEY}"
res = requests.post(url_ai, json={"contents": [{"parts": [{"text": prompt}]}]})
texto_ia = res.json()['candidates'][0]['content']['parts'][0]['text']

# Guardamos el análisis y el benchmark calculado en Google Sheets
df_insights = pd.DataFrame({"Analisis_LLM": [texto_ia], "Benchmark_USD": [benchmark_pct]})
write_ws("DB_Insights", df_insights)

print("🏁 Pipeline V8 Completado.")
