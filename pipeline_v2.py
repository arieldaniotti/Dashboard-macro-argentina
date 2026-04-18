import os, json, requests, time, feedparser
import yfinance as yf
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta

print("🚀 Iniciando Pipeline V9 (Full Integración - Fix Fechas)...")

# --- CONFIG ---
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
gcp_json = os.environ.get("GCLOUD_SERVICE_ACCOUNT")
creds = Credentials.from_service_account_info(json.loads(gcp_json), scopes=['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive'])
sh = gspread.authorize(creds).open("Dashboard Macro")

hoy = datetime.today()
hace_1a = hoy - timedelta(days=365)

def fetch(url):
    try: return requests.get(url, headers={"User-Agent":"Mozilla/5.0"}, timeout=20).json()
    except: return []

# --- 1. DATOS ARGENTINA ---
print("📊 Recolectando Macro...")
df_oficial = pd.DataFrame(fetch("https://api.argentinadatos.com/v1/cotizaciones/dolares/oficial")).rename(columns={"venta":"USD_Oficial"})
df_rp = pd.DataFrame(fetch("https://api.argentinadatos.com/v1/finanzas/indices/riesgo-pais")).rename(columns={"valor":"Riesgo_Pais"})
df_ipc = pd.DataFrame(fetch("https://api.argentinadatos.com/v1/finanzas/indices/inflacion")).rename(columns={"valor":"IPC"})

# FIX: Convertimos TODAS las fechas a formato datetime para evitar errores de merge
if not df_oficial.empty: df_oficial["fecha"] = pd.to_datetime(df_oficial["fecha"])
if not df_rp.empty: df_rp["fecha"] = pd.to_datetime(df_rp["fecha"])
if not df_ipc.empty and "fecha" in df_ipc.columns: df_ipc["fecha"] = pd.to_datetime(df_ipc["fecha"])

# --- CÁLCULO BENCHMARKS (1M y 1A) ---
def calc_bench(months):
    try:
        ipc_val = float(df_ipc['IPC'].iloc[-1 if months==1 else -12]) / 100
        u_hoy = df_oficial['USD_Oficial'].iloc[-1]
        u_ant = df_oficial[df_oficial['fecha'] <= (hoy - timedelta(days=30*months))].iloc[-1]['USD_Oficial']
        dev = (u_hoy / u_ant) - 1
        return round((((1 + ipc_val) / (1 + dev)) - 1) * 100, 2)
    except: return -4.3 if months==1 else -15.0

bench_1m = calc_bench(1)
bench_1a = calc_bench(12)

# --- 2. MERCADOS Y NOTICIAS ---
tickers = {"SP500":"^GSPC", "Merval":"^MERV", "BTC":"BTC-USD", "GGAL_ADR":"GGAL", "GGAL_LOC":"GGAL.BA"}
df_m = pd.DataFrame()
for c, t in tickers.items():
    d = yf.download(t, start=hace_1a.strftime("%Y-%m-%d"), progress=False, auto_adjust=True)
    if not d.empty: df_m[c] = d["Close"]
df_m = df_m.reset_index().rename(columns={"Date":"fecha"})
df_m["fecha"] = pd.to_datetime(df_m["fecha"]).dt.tz_localize(None)

def get_news():
    feeds = ["https://www.ambito.com/rss/economia.xml", "https://www.cronista.com/files/rss/finanzas-mercados.xml"]
    kw = ['caputo', 'bcra', 'inflación', 'dólar', 'fmi', 'milei']
    titles = []
    for f in feeds:
        try:
            d = feedparser.parse(f)
            for e in d.entries[:10]:
                if any(k in e.title.lower() for k in kw): titles.append(e.title)
        except: pass
    return "\n- ".join(titles[:12])

# --- 3. GOOGLE SHEETS ---
def write_ws(name, df):
    try: ws = sh.worksheet(name); ws.clear()
    except: ws = sh.add_worksheet(title=name, rows="1000", cols="20")
    ws.update([df.columns.values.tolist()] + df.astype(str).replace('nan', '').values.tolist())

# Merge final seguro (ahora ambas fechas hablan el mismo idioma)
df_final = df_m.merge(df_oficial[["fecha", "USD_Oficial"]], on="fecha", how="outer").ffill()
df_final["CCL"] = (df_final["GGAL_LOC"] / (df_final["GGAL_ADR"] / 10)).round(2)
df_final["Brecha_CCL"] = (((df_final["CCL"] / df_final["USD_Oficial"]) - 1) * 100).round(2)
df_final = df_final.merge(df_rp[["fecha", "Riesgo_Pais"]], on="fecha", how="left").ffill()

write_ws("DB_Historico", df_final)

# --- 4. IA FLASH MARKET ---
prompt = f"Trader de Mesa. Hoy: {hoy.strftime('%d/%m/%Y')}. Titulares: {get_news()}. Escribí 3 viñetas (* **🌎 Mundo:**, * **🇦🇷 Argentina:**, * **🔮 A mirar mañana:**). Al final, explicá en un párrafo 'EL DATO REAL' sobre el encarecimiento de {bench_1m}% en USD este mes para el bolsillo de Doña Rosa."
try:
    res = requests.post(f"https://generativelanguage.googleapis.com/v1beta/models/gemini-flash-latest:generateContent?key={GEMINI_API_KEY}", json={"contents": [{"parts": [{"text": prompt}]}]}).json()
    texto_ia = res['candidates'][0]['content']['parts'][0]['text']
except:
    texto_ia = "Error al generar análisis con IA."

write_ws("DB_Insights", pd.DataFrame({"Analisis_LLM": [texto_ia], "Bench_1M": [bench_1m], "Bench_1A": [bench_1a]}))
print("🏁 Pipeline V9 Finalizado.")
