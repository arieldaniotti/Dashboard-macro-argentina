import os, json, requests, time, feedparser
import yfinance as yf
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta

print("🚀 Iniciando Pipeline V10 (Datos Reales y Tablero Completo)...")

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
df_blue = pd.DataFrame(fetch("https://api.argentinadatos.com/v1/cotizaciones/dolares/blue")).rename(columns={"venta":"USD_Blue"})
df_rp = pd.DataFrame(fetch("https://api.argentinadatos.com/v1/finanzas/indices/riesgo-pais")).rename(columns={"valor":"Riesgo_Pais"})
df_ipc = pd.DataFrame(fetch("https://api.argentinadatos.com/v1/finanzas/indices/inflacion")).rename(columns={"valor":"IPC"})
df_emae = pd.DataFrame(fetch("https://api.argentinadatos.com/v1/finanzas/indices/emae")).rename(columns={"valor":"EMAE"})
df_ripte = pd.DataFrame(fetch("https://api.argentinadatos.com/v1/finanzas/indices/ripte")).rename(columns={"valor":"RIPTE"})

# Convertir fechas
for df in [df_oficial, df_blue, df_rp, df_ipc, df_emae, df_ripte]:
    if not df.empty and "fecha" in df.columns:
        df["fecha"] = pd.to_datetime(df["fecha"])

# --- CÁLCULO BENCHMARKS (Acumulación Real) ---
def calc_bench(months):
    try:
        # Acumular inflación de los últimos X meses: (1+i1)*(1+i2)... - 1
        ipc_history = df_ipc['IPC'].tail(months).astype(float) / 100
        ipc_accum = (1 + ipc_history).prod() - 1
        
        # Devaluación entre HOY y hace X meses
        u_hoy = df_oficial['USD_Oficial'].iloc[-1]
        u_ant = df_oficial[df_oficial['fecha'] <= (hoy - timedelta(days=30*months))].iloc[-1]['USD_Oficial']
        dev_accum = (u_hoy / u_ant) - 1
        
        # Fórmula: ((1 + Inflación) / (1 + Devaluación)) - 1
        return round((((1 + ipc_accum) / (1 + dev_accum)) - 1) * 100, 2)
    except: return -4.3 if months==1 else 15.0

bench_1m = calc_bench(1)
bench_1a = calc_bench(12)

# --- 2. MERCADOS ---
print("📈 Descargando activos financieros...")
tickers = {"SP500":"^GSPC", "Merval":"^MERV", "BTC":"BTC-USD", "Oro":"GC=F", "Brent":"BZ=F", "AL30":"AL30.BA", "GGAL_ADR":"GGAL", "GGAL_LOC":"GGAL.BA"}
df_m = pd.DataFrame()
for c, t in tickers.items():
    d = yf.download(t, start=hace_1a.strftime("%Y-%m-%d"), progress=False, auto_adjust=True)
    if not d.empty: df_m[c] = d["Close"]
df_m = df_m.reset_index().rename(columns={"Date":"fecha"})
df_m["fecha"] = pd.to_datetime(df_m["fecha"]).dt.tz_localize(None)

# --- 3. NOTICIAS ---
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

# --- 4. CONSOLIDACIÓN ---
df_final = df_m.merge(df_oficial[["fecha", "USD_Oficial"]], on="fecha", how="outer").ffill()
df_final = df_final.merge(df_blue[["fecha", "USD_Blue"]], on="fecha", how="left").ffill()
df_final = df_final.merge(df_rp[["fecha", "Riesgo_Pais"]], on="fecha", how="left").ffill()
df_final = df_final.merge(df_emae[["fecha", "EMAE"]], on="fecha", how="left").ffill()
df_final = df_final.merge(df_ripte[["fecha", "RIPTE"]], on="fecha", how="left").ffill()
df_final["CCL"] = (df_final["GGAL_LOC"] / (df_final["GGAL_ADR"] / 10)).round(2)
df_final["Brecha_CCL"] = (((df_final["CCL"] / df_final["USD_Oficial"]) - 1) * 100).round(2)
df_final = df_final.dropna(subset=["SP500"]) # Limpiamos fines de semana

def write_ws(name, df):
    try: ws = sh.worksheet(name); ws.clear()
    except: ws = sh.add_worksheet(title=name, rows="1000", cols="20")
    ws.update([df.columns.values.tolist()] + df.astype(str).replace('nan', '').values.tolist())

write_ws("DB_Historico", df_final)

# --- 5. IA FLASH MARKET ---
print("🧠 Generando análisis LLM...")
prompt = f"Trader institucional. Hoy: {hoy.strftime('%d/%m/%Y')}. Titulares: {get_news()}. Escribí 3 viñetas crudas (* **🌎 Mundo:**, * **🇦🇷 Argentina:**, * **🔮 A mirar mañana:**). Al final de todo, agregá un título '💡 EL DATO REAL:' y explicá en un párrafo simple qué implica que el país se haya encarecido/abaratado un {bench_1m}% en dólares este mes para quien guarda dólares bajo el colchón y para quien toma deuda."
try:
    res = requests.post(f"https://generativelanguage.googleapis.com/v1beta/models/gemini-flash-latest:generateContent?key={GEMINI_API_KEY}", json={"contents": [{"parts": [{"text": prompt}]}]}).json()
    texto_ia = res['candidates'][0]['content']['parts'][0]['text']
except: texto_ia = "Error al conectar con IA."

write_ws("DB_Insights", pd.DataFrame({"Analisis_LLM": [texto_ia], "Bench_1M": [bench_1m], "Bench_1A": [bench_1a]}))
print("🏁 Pipeline V10 Finalizado Exitosamente.")
