import os, json, requests, feedparser
import yfinance as yf
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta

print("Iniciando Pipeline V14 (Estable)...")

# --- CONFIGURACIÓN ---
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
gcp_json = os.environ.get("GCLOUD_SERVICE_ACCOUNT")
creds = Credentials.from_service_account_info(json.loads(gcp_json), scopes=['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive'])
sh = gspread.authorize(creds).open("Dashboard Macro")

hoy = datetime.today()
hace_1a = hoy - timedelta(days=365)

def fetch(url):
    try:
        r = requests.get(url, headers={"User-Agent":"Mozilla/5.0"}, timeout=15)
        return r.json() if r.status_code == 200 else []
    except: return []

# --- 1. DATOS MACRO ---
df_oficial = pd.DataFrame(fetch("https://api.argentinadatos.com/v1/cotizaciones/dolares/oficial")).rename(columns={"venta":"USD_Oficial"})
df_blue = pd.DataFrame(fetch("https://api.argentinadatos.com/v1/cotizaciones/dolares/blue")).rename(columns={"venta":"USD_Blue"})
df_rp = pd.DataFrame(fetch("https://api.argentinadatos.com/v1/finanzas/indices/riesgo-pais")).rename(columns={"valor":"Riesgo_Pais"})
df_ipc = pd.DataFrame(fetch("https://api.argentinadatos.com/v1/finanzas/indices/inflacion")).rename(columns={"valor":"IPC"})
df_emae = pd.DataFrame(fetch("https://api.argentinadatos.com/v1/finanzas/indices/emae")).rename(columns={"valor":"EMAE"})
df_ripte = pd.DataFrame(fetch("https://api.argentinadatos.com/v1/finanzas/indices/ripte")).rename(columns={"valor":"RIPTE"})

for df in [df_oficial, df_blue, df_rp, df_ipc, df_emae, df_ripte]:
    if not df.empty and "fecha" in df.columns: 
        df["fecha"] = pd.to_datetime(df["fecha"])

# --- 2. BENCHMARKS ---
def get_bench(months):
    try:
        ipc_history = df_ipc['IPC'].tail(months).astype(float) / 100
        ipc_acc = (1 + ipc_history).prod() - 1
        u_hoy = df_oficial['USD_Oficial'].iloc[-1]
        u_ant = df_oficial[df_oficial['fecha'] <= (hoy - timedelta(days=30*months))].iloc[-1]['USD_Oficial']
        dev = (u_hoy / u_ant) - 1
        return round((((1 + ipc_acc) / (1 + dev)) - 1) * 100, 2)
    except: return 4.0 if months==1 else 20.0

bench_1m = get_bench(1)
bench_1a = get_bench(12)

# --- 3. MERCADOS ---
tickers = {"SP500":"^GSPC", "Merval":"^MERV", "BTC":"BTC-USD", "Oro":"GC=F", "Brent":"BZ=F", "AL30":"AL30.BA", "GGAL_ADR":"GGAL", "GGAL_LOC":"GGAL.BA"}
df_m = pd.DataFrame()
for c, t in tickers.items():
    try:
        d = yf.download(t, start=hace_1a.strftime("%Y-%m-%d"), progress=False, auto_adjust=True)
        if not d.empty: df_m[c] = d["Close"]
    except: pass

if not df_m.empty:
    df_m = df_m.reset_index().rename(columns={"Date":"fecha"})
    df_m["fecha"] = pd.to_datetime(df_m["fecha"]).dt.tz_localize(None)

# --- 4. CONSOLIDACIÓN DE DATOS ---
df_final = df_m.copy()
for df_extra in [df_oficial, df_blue, df_rp, df_ipc, df_emae, df_ripte]:
    if not df_extra.empty:
        df_final = df_final.merge(df_extra, on="fecha", how="outer")

# Sanitización estricta de Nulos
df_final = df_final.sort_values("fecha").ffill().bfill()
df_final = df_final.fillna("")

if "GGAL_LOC" in df_final.columns and "GGAL_ADR" in df_final.columns:
    # Conversión numérica de seguridad antes de operar
    loc = pd.to_numeric(df_final["GGAL_LOC"], errors='coerce')
    adr = pd.to_numeric(df_final["GGAL_ADR"], errors='coerce')
    oficial = pd.to_numeric(df_final.get("USD_Oficial", pd.Series()), errors='coerce')
    
    df_final["CCL"] = (loc / (adr / 10)).round(2)
    if not oficial.empty:
        df_final["Brecha_CCL"] = (((df_final["CCL"] / oficial) - 1) * 100).round(2)

df_final = df_final.fillna("")

def write_ws(name, df):
    try: ws = sh.worksheet(name); ws.clear()
    except: ws = sh.add_worksheet(title=name, rows="1000", cols="20")
    ws.update([df.columns.values.tolist()] + df.astype(str).values.tolist())

write_ws("DB_Historico", df_final)

# --- 5. INTELIGENCIA ARTIFICIAL ---
prompt = f"""
Sos un Analista Financiero. Hoy es {hoy.strftime('%d/%m/%Y')}. 
1. Escribí un FLASH MARKET de máximo 3 items cortos y precisos.
2. Agregá un separador '---'.
3. Escribí un párrafo titulado '💡 EL DATO REAL:' explicando de forma técnica pero accesible qué significa que el país se haya encarecido/abaratado un {bench_1m}% en dólares este mes.
"""
# Actualización al modelo de producción estable gemini-1.5-flash
url_ia = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={GEMINI_API_KEY}"
try:
    res = requests.post(url_ia, json={"contents": [{"parts": [{"text": prompt}]}]})
    if res.status_code == 200:
        texto_ia = res.json()['candidates'][0]['content']['parts'][0]['text']
    else:
        texto_ia = "Error de API Gemini --- 💡 EL DATO REAL: No se pudo generar el análisis."
except: 
    texto_ia = "Error de Conexión --- 💡 EL DATO REAL: Fallo en la red del LLM."

write_ws("DB_Insights", pd.DataFrame({"Analisis_LLM": [texto_ia], "Bench_1M": [bench_1m], "Bench_1A": [bench_1a]}))
print("Pipeline V14 Finalizado.")
