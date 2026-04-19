import os, json, requests, time, feedparser
import yfinance as yf
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta

print("🚀 Iniciando Pipeline V13 (Fijando datos faltantes)...")

# --- CONFIG ---
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

# --- 1. RECOLECCIÓN MACRO ---
print("📊 Recolectando Macro Arg...")
df_oficial = pd.DataFrame(fetch("https://api.argentinadatos.com/v1/cotizaciones/dolares/oficial")).rename(columns={"venta":"USD_Oficial"})
df_blue = pd.DataFrame(fetch("https://api.argentinadatos.com/v1/cotizaciones/dolares/blue")).rename(columns={"venta":"USD_Blue"})
df_rp = pd.DataFrame(fetch("https://api.argentinadatos.com/v1/finanzas/indices/riesgo-pais")).rename(columns={"valor":"Riesgo_Pais"})
df_ipc = pd.DataFrame(fetch("https://api.argentinadatos.com/v1/finanzas/indices/inflacion")).rename(columns={"valor":"IPC"})
df_emae = pd.DataFrame(fetch("https://api.argentinadatos.com/v1/finanzas/indices/emae")).rename(columns={"valor":"EMAE"})
df_ripte = pd.DataFrame(fetch("https://api.argentinadatos.com/v1/finanzas/indices/ripte")).rename(columns={"valor":"RIPTE"})

# Formateo de fechas
for df in [df_oficial, df_blue, df_rp, df_ipc, df_emae, df_ripte]:
    if not df.empty: df["fecha"] = pd.to_datetime(df["fecha"])

# --- 2. CÁLCULO DE BENCHMARKS ---
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

# --- 3. MERCADO ---
print("📈 Descargando tickers globales y locales...")
# FIX: Volvieron el Oro y el Brent a la lista de descargas
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

# --- 4. CONSOLIDACIÓN FINAL ---
df_final = df_m.copy()
for df_extra in [df_oficial, df_blue, df_rp, df_ipc, df_emae, df_ripte]:
    if not df_extra.empty:
        df_final = df_final.merge(df_extra, on="fecha", how="outer")

# FIX: Ordenar por fecha ANTES de rellenar los vacíos (vital para EMAE y RIPTE)
df_final = df_final.sort_values("fecha").ffill()

# Seguro por si la API de ArgentinaDatos se cae un día, para que no rompa el dashboard
for col in ["Oro", "Brent", "EMAE", "RIPTE", "IPC"]:
    if col not in df_final.columns:
        df_final[col] = pd.NA

if "GGAL_LOC" in df_final.columns and "GGAL_ADR" in df_final.columns:
    df_final["CCL"] = (df_final["GGAL_LOC"] / (df_final["GGAL_ADR"] / 10)).round(2)
    if "USD_Oficial" in df_final.columns:
        df_final["Brecha_CCL"] = (((df_final["CCL"] / df_final["USD_Oficial"]) - 1) * 100).round(2)

df_final = df_final.dropna(subset=["SP500"])

def write_ws(name, df):
    try: ws = sh.worksheet(name); ws.clear()
    except: ws = sh.add_worksheet(title=name, rows="1000", cols="20")
    ws.update([df.columns.values.tolist()] + df.astype(str).replace('nan', '').values.tolist())

write_ws("DB_Historico", df_final)

# --- 5. IA FLASH MARKET ---
print("🧠 IA Generando reportes...")
prompt = f"""
Sos un Trader. Hoy es {hoy.strftime('%d/%m/%Y')}. 
1. Escribí un FLASH MARKET de máximo 3 items cortos (Mundo, Arg, Mañana).
2. Agregá un separador '---'.
3. Escribí un párrafo titulado '💡 EL DATO REAL:' explicando que el país se encareció/abarató un {bench_1m}% en dólares este mes. Sé muy didáctico para 'Doña Rosa'.
"""
url_ia = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-flash-latest:generateContent?key={GEMINI_API_KEY}"
try:
    res = requests.post(url_ia, json={"contents": [{"parts": [{"text": prompt}]}]}).json()
    texto_ia = res['candidates'][0]['content']['parts'][0]['text']
except: texto_ia = "Error LLM --- 💡 EL DATO REAL: Hubo un problema al cargar el análisis."

df_insights = pd.DataFrame({"Analisis_LLM": [texto_ia], "Bench_1M": [bench_1m], "Bench_1A": [bench_1a]})
write_ws("DB_Insights", df_insights)

print("🏁 Pipeline V13 Finalizado.")
