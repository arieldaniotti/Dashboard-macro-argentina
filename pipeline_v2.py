import os
import json
import time
import requests
import feedparser
import yfinance as yf
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime

print("=" * 60)
print("Pipeline V21 - Fix Sintaxis JSON")
print(f"Fecha corrida: {datetime.now(timezone.utc).isoformat()}")
print("=" * 60)

# ---------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
GCP_JSON = os.environ.get("GCLOUD_SERVICE_ACCOUNT")
GEMINI_MODEL = "gemini-2.5-flash"

if not GEMINI_API_KEY or not GCP_JSON:
    raise RuntimeError("Faltan secrets: GEMINI_API_KEY o GCLOUD_SERVICE_ACCOUNT")

creds = Credentials.from_service_account_info(
    json.loads(GCP_JSON),
    scopes=[
        "[https://www.googleapis.com/auth/spreadsheets](https://www.googleapis.com/auth/spreadsheets)",
        "[https://www.googleapis.com/auth/drive](https://www.googleapis.com/auth/drive)",
    ],
)
sh = gspread.authorize(creds).open("Dashboard Macro")

HOY = datetime.today()
HACE_1A = HOY - timedelta(days=365)

# ---------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------
def fetch_json(url, timeout=15):
    try:
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=timeout)
        if r.status_code == 200:
            return r.json()
        print(f"  ⚠ HTTP {r.status_code} en {url[:80]}")
        return []
    except Exception as e:
        print(f"  ⚠ Error fetch {url[:80]}: {e}")
        return []

def pct_change(new, old):
    try:
        if old is None or old == 0 or pd.isna(old): return 0.0
        return ((new / old) - 1) * 100
    except Exception: return 0.0

def abs_diff(new, old):
    try:
        if old is None or pd.isna(old): return 0.0
        return new - old
    except Exception: return 0.0

def get_historical_value(df, col, days_back):
    try:
        serie = df[["fecha", col]].copy()
        serie[col] = pd.to_numeric(serie[col], errors="coerce")
        serie = serie.dropna(subset=[col]).drop_duplicates(subset=["fecha"])
        if serie.empty: return None
        target = serie["fecha"].max() - timedelta(days=days_back)
        prev = serie[serie["fecha"] <= target]
        return prev[col].iloc[-1] if not prev.empty else serie[col].iloc[0]
    except Exception: return None

# ---------------------------------------------------------------
# 1. DATOS MACRO (argentinadatos.com para evitar bloqueos)
# ---------------------------------------------------------------
print("\n[1/8] Ingesta macro Argentina...")

endpoints_argdatos = {
    "oficial": ("[https://api.argentinadatos.com/v1/cotizaciones/dolares/oficial](https://api.argentinadatos.com/v1/cotizaciones/dolares/oficial)", "venta", "USD_Oficial"),
    "blue": ("[https://api.argentinadatos.com/v1/cotizaciones/dolares/blue](https://api.argentinadatos.com/v1/cotizaciones/dolares/blue)", "venta", "USD_Blue"),
    "rp": ("[https://api.argentinadatos.com/v1/finanzas/indices/riesgo-pais](https://api.argentinadatos.com/v1/finanzas/indices/riesgo-pais)", "valor", "Riesgo_Pais"),
    "ipc": ("[https://api.argentinadatos.com/v1/finanzas/indices/inflacion](https://api.argentinadatos.com/v1/finanzas/indices/inflacion)", "valor", "IPC"),
    "emae": ("[https://api.argentinadatos.com/v1/finanzas/indices/emae](https://api.argentinadatos.com/v1/finanzas/indices/emae)", "valor", "EMAE"),
    "salarios": ("[https://api.argentinadatos.com/v1/finanzas/indices/ripte](https://api.argentinadatos.com/v1/finanzas/indices/ripte)", "valor", "IndiceSalarios")
}

macro_dfs = {}
for key, (url, src_col, dest_col) in endpoints_argdatos.items():
    raw = fetch_json(url)
    df = pd.DataFrame(raw)
    if not df.empty and "fecha" in df.columns:
        df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
        df = df.rename(columns={src_col: dest_col})
        df = df[["fecha", dest_col]].dropna(subset=["fecha"]).sort_values("fecha")
        macro_dfs[key] = df
        print(f"  ✓ {dest_col}: {len(df)} filas, último {df['fecha'].max().date()}")
    else:
        print(f"  ✗ {dest_col}: sin datos")
        macro_dfs[key] = pd.DataFrame(columns=["fecha", dest_col])

df_emae_raw = macro_dfs["emae"]
df_salarios_raw = macro_dfs["salarios"]

# ---------------------------------------------------------------
# 3. IPC: métricas derivadas
# ---------------------------------------------------------------
print("\n[3/8] IPC: métricas derivadas...")

def ipc_serie_12m(df_ipc):
    try:
        s = df_ipc.tail(12)
        return (s["fecha"].dt.strftime("%b %y").tolist(), s["IPC"].astype(float).round(2).tolist())
    except Exception: return [], []

def ipc_interanual(df_ipc, meses=12):
    try:
        serie = df_ipc["IPC"].astype(float).tail(meses) / 100
        acum = (1 + serie).prod() - 1
        return round(acum * 100, 2)
    except Exception: return None

def ipc_mensual_ultimo(df_ipc):
    try: return float(df_ipc["IPC"].astype(float).iloc[-1])
    except Exception: return None

def ipc_aceleracion_pp(df_ipc):
    try:
        serie = df_ipc["IPC"].astype(float).tail(2)
        if len(serie) < 2: return None
        return round(float(serie.iloc[-1]) - float(serie.iloc[-2]), 2)
    except Exception: return None

df_ipc = macro_dfs["ipc"]
ipc_mes = ipc_mensual_ultimo(df_ipc) if not df_ipc.empty else None
ipc_yoy = ipc_interanual(df_ipc, 12) if not df_ipc.empty else None
ipc_accel = ipc_aceleracion_pp(df_ipc) if not df_ipc.empty else None
ipc_fechas_12m, ipc_valores_12m = ipc_serie_12m(df_ipc) if not df_ipc.empty else ([], [])

# ---------------------------------------------------------------
# 4. EMAE derivados
# ---------------------------------------------------------------
print("\n[4/8] EMAE derivados...")

def serie_12m(df, col):
    try:
        s = df.tail(12)
        return (s["fecha"].dt.strftime("%b %y").tolist(), pd.to_numeric(s[col], errors="coerce").round(2).tolist())
    except Exception: return [], []

emae_val = emae_yoy = emae_age_days = None
emae_fechas_12m, emae_valores_12m = [], []

if not df_emae_raw.empty:
    emae_val = float(df_emae_raw["EMAE"].iloc[-1])
    last_date = df_emae_raw["fecha"].iloc[-1]
    emae_age_days = (pd.Timestamp.now() - last_date).days
    target = last_date - timedelta(days=330)
    ant = df_emae_raw[df_emae_raw["fecha"] <= target]
    if not ant.empty:
        emae_yoy = round(pct_change(emae_val, ant["EMAE"].iloc[-1]), 2)
    emae_fechas_12m, emae_valores_12m = serie_12m(df_emae_raw, "EMAE")

# ---------------------------------------------------------------
# 5. SALARIO REAL
# ---------------------------------------------------------------
print("\n[5/8] Salario real (base 100 hace 12 meses)...")

salario_real_yoy = salario_real_age_days = None
salario_real_fechas, salario_real_valores = [], []

if not df_salarios_raw.empty and not df_ipc.empty:
    df_s = df_salarios_raw.copy()
    df_s["ym"] = df_s["fecha"].dt.to_period("M")
    df_s = df_s.drop_duplicates(subset=["ym"], keep="last")

    df_i = df_ipc.copy()
    df_i["ym"] = df_i["fecha"].dt.to_period("M")
    df_i = df_i.drop_duplicates(subset=["ym"], keep="last")

    merged = df_s.merge(df_i[["ym", "IPC"]], on="ym", how="inner").sort_values("ym")

    if len(merged) >= 13:
        slice_ = merged.tail(13).reset_index(drop=True)
        base_salario = float(slice_.iloc[0]["IndiceSalarios"])
        factor_ipc = 1.0
        for i in range(1, len(slice_)):
            factor_ipc *= (1 + float(slice_.iloc[i]["IPC"]) / 100)
            salario_nominal_idx = float(slice_.iloc[i]["IndiceSalarios"]) / base_salario
            salario_real = salario_nominal_idx / factor_ipc * 100
            salario_real_valores.append(round(salario_real, 2))
            salario_real_fechas.append(slice_.iloc[i]["ym"].strftime("%b %y"))

        if salario_real_valores:
            salario_real_yoy = round(salario_real_valores[-1] - 100, 2)
        salario_real_age_days = (pd.Timestamp.now() - df_salarios_raw["fecha"].iloc[-1]).days

# ---------------------------------------------------------------
# 6. BENCHMARK VALOR REAL USD
# ---------------------------------------------------------------
print("\n[6/8] Benchmark valor real USD...")

def get_bench(months):
    try:
        df_ipc_b = macro_dfs["ipc"]
        df_of = macro_dfs["oficial"]
        if df_ipc_b.empty or df_of.empty: return None
        ipc_hist = df_ipc_b["IPC"].astype(float).tail(months) / 100
        ipc_acc = (1 + ipc_hist).prod() - 1
        usd_hoy = float(df_of["USD_Oficial"].iloc[-1])
        target_date = HOY - timedelta(days=30 * months)
        usd_ant_rows = df_of[df_of["fecha"] <= target_date]
        if usd_ant_rows.empty: return None
        usd_ant = float(usd_ant_rows["USD_Oficial"].iloc[-1])
        dev = (usd_hoy / usd_ant) - 1
        return round((((1 + ipc_acc) / (1 + dev)) - 1) * 100, 2)
    except Exception: return None

bench_1m = get_bench(1)
bench_1a = get_bench(12)

# ---------------------------------------------------------------
# 7. MERCADOS + CONSOLIDACIÓN
# ---------------------------------------------------------------
print("\n[7/8] Mercados y consolidación...")

tickers = {"SP500": "^GSPC", "Merval": "^MERV", "BTC": "BTC-USD", "Oro": "GC=F", "Brent": "BZ=F", "AL30": "AL30.BA", "GGAL_ADR": "GGAL", "GGAL_LOC": "GGAL.BA"}
df_m = pd.DataFrame()
for col, tk in tickers.items():
    try:
        d = yf.download(tk, start=HACE_1A.strftime("%Y-%m-%d"), progress=False, auto_adjust=True)
        if not d.empty: df_m[col] = d["Close"]
    except Exception: pass

if not df_m.empty:
    df_m = df_m.reset_index().rename(columns={"Date": "fecha"})
    df_m["fecha"] = pd.to_datetime(df_m["fecha"]).dt.tz_localize(None)

df_final = df_m.copy()
for key in ["oficial", "blue", "rp", "ipc", "emae", "salarios"]:
    df = macro_dfs.get(key, pd.DataFrame())
    if not df.empty: df_final = df_final.merge(df, on="fecha", how="outer")

df_final = df_final.sort_values("fecha").reset_index(drop=True)

if "GGAL_LOC" in df_final.columns and "GGAL_ADR" in df_final.columns:
    loc = pd.to_numeric(df_final["GGAL_LOC"], errors="coerce")
    adr = pd.to_numeric(df_final["GGAL_ADR"], errors="coerce")
    df_final["CCL"] = (loc / (adr / 10)).round(2)
    oficial = pd.to_numeric(df_final.get("USD_Oficial", pd.Series()), errors="coerce")
    if not oficial.empty:
        df_final["Brecha_CCL"] = (((df_final["CCL"] / oficial) - 1) * 100).round(2)

df_final = df_final.ffill(limit=7)

def snapshot_ratio(col):
    if col not in df_final.columns: return None
    serie = df_final[["fecha", col]].copy()
    serie[col] = pd.to_numeric(serie[col], errors="coerce")
    serie = serie.dropna(subset=[col]).drop_duplicates(subset=["fecha"])
    if serie.empty: return None
    val = float(serie[col].iloc[-1])
    return {
        "val": round(val, 2), "mode": "ratio",
        "d1": round(pct_change(val, get_historical_value(df_final, col, 1)), 2),
        "m1": round(pct_change(val, get_historical_value(df_final, col, 30)), 2),
        "a1": round(pct_change(val, get_historical_value(df_final, col, 365)), 2),
    }

def snapshot_points(col):
    if col not in df_final.columns: return None
    serie = df_final[["fecha", col]].copy()
    serie[col] = pd.to_numeric(serie[col], errors="coerce")
    serie = serie.dropna(subset=[col]).drop_duplicates(subset=["fecha"])
    if serie.empty: return None
    val = float(serie[col].iloc[-1])
    return {
        "val": round(val, 0), "mode": "points",
        "d1": round(abs_diff(val, get_historical_value(df_final, col, 1)), 0),
        "m1": round(abs_diff(val, get_historical_value(df_final, col, 30)), 0),
        "a1": round(abs_diff(val, get_historical_value(df_final, col, 365)), 0),
    }

def snapshot_pp(col):
    if col not in df_final.columns: return None
    serie = df_final[["fecha", col]].copy()
    serie[col] = pd.to_numeric(serie[col], errors="coerce")
    serie = serie.dropna(subset=[col]).drop_duplicates(subset=["fecha"])
    if serie.empty: return None
    val = float(serie[col].iloc[-1])
    return {
        "val": round(val, 2), "mode": "pp",
        "d1": round(abs_diff(val, get_historical_value(df_final, col, 1)), 2),
        "m1": round(abs_diff(val, get_historical_value(df_final, col, 30)), 2),
        "a1": round(abs_diff(val, get_historical_value(df_final, col, 365)), 2),
    }

snapshots = {
    "sp500": snapshot_ratio("SP500"), "merval": snapshot_ratio("Merval"),
    "brent": snapshot_ratio("Brent"), "btc": snapshot_ratio("BTC"), "oro": snapshot_ratio("Oro"),
    "al30": snapshot_ratio("AL30"), "usd_oficial": snapshot_ratio("USD_Oficial"),
    "usd_blue": snapshot_ratio("USD_Blue"), "ccl": snapshot_ratio("CCL"),
    "brecha_ccl": snapshot_pp("Brecha_CCL"), "riesgo_pais": snapshot_points("Riesgo_Pais"),
}

def rend_valor_real(col, es_pesos, months):
    try:
        cols = ["fecha", col, "CCL"] if es_pesos else ["fecha", col]
        df = df_final[cols].copy()
        df[col] = pd.to_numeric(df[col], errors="coerce")
        if es_pesos:
            df["CCL"] = pd.to_numeric(df["CCL"], errors="coerce")
            df = df.dropna(subset=[col, "CCL"])
        else:
            df = df.dropna(subset=[col])
        if df.empty: return None
        v_h = df[col].iloc[-1]
        last = df["fecha"].iloc[-1]
        ccl_h = df["CCL"].iloc[-1] if es_pesos else 1
        df_a = df[df["fecha"] <= (last - timedelta(days=30 * months))]
        if df_a.empty:
            v_a = df[col].iloc[0]
            ccl_a = df["CCL"].iloc[0] if es_pesos else 1
        else:
            v_a = df_a[col].iloc[-1]
            ccl_a = df_a["CCL"].iloc[-1] if es_pesos else 1
        usd_h = v_h / ccl_h if es_pesos else v_h
        usd_a = v_a / ccl_a if es_pesos else v_a
        return round(((usd_h / usd_a) - 1) * 100, 2)
    except Exception: return None

valor_real_1m = {"Merval": rend_valor_real("Merval", True, 1), "AL30": rend_valor_real("AL30", True, 1), "S&P 500": rend_valor_real("SP500", False, 1), "BTC": rend_valor_real("BTC", False, 1), "Oro": rend_valor_real("Oro", False, 1)}
valor_real_1a = {"Merval": rend_valor_real("Merval", True, 12), "AL30": rend_valor_real("AL30", True, 12), "S&P 500": rend_valor_real("SP500", False, 12), "BTC": rend_valor_real("BTC", False, 12), "Oro": rend_valor_real("Oro", False, 12)}

# ---------------------------------------------------------------
# 8. NOTICIAS + PROMPTS
# ---------------------------------------------------------------
print("\n[8/8] Noticias + Gemini...")

def parse_date_safe(entry):
    for attr in ["published", "updated", "created"]:
        val = entry.get(attr)
        if not val: continue
        try: return parsedate_to_datetime(val).replace(tzinfo=None)
        except Exception: pass
        try: return datetime.fromisoformat(val.replace("Z", "+00:00")).replace(tzinfo=None)
        except Exception: pass
    return datetime.now()

RSS_SOURCES = {"Ámbito": "[https://www.ambito.com/rss/pages/economia.xml](https://www.ambito.com/rss/pages/economia.xml)", "Infobae": "[https://www.infobae.com/feeds/rss/economia/](https://www.infobae.com/feeds/rss/economia/)", "Cronista": "[https://www.cronista.com/files/rss/economia.xml](https://www.cronista.com/files/rss/economia.xml)"}
noticias = []
for medio, url in RSS_SOURCES.items():
    try:
        feed = feedparser.parse(url)
        for e in feed.entries[:10]:
            fecha = parse_date_safe(e)
            if (datetime.now() - fecha).total_seconds() / 3600 > 48: continue
            noticias.append({"medio": medio, "titulo": e.get("title", ""), "url": e.get("link", ""), "resumen": e.get("summary", "")[:100]})
    except Exception: pass

top_noticias_llm = noticias[:8]

def build_prompt_resumen():
    n_txt = "\n".join([f"[{n['medio']}] {n['titulo']}" for n in top_noticias_llm]) if top_noticias_llm else "Sin noticias."
    return f"""Analista financiero. Fecha: {HOY.strftime('%d/%m/%Y')}. Noticias: {n_txt}
Devolvé UNICAMENTE este JSON válido:
{{
  "mundo": "una oración global sobre las noticias",
  "argentina": "una oración local sobre las noticias",
  "a_mirar": "un evento clave a seguir",
  "noticias_destacadas": [{{"titular": "...", "medio": "...", "url": "...", "por_que_importa": "..."}}]
}}"""

def build_prompt_valor_real(rendimientos, bench, periodo_label):
    bench_str = f"{bench:+.2f}" if bench is not None else "0.00"
    return f"""Analista. Benchmark dólares quietos: {bench_str}%.
Devolvé UNICAMENTE este JSON válido:
{{"analisis": "Tres oraciones exactas. Qué significa el benchmark de {bench_str}% para quien guardó dólares. Qué activo rindió mejor/peor. Observación final."}}"""

def build_prompt_lectura_macro():
    return f"""Analista económico. Último IPC: {ipc_mes}%. EMAE YoY: {emae_yoy}%. Salario Real YoY: {salario_real_yoy}%.
Devolvé UNICAMENTE este JSON válido:
{{"lectura_macro": "Dos oraciones relacionando la actividad económica, los salarios y la inflación."}}"""

def llamar_gemini(prompt):
    url = f"[https://generativelanguage.googleapis.com/v1beta/models/](https://generativelanguage.googleapis.com/v1beta/models/){GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}"
    payload = {"contents": [{"parts": [{"text": prompt}]}], "generationConfig": {"temperature": 0.2}}
    for _ in range(3):
        try:
            r = requests.post(url, json=payload, timeout=60)
            if r.status_code == 200:
                return r.json()["candidates"][0]["content"]["parts"][0]["text"]
        except Exception: time.sleep(3)
    return None

# MÉTODO SEGURO DE PARSEO SIN REGEX (Soluciona el SyntaxError)
def parsear_json(texto):
    if not texto: return None
    limpio = texto.strip()
    if limpio.startswith("```json"):
        limpio = limpio[7:]
    elif limpio.startswith("```"):
        limpio = limpio[3:]
    if limpio.endswith("```"):
        limpio = limpio[:-3]
    try:
        return json.loads(limpio.strip())
    except Exception:
        return None

print("  → Llamadas a Gemini...")
resp_resumen = parsear_json(llamar_gemini(build_prompt_resumen())) or {}
resp_vr_1m = parsear_json(llamar_gemini(build_prompt_valor_real(valor_real_1m, bench_1m, "Mensual"))) or {}
resp_vr_1a = parsear_json(llamar_gemini(build_prompt_valor_real(valor_real_1a, bench_1a, "Anual"))) or {}
resp_macro = parsear_json(llamar_gemini(build_prompt_lectura_macro())) or {}

# ---------------------------------------------------------------
# ESCRITURA
# ---------------------------------------------------------------
print("\n[Escritura] Google Sheets...")
def write_ws(name, df):
    try:
        ws = sh.worksheet(name)
        ws.clear()
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=name, rows="1000", cols="30")
    ws.update([df.columns.values.tolist()] + df.astype(str).values.tolist())

df_out = df_final.fillna("")
write_ws("DB_Historico", df_out)

insights_df = pd.DataFrame({
    "fecha_corrida": [HOY.isoformat()],
    "mundo": [resp_resumen.get("mundo", "")],
    "argentina": [resp_resumen.get("argentina", "")],
    "a_mirar": [resp_resumen.get("a_mirar", "")],
    "analisis_vr_1m": [resp_vr_1m.get("analisis", "")],
    "analisis_vr_1a": [resp_vr_1a.get("analisis", "")],
    "lectura_macro": [resp_macro.get("lectura_macro", "")],
    "bench_1m": [bench_1m if bench_1m is not None else ""],
    "bench_1a": [bench_1a if bench_1a is not None else ""],
    "ipc_mes": [ipc_mes if ipc_mes is not None else ""],
    "ipc_yoy": [ipc_yoy if ipc_yoy is not None else ""],
    "ipc_accel_pp": [ipc_accel if ipc_accel is not None else ""],
    "emae_val": [emae_val if emae_val is not None else ""],
    "emae_yoy": [emae_yoy if emae_yoy is not None else ""],
    "emae_age_days": [emae_age_days if emae_age_days is not None else ""],
    "salario_real_yoy": [salario_real_yoy if salario_real_yoy is not None else ""],
    "salario_real_age_days": [salario_real_age_days if salario_real_age_days is not None else ""],
    "ipc_serie_json": [json.dumps({"fechas": ipc_fechas_12m, "valores": ipc_valores_12m}, ensure_ascii=False)],
    "emae_serie_json": [json.dumps({"fechas": emae_fechas_12m, "valores": emae_valores_12m}, ensure_ascii=False)],
    "salario_real_serie_json": [json.dumps({"fechas": salario_real_fechas, "valores": salario_real_valores}, ensure_ascii=False)],
    "snapshots_json": [json.dumps(snapshots, ensure_ascii=False)],
    "destacadas_json": [json.dumps(resp_resumen.get("noticias_destacadas", []), ensure_ascii=False)],
    "valor_real_1m_json": [json.dumps(valor_real_1m, ensure_ascii=False)],
    "valor_real_1a_json": [json.dumps(valor_real_1a, ensure_ascii=False)],
}).fillna("")

write_ws("DB_Insights", insights_df)

print("Pipeline V21 - Completado con éxito.")
