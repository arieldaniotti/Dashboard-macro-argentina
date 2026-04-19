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
print("Pipeline V15 - Iniciando")
print(f"Fecha corrida: {datetime.now(timezone.utc).isoformat()}")
print("=" * 60)
 
# ---------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
GCP_JSON = os.environ.get("GCLOUD_SERVICE_ACCOUNT")
 
if not GEMINI_API_KEY or not GCP_JSON:
    raise RuntimeError("Faltan secrets: GEMINI_API_KEY o GCLOUD_SERVICE_ACCOUNT")
 
creds = Credentials.from_service_account_info(
    json.loads(GCP_JSON),
    scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
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
        r = requests.get(
            url,
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=timeout,
        )
        if r.status_code == 200:
            return r.json()
        print(f"  ⚠ HTTP {r.status_code} en {url[:80]}")
        return []
    except Exception as e:
        print(f"  ⚠ Error fetch {url[:80]}: {e}")
        return []
 
 
def pct_change(new, old):
    """Variación porcentual segura."""
    try:
        if old is None or old == 0 or pd.isna(old):
            return 0.0
        return ((new / old) - 1) * 100
    except Exception:
        return 0.0
 
 
def points_change(new, old):
    try:
        if old is None or pd.isna(old):
            return 0.0
        return new - old
    except Exception:
        return 0.0
 
 
def get_historical_value(df, col, days_back):
    """
    Devuelve el valor de col hace days_back días.
    Usa el último registro con fecha <= target. Si no hay, usa el primero.
 
    Importante: hace dropna primero, así si el ffill dejó duplicados, no afecta.
    """
    try:
        serie = df[["fecha", col]].copy()
        serie[col] = pd.to_numeric(serie[col], errors="coerce")
        serie = serie.dropna(subset=[col]).drop_duplicates(subset=["fecha"])
        if serie.empty:
            return None
        target = serie["fecha"].max() - timedelta(days=days_back)
        prev = serie[serie["fecha"] <= target]
        return prev[col].iloc[-1] if not prev.empty else serie[col].iloc[0]
    except Exception as e:
        print(f"  ⚠ get_historical_value({col}, {days_back}d): {e}")
        return None
 
 
# ---------------------------------------------------------------
# 1. DATOS MACRO (APIs argentinadatos)
# ---------------------------------------------------------------
print("\n[1/5] Ingesta macro Argentina...")
 
endpoints = {
    "oficial": (
        "https://api.argentinadatos.com/v1/cotizaciones/dolares/oficial",
        "venta", "USD_Oficial",
    ),
    "blue": (
        "https://api.argentinadatos.com/v1/cotizaciones/dolares/blue",
        "venta", "USD_Blue",
    ),
    "rp": (
        "https://api.argentinadatos.com/v1/finanzas/indices/riesgo-pais",
        "valor", "Riesgo_Pais",
    ),
    "ipc": (
        "https://api.argentinadatos.com/v1/finanzas/indices/inflacion",
        "valor", "IPC",
    ),
    "emae": (
        "https://api.argentinadatos.com/v1/finanzas/indices/emae",
        "valor", "EMAE",
    ),
    "ripte": (
        "https://api.argentinadatos.com/v1/finanzas/indices/ripte",
        "valor", "RIPTE",
    ),
}
 
macro_dfs = {}
for key, (url, src_col, dest_col) in endpoints.items():
    raw = fetch_json(url)
    df = pd.DataFrame(raw)
    if not df.empty and "fecha" in df.columns:
        df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
        df = df.rename(columns={src_col: dest_col})
        df = df[["fecha", dest_col]].dropna(subset=["fecha"])
        macro_dfs[key] = df
        print(f"  ✓ {dest_col}: {len(df)} filas")
    else:
        print(f"  ✗ {dest_col}: sin datos")
        macro_dfs[key] = pd.DataFrame(columns=["fecha", dest_col])
 
 
# ---------------------------------------------------------------
# 2. BENCHMARK VALOR REAL USD
# ---------------------------------------------------------------
print("\n[2/5] Cálculo benchmark valor real USD...")
 
 
def get_bench(months):
    """
    Benchmark = (1 + IPC_acum) / (1 + devaluación_oficial) - 1.
    Mide cuánto se abarató/encareció Argentina en USD.
 
    Positivo = Argentina se encareció (malo para dólares quietos).
    Negativo = Argentina se abarató (bueno para dólares quietos).
    """
    try:
        df_ipc = macro_dfs["ipc"]
        df_of = macro_dfs["oficial"]
        if df_ipc.empty or df_of.empty:
            return None
 
        ipc_hist = df_ipc["IPC"].astype(float).tail(months) / 100
        ipc_acc = (1 + ipc_hist).prod() - 1
 
        usd_hoy = df_of["USD_Oficial"].astype(float).iloc[-1]
        target_date = HOY - timedelta(days=30 * months)
        usd_ant_rows = df_of[df_of["fecha"] <= target_date]
        if usd_ant_rows.empty:
            return None
        usd_ant = usd_ant_rows["USD_Oficial"].astype(float).iloc[-1]
        dev = (usd_hoy / usd_ant) - 1
 
        return round((((1 + ipc_acc) / (1 + dev)) - 1) * 100, 2)
    except Exception as e:
        print(f"  ⚠ get_bench({months}m): {e}")
        return None
 
 
bench_1m = get_bench(1)
bench_1a = get_bench(12)
print(f"  ✓ Benchmark 1M: {bench_1m}% | Benchmark 1A: {bench_1a}%")
 
 
# ---------------------------------------------------------------
# 3. MERCADOS (Yahoo Finance)
# ---------------------------------------------------------------
print("\n[3/5] Ingesta mercados (Yahoo)...")
 
tickers = {
    "SP500": "^GSPC",
    "Merval": "^MERV",
    "BTC": "BTC-USD",
    "Oro": "GC=F",
    "Brent": "BZ=F",
    "AL30": "AL30.BA",
    "GGAL_ADR": "GGAL",
    "GGAL_LOC": "GGAL.BA",
}
 
df_m = pd.DataFrame()
for col, tk in tickers.items():
    try:
        d = yf.download(
            tk,
            start=HACE_1A.strftime("%Y-%m-%d"),
            progress=False,
            auto_adjust=True,
        )
        if not d.empty:
            df_m[col] = d["Close"]
            print(f"  ✓ {col}: {len(d)} filas")
        else:
            print(f"  ✗ {col}: vacío")
    except Exception as e:
        print(f"  ✗ {col}: {e}")
 
if not df_m.empty:
    df_m = df_m.reset_index().rename(columns={"Date": "fecha"})
    df_m["fecha"] = pd.to_datetime(df_m["fecha"]).dt.tz_localize(None)
 
 
# ---------------------------------------------------------------
# 4. CONSOLIDACIÓN
# ---------------------------------------------------------------
print("\n[4/5] Consolidación...")
 
df_final = df_m.copy()
for key, df in macro_dfs.items():
    if not df.empty:
        df_final = df_final.merge(df, on="fecha", how="outer")
 
df_final = df_final.sort_values("fecha").reset_index(drop=True)
 
# Calcular CCL antes de ffill, con valores reales
if "GGAL_LOC" in df_final.columns and "GGAL_ADR" in df_final.columns:
    loc = pd.to_numeric(df_final["GGAL_LOC"], errors="coerce")
    adr = pd.to_numeric(df_final["GGAL_ADR"], errors="coerce")
    df_final["CCL"] = (loc / (adr / 10)).round(2)
    oficial = pd.to_numeric(df_final.get("USD_Oficial", pd.Series()), errors="coerce")
    if not oficial.empty:
        df_final["Brecha_CCL"] = (((df_final["CCL"] / oficial) - 1) * 100).round(2)
 
# Forward-fill con límite de 7 días para no propagar datos viejos para siempre
df_final = df_final.ffill(limit=7)
 
print(f"  ✓ df_final: {len(df_final)} filas, {len(df_final.columns)} columnas")
 
 
# ---------------------------------------------------------------
# 5. NOTICIAS (RSS)
# ---------------------------------------------------------------
print("\n[5/5] Ingesta noticias (RSS)...")
 
RSS_SOURCES = {
    "Ámbito": "https://www.ambito.com/rss/pages/economia.xml",
    "Infobae": "https://www.infobae.com/feeds/rss/economia/",
    "Cronista": "https://www.cronista.com/files/rss/economia.xml",
    "iProfesional": "https://www.iprofesional.com/rss",
    "El Economista": "https://eleconomista.com.ar/arc/outboundfeeds/rss/?outputType=xml",
    "Investing": "https://es.investing.com/rss/news_25.rss",
}
 
KEYWORDS_ALTA = [
    "caputo", "milei", "fmi", "bcra", "riesgo país", "riesgo pais",
    "fed", "powell", "dólar", "dolar", "cepo", "reservas",
    "licitación", "licitacion", "trump", "ipc", "inflación", "inflacion",
    "lagarde", "tasa de interés", "tasa de interes",
]
KEYWORDS_MEDIA = [
    "bonos", "merval", "ccl", "mep", "lecap", "plazo fijo", "tasa",
    "bopreal", "acciones", "wall street", "s&p", "nasdaq",
    "china", "petróleo", "petroleo", "brent", "oro", "bitcoin",
    "brasil", "selic", "yuan", "euro",
]
 
 
def parse_date_safe(entry):
    """RSS fechas vienen en mil formatos. Intenta parsearlas todas."""
    for attr in ["published", "updated", "created"]:
        val = entry.get(attr)
        if not val:
            continue
        try:
            return parsedate_to_datetime(val).replace(tzinfo=None)
        except Exception:
            pass
        try:
            return datetime.fromisoformat(val.replace("Z", "+00:00")).replace(tzinfo=None)
        except Exception:
            pass
    return datetime.now()
 
 
def score_noticia(titulo, resumen, fecha_pub):
    texto = (titulo + " " + resumen).lower()
    score = 0
    score += sum(3 for kw in KEYWORDS_ALTA if kw in texto)
    score += sum(1 for kw in KEYWORDS_MEDIA if kw in texto)
    horas = max(0, (datetime.now() - fecha_pub).total_seconds() / 3600)
    score *= max(0.3, 1 - horas / 48)
    return round(score, 2)
 
 
noticias = []
for medio, url in RSS_SOURCES.items():
    try:
        feed = feedparser.parse(url)
        entries = feed.entries[:10]
        if not entries:
            print(f"  ✗ {medio}: sin entradas")
            continue
        for e in entries:
            fecha = parse_date_safe(e)
            horas = (datetime.now() - fecha).total_seconds() / 3600
            if horas > 48:
                continue
            titulo = e.get("title", "").strip()
            resumen = e.get("summary", "").strip()[:300]
            resumen = resumen.replace("<p>", "").replace("</p>", "").replace("&nbsp;", " ")
            noticias.append({
                "medio": medio,
                "titulo": titulo,
                "resumen": resumen,
                "url": e.get("link", ""),
                "fecha": fecha.isoformat(),
                "score": score_noticia(titulo, resumen, fecha),
            })
        print(f"  ✓ {medio}: {len(entries)} entradas")
    except Exception as ex:
        print(f"  ✗ {medio}: {ex}")
 
noticias = sorted(noticias, key=lambda x: x["score"], reverse=True)
top15 = noticias[:15]
print(f"  → Total noticias: {len(noticias)} | Top 15 para LLM")
 
 
# ---------------------------------------------------------------
# 6. PREPARAR CONTEXTO PARA GEMINI
# ---------------------------------------------------------------
print("\n[6/7] Armando contexto para Gemini...")
 
 
def snapshot(col):
    """Snapshot de un activo: valor actual + variaciones 1D/1M/1A."""
    if col not in df_final.columns:
        return None
    serie = df_final[["fecha", col]].copy()
    serie[col] = pd.to_numeric(serie[col], errors="coerce")
    serie = serie.dropna(subset=[col]).drop_duplicates(subset=["fecha"])
    if serie.empty:
        return None
    val = float(serie[col].iloc[-1])
    return {
        "val": round(val, 2),
        "d1": round(pct_change(val, get_historical_value(df_final, col, 1)), 2),
        "m1": round(pct_change(val, get_historical_value(df_final, col, 30)), 2),
        "a1": round(pct_change(val, get_historical_value(df_final, col, 365)), 2),
    }
 
 
def snapshot_pts(col):
    """Snapshot en puntos (para riesgo país)."""
    if col not in df_final.columns:
        return None
    serie = df_final[["fecha", col]].copy()
    serie[col] = pd.to_numeric(serie[col], errors="coerce")
    serie = serie.dropna(subset=[col]).drop_duplicates(subset=["fecha"])
    if serie.empty:
        return None
    val = float(serie[col].iloc[-1])
    return {
        "val": round(val, 2),
        "d1": round(points_change(val, get_historical_value(df_final, col, 1)), 0),
        "m1": round(points_change(val, get_historical_value(df_final, col, 30)), 0),
        "a1": round(points_change(val, get_historical_value(df_final, col, 365)), 0),
    }
 
 
snapshots = {
    "sp500": snapshot("SP500"),
    "merval": snapshot("Merval"),
    "brent": snapshot("Brent"),
    "btc": snapshot("BTC"),
    "oro": snapshot("Oro"),
    "al30": snapshot("AL30"),
    "usd_oficial": snapshot("USD_Oficial"),
    "usd_blue": snapshot("USD_Blue"),
    "ccl": snapshot("CCL"),
    "brecha_ccl": snapshot("Brecha_CCL"),
    "riesgo_pais": snapshot_pts("Riesgo_Pais"),
    "ipc": snapshot("IPC"),
}
 
 
# ---------------------------------------------------------------
# 7. LLAMADA A GEMINI (JSON estructurado)
# ---------------------------------------------------------------
def build_prompt():
    # Formatear datos para el prompt (solo los que existan)
    def fmt(label, s, unit="%", prefix=""):
        if not s:
            return f"- {label}: sin datos"
        return (
            f"- {label}: {prefix}{s['val']} | "
            f"1D: {s['d1']:+.2f}{unit} | "
            f"1M: {s['m1']:+.2f}{unit} | "
            f"1A: {s['a1']:+.2f}{unit}"
        )
 
    def fmt_pts(label, s):
        if not s:
            return f"- {label}: sin datos"
        return (
            f"- {label}: {s['val']:.0f} bps | "
            f"1D: {s['d1']:+.0f} bps | "
            f"1M: {s['m1']:+.0f} bps | "
            f"1A: {s['a1']:+.0f} bps"
        )
 
    bloque_mundo = "\n".join([
        fmt("S&P 500", snapshots["sp500"]),
        fmt("Brent", snapshots["brent"], prefix="USD "),
        fmt("Bitcoin", snapshots["btc"], prefix="USD "),
        fmt("Oro", snapshots["oro"], prefix="USD "),
    ])
 
    bloque_ar = "\n".join([
        fmt("Merval", snapshots["merval"]),
        fmt("AL30", snapshots["al30"]),
        fmt("Dólar oficial", snapshots["usd_oficial"], prefix="$"),
        fmt("CCL", snapshots["ccl"], prefix="$"),
        fmt("Brecha CCL", snapshots["brecha_ccl"]),
        fmt_pts("Riesgo País", snapshots["riesgo_pais"]),
        fmt("IPC último mes", snapshots["ipc"]),
    ])
 
    bench_txt = (
        f"- Benchmark real USD mensual: {bench_1m:+.2f}% "
        f"(positivo = Argentina se encareció; negativo = Argentina se abarató)"
        if bench_1m is not None
        else "- Benchmark real USD: sin datos"
    )
 
    noticias_txt = "\n".join([
        f"[{n['medio']}] {n['titulo']} | {n['resumen'][:150]}..."
        for n in top15
    ]) if top15 else "(sin noticias disponibles)"
 
    bench_display = f"{bench_1m:+.2f}" if bench_1m is not None else "0.00"
 
    return f"""Sos un analista financiero escribiendo para un inversor argentino. Hoy es {HOY.strftime('%d/%m/%Y')}.
 
=== DATOS DE MERCADO (usá SOLO estos números, no inventes otros) ===
 
MUNDO:
{bloque_mundo}
 
ARGENTINA:
{bloque_ar}
 
{bench_txt}
 
=== NOTICIAS RELEVANTES (últimas 48h, ordenadas por relevancia) ===
 
{noticias_txt}
 
=== TAREA ===
 
Devolvé UN ÚNICO OBJETO JSON con esta estructura exacta:
 
{{
  "mundo": "una línea sobre el contexto global de hoy, máximo 160 caracteres, basada en los datos y las noticias internacionales",
  "argentina": "una línea sobre Argentina, máximo 160 caracteres, basada en los datos locales y las noticias argentinas",
  "a_mirar": "una línea sobre qué evento concreto observar los próximos días (licitación, dato económico, reunión de banco central), basado SOLO en noticias que lo mencionen explícitamente, máximo 160 caracteres",
  "dato_real": "párrafo de 2-3 oraciones explicando qué significa el benchmark real USD de {bench_display}% este mes para alguien que tiene dólares en Argentina",
  "noticias_destacadas": [
    {{"titular": "...", "medio": "...", "url": "...", "por_que_importa": "una línea explicando qué mueve"}},
    ... (3 a 5 items elegidos del listado de arriba)
  ]
}}
 
=== REGLAS ESTRICTAS ===
- NO uses números que no estén en los datos de arriba.
- NO recomiendes comprar o vender ningún activo.
- NO predigas precios futuros ni digas "el dólar va a subir/bajar".
- Para "a_mirar": solo eventos concretos mencionados en las noticias (ej: "Licitación de LECAPs el martes"). Si no hay, decí "Sin eventos destacados próximos".
- Las URLs y titulares de noticias_destacadas deben copiarse EXACTO del listado (no los inventes ni los modifiques).
- Español rioplatense, directo, sin jerga.
- NO uses emojis.
- Respondé SOLO el JSON, sin ```json ni texto antes o después."""
 
 
def llamar_gemini(prompt, intentos=3):
    url = (
        "https://generativelanguage.googleapis.com/v1beta/"
        f"models/gemini-1.5-flash:generateContent?key={GEMINI_API_KEY}"
    )
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "response_mime_type": "application/json",
            "temperature": 0.3,
            "maxOutputTokens": 2048,
        },
    }
    for i in range(intentos):
        try:
            r = requests.post(url, json=payload, timeout=60)
            if r.status_code == 200:
                return r.json()["candidates"][0]["content"]["parts"][0]["text"]
            print(f"  ⚠ Intento {i+1}: HTTP {r.status_code} - {r.text[:200]}")
        except Exception as e:
            print(f"  ⚠ Intento {i+1}: {e}")
        time.sleep(5)
    return None
 
 
def parsear_respuesta(texto):
    if not texto:
        return None
    try:
        return json.loads(texto)
    except json.JSONDecodeError:
        # Fallback: limpiar backticks de markdown si aparecen
        import re
        limpio = re.sub(r"^```(?:json)?\s*|\s*```$", "", texto.strip(), flags=re.MULTILINE)
        try:
            return json.loads(limpio)
        except json.JSONDecodeError as e:
            print(f"  ⚠ JSON inválido: {e}")
            print(f"  Texto recibido: {texto[:500]}")
            return None
 
 
print("\n[7/7] Llamada a Gemini...")
prompt = build_prompt()
respuesta_raw = llamar_gemini(prompt)
respuesta = parsear_respuesta(respuesta_raw)
 
if not respuesta:
    respuesta = {
        "mundo": "Error generando análisis",
        "argentina": "Error generando análisis",
        "a_mirar": "Error generando análisis",
        "dato_real": "No se pudo generar el análisis del LLM.",
        "noticias_destacadas": [],
    }
    print("  ✗ Fallback a respuesta por defecto")
else:
    print("  ✓ Gemini respondió correctamente")
 
 
# ---------------------------------------------------------------
# 8. ESCRITURA EN SHEETS
# ---------------------------------------------------------------
print("\n[8/8] Escribiendo a Google Sheets...")
 
 
def write_ws(name, df):
    try:
        ws = sh.worksheet(name)
        ws.clear()
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=name, rows="1000", cols="30")
    ws.update([df.columns.values.tolist()] + df.astype(str).values.tolist())
 
 
# Hoja 1: histórico (igual que antes, base para gráficos)
df_final_out = df_final.fillna("")
write_ws("DB_Historico", df_final_out)
print(f"  ✓ DB_Historico: {len(df_final_out)} filas")
 
# Hoja 2: insights estructurados
insights_df = pd.DataFrame({
    "fecha_corrida": [HOY.isoformat()],
    "mundo": [respuesta.get("mundo", "")],
    "argentina": [respuesta.get("argentina", "")],
    "a_mirar": [respuesta.get("a_mirar", "")],
    "dato_real": [respuesta.get("dato_real", "")],
    "bench_1m": [bench_1m if bench_1m is not None else ""],
    "bench_1a": [bench_1a if bench_1a is not None else ""],
    "snapshots_json": [json.dumps(snapshots, ensure_ascii=False)],
    "destacadas_json": [json.dumps(respuesta.get("noticias_destacadas", []), ensure_ascii=False)],
})
write_ws("DB_Insights", insights_df)
print("  ✓ DB_Insights")
 
# Hoja 3: todas las noticias ranqueadas
if noticias:
    noticias_df = pd.DataFrame(noticias)
    noticias_df = noticias_df[["fecha", "medio", "titulo", "resumen", "url", "score"]]
    write_ws("DB_Noticias", noticias_df)
    print(f"  ✓ DB_Noticias: {len(noticias_df)} noticias")
else:
    print("  ✗ DB_Noticias: no hay noticias para guardar")
 
print("\n" + "=" * 60)
print("Pipeline V15 - Completado")
print("=" * 60)
 
