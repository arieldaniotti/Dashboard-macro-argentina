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
print("Pipeline V18 - Iniciando")
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
        if old is None or old == 0 or pd.isna(old):
            return 0.0
        return ((new / old) - 1) * 100
    except Exception:
        return 0.0


def abs_diff(new, old):
    try:
        if old is None or pd.isna(old):
            return 0.0
        return new - old
    except Exception:
        return 0.0


def get_historical_value(df, col, days_back):
    try:
        serie = df[["fecha", col]].copy()
        serie[col] = pd.to_numeric(serie[col], errors="coerce")
        serie = serie.dropna(subset=[col]).drop_duplicates(subset=["fecha"])
        if serie.empty:
            return None
        target = serie["fecha"].max() - timedelta(days=days_back)
        prev = serie[serie["fecha"] <= target]
        return prev[col].iloc[-1] if not prev.empty else serie[col].iloc[0]
    except Exception:
        return None


# ---------------------------------------------------------------
# FUENTE NUEVA: apis.datos.gob.ar (Series de Tiempo INDEC)
# ---------------------------------------------------------------
def fetch_indec_series(serie_id, col_name, limit=500):
    """
    Consume la API oficial datos.gob.ar.
    Doc: https://apis.datos.gob.ar/series/api/series/
    Devuelve DataFrame con columnas 'fecha' y col_name, ordenado ascendente.
    """
    url = f"https://apis.datos.gob.ar/series/api/series/?ids={serie_id}&limit={limit}&format=json&sort=asc"
    try:
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=20)
        if r.status_code != 200:
            print(f"  ⚠ HTTP {r.status_code} serie {serie_id}")
            return pd.DataFrame(columns=["fecha", col_name])
        data = r.json()
        # La API devuelve {"data": [[fecha, valor], [fecha, valor], ...]}
        rows = data.get("data", [])
        if not rows:
            print(f"  ⚠ Serie {serie_id}: sin datos en respuesta")
            return pd.DataFrame(columns=["fecha", col_name])
        df = pd.DataFrame(rows, columns=["fecha", col_name])
        df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
        df[col_name] = pd.to_numeric(df[col_name], errors="coerce")
        df = df.dropna(subset=["fecha", col_name]).sort_values("fecha").reset_index(drop=True)
        return df
    except Exception as e:
        print(f"  ⚠ Error fetch serie {serie_id}: {e}")
        return pd.DataFrame(columns=["fecha", col_name])


# ---------------------------------------------------------------
# 1. DATOS MACRO (argentinadatos.com para lo que sí tiene)
# ---------------------------------------------------------------
print("\n[1/8] Ingesta macro Argentina (argentinadatos.com)...")

endpoints_argdatos = {
    "oficial": ("https://api.argentinadatos.com/v1/cotizaciones/dolares/oficial", "venta", "USD_Oficial"),
    "blue": ("https://api.argentinadatos.com/v1/cotizaciones/dolares/blue", "venta", "USD_Blue"),
    "rp": ("https://api.argentinadatos.com/v1/finanzas/indices/riesgo-pais", "valor", "Riesgo_Pais"),
    "ipc": ("https://api.argentinadatos.com/v1/finanzas/indices/inflacion", "valor", "IPC"),
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


# ---------------------------------------------------------------
# 2. EMAE + SALARIOS desde datos.gob.ar
# ---------------------------------------------------------------
print("\n[2/8] EMAE y Salarios desde apis.datos.gob.ar...")

# EMAE nivel general desestacionalizado, base 2004=100
EMAE_SERIE_ID = "143.3_NO_PR_2004_A_21"
df_emae_raw = fetch_indec_series(EMAE_SERIE_ID, "EMAE")
if not df_emae_raw.empty:
    print(f"  ✓ EMAE: {len(df_emae_raw)} filas, último {df_emae_raw['fecha'].max().date()}, último valor {df_emae_raw['EMAE'].iloc[-1]}")
else:
    print(f"  ✗ EMAE: sin datos")
macro_dfs["emae"] = df_emae_raw

# Índice de Salarios total, INDEC (mensual)
SALARIOS_SERIE_ID = "152.1_INDICE_SIRS_0_M_18"
df_salarios_raw = fetch_indec_series(SALARIOS_SERIE_ID, "IndiceSalarios")
if not df_salarios_raw.empty:
    print(f"  ✓ Salarios: {len(df_salarios_raw)} filas, último {df_salarios_raw['fecha'].max().date()}, último valor {df_salarios_raw['IndiceSalarios'].iloc[-1]}")
else:
    print(f"  ✗ Salarios: sin datos")
macro_dfs["salarios"] = df_salarios_raw


# ---------------------------------------------------------------
# 3. IPC: mensual, interanual compuesto, aceleración + serie 12m
# ---------------------------------------------------------------
print("\n[3/8] IPC: métricas derivadas...")


def ipc_serie_12m(df_ipc):
    try:
        s = df_ipc.tail(12)
        return (s["fecha"].dt.strftime("%b %y").tolist(),
                s["IPC"].astype(float).round(2).tolist())
    except Exception:
        return [], []


def ipc_interanual(df_ipc, meses=12):
    try:
        serie = df_ipc["IPC"].astype(float).tail(meses) / 100
        acum = (1 + serie).prod() - 1
        return round(acum * 100, 2)
    except Exception:
        return None


def ipc_mensual_ultimo(df_ipc):
    try:
        return float(df_ipc["IPC"].astype(float).iloc[-1])
    except Exception:
        return None


def ipc_aceleracion_pp(df_ipc):
    try:
        serie = df_ipc["IPC"].astype(float).tail(2)
        if len(serie) < 2:
            return None
        return round(float(serie.iloc[-1]) - float(serie.iloc[-2]), 2)
    except Exception:
        return None


df_ipc = macro_dfs["ipc"]
ipc_mes = ipc_mensual_ultimo(df_ipc) if not df_ipc.empty else None
ipc_yoy = ipc_interanual(df_ipc, 12) if not df_ipc.empty else None
ipc_accel = ipc_aceleracion_pp(df_ipc) if not df_ipc.empty else None
ipc_fechas_12m, ipc_valores_12m = ipc_serie_12m(df_ipc) if not df_ipc.empty else ([], [])

accel_str = f"{ipc_accel:+.2f}pp" if ipc_accel is not None else "N/D"
print(f"  ✓ IPC último: {ipc_mes}% | YoY: {ipc_yoy}% | Acel: {accel_str}")


# ---------------------------------------------------------------
# 4. EMAE derivado + serie 12m
# ---------------------------------------------------------------
print("\n[4/8] EMAE derivados...")


def serie_12m(df, col):
    try:
        s = df.tail(12)
        return (s["fecha"].dt.strftime("%b %y").tolist(),
                pd.to_numeric(s[col], errors="coerce").round(2).tolist())
    except Exception:
        return [], []


emae_val = None
emae_yoy = None
emae_fechas_12m, emae_valores_12m = [], []
emae_age_days = None

if not df_emae_raw.empty:
    emae_val = float(df_emae_raw["EMAE"].iloc[-1])
    last_date = df_emae_raw["fecha"].iloc[-1]
    emae_age_days = (pd.Timestamp.now() - last_date).days
    # YoY: comparar con valor ~12 meses atrás
    target = last_date - timedelta(days=330)
    ant = df_emae_raw[df_emae_raw["fecha"] <= target]
    if not ant.empty:
        emae_yoy = round(pct_change(emae_val, ant["EMAE"].iloc[-1]), 2)
    emae_fechas_12m, emae_valores_12m = serie_12m(df_emae_raw, "EMAE")

print(f"  ✓ EMAE: val={emae_val} | YoY={emae_yoy}% | age={emae_age_days}d | serie {len(emae_valores_12m)} puntos")


# ---------------------------------------------------------------
# 5. SALARIO REAL: Índice de Salarios deflactado por IPC
# ---------------------------------------------------------------
print("\n[5/8] Salario real (base 100 hace 12 meses)...")

salario_real_yoy = None
salario_real_fechas = []
salario_real_valores = []
salario_real_age_days = None

if not df_salarios_raw.empty and not df_ipc.empty:
    # Alinear por mes-año
    df_s = df_salarios_raw.copy()
    df_s["ym"] = df_s["fecha"].dt.to_period("M")
    df_s = df_s.drop_duplicates(subset=["ym"], keep="last")

    df_i = df_ipc.copy()
    df_i["ym"] = df_i["fecha"].dt.to_period("M")
    df_i = df_i.drop_duplicates(subset=["ym"], keep="last")

    merged = df_s.merge(df_i[["ym", "IPC"]], on="ym", how="inner").sort_values("ym")

    if len(merged) >= 13:
        # Tomamos los últimos 13 (base + 12 siguientes)
        slice_ = merged.tail(13).reset_index(drop=True)
        base_salario = slice_.iloc[0]["IndiceSalarios"]
        factor_ipc = 1.0
        for i in range(1, len(slice_)):
            factor_ipc *= (1 + slice_.iloc[i]["IPC"] / 100)
            salario_nominal_idx = slice_.iloc[i]["IndiceSalarios"] / base_salario
            salario_real = salario_nominal_idx / factor_ipc * 100
            salario_real_valores.append(round(salario_real, 2))
            salario_real_fechas.append(slice_.iloc[i]["ym"].strftime("%b %y"))

        if salario_real_valores:
            salario_real_yoy = round(salario_real_valores[-1] - 100, 2)
        salario_real_age_days = (pd.Timestamp.now() - df_salarios_raw["fecha"].iloc[-1]).days

print(f"  ✓ Salario real: YoY={salario_real_yoy}% | age={salario_real_age_days}d | serie {len(salario_real_valores)} puntos")


# ---------------------------------------------------------------
# 6. BENCHMARK VALOR REAL USD
# ---------------------------------------------------------------
print("\n[6/8] Benchmark valor real USD...")


def get_bench(months):
    try:
        df_ipc_b = macro_dfs["ipc"]
        df_of = macro_dfs["oficial"]
        if df_ipc_b.empty or df_of.empty:
            return None
        ipc_hist = df_ipc_b["IPC"].astype(float).tail(months) / 100
        ipc_acc = (1 + ipc_hist).prod() - 1
        usd_hoy = float(df_of["USD_Oficial"].iloc[-1])
        target_date = HOY - timedelta(days=30 * months)
        usd_ant_rows = df_of[df_of["fecha"] <= target_date]
        if usd_ant_rows.empty:
            return None
        usd_ant = float(usd_ant_rows["USD_Oficial"].iloc[-1])
        dev = (usd_hoy / usd_ant) - 1
        return round((((1 + ipc_acc) / (1 + dev)) - 1) * 100, 2)
    except Exception as e:
        print(f"  ⚠ get_bench({months}m): {e}")
        return None


bench_1m = get_bench(1)
bench_1a = get_bench(12)
print(f"  ✓ Benchmark 1M: {bench_1m}% | 1A: {bench_1a}%")


# ---------------------------------------------------------------
# 7. MERCADOS + CONSOLIDACIÓN
# ---------------------------------------------------------------
print("\n[7/8] Mercados y consolidación...")

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
        d = yf.download(tk, start=HACE_1A.strftime("%Y-%m-%d"), progress=False, auto_adjust=True)
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

df_final = df_m.copy()
# Mergeamos los que van en df_final (mercados + indicadores con frecuencia diaria/mensual)
for key in ["oficial", "blue", "rp", "ipc"]:
    df = macro_dfs.get(key, pd.DataFrame())
    if not df.empty:
        df_final = df_final.merge(df, on="fecha", how="outer")

df_final = df_final.sort_values("fecha").reset_index(drop=True)

if "GGAL_LOC" in df_final.columns and "GGAL_ADR" in df_final.columns:
    loc = pd.to_numeric(df_final["GGAL_LOC"], errors="coerce")
    adr = pd.to_numeric(df_final["GGAL_ADR"], errors="coerce")
    df_final["CCL"] = (loc / (adr / 10)).round(2)
    oficial = pd.to_numeric(df_final.get("USD_Oficial", pd.Series()), errors="coerce")
    if not oficial.empty:
        df_final["Brecha_CCL"] = (((df_final["CCL"] / oficial) - 1) * 100).round(2)

df_final = df_final.ffill(limit=7)
print(f"  ✓ df_final: {len(df_final)} filas, {len(df_final.columns)} columnas")


# ---------------------------------------------------------------
# 8. NOTICIAS RSS + SNAPSHOTS + PROMPTS GEMINI
# ---------------------------------------------------------------
print("\n[8/8] Noticias + Gemini...")

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
            resumen = e.get("summary", "").strip()[:250]
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
top_noticias_llm = noticias[:8]  # reducido de 15 a 8 para no saturar el prompt
print(f"  → Total: {len(noticias)} | Top 8 al LLM")


def snapshot_ratio(col):
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
        "mode": "ratio",
        "d1": round(pct_change(val, get_historical_value(df_final, col, 1)), 2),
        "m1": round(pct_change(val, get_historical_value(df_final, col, 30)), 2),
        "a1": round(pct_change(val, get_historical_value(df_final, col, 365)), 2),
    }


def snapshot_points(col):
    if col not in df_final.columns:
        return None
    serie = df_final[["fecha", col]].copy()
    serie[col] = pd.to_numeric(serie[col], errors="coerce")
    serie = serie.dropna(subset=[col]).drop_duplicates(subset=["fecha"])
    if serie.empty:
        return None
    val = float(serie[col].iloc[-1])
    return {
        "val": round(val, 0),
        "mode": "points",
        "d1": round(abs_diff(val, get_historical_value(df_final, col, 1)), 0),
        "m1": round(abs_diff(val, get_historical_value(df_final, col, 30)), 0),
        "a1": round(abs_diff(val, get_historical_value(df_final, col, 365)), 0),
    }


def snapshot_pp(col):
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
        "mode": "pp",
        "d1": round(abs_diff(val, get_historical_value(df_final, col, 1)), 2),
        "m1": round(abs_diff(val, get_historical_value(df_final, col, 30)), 2),
        "a1": round(abs_diff(val, get_historical_value(df_final, col, 365)), 2),
    }


snapshots = {
    "sp500": snapshot_ratio("SP500"),
    "merval": snapshot_ratio("Merval"),
    "brent": snapshot_ratio("Brent"),
    "btc": snapshot_ratio("BTC"),
    "oro": snapshot_ratio("Oro"),
    "al30": snapshot_ratio("AL30"),
    "usd_oficial": snapshot_ratio("USD_Oficial"),
    "usd_blue": snapshot_ratio("USD_Blue"),
    "ccl": snapshot_ratio("CCL"),
    "brecha_ccl": snapshot_pp("Brecha_CCL"),
    "riesgo_pais": snapshot_points("Riesgo_Pais"),
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
        if df.empty:
            return None
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
    except Exception:
        return None


valor_real_1m = {
    "Merval": rend_valor_real("Merval", True, 1),
    "AL30": rend_valor_real("AL30", True, 1),
    "S&P 500": rend_valor_real("SP500", False, 1),
    "BTC": rend_valor_real("BTC", False, 1),
    "Oro": rend_valor_real("Oro", False, 1),
}

valor_real_1a = {
    "Merval": rend_valor_real("Merval", True, 12),
    "AL30": rend_valor_real("AL30", True, 12),
    "S&P 500": rend_valor_real("SP500", False, 12),
    "BTC": rend_valor_real("BTC", False, 12),
    "Oro": rend_valor_real("Oro", False, 12),
}

print(f"  ✓ Valor real 1M: {valor_real_1m}")
print(f"  ✓ Valor real 1A: {valor_real_1a}")


# ---------------------------------------------------------------
# PROMPTS
# ---------------------------------------------------------------
def build_prompt_resumen():
    def fmt_ratio(label, s, prefix=""):
        if not s:
            return f"- {label}: sin datos"
        return f"- {label}: {prefix}{s['val']} (1M: {s['m1']:+.1f}%, 1A: {s['a1']:+.1f}%)"

    def fmt_pp(label, s, suffix="%"):
        if not s:
            return f"- {label}: sin datos"
        return f"- {label}: {s['val']:.1f}{suffix} (1M: {s['m1']:+.1f}pp)"

    def fmt_pts(label, s):
        if not s:
            return f"- {label}: sin datos"
        return f"- {label}: {s['val']:.0f} bps (1M: {s['m1']:+.0f})"

    bloque_mundo = "\n".join([
        fmt_ratio("S&P 500", snapshots["sp500"]),
        fmt_ratio("Brent", snapshots["brent"], "USD "),
        fmt_ratio("Bitcoin", snapshots["btc"], "USD "),
        fmt_ratio("Oro", snapshots["oro"], "USD "),
    ])

    ipc_line = (
        f"- IPC: {ipc_mes}% mensual, {ipc_yoy}% interanual"
        if ipc_mes is not None
        else "- IPC: sin datos"
    )

    bloque_ar = "\n".join([
        fmt_ratio("Merval", snapshots["merval"]),
        fmt_ratio("Dólar oficial", snapshots["usd_oficial"], "$"),
        fmt_pp("Brecha CCL", snapshots["brecha_ccl"]),
        fmt_pts("Riesgo País", snapshots["riesgo_pais"]),
        ipc_line,
    ])

    noticias_txt = "\n".join([
        f"[{n['medio']}] {n['titulo']}"
        for n in top_noticias_llm
    ]) if top_noticias_llm else "(sin noticias)"

    return f"""Analista financiero argentino. Fecha: {HOY.strftime('%d/%m/%Y')}.

DATOS MUNDO:
{bloque_mundo}

DATOS ARGENTINA:
{bloque_ar}

NOTICIAS TOP (últimas 48h):
{noticias_txt}

Devolvé JSON con resumen basado en las NOTICIAS (no en los datos):
{{
  "mundo": "una línea sobre contexto global mencionando tema concreto de las noticias. Máximo 160 caracteres.",
  "argentina": "una línea sobre Argentina mencionando tema concreto de las noticias. Máximo 160 caracteres.",
  "a_mirar": "evento concreto próximos días SOLO si aparece en las noticias. Si no, 'Sin eventos destacados'. Máximo 160 caracteres.",
  "noticias_destacadas": [
    {{"titular": "exacto", "medio": "exacto", "url": "exacta", "por_que_importa": "80 caracteres máx"}},
    {{"titular": "...", "medio": "...", "url": "...", "por_que_importa": "..."}},
    {{"titular": "...", "medio": "...", "url": "...", "por_que_importa": "..."}}
  ]
}}

REGLAS: no inventes, no recomiendes comprar/vender, no predigas. Español rioplatense, sin emojis, JSON sin markdown."""


def build_prompt_valor_real(rendimientos, bench, periodo_label):
    def fmt_dict(d):
        return "\n".join([
            f"  {k}: {v:+.2f}%" if v is not None else f"  {k}: sin datos"
            for k, v in d.items()
        ])

    bench_str = f"{bench:+.2f}" if bench is not None else "0.00"

    return f"""Analista financiero argentino. Dashboard muestra rendimientos {periodo_label.lower()} de inversiones en USD:

{fmt_dict(rendimientos)}

Benchmark dólares quietos: {bench_str}%
(positivo = Argentina se encareció, dólares quietos perdieron)
(negativo = Argentina se abarató, dólares quietos ganaron)

Explicá en 3 oraciones qué pasó:
1. Qué significa el benchmark de {bench_str}% para alguien con dólares en Argentina (período: {periodo_label.lower()}).
2. Qué activo quedó más arriba y cuál más abajo vs benchmark.
3. Observación objetiva del patrón, sin predicciones.

Devolvé JSON: {{"analisis": "párrafo de 3 oraciones, máximo 500 caracteres"}}

REGLAS: no recomiendes, no predigas, usá solo estos números, español rioplatense, sin emojis, JSON puro."""


def build_prompt_lectura_macro():
    ipc_series = ", ".join([f"{f}:{v}%" for f, v in zip(ipc_fechas_12m, ipc_valores_12m)]) if ipc_valores_12m else "sin datos"
    emae_series = ", ".join([f"{f}:{v}" for f, v in zip(emae_fechas_12m, emae_valores_12m)]) if emae_valores_12m else "sin datos"
    sal_series = ", ".join([f"{f}:{v}" for f, v in zip(salario_real_fechas, salario_real_valores)]) if salario_real_valores else "sin datos"

    return f"""Analista económico argentino. Series macro últimos 12 meses:

1. IPC MENSUAL: {ipc_series}
   Último: {ipc_mes}% | Interanual: {ipc_yoy}%

2. EMAE (actividad económica): {emae_series}
   Último: {emae_val} | YoY: {emae_yoy}%

3. SALARIO REAL (base 100 hace 12m): {sal_series}
   YoY: {salario_real_yoy}% (valores >100 = salarios le ganaron a inflación)

Lectura transversal en 2-3 oraciones. Relacioná las variables: qué se mueve junto, qué se desacopla, qué tendencia es clara.

Devolvé JSON: {{"lectura_macro": "párrafo de 2-3 oraciones, máximo 400 caracteres"}}

REGLAS: solo datos de arriba, no inventes, no predigas, no opines política, lectura objetiva, español rioplatense, sin emojis, JSON puro."""


def llamar_gemini(prompt, intentos=3, model=GEMINI_MODEL):
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={GEMINI_API_KEY}"
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "response_mime_type": "application/json",
            "temperature": 0.3,
            "maxOutputTokens": 4096,
            "thinkingConfig": {"thinkingBudget": 0},
        },
    }

    prompt_size = len(prompt)

    for i in range(intentos):
        try:
            r = requests.post(url, json=payload, timeout=90)
            if r.status_code == 200:
                data = r.json()
                if "candidates" in data and data["candidates"]:
                    cand = data["candidates"][0]
                    finish = cand.get("finishReason", "unknown")
                    content = cand.get("content", {})
                    parts = content.get("parts", [])
                    if parts and "text" in parts[0]:
                        return parts[0]["text"]
                    print(f"  ⚠ Intento {i+1}: prompt_size={prompt_size}, finish={finish}, sin text en parts")
                else:
                    print(f"  ⚠ Intento {i+1}: sin candidates. {str(data)[:300]}")
            else:
                print(f"  ⚠ Intento {i+1}: HTTP {r.status_code} - {r.text[:300]}")
        except Exception as e:
            print(f"  ⚠ Intento {i+1}: {e}")
        time.sleep(5)
    print(f"  ✗ Gemini falló tras {intentos} intentos. Tamaño del prompt: {prompt_size} chars.")
    return None


def parsear_json(texto):
    if not texto:
        return None
    try:
        return json.loads(texto)
    except json.JSONDecodeError:
        import re
        limpio = re.sub(r"^```(?:json)?\s*|\s*```$", "", texto.strip(), flags=re.MULTILINE)
        try:
            return json.loads(limpio)
        except json.JSONDecodeError as e:
            print(f"  ⚠ JSON inválido: {e}. Texto: {texto[:400]}")
            return None


print("\n  → Llamada 1/4: resumen diario...")
resp_resumen = parsear_json(llamar_gemini(build_prompt_resumen())) or {}
if resp_resumen:
    print("    ✓ OK")
else:
    print("    ✗ Fallback")
    resp_resumen = {
        "mundo": "Sin análisis disponible",
        "argentina": "Sin análisis disponible",
        "a_mirar": "Sin eventos destacados",
        "noticias_destacadas": [],
    }

print("  → Llamada 2/4: valor real MENSUAL...")
resp_vr_1m = parsear_json(llamar_gemini(build_prompt_valor_real(valor_real_1m, bench_1m, "Mensual"))) or {}
analisis_vr_1m = resp_vr_1m.get("analisis", "Sin análisis disponible")
print(f"    ✓" if resp_vr_1m else "    ✗")

print("  → Llamada 3/4: valor real ANUAL...")
resp_vr_1a = parsear_json(llamar_gemini(build_prompt_valor_real(valor_real_1a, bench_1a, "Anual"))) or {}
analisis_vr_1a = resp_vr_1a.get("analisis", "Sin análisis disponible")
print(f"    ✓" if resp_vr_1a else "    ✗")

print("  → Llamada 4/4: lectura macro...")
resp_macro = parsear_json(llamar_gemini(build_prompt_lectura_macro())) or {}
lectura_macro = resp_macro.get("lectura_macro", "Sin análisis disponible")
print(f"    ✓" if resp_macro else "    ✗")


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
print(f"  ✓ DB_Historico: {len(df_out)} filas")

insights_df = pd.DataFrame({
    "fecha_corrida": [HOY.isoformat()],
    "mundo": [resp_resumen.get("mundo", "")],
    "argentina": [resp_resumen.get("argentina", "")],
    "a_mirar": [resp_resumen.get("a_mirar", "")],
    "analisis_vr_1m": [analisis_vr_1m],
    "analisis_vr_1a": [analisis_vr_1a],
    "lectura_macro": [lectura_macro],
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
})
write_ws("DB_Insights", insights_df)
print("  ✓ DB_Insights")

if noticias:
    df_news = pd.DataFrame(noticias)[["fecha", "medio", "titulo", "resumen", "url", "score"]]
    write_ws("DB_Noticias", df_news)
    print(f"  ✓ DB_Noticias: {len(df_news)} noticias")

print("\n" + "=" * 60)
print("Pipeline V18 - Completado")
print("=" * 60)
