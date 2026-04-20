"""
Pipeline V17 - Dashboard Macro Argentina
Corre una vez por día vía GitHub Actions.

Cambios vs V16:
- Gemini 2.5 Flash: thinkingBudget=0 + maxOutputTokens=8192 (fix del error silencioso de thinking)
- Cálculo de salario real = RIPTE deflactado por IPC acumulado
- Series de últimos 12 meses (IPC, EMAE, salario real) guardadas como JSON para sparklines
- 4 prompts separados al LLM:
    1. resumen diario (mundo/argentina/a_mirar) basado en noticias
    2. análisis valor real MENSUAL
    3. análisis valor real ANUAL
    4. lectura macro (relaciones entre IPC/EMAE/salario real)
- EMAE/RIPTE tolerantes a delay: se aceptan datos de hasta 90 días atrás
"""

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
print("Pipeline V17 - Iniciando")
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
# 1. DATOS MACRO
# ---------------------------------------------------------------
print("\n[1/8] Ingesta macro Argentina...")

endpoints = {
    "oficial": ("https://api.argentinadatos.com/v1/cotizaciones/dolares/oficial", "venta", "USD_Oficial"),
    "blue": ("https://api.argentinadatos.com/v1/cotizaciones/dolares/blue", "venta", "USD_Blue"),
    "rp": ("https://api.argentinadatos.com/v1/finanzas/indices/riesgo-pais", "valor", "Riesgo_Pais"),
    "ipc": ("https://api.argentinadatos.com/v1/finanzas/indices/inflacion", "valor", "IPC"),
    "emae": ("https://api.argentinadatos.com/v1/finanzas/indices/emae", "valor", "EMAE"),
    "ripte": ("https://api.argentinadatos.com/v1/finanzas/indices/ripte", "valor", "RIPTE"),
}

macro_dfs = {}
for key, (url, src_col, dest_col) in endpoints.items():
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
# 2. IPC: mensual, interanual compuesto, aceleración + serie 12m
# ---------------------------------------------------------------
print("\n[2/8] IPC: mensual / interanual / aceleración / serie 12m...")


def ipc_serie_12m(df_ipc):
    """Últimos 12 valores mensuales con fechas. Devuelve (fechas, valores)."""
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


df_ipc_raw = macro_dfs["ipc"]
ipc_mes = ipc_mensual_ultimo(df_ipc_raw) if not df_ipc_raw.empty else None
ipc_yoy = ipc_interanual(df_ipc_raw, 12) if not df_ipc_raw.empty else None
ipc_accel = ipc_aceleracion_pp(df_ipc_raw) if not df_ipc_raw.empty else None
ipc_fechas_12m, ipc_valores_12m = ipc_serie_12m(df_ipc_raw) if not df_ipc_raw.empty else ([], [])

accel_str = f"{ipc_accel:+.2f}pp" if ipc_accel is not None else "N/D"
print(f"  ✓ IPC último: {ipc_mes}% | YoY: {ipc_yoy}% | Acel: {accel_str} | Serie: {len(ipc_valores_12m)} meses")


# ---------------------------------------------------------------
# 3. EMAE + SALARIO REAL + series 12m
# ---------------------------------------------------------------
print("\n[3/8] EMAE y Salario Real...")


def serie_12m_generica(df, col):
    """Últimos 12 valores + fechas de cualquier serie macro."""
    try:
        s = df.tail(12)
        return (s["fecha"].dt.strftime("%b %y").tolist(),
                pd.to_numeric(s[col], errors="coerce").round(2).tolist())
    except Exception:
        return [], []


# EMAE
df_emae_raw = macro_dfs["emae"]
emae_val = None
emae_yoy = None
emae_fechas_12m, emae_valores_12m = [], []
emae_age_days = None

if not df_emae_raw.empty:
    df_emae = df_emae_raw.copy()
    df_emae["EMAE"] = pd.to_numeric(df_emae["EMAE"], errors="coerce")
    df_emae = df_emae.dropna(subset=["EMAE"])
    if not df_emae.empty:
        emae_val = float(df_emae["EMAE"].iloc[-1])
        last_date = df_emae["fecha"].iloc[-1]
        emae_age_days = (pd.Timestamp.now() - last_date).days

        # YoY: comparar con dato de hace ~365 días
        ant_idx = df_emae[df_emae["fecha"] <= (last_date - timedelta(days=330))]
        if not ant_idx.empty:
            emae_yoy = round(pct_change(emae_val, ant_idx["EMAE"].iloc[-1]), 2)

        emae_fechas_12m, emae_valores_12m = serie_12m_generica(df_emae, "EMAE")

print(f"  ✓ EMAE: val={emae_val} | YoY={emae_yoy}% | age={emae_age_days}d | serie {len(emae_valores_12m)} puntos")


# Salario real: RIPTE deflactado por IPC acumulado
# Metodología:
#   ipc_base_t = (1 + ipc_1)*(1 + ipc_2)*... *(1 + ipc_t) * 100   [base 100 en t=0]
#   salario_real_t = (RIPTE_t / RIPTE_0) / (ipc_base_t / 100) * 100
# O de forma equivalente: tomamos RIPTE a pesos de hoy dividido por factor de inflación.
print("\n  Calculando salario real base 100...")

df_ripte_raw = macro_dfs["ripte"]
salario_real_yoy = None
salario_real_valores = []
salario_real_fechas = []
salario_real_age_days = None

if not df_ripte_raw.empty and not df_ipc_raw.empty:
    df_r = df_ripte_raw.copy()
    df_r["RIPTE"] = pd.to_numeric(df_r["RIPTE"], errors="coerce")
    df_r = df_r.dropna(subset=["RIPTE"]).sort_values("fecha").reset_index(drop=True)

    df_i = df_ipc_raw.copy()
    df_i["IPC"] = pd.to_numeric(df_i["IPC"], errors="coerce")
    df_i = df_i.dropna(subset=["IPC"]).sort_values("fecha").reset_index(drop=True)

    # Alinear mes a mes (aproximando por mes-año)
    df_r["ym"] = df_r["fecha"].dt.to_period("M")
    df_i["ym"] = df_i["fecha"].dt.to_period("M")
    df_r = df_r.drop_duplicates(subset=["ym"], keep="last")
    df_i = df_i.drop_duplicates(subset=["ym"], keep="last")

    merged = df_r.merge(df_i[["ym", "IPC"]], on="ym", how="inner").sort_values("ym")

    if len(merged) >= 12:
        # base 100 hace 12 meses desde el último dato de RIPTE disponible
        ultimos_12 = merged.tail(13)  # 13 para tener "hace 12" + los 12 siguientes
        if len(ultimos_12) >= 12:
            base_idx = ultimos_12.iloc[0]
            ripte_base = base_idx["RIPTE"]

            # Factor de inflación acumulado desde base
            valores = []
            fechas = []
            factor = 1.0
            for i, row in ultimos_12.iloc[1:].iterrows():
                factor *= (1 + row["IPC"] / 100)
                salario_real = (row["RIPTE"] / ripte_base) / factor * 100
                valores.append(round(salario_real, 2))
                fechas.append(row["ym"].strftime("%b %y"))

            salario_real_fechas = fechas
            salario_real_valores = valores
            salario_real_yoy = round(valores[-1] - 100, 2)  # variación vs base 100
            salario_real_age_days = (pd.Timestamp.now() - df_r["fecha"].iloc[-1]).days

print(f"  ✓ Salario real: YoY={salario_real_yoy}% | age={salario_real_age_days}d | serie {len(salario_real_valores)} puntos")


# ---------------------------------------------------------------
# 4. BENCHMARK VALOR REAL USD
# ---------------------------------------------------------------
print("\n[4/8] Benchmark valor real USD...")


def get_bench(months):
    try:
        df_ipc = macro_dfs["ipc"]
        df_of = macro_dfs["oficial"]
        if df_ipc.empty or df_of.empty:
            return None
        ipc_hist = df_ipc["IPC"].astype(float).tail(months) / 100
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
# 5. MERCADOS
# ---------------------------------------------------------------
print("\n[5/8] Ingesta mercados...")

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


# ---------------------------------------------------------------
# 6. CONSOLIDACIÓN + CCL + BRECHA
# ---------------------------------------------------------------
print("\n[6/8] Consolidación...")

df_final = df_m.copy()
for key, df in macro_dfs.items():
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
# 7. NOTICIAS (RSS)
# ---------------------------------------------------------------
print("\n[7/8] Ingesta noticias...")

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
print(f"  → Total noticias: {len(noticias)} | Top 15 al LLM")


# ---------------------------------------------------------------
# 8. SNAPSHOTS + RENDIMIENTOS VALOR REAL
# ---------------------------------------------------------------
print("\n[8/8] Snapshots y llamadas Gemini...")


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
        return f"- {label}: {prefix}{s['val']} | 1D: {s['d1']:+.2f}% | 1M: {s['m1']:+.2f}% | 1A: {s['a1']:+.2f}%"

    def fmt_pp(label, s, suffix="%"):
        if not s:
            return f"- {label}: sin datos"
        return f"- {label}: {s['val']:.2f}{suffix} | 1M: {s['m1']:+.2f}pp | 1A: {s['a1']:+.2f}pp"

    def fmt_pts(label, s):
        if not s:
            return f"- {label}: sin datos"
        return f"- {label}: {s['val']:.0f} bps | 1M: {s['m1']:+.0f} bps | 1A: {s['a1']:+.0f} bps"

    bloque_mundo = "\n".join([
        fmt_ratio("S&P 500", snapshots["sp500"]),
        fmt_ratio("Brent", snapshots["brent"], prefix="USD "),
        fmt_ratio("Bitcoin", snapshots["btc"], prefix="USD "),
        fmt_ratio("Oro", snapshots["oro"], prefix="USD "),
    ])

    ipc_line = (
        f"- IPC último mes: {ipc_mes}% | Interanual: {ipc_yoy}% | Aceleración vs mes previo: {ipc_accel:+.2f}pp"
        if ipc_mes is not None and ipc_accel is not None
        else "- IPC: sin datos completos"
    )

    bloque_ar = "\n".join([
        fmt_ratio("Merval", snapshots["merval"]),
        fmt_ratio("AL30", snapshots["al30"]),
        fmt_ratio("Dólar oficial", snapshots["usd_oficial"], prefix="$"),
        fmt_ratio("CCL", snapshots["ccl"], prefix="$"),
        fmt_pp("Brecha CCL", snapshots["brecha_ccl"]),
        fmt_pts("Riesgo País", snapshots["riesgo_pais"]),
        ipc_line,
    ])

    noticias_txt = "\n".join([
        f"[{n['medio']}] {n['titulo']}\n  → {n['resumen'][:180]}"
        for n in top15
    ]) if top15 else "(sin noticias disponibles)"

    return f"""Sos un analista financiero argentino escribiendo un resumen matutino para un inversor. Hoy es {HOY.strftime('%d/%m/%Y')}.

=== DATOS DE MERCADO (contexto, NO los cites a menos que sea imprescindible) ===

MUNDO:
{bloque_mundo}

ARGENTINA:
{bloque_ar}

=== NOTICIAS DE MEDIOS ARGENTINOS (últimas 48h, ordenadas por relevancia) ===

{noticias_txt}

=== TU TAREA ===

Escribí un resumen en 3 líneas. Las 3 líneas deben basarse en lo que dicen las NOTICIAS de arriba, no en tu interpretación de los números.

Devolvé JSON:

{{
  "mundo": "una línea basada en noticias internacionales, con tema concreto. Máximo 180 caracteres.",
  "argentina": "una línea basada en noticias argentinas, con tema/evento concreto. Máximo 180 caracteres.",
  "a_mirar": "evento concreto de los próximos días SOLO si aparece mencionado en alguna noticia. Si no hay nada, 'Sin eventos destacados en la agenda'. Máximo 180 caracteres.",
  "noticias_destacadas": [
    {{"titular": "exacto del listado", "medio": "exacto", "url": "exacto", "por_que_importa": "una línea 100 caracteres"}},
    {{"titular": "...", "medio": "...", "url": "...", "por_que_importa": "..."}},
    {{"titular": "...", "medio": "...", "url": "...", "por_que_importa": "..."}}
  ]
}}

REGLAS: no inventes datos/noticias, no recomiendes compra/venta, no predigas, español rioplatense, sin emojis, JSON puro sin markdown."""


def build_prompt_valor_real(rendimientos, bench, periodo_label):
    def fmt_dict(d):
        return "\n".join([
            f"  - {k}: {v:+.2f}%" if v is not None else f"  - {k}: sin datos"
            for k, v in d.items()
        ])

    bench_str = f"{bench:+.2f}" if bench is not None else "0.00"

    return f"""Sos un analista financiero argentino. Un dashboard muestra rendimientos {periodo_label.lower()} de inversiones medidos en USD:

{fmt_dict(rendimientos)}

Benchmark "dólares quietos": {bench_str}%
(positivo = Argentina se encareció, dólares quietos perdieron poder de compra)
(negativo = Argentina se abarató, dólares quietos ganaron poder de compra)

=== TU TAREA ===

Explicá en 3 oraciones qué pasó con estos activos en este período:
- Frase 1: qué significa el benchmark de {bench_str}% para alguien con dólares en Argentina (período: {periodo_label.lower()}).
- Frase 2: qué activo quedó más arriba y cuál más abajo respecto del benchmark.
- Frase 3: observación objetiva sobre el patrón visible, sin predicciones ni consejos.

Devolvé JSON:
{{"analisis": "párrafo corrido de 3 oraciones, máximo 500 caracteres"}}

REGLAS: no recomiendes, no predigas, usá solo los números de arriba, español rioplatense, sin emojis, JSON puro."""


def build_prompt_lectura_macro():
    """Lectura transversal de IPC + EMAE + salario real."""
    ipc_series_str = ", ".join([f"{f}: {v}%" for f, v in zip(ipc_fechas_12m, ipc_valores_12m)]) if ipc_valores_12m else "sin datos"
    emae_series_str = ", ".join([f"{f}: {v}" for f, v in zip(emae_fechas_12m, emae_valores_12m)]) if emae_valores_12m else "sin datos"
    sal_series_str = ", ".join([f"{f}: {v}" for f, v in zip(salario_real_fechas, salario_real_valores)]) if salario_real_valores else "sin datos"

    return f"""Sos un analista económico argentino. El dashboard muestra tres series macroeconómicas de los últimos 12 meses:

1. IPC MENSUAL (% de inflación cada mes):
   {ipc_series_str}
   Último mes: {ipc_mes}% | Interanual compuesto: {ipc_yoy}%

2. EMAE (índice de actividad económica, mensual):
   {emae_series_str}
   Último valor: {emae_val} | Variación interanual: {emae_yoy}%

3. SALARIO REAL (base 100 hace 12 meses, mensual):
   {sal_series_str}
   Variación interanual: {salario_real_yoy}%
   (base 100: valores >100 = salarios le ganaron a la inflación, <100 = perdieron)

=== TU TAREA ===

Escribí una lectura transversal de 2-3 oraciones que relacione las tres variables. Qué se mueve junto, qué se desacopla, qué tendencia es clara.

Devolvé JSON:
{{"lectura_macro": "párrafo de 2-3 oraciones, máximo 400 caracteres"}}

REGLAS:
- Basate SOLO en los datos de arriba. No inventes números.
- NO hagas predicciones ni digas "se espera que...".
- NO des consejos ni opiniones políticas.
- NO recomiendes acciones.
- Lectura fría, objetiva, tipo informe técnico.
- Español rioplatense, directo, sin jerga.
- Sin emojis.
- JSON puro sin markdown."""


def llamar_gemini(prompt, intentos=3, model=GEMINI_MODEL):
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={GEMINI_API_KEY}"
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "response_mime_type": "application/json",
            "temperature": 0.3,
            "maxOutputTokens": 8192,
            "thinkingConfig": {"thinkingBudget": 0},
        },
    }
    for i in range(intentos):
        try:
            r = requests.post(url, json=payload, timeout=90)
            if r.status_code == 200:
                data = r.json()
                if "candidates" in data and data["candidates"]:
                    finish = data["candidates"][0].get("finishReason", "")
                    content = data["candidates"][0].get("content", {})
                    parts = content.get("parts", [])
                    if parts and "text" in parts[0]:
                        return parts[0]["text"]
                    print(f"  ⚠ Intento {i+1}: respuesta sin text. finish={finish}, content={str(content)[:200]}")
                else:
                    print(f"  ⚠ Intento {i+1}: sin candidates. {str(data)[:300]}")
            else:
                print(f"  ⚠ Intento {i+1}: HTTP {r.status_code} - {r.text[:300]}")
        except Exception as e:
            print(f"  ⚠ Intento {i+1}: {e}")
        time.sleep(5)
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
            print(f"  ⚠ JSON inválido: {e}")
            print(f"  Texto: {texto[:400]}")
            return None


# Llamada 1: resumen diario
print("\n  → Llamada 1/4: Gemini resumen diario...")
resp_resumen = parsear_json(llamar_gemini(build_prompt_resumen())) or {}
if resp_resumen:
    print("    ✓ OK")
else:
    print("    ✗ Falló")
    resp_resumen = {
        "mundo": "Sin análisis disponible",
        "argentina": "Sin análisis disponible",
        "a_mirar": "Sin eventos destacados",
        "noticias_destacadas": [],
    }

# Llamada 2: análisis valor real mensual
print("  → Llamada 2/4: Gemini análisis valor real MENSUAL...")
resp_vr_1m = parsear_json(llamar_gemini(build_prompt_valor_real(valor_real_1m, bench_1m, "Mensual"))) or {}
analisis_vr_1m = resp_vr_1m.get("analisis", "Sin análisis disponible")
print(f"    ✓" if resp_vr_1m else "    ✗")

# Llamada 3: análisis valor real anual
print("  → Llamada 3/4: Gemini análisis valor real ANUAL...")
resp_vr_1a = parsear_json(llamar_gemini(build_prompt_valor_real(valor_real_1a, bench_1a, "Anual"))) or {}
analisis_vr_1a = resp_vr_1a.get("analisis", "Sin análisis disponible")
print(f"    ✓" if resp_vr_1a else "    ✗")

# Llamada 4: lectura macro
print("  → Llamada 4/4: Gemini lectura macro...")
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
print("Pipeline V17 - Completado")
print("=" * 60)
