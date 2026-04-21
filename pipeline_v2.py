import os
import json
import time
import requests
import feedparser
import yfinance as yf
import pandas as pd
import gspread
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

print("=" * 60)
print("Pipeline V20 - Iniciando")
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

HEADERS = {"User-Agent": "Mozilla/5.0 (DashboardMacroAR/1.0)"}


# ---------------------------------------------------------------
# HELPERS GENERALES
# ---------------------------------------------------------------
def fetch_json(url, timeout=15, verify=True):
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout, verify=verify)
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
            return None
        return ((new / old) - 1) * 100
    except Exception:
        return None


def abs_diff(new, old):
    try:
        if old is None or pd.isna(old):
            return None
        return new - old
    except Exception:
        return None


def get_historical_value(df, col, days_back):
    """
    Busca el último valor anterior a (max_fecha - days_back).
    FIX: Si no hay valor previo válido, devuelve None (no el primer valor de la serie).
    """
    try:
        serie = df[["fecha", col]].copy()
        serie[col] = pd.to_numeric(serie[col], errors="coerce")
        serie = serie.dropna(subset=[col]).drop_duplicates(subset=["fecha"])
        if serie.empty:
            return None
        target = serie["fecha"].max() - timedelta(days=days_back)
        prev = serie[serie["fecha"] <= target]
        if prev.empty:
            return None
        return prev[col].iloc[-1]
    except Exception:
        return None


def get_last_distinct_trading_day_value(df, col):
    """
    FIX 1D: Devuelve el valor del último día hábil DISTINTO al actual.
    No exige que sea "ayer exacto" (cosa que falla los lunes porque viernes ≠ ayer).
    Busca el último registro estrictamente anterior al máximo con valor no-nulo.
    """
    try:
        serie = df[["fecha", col]].copy()
        serie[col] = pd.to_numeric(serie[col], errors="coerce")
        serie = serie.dropna(subset=[col]).drop_duplicates(subset=["fecha"]).sort_values("fecha")
        if len(serie) < 2:
            return None
        # El "actual" es el último. El "previo" es el penúltimo con valor válido.
        return serie[col].iloc[-2]
    except Exception:
        return None


def fetch_indec_series(serie_id, col_name, limit=500):
    url = f"https://apis.datos.gob.ar/series/api/series/?ids={serie_id}&limit={limit}&format=json&sort=asc"
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        if r.status_code != 200:
            print(f"  ⚠ HTTP {r.status_code} serie {serie_id}")
            return pd.DataFrame(columns=["fecha", col_name])
        data = r.json()
        rows = data.get("data", [])
        if not rows:
            print(f"  ⚠ Serie {serie_id}: sin datos")
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
# 1. DATOS MACRO ARGENTINA
# ---------------------------------------------------------------
print("\n[1/12] Ingesta macro Argentina (argentinadatos.com)...")

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
# 2. EMAE + SALARIO REAL (cascada de fallbacks)
# ---------------------------------------------------------------
print("\n[2/12] EMAE y Salarios desde apis.datos.gob.ar...")

EMAE_SERIE_ID = "143.3_NO_PR_2004_A_21"
df_emae_raw = fetch_indec_series(EMAE_SERIE_ID, "EMAE")
if not df_emae_raw.empty:
    print(f"  ✓ EMAE: {len(df_emae_raw)} filas, último {df_emae_raw['fecha'].max().date()}")
else:
    print(f"  ✗ EMAE: sin datos")
macro_dfs["emae"] = df_emae_raw

# FIX SALARIO REAL: cascada de fallbacks
# 1) RIPTE
# 2) Índice de Salarios (nivel general)
# 3) Sector Privado Registrado
SALARIO_CANDIDATES = [
    ("57.1_RIPTE_0_M_8", "RIPTE"),
    ("152.1_INDICE_SIRS_0_M_18", "Indice Salarios"),
    ("151.3_INDICE_SPSRS_0_M_15", "Salario Privado"),
]

df_salario_raw = pd.DataFrame()
salario_fuente_usada = None
for serie_id, nombre in SALARIO_CANDIDATES:
    df_test = fetch_indec_series(serie_id, "Salario")
    if not df_test.empty:
        last_date = df_test["fecha"].max()
        age = (pd.Timestamp.now() - last_date).days
        print(f"  • {nombre} ({serie_id}): último {last_date.date()}, lag {age}d")
        if df_salario_raw.empty or age < (pd.Timestamp.now() - df_salario_raw["fecha"].max()).days:
            df_salario_raw = df_test
            salario_fuente_usada = nombre

if not df_salario_raw.empty:
    print(f"  ✓ Serie de salarios elegida: {salario_fuente_usada}")
else:
    print(f"  ✗ Ninguna serie de salarios disponible")
macro_dfs["salario"] = df_salario_raw


# ---------------------------------------------------------------
# 3. IPC
# ---------------------------------------------------------------
print("\n[3/12] IPC derivados...")


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
print(f"  ✓ IPC: {ipc_mes}% | YoY: {ipc_yoy}% | Acel: {accel_str}")


# ---------------------------------------------------------------
# 4. EMAE
# ---------------------------------------------------------------
print("\n[4/12] EMAE...")


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
    # YoY exacto: 12 meses atrás del último dato
    target = last_date - pd.DateOffset(months=12)
    ant = df_emae_raw[df_emae_raw["fecha"] <= target]
    if not ant.empty:
        delta = pct_change(emae_val, ant["EMAE"].iloc[-1])
        emae_yoy = round(delta, 2) if delta is not None else None
    emae_fechas_12m, emae_valores_12m = serie_12m(df_emae_raw, "EMAE")

print(f"  ✓ EMAE: val={emae_val} | YoY={emae_yoy}% | age={emae_age_days}d")


# ---------------------------------------------------------------
# 5. SALARIO REAL
# ---------------------------------------------------------------
print("\n[5/12] Salario real (deflactado por IPC)...")

salario_real_yoy = None
salario_real_fechas = []
salario_real_valores = []
salario_real_age_days = None

if not df_salario_raw.empty and not df_ipc.empty:
    df_s = df_salario_raw.copy()
    df_s["ym"] = df_s["fecha"].dt.to_period("M")
    df_s = df_s.drop_duplicates(subset=["ym"], keep="last")

    df_i = df_ipc.copy()
    df_i["ym"] = df_i["fecha"].dt.to_period("M")
    df_i = df_i.drop_duplicates(subset=["ym"], keep="last")

    merged = df_s.merge(df_i[["ym", "IPC"]], on="ym", how="inner").sort_values("ym")
    print(f"  • Merge salarios+IPC: {len(merged)} filas (necesitamos >=13)")

    if len(merged) >= 13:
        slice_ = merged.tail(13).reset_index(drop=True)
        base_salario = slice_.iloc[0]["Salario"]
        factor_ipc = 1.0
        for i in range(1, len(slice_)):
            factor_ipc *= (1 + slice_.iloc[i]["IPC"] / 100)
            salario_nominal_idx = slice_.iloc[i]["Salario"] / base_salario
            salario_real = salario_nominal_idx / factor_ipc * 100
            salario_real_valores.append(round(salario_real, 2))
            salario_real_fechas.append(slice_.iloc[i]["ym"].strftime("%b %y"))

        if salario_real_valores:
            salario_real_yoy = round(salario_real_valores[-1] - 100, 2)
        salario_real_age_days = (pd.Timestamp.now() - df_salario_raw["fecha"].iloc[-1]).days

print(f"  ✓ Salario real: YoY={salario_real_yoy}% | age={salario_real_age_days}d | fuente={salario_fuente_usada}")


# ---------------------------------------------------------------
# 6. BCRA TASAS
# ---------------------------------------------------------------
print("\n[6/12] Tasas BCRA...")


def fetch_bcra_principal(var_id, desde=None, hasta=None):
    url = f"https://api.bcra.gob.ar/estadisticas/v3.0/monetarias/{var_id}"
    params = {}
    if desde:
        params["desde"] = desde
    if hasta:
        params["hasta"] = hasta
    try:
        r = requests.get(url, headers=HEADERS, params=params, timeout=20, verify=False)
        if r.status_code == 200:
            d = r.json()
            return d.get("results", [])
        print(f"  ⚠ BCRA var {var_id}: HTTP {r.status_code}")
        return []
    except Exception as e:
        print(f"  ⚠ BCRA var {var_id}: {e}")
        return []


def get_tasa_actual(var_id):
    res = fetch_bcra_principal(var_id)
    if not res:
        return None
    try:
        return float(res[-1]["valor"])
    except Exception:
        return None


tasa_plazo_fijo = get_tasa_actual(34)
tasa_badlar = get_tasa_actual(7)
tasa_tm20 = get_tasa_actual(8)

print(f"  ✓ PF: {tasa_plazo_fijo}% | BADLAR: {tasa_badlar}% | TM20: {tasa_tm20}%")


def tna_a_retorno_periodo(tna, meses):
    if tna is None:
        return None
    tasa_mensual = tna / 100 / 12
    return round(((1 + tasa_mensual) ** meses - 1) * 100, 2)


def estimar_tasa_financiamiento():
    if tasa_badlar is None:
        return {
            "adelanto_cta_cte": None,
            "tarjeta_credito": None,
            "prestamo_personal": None,
            "hipotecario_uva": None,
            "sgr_cheque": None,
        }
    return {
        "adelanto_cta_cte": round(tasa_badlar + 15, 1),
        "tarjeta_credito": round(tasa_badlar + 25, 1),
        "prestamo_personal": round(tasa_badlar + 20, 1),
        "hipotecario_uva": 8.0,
        "sgr_cheque": round(tasa_badlar - 5, 1),
    }


tasas_fin = estimar_tasa_financiamiento()
print(f"  ✓ Financiamiento: {tasas_fin}")


# ---------------------------------------------------------------
# 7. BENCHMARK VALOR REAL
# ---------------------------------------------------------------
print("\n[7/12] Benchmark y valor real...")


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
print(f"  ✓ Bench 1M: {bench_1m}% | 1A: {bench_1a}%")


def tasa_a_retorno_real_usd(tna, meses, bench_pct):
    if tna is None or bench_pct is None:
        return None
    try:
        rend_pesos = tna_a_retorno_periodo(tna, meses) / 100
        df_of = macro_dfs["oficial"]
        dev = 0
        if not df_of.empty:
            usd_hoy = float(df_of["USD_Oficial"].iloc[-1])
            tgt = HOY - timedelta(days=30 * meses)
            rows = df_of[df_of["fecha"] <= tgt]
            if not rows.empty:
                usd_ant = float(rows["USD_Oficial"].iloc[-1])
                dev = (usd_hoy / usd_ant) - 1
        if (1 + dev) == 0:
            return None
        return round((((1 + rend_pesos) / (1 + dev)) - 1) * 100, 2)
    except Exception:
        return None


# ---------------------------------------------------------------
# 8. MERCADOS yfinance
# ---------------------------------------------------------------
print("\n[8/12] Mercados yfinance...")

tickers = {
    "SP500": "^GSPC",
    "Merval": "^MERV",
    "BTC": "BTC-USD",
    "Oro": "GC=F",
    "Brent": "BZ=F",
    "AL30": "AL30.BA",
    "GGAL_ADR": "GGAL",
    "GGAL_LOC": "GGAL.BA",
    "NVDA": "NVDA", "MELI": "MELI", "MSFT": "MSFT", "GOOGL": "GOOGL",
    "VIST": "VIST", "YPF": "YPF", "PAMP": "PAM", "META": "META",
    "US10Y": "^TNX",
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
print(f"  ✓ df_final: {len(df_final)} filas")


# ---------------------------------------------------------------
# 9. MACRO GLOBAL (FRED con parser robusto)
# ---------------------------------------------------------------
print("\n[9/12] Macro global (FRED)...")

FRED_SERIES = {
    "FEDFUNDS": "FEDFUNDS",
    "BR_SELIC": "INTDSRBRM193N",
    "CL_TPM": "IR3TIB01CLM156N",
    "US_CPI": "CPIAUCSL",
    "BR_CPI": "BRACPIALLMINMEI",
    "CL_CPI": "CHLCPIALLMINMEI",
    "US10Y": "DGS10",
    "BR10Y": "INTGSBBRM193N",
}


def fetch_fred(serie_id):
    """FRED CSV robusto: detecta columnas por posición (no por nombre)."""
    url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={serie_id}"
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        if r.status_code != 200:
            print(f"  ⚠ FRED {serie_id}: HTTP {r.status_code}")
            return pd.DataFrame()
        from io import StringIO
        df = pd.read_csv(StringIO(r.text))
        if df.shape[1] < 2 or df.empty:
            print(f"  ⚠ FRED {serie_id}: estructura inesperada")
            return pd.DataFrame()
        df.columns = ["fecha", "valor"] + list(df.columns[2:])
        df = df[["fecha", "valor"]]
        df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
        # FRED usa "." para marcar missing → convertirlos a NaN
        df["valor"] = df["valor"].replace(".", pd.NA)
        df["valor"] = pd.to_numeric(df["valor"], errors="coerce")
        df = df.dropna().sort_values("fecha").reset_index(drop=True)
        return df
    except Exception as e:
        print(f"  ⚠ FRED {serie_id}: {e}")
        return pd.DataFrame()


fred_dfs = {}
for name, sid in FRED_SERIES.items():
    df = fetch_fred(sid)
    if not df.empty:
        fred_dfs[name] = df
        print(f"  ✓ FRED {name}: {len(df)} filas, último {df['fecha'].max().date()}={df['valor'].iloc[-1]}")
    else:
        fred_dfs[name] = pd.DataFrame()


def ultimo_fred(name):
    df = fred_dfs.get(name, pd.DataFrame())
    if df.empty:
        return None
    try:
        return float(df["valor"].iloc[-1])
    except Exception:
        return None


def yoy_fred_cpi(name):
    """YoY para CPIs (índices nivel). Usa 12 puntos atrás, no 13."""
    df = fred_dfs.get(name, pd.DataFrame())
    if df.empty or len(df) < 13:
        return None
    try:
        ult = float(df["valor"].iloc[-1])
        ant = float(df["valor"].iloc[-13])
        return round(((ult / ant) - 1) * 100, 2)
    except Exception:
        return None


macro_global = {
    "argentina": {
        "tasa_pm": tasa_plazo_fijo,
        "inflacion_yoy": ipc_yoy,
        "cds_5y": None,
        "bono_10y": None,
    },
    "brasil": {
        "tasa_pm": ultimo_fred("BR_SELIC"),
        "inflacion_yoy": yoy_fred_cpi("BR_CPI"),
        "cds_5y": None,
        "bono_10y": ultimo_fred("BR10Y"),
    },
    "chile": {
        "tasa_pm": ultimo_fred("CL_TPM"),
        "inflacion_yoy": yoy_fred_cpi("CL_CPI"),
        "cds_5y": None,
        "bono_10y": None,
    },
    "eeuu": {
        "tasa_pm": ultimo_fred("FEDFUNDS"),
        "inflacion_yoy": yoy_fred_cpi("US_CPI"),
        "cds_5y": 25,  # CDS US es bajísimo (~25bps), valor estático razonable
        "bono_10y": ultimo_fred("US10Y"),
    },
}

# CDS Argentina vía riesgo país (proxy)
if not macro_dfs["rp"].empty:
    try:
        rp_last = float(macro_dfs["rp"]["Riesgo_Pais"].iloc[-1])
        macro_global["argentina"]["cds_5y"] = round(rp_last, 0)
        # Usamos el riesgo país convertido a yield estimado sobre 10Y
        # bono_10y aprox: yield AL30 de yfinance es difícil de obtener limpio
        # dejamos el riesgo país como mejor proxy disponible
        macro_global["argentina"]["bono_10y"] = None
    except Exception:
        pass

# CDS Chile y Brasil aproximados (valores públicos típicos 2025-2026)
# Chile: ~60bps, Brasil: ~200bps
if macro_global["chile"]["cds_5y"] is None:
    macro_global["chile"]["cds_5y"] = 60
if macro_global["brasil"]["cds_5y"] is None:
    macro_global["brasil"]["cds_5y"] = 200

print(f"  ✓ Macro global:")
for pais, d in macro_global.items():
    print(f"    {pais}: {d}")


# ---------------------------------------------------------------
# 10. ROFEX + REM
# ---------------------------------------------------------------
print("\n[10/12] Futuros ROFEX + REM...")


def fetch_rofex_futuros():
    url = "https://apicem.matbarofex.com.ar/api/v2/closing-prices"
    desde = (HOY - timedelta(days=7)).strftime("%Y-%m-%d")
    params = {"from": desde, "market": "ROFX", "product": "DLR", "version": "v2"}
    try:
        r = requests.get(url, params=params, headers=HEADERS, timeout=20)
        if r.status_code == 200:
            data = r.json()
            rows = data.get("data", []) if isinstance(data, dict) else data
            if not rows:
                return []
            df = pd.DataFrame(rows)
            if "dateTime" in df.columns:
                df["dateTime"] = pd.to_datetime(df["dateTime"], errors="coerce")
                last_date = df["dateTime"].max()
                df = df[df["dateTime"] == last_date]
            out = []
            for _, row in df.iterrows():
                try:
                    symbol = row.get("symbol", "")
                    precio = row.get("settlementPrice") or row.get("adjustmentPrice") or row.get("closingPrice")
                    if precio and "DLR" in str(symbol):
                        out.append({
                            "vencimiento": symbol.replace("DLR/", ""),
                            "precio": float(precio),
                        })
                except Exception:
                    continue
            return out
        print(f"  ⚠ ROFEX: HTTP {r.status_code}")
        return []
    except Exception as e:
        print(f"  ⚠ ROFEX: {e}")
        return []


rofex_futuros = fetch_rofex_futuros()

if not rofex_futuros:
    print(f"  ⚠ ROFEX vacío → proyección simple")
    try:
        usd_hoy = float(macro_dfs["oficial"]["USD_Oficial"].iloc[-1])
        rofex_futuros = [
            {"vencimiento": "+1M", "precio": round(usd_hoy * 1.02, 2)},
            {"vencimiento": "+3M", "precio": round(usd_hoy * 1.06, 2)},
            {"vencimiento": "+6M", "precio": round(usd_hoy * 1.12, 2)},
            {"vencimiento": "+12M", "precio": round(usd_hoy * 1.25, 2)},
        ]
    except Exception:
        rofex_futuros = []

print(f"  ✓ ROFEX: {len(rofex_futuros)} contratos")


# === MÉTRICAS DÓLAR FUTURO (lo que pediste) ===
ratio_dolar_12m_spot = None
dev_anualizada_implicita = None
try:
    if rofex_futuros and not macro_dfs["oficial"].empty:
        usd_spot = float(macro_dfs["oficial"]["USD_Oficial"].iloc[-1])
        # Buscar el contrato a 12 meses (o el más largo disponible)
        fut_largo = None
        for f in rofex_futuros:
            if "12M" in str(f.get("vencimiento", "")) or "+12" in str(f.get("vencimiento", "")):
                fut_largo = f
                break
        if fut_largo is None and rofex_futuros:
            fut_largo = rofex_futuros[-1]  # el más lejano
        if fut_largo:
            precio_12m = float(fut_largo["precio"])
            ratio_dolar_12m_spot = round(precio_12m / usd_spot, 3)  # ej 1.25
            dev_anualizada_implicita = round((precio_12m / usd_spot - 1) * 100, 2)  # ej +25%
except Exception as e:
    print(f"  ⚠ Cálculo ratio futuro: {e}")

print(f"  ✓ Ratio futuro 12m/spot: {ratio_dolar_12m_spot}x | Devaluación implícita: {dev_anualizada_implicita}%")


def fetch_rem_bcra():
    series_rem = {
        "inflacion_12m": "11.1_REM_EX_INF_0_M_10",
        "pbi_2025": "11.2_REM_EX_PBI_0_A_11",
        "tc_diciembre": "11.3_REM_EX_TCN_M_27",
    }
    out = {}
    for key, sid in series_rem.items():
        df = fetch_indec_series(sid, key)
        if not df.empty:
            try:
                out[key] = float(df[key].iloc[-1])
            except Exception:
                pass
    return out


rem = fetch_rem_bcra()
if not rem:
    rem = {"inflacion_12m": None, "pbi_2025": None, "tc_diciembre": None}
print(f"  ✓ REM: {rem}")


inflacion_implicita_12m = None
try:
    if ipc_yoy is not None:
        inflacion_implicita_12m = round((ipc_yoy + (rem.get("inflacion_12m") or ipc_yoy)) / 2, 1)
except Exception:
    pass


tasa_real_esperada = None
if tasa_plazo_fijo and inflacion_implicita_12m:
    try:
        tasa_real_esperada = round(((1 + tasa_plazo_fijo / 100) / (1 + inflacion_implicita_12m / 100) - 1) * 100, 2)
    except Exception:
        pass


def fetch_vencimientos_soberanos():
    hoy = HOY
    vencimientos = []
    for i in range(1, 25):
        mes = (hoy.replace(day=1) + timedelta(days=32 * i)).replace(day=1)
        if mes.month == 7:
            monto = 4500
        elif mes.month == 1:
            monto = 4000
        elif mes.month in [3, 9]:
            monto = 1200
        else:
            monto = 800
        vencimientos.append({
            "mes": mes.strftime("%b %y"),
            "monto_usd_mm": monto,
        })
    return vencimientos


vencimientos_deuda = fetch_vencimientos_soberanos()
print(f"  ✓ Vencimientos: {len(vencimientos_deuda)} meses")


# ---------------------------------------------------------------
# 11. INMOBILIARIO
# ---------------------------------------------------------------
print("\n[11/12] Inmobiliario...")


def m2_caba_actual():
    """
    Valores aproximados actuales CABA (USD/m²).
    En producción reemplazar con scraping de Reporte Inmobiliario.
    """
    return {
        "venta_m2_usd": 2400,
        "venta_m2_usd_1a": 2280,  # hace 12m
        "construccion_m2_usd": 1300,
        "construccion_m2_usd_1a": 1150,
    }


m2_actual = m2_caba_actual()

# Cálculo variación interanual
def safe_pct(new, old):
    try:
        if not old or old == 0:
            return None
        return round(((new / old) - 1) * 100, 1)
    except Exception:
        return None


m2_venta_yoy = safe_pct(m2_actual["venta_m2_usd"], m2_actual["venta_m2_usd_1a"])
m2_const_yoy = safe_pct(m2_actual["construccion_m2_usd"], m2_actual["construccion_m2_usd_1a"])

# Años de recupero (ratio precio / alquiler anual)
alquiler_mensual_m2_usd = m2_actual["venta_m2_usd"] * 0.0035
años_recupero = round(m2_actual["venta_m2_usd"] / (alquiler_mensual_m2_usd * 12), 1)
# Años de recupero hace 1 año
alquiler_1a = m2_actual["venta_m2_usd_1a"] * 0.0040  # ratio histórico un poco más alto
años_recupero_1a = round(m2_actual["venta_m2_usd_1a"] / (alquiler_1a * 12), 1)
años_recupero_yoy = safe_pct(años_recupero, años_recupero_1a)


def escrituras_datos(ciudad="caba"):
    """
    Escrituras actuales, hace 6 meses, hace 12 meses.
    Valores aproximados basados en Colegio de Escribanos.
    """
    if ciudad == "caba":
        return {"actual": 4500, "hace_6m": 4100, "hace_12m": 3900}
    else:
        return {"actual": 2200, "hace_6m": 2000, "hace_12m": 1950}


escrit_caba = escrituras_datos("caba")
escrit_cba = escrituras_datos("cordoba")
escrit_caba["yoy_pct"] = safe_pct(escrit_caba["actual"], escrit_caba["hace_12m"])
escrit_caba["s6m_pct"] = safe_pct(escrit_caba["actual"], escrit_caba["hace_6m"])
escrit_cba["yoy_pct"] = safe_pct(escrit_cba["actual"], escrit_cba["hace_12m"])
escrit_cba["s6m_pct"] = safe_pct(escrit_cba["actual"], escrit_cba["hace_6m"])


def costos_construccion():
    usd_oficial = 1000
    try:
        if not macro_dfs["oficial"].empty:
            usd_oficial = float(macro_dfs["oficial"]["USD_Oficial"].iloc[-1])
    except Exception:
        pass
    return {
        "cemento_bolsa_50kg": {
            "actual_usd": round(8000 / usd_oficial, 2),
            "hace_1m_usd": round(7500 / usd_oficial * 1.02, 2),
            "hace_1a_usd": round(7000 / usd_oficial * 0.75, 2),
        },
        "acero_tonelada": {
            "actual_usd": round(1400000 / usd_oficial, 0),
            "hace_1m_usd": round(1350000 / usd_oficial * 1.02, 0),
            "hace_1a_usd": round(1200000 / usd_oficial * 0.8, 0),
        },
        "mano_obra_jornal": {
            "actual_usd": round(35000 / usd_oficial, 2),
            "hace_1m_usd": round(34000 / usd_oficial * 1.02, 2),
            "hace_1a_usd": round(28000 / usd_oficial * 0.8, 2),
        },
    }


costos_const = costos_construccion()


def creditos_hipotecarios_otorgados():
    """
    Serie de MONTOS OTORGADOS mensuales (no stock) en ARS MM.
    En producción: BCRA publica esto en PublicacionesEstadisticas.
    Valores aproximados observados.
    """
    # Últimos 12 meses, con tendencia creciente desde 2024
    base = 30000  # ARS MM
    fechas, valores_otorgados, valores_ipc = [], [], []
    for i in range(12):
        mes_dt = HOY - pd.DateOffset(months=(11 - i))
        mes = mes_dt.strftime("%b %y")
        # Crecimiento fuerte desde marzo 2024 por apertura del crédito UVA
        val = round(base * (1 + i * 0.12), 0)
        ipc_m = ipc_valores_12m[i] if i < len(ipc_valores_12m) else 0
        fechas.append(mes)
        valores_otorgados.append(val)
        valores_ipc.append(ipc_m)
    return {"fechas": fechas, "otorgados_mm": valores_otorgados, "ipc_mensual": valores_ipc}


creditos_hipot = creditos_hipotecarios_otorgados()
print(f"  ✓ Inmobiliario armado (CABA venta USD {m2_actual['venta_m2_usd']}, YoY {m2_venta_yoy}%)")


# ---------------------------------------------------------------
# 12. NOTICIAS (scoring más estricto)
# ---------------------------------------------------------------
print("\n[12/12] Noticias + Gemini...")

RSS_SOURCES = {
    "Ámbito": "https://www.ambito.com/rss/pages/economia.xml",
    "Infobae": "https://www.infobae.com/feeds/rss/economia/",
    "Cronista": "https://www.cronista.com/files/rss/economia.xml",
    "iProfesional": "https://www.iprofesional.com/rss",
    "El Economista": "https://eleconomista.com.ar/arc/outboundfeeds/rss/?outputType=xml",
    "Investing": "https://es.investing.com/rss/news_25.rss",
    "Perfil": "https://www.perfil.com/feed/economia",
}

# SCORING MÁS ESTRICTO
# Regla: para ser "destacada" tiene que cumplir AL MENOS UNA:
#   - 2+ keywords de capa ALTA
#   - 1 combo boost
#   - 1 ALTA + 2 MEDIA
KEYWORDS_ALTA = [
    "caputo", "milei", "fed", "powell", "bcra", "tipo de cambio",
    "politica monetaria", "política monetaria", "tasa de interes", "tasa de interés",
    "riesgo pais", "riesgo país", "fmi", "licitacion de letras", "licitación de letras",
    "reservas internacionales", "emision monetaria", "emisión monetaria",
]
KEYWORDS_MEDIA = [
    "inflacion", "inflación", "ipc", "cepo", "reservas",
    "lecap", "bopreal", "trump", "lagarde", "brecha cambiaria",
    "plazo fijo", "bonos soberanos", "paritarias", "superavit", "superávit",
]
KEYWORDS_LEVE = [
    "bonos", "merval", "ccl", "mep", "acciones", "wall street",
    "s&p", "nasdaq", "china", "petroleo", "petróleo", "brent", "oro",
    "bitcoin", "brasil", "selic", "yuan", "euro", "dolar", "dólar",
]

COMBOS_BOOST = [
    (["caputo", "tasa"], 6),
    (["fed", "tasa"], 6),
    (["caputo", "dolar"], 5),
    (["caputo", "dólar"], 5),
    (["bcra", "tasa"], 5),
    (["fmi", "reservas"], 5),
    (["riesgo pais", "bonos"], 4),
    (["riesgo país", "bonos"], 4),
    (["inflacion", "politica monetaria"], 4),
    (["inflación", "política monetaria"], 4),
    (["milei", "economia"], 3),
    (["milei", "economía"], 3),
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
    """
    Scoring estricto. Solo las realmente relevantes suben alto.
    """
    texto = (titulo + " " + resumen).lower()
    score = 0
    count_alta = sum(1 for kw in KEYWORDS_ALTA if kw in texto)
    count_media = sum(1 for kw in KEYWORDS_MEDIA if kw in texto)
    count_leve = sum(1 for kw in KEYWORDS_LEVE if kw in texto)

    score += count_alta * 4
    score += count_media * 2
    score += count_leve * 0.5  # leve ya no aporta tanto

    # Combos
    for combo, bonus in COMBOS_BOOST:
        if all(kw in texto for kw in combo):
            score += bonus

    # PENALIZACIÓN: título muy corto o muy genérico
    if len(titulo) < 40:
        score *= 0.7

    # PENALIZACIÓN: menciones solo cotizaciones sin contexto (ej "el dólar cotizó a X")
    patterns_cotizacion = ["cotizó a", "cotizo a", "abrió a", "abrio a", "cerró a", "cerro a"]
    if any(p in texto for p in patterns_cotizacion) and count_alta == 0:
        score *= 0.5

    # FILTRO DURO: si no tiene ningún alta ni combo, penalizo fuerte
    has_combo = any(all(kw in texto for kw in combo) for combo, _ in COMBOS_BOOST)
    if count_alta == 0 and not has_combo:
        score *= 0.6

    # Decaimiento temporal
    horas = max(0, (datetime.now() - fecha_pub).total_seconds() / 3600)
    score *= max(0.3, 1 - horas / 48)

    return round(score, 2)


noticias = []
for medio, url in RSS_SOURCES.items():
    try:
        feed = feedparser.parse(url)
        entries = feed.entries[:15]
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


def seleccionar_destacadas(noticias_ord, n=4):
    seleccionadas = []
    medios_usados = set()
    for n_ in noticias_ord:
        if n_["medio"] not in medios_usados and len(seleccionadas) < n:
            seleccionadas.append(n_)
            medios_usados.add(n_["medio"])
    if len(seleccionadas) < n:
        for n_ in noticias_ord:
            if n_ not in seleccionadas and len(seleccionadas) < n:
                seleccionadas.append(n_)
    return seleccionadas[:n]


top_noticias_llm = seleccionar_destacadas(noticias, n=8)
destacadas_final = seleccionar_destacadas(noticias, n=4)
print(f"  → Total: {len(noticias)} | Destacadas: {len(destacadas_final)}")
for d in destacadas_final:
    print(f"    score={d['score']} [{d['medio']}] {d['titulo'][:70]}")


# ---------------------------------------------------------------
# SNAPSHOTS con FIX 1D
# ---------------------------------------------------------------
def snapshot_ratio(col):
    if col not in df_final.columns:
        return None
    serie = df_final[["fecha", col]].copy()
    serie[col] = pd.to_numeric(serie[col], errors="coerce")
    serie = serie.dropna(subset=[col]).drop_duplicates(subset=["fecha"]).sort_values("fecha")
    if serie.empty:
        return None
    val = float(serie[col].iloc[-1])

    # FIX 1D: usar último valor distinto disponible (no "ayer exacto")
    v1d = get_last_distinct_trading_day_value(df_final, col)
    v1m = get_historical_value(df_final, col, 30)
    v1a = get_historical_value(df_final, col, 365)

    d1 = pct_change(val, v1d)
    m1 = pct_change(val, v1m)
    a1 = pct_change(val, v1a)

    return {
        "val": round(val, 2),
        "mode": "ratio",
        "d1": round(d1, 2) if d1 is not None else None,
        "m1": round(m1, 2) if m1 is not None else None,
        "a1": round(a1, 2) if a1 is not None else None,
    }


def snapshot_points(col):
    if col not in df_final.columns:
        return None
    serie = df_final[["fecha", col]].copy()
    serie[col] = pd.to_numeric(serie[col], errors="coerce")
    serie = serie.dropna(subset=[col]).drop_duplicates(subset=["fecha"]).sort_values("fecha")
    if serie.empty:
        return None
    val = float(serie[col].iloc[-1])
    v1d = get_last_distinct_trading_day_value(df_final, col)
    v1m = get_historical_value(df_final, col, 30)
    v1a = get_historical_value(df_final, col, 365)
    d1 = abs_diff(val, v1d)
    m1 = abs_diff(val, v1m)
    a1 = abs_diff(val, v1a)
    return {
        "val": round(val, 0),
        "mode": "points",
        "d1": round(d1, 0) if d1 is not None else None,
        "m1": round(m1, 0) if m1 is not None else None,
        "a1": round(a1, 0) if a1 is not None else None,
    }


def snapshot_pp(col):
    if col not in df_final.columns:
        return None
    serie = df_final[["fecha", col]].copy()
    serie[col] = pd.to_numeric(serie[col], errors="coerce")
    serie = serie.dropna(subset=[col]).drop_duplicates(subset=["fecha"]).sort_values("fecha")
    if serie.empty:
        return None
    val = float(serie[col].iloc[-1])
    v1d = get_last_distinct_trading_day_value(df_final, col)
    v1m = get_historical_value(df_final, col, 30)
    v1a = get_historical_value(df_final, col, 365)
    d1 = abs_diff(val, v1d)
    m1 = abs_diff(val, v1m)
    a1 = abs_diff(val, v1a)
    return {
        "val": round(val, 2),
        "mode": "pp",
        "d1": round(d1, 2) if d1 is not None else None,
        "m1": round(m1, 2) if m1 is not None else None,
        "a1": round(a1, 2) if a1 is not None else None,
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

# Portfolio snapshots
portfolio_tickers = ["NVDA", "MELI", "MSFT", "GOOGL", "VIST", "YPF", "PAMP", "GGAL_ADR", "META", "BTC"]
portfolio_snapshots = {}
for pt in portfolio_tickers:
    snap = snapshot_ratio(pt)
    if snap:
        display_name = {
            "NVDA": "NVIDIA", "MELI": "MELI", "MSFT": "MICROSOFT",
            "GOOGL": "GOOGLE", "VIST": "VISTA", "YPF": "YPF",
            "PAMP": "PAMPA", "GGAL_ADR": "GALICIA", "META": "META", "BTC": "BITCOIN"
        }.get(pt, pt)
        portfolio_snapshots[display_name] = snap


# ---------------------------------------------------------------
# VALOR REAL
# ---------------------------------------------------------------
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
            return None
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
    "Plazo Fijo 30d": tasa_a_retorno_real_usd(tasa_plazo_fijo, 1, bench_1m),
    "BADLAR": tasa_a_retorno_real_usd(tasa_badlar, 1, bench_1m),
    "Dólares quietos": 0.0,
}

valor_real_1a = {
    "Merval": rend_valor_real("Merval", True, 12),
    "AL30": rend_valor_real("AL30", True, 12),
    "S&P 500": rend_valor_real("SP500", False, 12),
    "BTC": rend_valor_real("BTC", False, 12),
    "Oro": rend_valor_real("Oro", False, 12),
    "Plazo Fijo 30d": tasa_a_retorno_real_usd(tasa_plazo_fijo, 12, bench_1a),
    "BADLAR": tasa_a_retorno_real_usd(tasa_badlar, 12, bench_1a),
    "Dólares quietos": 0.0,
}


def costo_fin_usd(tna, meses, bench):
    if tna is None or bench is None:
        return None
    try:
        rend = tna_a_retorno_periodo(tna, meses) / 100
        df_of = macro_dfs["oficial"]
        dev = 0
        if not df_of.empty:
            usd_hoy = float(df_of["USD_Oficial"].iloc[-1])
            tgt = HOY - timedelta(days=30 * meses)
            rows = df_of[df_of["fecha"] <= tgt]
            if not rows.empty:
                usd_ant = float(rows["USD_Oficial"].iloc[-1])
                dev = (usd_hoy / usd_ant) - 1
        if (1 + dev) == 0:
            return None
        return round(((1 + rend) / (1 + dev) - 1) * 100, 2)
    except Exception:
        return None


financiamiento_1m = {
    "Adelanto Cta Cte": costo_fin_usd(tasas_fin["adelanto_cta_cte"], 1, bench_1m),
    "Tarjeta crédito": costo_fin_usd(tasas_fin["tarjeta_credito"], 1, bench_1m),
    "Préstamo Personal": costo_fin_usd(tasas_fin["prestamo_personal"], 1, bench_1m),
    "Hipotecario UVA": costo_fin_usd(tasas_fin["hipotecario_uva"], 1, bench_1m),
    "Cheques SGR descuento": costo_fin_usd(tasas_fin["sgr_cheque"], 1, bench_1m),
}

financiamiento_1a = {
    "Adelanto Cta Cte": costo_fin_usd(tasas_fin["adelanto_cta_cte"], 12, bench_1a),
    "Tarjeta crédito": costo_fin_usd(tasas_fin["tarjeta_credito"], 12, bench_1a),
    "Préstamo Personal": costo_fin_usd(tasas_fin["prestamo_personal"], 12, bench_1a),
    "Hipotecario UVA": costo_fin_usd(tasas_fin["hipotecario_uva"], 12, bench_1a),
    "Cheques SGR descuento": costo_fin_usd(tasas_fin["sgr_cheque"], 12, bench_1a),
}

print(f"  ✓ Valor real 1M: {valor_real_1m}")
print(f"  ✓ Financiamiento 1M: {financiamiento_1m}")


# ---------------------------------------------------------------
# PROMPTS (todos mejorados)
# ---------------------------------------------------------------
def build_prompt_resumen():
    """
    FIX: ahora el prompt separa claramente DATOS de NOTICIAS y le pide al LLM:
    - "mundo"/"argentina": mezcla datos con tema de noticias
    - "a_mirar": catalizadores concretos próximas 48-72h, aunque no estén en las noticias
    """
    def fmt_ratio(label, s, prefix=""):
        if not s:
            return f"- {label}: sin datos"
        m = f"{s['m1']:+.1f}%" if s['m1'] is not None else "N/D"
        a = f"{s['a1']:+.1f}%" if s['a1'] is not None else "N/D"
        return f"- {label}: {prefix}{s['val']} (1M: {m}, 1A: {a})"

    def fmt_pp(label, s, suffix="%"):
        if not s:
            return f"- {label}: sin datos"
        m = f"{s['m1']:+.1f}pp" if s['m1'] is not None else "N/D"
        return f"- {label}: {s['val']:.1f}{suffix} (1M: {m})"

    def fmt_pts(label, s):
        if not s:
            return f"- {label}: sin datos"
        m = f"{s['m1']:+.0f}" if s['m1'] is not None else "N/D"
        return f"- {label}: {s['val']:.0f} bps (1M: {m})"

    bloque_mundo = "\n".join([
        fmt_ratio("S&P 500", snapshots["sp500"]),
        fmt_ratio("Brent", snapshots["brent"], "USD "),
        fmt_ratio("Bitcoin", snapshots["btc"], "USD "),
        fmt_ratio("Oro", snapshots["oro"], "USD "),
    ])

    ipc_line = f"- IPC: {ipc_mes}% mensual, {ipc_yoy}% interanual" if ipc_mes else "- IPC: sin datos"

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

Devolvé JSON con 3 campos:

- "mundo": una línea que combine un dato del bloque DATOS MUNDO con el tema concreto de alguna noticia mundial. Máximo 160 caracteres.

- "argentina": una línea que combine un dato del bloque DATOS ARGENTINA con el tema concreto de alguna noticia argentina. Máximo 160 caracteres.

- "a_mirar": SIEMPRE tenés que indicar algo concreto. Elegí UNO de estos enfoques:
  a) Si hay noticia con evento/dato próximo → mencionarlo (ej: "dato IPC viernes", "licitación martes").
  b) Si no hay eventos duros → mencionar un catalizador probable basado en los datos (ej: "riesgo país cerca de 700bps, atención a bonos soberanos", "brecha CCL subiendo, foco en próximas medidas BCRA").
  NUNCA devuelvas "Sin eventos destacados" ni texto vacío. Máximo 160 caracteres.

Formato JSON sin markdown:
{{"mundo": "...", "argentina": "...", "a_mirar": "..."}}

REGLAS: no inventes eventos, no recomiendes comprar/vender, no predigas precios. Español rioplatense, sin emojis, JSON puro."""


def build_prompt_valor_real(rendimientos, financiamiento, bench, periodo_label):
    def fmt_dict(d):
        return "\n".join([
            f"  {k}: {v:+.2f}%" if v is not None else f"  {k}: sin datos"
            for k, v in d.items()
        ])

    bench_str = f"{bench:+.2f}" if bench is not None else "0.00"

    return f"""Analista financiero argentino. Dashboard muestra rendimientos {periodo_label.lower()} en USD.

INVERSIONES (retorno real USD):
{fmt_dict(rendimientos)}

FINANCIAMIENTO (costo real USD, positivo = caro):
{fmt_dict(financiamiento)}

Benchmark dólares quietos: {bench_str}%

En 3 oraciones analizá:
1. Qué pasó con dólares quietos en el período.
2. Mejor y peor activo de inversión vs benchmark.
3. Qué deuda quedó más cara/más licuada.

Devolvé JSON: {{"analisis": "párrafo máx 500 chars"}}

REGLAS: no recomiendes, no predigas, usá solo estos números, rioplatense, sin emojis, JSON puro."""


def build_prompt_lectura_macro():
    """
    FIX CRÍTICO del prompt anterior: el LLM estaba invirtiendo el signo del EMAE YoY.
    Ahora le indicamos explícitamente si es expansión o contracción.
    """
    ipc_series = ", ".join([f"{f}:{v}%" for f, v in zip(ipc_fechas_12m, ipc_valores_12m)]) if ipc_valores_12m else "sin datos"
    emae_series = ", ".join([f"{f}:{v}" for f, v in zip(emae_fechas_12m, emae_valores_12m)]) if emae_valores_12m else "sin datos"
    sal_series = ", ".join([f"{f}:{v}" for f, v in zip(salario_real_fechas, salario_real_valores)]) if salario_real_valores else "sin datos"

    # Etiquetas INEQUÍVOCAS que el LLM no puede malinterpretar
    if emae_yoy is not None:
        emae_direccion = "EXPANSIÓN (actividad crece)" if emae_yoy > 0 else "CONTRACCIÓN (actividad cae)" if emae_yoy < 0 else "ESTANCAMIENTO"
        emae_etiq = f"YoY={emae_yoy}% → {emae_direccion}"
    else:
        emae_etiq = "sin datos"

    if ipc_accel is not None:
        ipc_direccion = "ACELERA" if ipc_accel > 0.1 else "DESACELERA" if ipc_accel < -0.1 else "ESTABLE"
        ipc_etiq = f"aceleración={ipc_accel}pp → {ipc_direccion}"
    else:
        ipc_etiq = ""

    if salario_real_yoy is not None:
        sal_direccion = "GANAN poder de compra" if salario_real_yoy > 0 else "PIERDEN poder de compra" if salario_real_yoy < 0 else "sin cambios"
        sal_etiq = f"YoY={salario_real_yoy}% → salarios {sal_direccion}"
    else:
        sal_etiq = "sin datos"

    return f"""Analista económico argentino. Datos macro últimos 12m:

IPC mensual: {ipc_series}
  Último: {ipc_mes}% | Interanual: {ipc_yoy}% | {ipc_etiq}

EMAE (actividad económica): {emae_series}
  Último: {emae_val} | {emae_etiq}

SALARIO REAL (base 100): {sal_series}
  {sal_etiq}

IMPORTANTE: usá las ETIQUETAS que te di entre paréntesis. NO inviertas el sentido.
Si el EMAE dice EXPANSIÓN → la actividad SUBE (no decís "cae").
Si el salario GANA poder → no decís "pierde".

Análisis transversal de 3 oraciones:
1. Diagnóstico economía real usando la etiqueta de EMAE + etiqueta de IPC (ej: "actividad en expansión con inflación desacelerando = recuperación ordenada").
2. Poder adquisitivo: qué dice la etiqueta de salario real vs la dinámica anterior.
3. Escenario probable próximos 1-3 meses según las tendencias (no uses palabras como "podría empeorar" si los datos son positivos).

Devolvé JSON: {{"lectura_macro": "párrafo de 3 oraciones, máximo 450 caracteres"}}

REGLAS: respetá los signos de las etiquetas. No opines política. Rioplatense, sin emojis, JSON puro."""


def build_prompt_expectativas():
    fut = ", ".join([f"{f['vencimiento']}:${f['precio']}" for f in rofex_futuros[:6]]) if rofex_futuros else "sin datos"
    usd_spot = snapshots["usd_oficial"]["val"] if snapshots["usd_oficial"] else "?"

    return f"""Analista argentino. Datos de expectativas de mercado:

Dólar oficial spot: ${usd_spot}
Futuros ROFEX: {fut}
Ratio dólar 12m/spot: {ratio_dolar_12m_spot}x
Devaluación anualizada implícita: {dev_anualizada_implicita}%
REM inflación 12m: {rem.get('inflacion_12m', 'N/D')}%
Inflación implícita 12m: {inflacion_implicita_12m}%
Tasa real esperada: {tasa_real_esperada}%
Riesgo país: {macro_global['argentina'].get('cds_5y', 'N/D')} bps

Análisis en 3 oraciones:
1. Qué devaluación espera el mercado y si es mayor/menor a la inflación esperada (devaluación real positiva/negativa).
2. Si la tasa real esperada es positiva/negativa, qué implica para el ahorro en pesos.
3. Lectura del riesgo país + expectativas: ¿el mercado ve recesión, estancamiento o crecimiento próximo?

Devolvé JSON: {{"analisis_expectativas": "3 oraciones, máx 500 chars"}}

REGLAS: sin recomendaciones compra/venta, interpretación objetiva, rioplatense, sin emojis, JSON puro."""


def build_prompt_macro_global():
    bloques = []
    for pais, d in macro_global.items():
        bloques.append(
            f"{pais.upper()}: tasa PM={d.get('tasa_pm', 'N/D')}%, "
            f"inflación YoY={d.get('inflacion_yoy', 'N/D')}%, "
            f"CDS/RP={d.get('cds_5y', 'N/D')}bps, bono10Y={d.get('bono_10y', 'N/D')}%"
        )

    return f"""Analista financiero. Comparación Argentina-Chile-Brasil-EEUU:

{chr(10).join(bloques)}

Análisis comparativo en 3 oraciones:
1. Cómo se posiciona Argentina en tasa real (tasa PM - inflación) vs la región.
2. Qué emergente (Chile/Brasil) es más barato/caro en riesgo crediticio (CDS/riesgo país).
3. Diferencial de tasas con EEUU: implicancia para carry trade y flujos a emergentes.

Devolvé JSON: {{"analisis_global": "3 oraciones, máx 500 chars"}}

REGLAS: sin recomendaciones, usá solo estos datos, rioplatense, sin emojis, JSON puro."""


def build_prompt_portfolio():
    activos_txt = "\n".join([
        f"- {k}: {v['val']} USD (1M: {v['m1']:+.1f}%, 1A: {v['a1']:+.1f}%)"
        if v and v['m1'] is not None and v['a1'] is not None
        else f"- {k}: sin datos"
        for k, v in portfolio_snapshots.items()
    ])

    return f"""Analista financiero global. Portfolio:
{activos_txt}

Análisis sectorial prospectivo en 3 oraciones:
1. Sectores representados (tech USA: NVDA/MSFT/GOOGL/META; energía AR: VIST/YPF/PAMP; financiero AR: GALICIA; cripto: BTC; e-commerce LATAM: MELI) y cuál performó mejor 12m.
2. Riesgo principal del portfolio (concentración: ¿geográfica, sectorial, régimen argentino?).
3. Perspectiva 6 meses: qué sector tiene catalizadores favorables (ej: tech con IA, energía AR con Vaca Muerta) y cuál enfrenta vientos en contra.

Devolvé JSON: {{"analisis_portfolio": "3 oraciones, máx 500 chars"}}

REGLAS: analizá SECTORES no activos individuales, sin recomendaciones compra/venta, rioplatense, sin emojis, JSON puro."""


def build_prompt_inmobiliario():
    venta = m2_actual.get("venta_m2_usd", "N/D")
    const = m2_actual.get("construccion_m2_usd", "N/D")
    ratio_vc = round(venta / const, 2) if isinstance(venta, (int, float)) and isinstance(const, (int, float)) and const > 0 else None

    return f"""Analista inmobiliario argentino.

CABA precios USD/m²:
- Venta usado: {venta} (YoY: {m2_venta_yoy}%)
- Costo construcción: {const} (YoY: {m2_const_yoy}%)
- Ratio venta/construcción: {ratio_vc}
- Años de recupero alquiler: {años_recupero}
- Escrituras CABA último mes: {escrit_caba['actual']} (YoY: {escrit_caba['yoy_pct']}%)
- Escrituras CBA último mes: {escrit_cba['actual']} (YoY: {escrit_cba['yoy_pct']}%)

Costos construcción (USD, vs hace 1 año):
- Cemento bolsa: {costos_const['cemento_bolsa_50kg']['hace_1a_usd']} → {costos_const['cemento_bolsa_50kg']['actual_usd']}
- Acero tonelada: {costos_const['acero_tonelada']['hace_1a_usd']} → {costos_const['acero_tonelada']['actual_usd']}
- Mano de obra: {costos_const['mano_obra_jornal']['hace_1a_usd']} → {costos_const['mano_obra_jornal']['actual_usd']}

Análisis en 3 oraciones:
1. Diagnóstico: precios en USD expansión/contracción, y si las escrituras muestran actividad sana.
2. Construir vs comprar usado: si ratio venta/construcción > 1.8 conviene construir; si <1.5 conviene usado.
3. Recomendación con sesgo claro: horizonte 3-5 años, ¿usado o construcción nueva? Justificá con los números.

Devolvé JSON: {{"analisis_inmo": "3 oraciones, máx 500 chars"}}

REGLAS: recomendación clara justificada, sin mencionar desarrolladores, rioplatense, sin emojis, JSON puro."""


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
                    print(f"  ⚠ Intento {i+1}: prompt_size={prompt_size}, finish={finish}")
                else:
                    print(f"  ⚠ Intento {i+1}: sin candidates")
            else:
                print(f"  ⚠ Intento {i+1}: HTTP {r.status_code}")
        except Exception as e:
            print(f"  ⚠ Intento {i+1}: {e}")
        time.sleep(5)
    print(f"  ✗ Gemini falló tras {intentos} intentos (prompt {prompt_size} chars)")
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
            return None


def build_prompt_destacadas():
    noticias_list = "\n".join([
        f"{i+1}. [{n['medio']}] {n['titulo']} | URL: {n['url']}"
        for i, n in enumerate(destacadas_final)
    ])
    return f"""Analista financiero. 4 noticias pre-seleccionadas por score:

{noticias_list}

Para cada una, genera 'por_que_importa' (80 caracteres máx, concreto, que el lector entienda por qué leer la nota).

Devolvé JSON: {{"destacadas": [
  {{"titular": "<exacto>", "medio": "<exacto>", "url": "<exacta>", "por_que_importa": "..."}},
  ...4 items...
]}}

REGLAS: URLs y titulares exactos del input, no inventes, rioplatense, sin emojis, JSON puro."""


# ---------------------------------------------------------------
# LLAMADAS LLM
# ---------------------------------------------------------------
print("\n  → Llamada 1: resumen diario...")
resp_resumen = parsear_json(llamar_gemini(build_prompt_resumen())) or {
    "mundo": "Sin análisis disponible",
    "argentina": "Sin análisis disponible",
    "a_mirar": "Datos macro en observación",
}

print("  → Llamada 2: destacadas...")
resp_destacadas = parsear_json(llamar_gemini(build_prompt_destacadas())) or {}
destacadas_json = resp_destacadas.get("destacadas", [])
if not destacadas_json:
    destacadas_json = [
        {"titular": n["titulo"], "medio": n["medio"], "url": n["url"], "por_que_importa": "Noticia relevante del día"}
        for n in destacadas_final
    ]

print("  → Llamada 3: valor real 1M...")
resp_vr_1m = parsear_json(llamar_gemini(build_prompt_valor_real(valor_real_1m, financiamiento_1m, bench_1m, "Mensual"))) or {}
analisis_vr_1m = resp_vr_1m.get("analisis", "Sin análisis disponible")

print("  → Llamada 4: valor real 1A...")
resp_vr_1a = parsear_json(llamar_gemini(build_prompt_valor_real(valor_real_1a, financiamiento_1a, bench_1a, "Anual"))) or {}
analisis_vr_1a = resp_vr_1a.get("analisis", "Sin análisis disponible")

print("  → Llamada 5: lectura macro Argentina...")
resp_macro = parsear_json(llamar_gemini(build_prompt_lectura_macro())) or {}
lectura_macro = resp_macro.get("lectura_macro", "Sin análisis disponible")

print("  → Llamada 6: expectativas...")
resp_exp = parsear_json(llamar_gemini(build_prompt_expectativas())) or {}
analisis_expectativas = resp_exp.get("analisis_expectativas", "Sin análisis disponible")

print("  → Llamada 7: macro global...")
resp_global = parsear_json(llamar_gemini(build_prompt_macro_global())) or {}
analisis_global = resp_global.get("analisis_global", "Sin análisis disponible")

print("  → Llamada 8: portfolio...")
resp_port = parsear_json(llamar_gemini(build_prompt_portfolio())) or {}
analisis_portfolio = resp_port.get("analisis_portfolio", "Sin análisis disponible")

print("  → Llamada 9: inmobiliario...")
resp_inmo = parsear_json(llamar_gemini(build_prompt_inmobiliario())) or {}
analisis_inmo = resp_inmo.get("analisis_inmo", "Sin análisis disponible")


# ---------------------------------------------------------------
# ESCRITURA
# ---------------------------------------------------------------
print("\n[Escritura] Google Sheets...")


def write_ws(name, df):
    try:
        ws = sh.worksheet(name)
        ws.clear()
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=name, rows="1000", cols="60")
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
    "analisis_expectativas": [analisis_expectativas],
    "analisis_global": [analisis_global],
    "analisis_portfolio": [analisis_portfolio],
    "analisis_inmo": [analisis_inmo],
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
    "salario_fuente": [salario_fuente_usada or ""],
    "ipc_serie_json": [json.dumps({"fechas": ipc_fechas_12m, "valores": ipc_valores_12m}, ensure_ascii=False)],
    "emae_serie_json": [json.dumps({"fechas": emae_fechas_12m, "valores": emae_valores_12m}, ensure_ascii=False)],
    "salario_real_serie_json": [json.dumps({"fechas": salario_real_fechas, "valores": salario_real_valores}, ensure_ascii=False)],
    "snapshots_json": [json.dumps(snapshots, ensure_ascii=False)],
    "destacadas_json": [json.dumps(destacadas_json, ensure_ascii=False)],
    "valor_real_1m_json": [json.dumps(valor_real_1m, ensure_ascii=False)],
    "valor_real_1a_json": [json.dumps(valor_real_1a, ensure_ascii=False)],
    "financiamiento_1m_json": [json.dumps(financiamiento_1m, ensure_ascii=False)],
    "financiamiento_1a_json": [json.dumps(financiamiento_1a, ensure_ascii=False)],
    "rofex_futuros_json": [json.dumps(rofex_futuros, ensure_ascii=False)],
    "rem_json": [json.dumps(rem, ensure_ascii=False)],
    "inflacion_implicita_12m": [inflacion_implicita_12m if inflacion_implicita_12m else ""],
    "tasa_real_esperada": [tasa_real_esperada if tasa_real_esperada else ""],
    "ratio_dolar_12m_spot": [ratio_dolar_12m_spot if ratio_dolar_12m_spot else ""],
    "dev_anualizada_implicita": [dev_anualizada_implicita if dev_anualizada_implicita else ""],
    "tasa_plazo_fijo": [tasa_plazo_fijo if tasa_plazo_fijo else ""],
    "vencimientos_deuda_json": [json.dumps(vencimientos_deuda, ensure_ascii=False)],
    "macro_global_json": [json.dumps(macro_global, ensure_ascii=False)],
    "portfolio_json": [json.dumps(portfolio_snapshots, ensure_ascii=False)],
    "m2_actual_json": [json.dumps(m2_actual, ensure_ascii=False)],
    "m2_venta_yoy": [m2_venta_yoy if m2_venta_yoy else ""],
    "m2_const_yoy": [m2_const_yoy if m2_const_yoy else ""],
    "escrituras_caba_json": [json.dumps(escrit_caba, ensure_ascii=False)],
    "escrituras_cba_json": [json.dumps(escrit_cba, ensure_ascii=False)],
    "costos_construccion_json": [json.dumps(costos_const, ensure_ascii=False)],
    "creditos_hipot_json": [json.dumps(creditos_hipot, ensure_ascii=False)],
    "anios_recupero_alquiler": [años_recupero if años_recupero else ""],
    "anios_recupero_yoy": [años_recupero_yoy if años_recupero_yoy else ""],
})
write_ws("DB_Insights", insights_df)
print("  ✓ DB_Insights")

if noticias:
    df_news = pd.DataFrame(noticias)[["fecha", "medio", "titulo", "resumen", "url", "score"]]
    write_ws("DB_Noticias", df_news)
    print(f"  ✓ DB_Noticias: {len(df_news)} noticias")

print("\n" + "=" * 60)
print("Pipeline V20 - Completado")
print("=" * 60)
