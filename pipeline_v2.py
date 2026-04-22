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
from io import StringIO

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

print("=" * 60)
print("Pipeline V21 - Iniciando")
print(f"Fecha corrida: {datetime.now(timezone.utc).isoformat()}")
print("=" * 60)

# ---------------------------------------------------------------
# CONFIG - SECRETS
# ---------------------------------------------------------------
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
GCP_JSON = os.environ.get("GCLOUD_SERVICE_ACCOUNT")
FRED_API_KEY = os.environ.get("FRED_API_KEY", "")
CHILE_USER = os.environ.get("CHILE_USER", "")
CHILE_PASS = os.environ.get("CHILE_PASS", "")
ROFEX_USER = os.environ.get("ROFEX_USER", "")
ROFEX_PASS = os.environ.get("ROFEX_PASS", "")
SHEET_NAME = os.environ.get("SHEET_NAME", "Dashboard Macro")

GEMINI_MODEL_FLASH = "gemini-2.5-flash"
GEMINI_MODEL_PRO = "gemini-2.5-pro"  # Más robusto para el flash market que fallaba

if not GEMINI_API_KEY or not GCP_JSON:
    raise RuntimeError("Faltan secrets críticos: GEMINI_API_KEY o GCLOUD_SERVICE_ACCOUNT")

creds = Credentials.from_service_account_info(
    json.loads(GCP_JSON),
    scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ],
)
sh = gspread.authorize(creds).open(SHEET_NAME)

HOY = datetime.today()
HACE_1A = HOY - timedelta(days=365)

HEADERS = {"User-Agent": "Mozilla/5.0 (DashboardMacroAR/21)"}


# ---------------------------------------------------------------
# HELPERS GENERALES
# ---------------------------------------------------------------
def fetch_json(url, timeout=20, verify=True, headers=None):
    try:
        h = HEADERS.copy()
        if headers:
            h.update(headers)
        r = requests.get(url, headers=h, timeout=timeout, verify=verify)
        if r.status_code == 200:
            return r.json()
        print(f"  ⚠ HTTP {r.status_code} en {url[:80]}")
        return None
    except Exception as e:
        print(f"  ⚠ Error fetch {url[:80]}: {e}")
        return None


def fetch_csv(url, timeout=20):
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout)
        if r.status_code == 200:
            return pd.read_csv(StringIO(r.text))
        print(f"  ⚠ HTTP {r.status_code} CSV en {url[:80]}")
        return pd.DataFrame()
    except Exception as e:
        print(f"  ⚠ Error fetch CSV: {e}")
        return pd.DataFrame()


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
    try:
        serie = df[["fecha", col]].copy()
        serie[col] = pd.to_numeric(serie[col], errors="coerce")
        serie = serie.dropna(subset=[col]).drop_duplicates(subset=["fecha"]).sort_values("fecha")
        if len(serie) < 2:
            return None
        return serie[col].iloc[-2]
    except Exception:
        return None


def fetch_indec_series(serie_id, col_name, limit=500):
    """API datos.gob.ar - sigue funcionando para EMAE."""
    url = f"https://apis.datos.gob.ar/series/api/series/?ids={serie_id}&limit={limit}&format=json&sort=asc"
    data = fetch_json(url)
    if not data:
        return pd.DataFrame(columns=["fecha", col_name])
    rows = data.get("data", []) if isinstance(data, dict) else []
    if not rows:
        return pd.DataFrame(columns=["fecha", col_name])
    df = pd.DataFrame(rows, columns=["fecha", col_name])
    df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
    df[col_name] = pd.to_numeric(df[col_name], errors="coerce")
    df = df.dropna().sort_values("fecha").reset_index(drop=True)
    return df


# ---------------------------------------------------------------
# 1. MACRO ARGENTINA (argentinadatos.com)
# ---------------------------------------------------------------
print("\n[1/10] Macro Argentina (argentinadatos.com)...")

endpoints_argdatos = {
    "oficial": ("https://api.argentinadatos.com/v1/cotizaciones/dolares/oficial", "venta", "USD_Oficial"),
    "blue": ("https://api.argentinadatos.com/v1/cotizaciones/dolares/blue", "venta", "USD_Blue"),
    "rp": ("https://api.argentinadatos.com/v1/finanzas/indices/riesgo-pais", "valor", "Riesgo_Pais"),
    "ipc": ("https://api.argentinadatos.com/v1/finanzas/indices/inflacion", "valor", "IPC"),
}

macro_dfs = {}
for key, (url, src_col, dest_col) in endpoints_argdatos.items():
    raw = fetch_json(url)
    if raw:
        df = pd.DataFrame(raw)
        if not df.empty and "fecha" in df.columns:
            df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
            df = df.rename(columns={src_col: dest_col})
            df = df[["fecha", dest_col]].dropna().sort_values("fecha")
            macro_dfs[key] = df
            print(f"  ✓ {dest_col}: {len(df)} filas, último {df['fecha'].max().date()}")
            continue
    macro_dfs[key] = pd.DataFrame(columns=["fecha", dest_col])
    print(f"  ✗ {dest_col}: sin datos")


# ---------------------------------------------------------------
# 2. EMAE + RIPTE (CSV directo)
# ---------------------------------------------------------------
print("\n[2/10] EMAE y RIPTE...")

# EMAE desde API datos.gob.ar
EMAE_SERIE_ID = "143.3_NO_PR_2004_A_21"
df_emae_raw = fetch_indec_series(EMAE_SERIE_ID, "EMAE")
if not df_emae_raw.empty:
    print(f"  ✓ EMAE: {len(df_emae_raw)} filas, último {df_emae_raw['fecha'].max().date()}")
macro_dfs["emae"] = df_emae_raw

# RIPTE desde CSV directo (URL verificada)
RIPTE_CSV_URL = "https://infra.datos.gob.ar/catalog/sspm/dataset/158/distribution/158.1/download/remuneracion-imponible-promedio-trabajadores-estables-ripte-total-pais-pesos-serie-mensual.csv"
df_ripte_raw = pd.DataFrame()
try:
    df_raw = fetch_csv(RIPTE_CSV_URL)
    if not df_raw.empty and "indice_tiempo" in df_raw.columns and "ripte" in df_raw.columns:
        df_ripte_raw = df_raw.rename(columns={"indice_tiempo": "fecha", "ripte": "RIPTE"})
        df_ripte_raw["fecha"] = pd.to_datetime(df_ripte_raw["fecha"], errors="coerce")
        df_ripte_raw["RIPTE"] = pd.to_numeric(df_ripte_raw["RIPTE"], errors="coerce")
        df_ripte_raw = df_ripte_raw.dropna().sort_values("fecha").reset_index(drop=True)
        print(f"  ✓ RIPTE: {len(df_ripte_raw)} filas, último {df_ripte_raw['fecha'].max().date()}, "
              f"valor {df_ripte_raw['RIPTE'].iloc[-1]}")
except Exception as e:
    print(f"  ✗ RIPTE: {e}")

macro_dfs["ripte"] = df_ripte_raw


# ---------------------------------------------------------------
# 3. IPC
# ---------------------------------------------------------------
print("\n[3/10] IPC derivados...")


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

print(f"  ✓ IPC: {ipc_mes}% | YoY: {ipc_yoy}% | Acel: {ipc_accel}pp")


# ---------------------------------------------------------------
# 4. EMAE derivados
# ---------------------------------------------------------------
print("\n[4/10] EMAE...")


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
    target = last_date - pd.DateOffset(months=12)
    ant = df_emae_raw[df_emae_raw["fecha"] <= target]
    if not ant.empty:
        delta = pct_change(emae_val, ant["EMAE"].iloc[-1])
        emae_yoy = round(delta, 2) if delta is not None else None
    emae_fechas_12m, emae_valores_12m = serie_12m(df_emae_raw, "EMAE")

print(f"  ✓ EMAE: val={emae_val} | YoY={emae_yoy}% | age={emae_age_days}d")


# ---------------------------------------------------------------
# 5. SALARIO REAL (RIPTE / IPC)
# ---------------------------------------------------------------
print("\n[5/10] Salario real...")

salario_real_yoy = None
salario_real_fechas = []
salario_real_valores = []
salario_real_age_days = None

if not df_ripte_raw.empty and not df_ipc.empty:
    df_s = df_ripte_raw.copy()
    df_s["ym"] = df_s["fecha"].dt.to_period("M")
    df_s = df_s.drop_duplicates(subset=["ym"], keep="last")

    df_i = df_ipc.copy()
    df_i["ym"] = df_i["fecha"].dt.to_period("M")
    df_i = df_i.drop_duplicates(subset=["ym"], keep="last")

    merged = df_s.merge(df_i[["ym", "IPC"]], on="ym", how="inner").sort_values("ym")
    print(f"  • Merge salarios+IPC: {len(merged)} filas")

    if len(merged) >= 13:
        slice_ = merged.tail(13).reset_index(drop=True)
        base_salario = slice_.iloc[0]["RIPTE"]
        factor_ipc = 1.0
        for i in range(1, len(slice_)):
            factor_ipc *= (1 + slice_.iloc[i]["IPC"] / 100)
            salario_nominal_idx = slice_.iloc[i]["RIPTE"] / base_salario
            salario_real = salario_nominal_idx / factor_ipc * 100
            salario_real_valores.append(round(salario_real, 2))
            salario_real_fechas.append(slice_.iloc[i]["ym"].strftime("%b %y"))

        if salario_real_valores:
            salario_real_yoy = round(salario_real_valores[-1] - 100, 2)
        salario_real_age_days = (pd.Timestamp.now() - df_ripte_raw["fecha"].iloc[-1]).days

print(f"  ✓ Salario real: YoY={salario_real_yoy}% | age={salario_real_age_days}d")


# ---------------------------------------------------------------
# 6. BCRA v4.0 - TASAS + RESERVAS
# ---------------------------------------------------------------
print("\n[6/10] BCRA API v4.0...")

BCRA_V4_BASE = "https://api.bcra.gob.ar/estadisticas/v4.0/Monetarias"


def fetch_bcra_v4(var_id, desde=None, hasta=None):
    """API BCRA v4.0 Monetarias."""
    url = f"{BCRA_V4_BASE}/{var_id}"
    params = {"limit": 3000}
    if desde:
        params["desde"] = desde
    if hasta:
        params["hasta"] = hasta
    try:
        r = requests.get(url, headers=HEADERS, params=params, timeout=30, verify=False)
        if r.status_code == 200:
            d = r.json()
            return d.get("results", [])
        print(f"  ⚠ BCRA v4 var {var_id}: HTTP {r.status_code}")
        return []
    except Exception as e:
        print(f"  ⚠ BCRA v4 var {var_id}: {e}")
        return []


def get_tasa_actual(var_id):
    res = fetch_bcra_v4(var_id, desde=(HOY - timedelta(days=30)).strftime("%Y-%m-%d"))
    if not res:
        return None
    try:
        # La API ahora devuelve results ordenados, tomamos el último
        return float(res[-1]["valor"])
    except Exception:
        return None


def get_serie_bcra(var_id, dias=400):
    """Devuelve DataFrame con fecha y valor."""
    res = fetch_bcra_v4(var_id, desde=(HOY - timedelta(days=dias)).strftime("%Y-%m-%d"))
    if not res:
        return pd.DataFrame()
    try:
        df = pd.DataFrame(res)
        df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
        df["valor"] = pd.to_numeric(df["valor"], errors="coerce")
        df = df.dropna().sort_values("fecha").reset_index(drop=True)
        return df
    except Exception:
        return pd.DataFrame()


# IDs BCRA v4.0 (confirmados):
# 1 = Reservas internacionales (USD millones)
# 6 = Tipo Cambio Mayorista (Com. A 3500)
# 7 = BADLAR Bancos Privados (TNA)
# 8 = TM20 Bancos Privados
# 34 = Tasa Plazo Fijo 30 días (TNA)
# 160 = TAMAR Bancos Privados (TNA) - nueva tasa referencia
tasa_plazo_fijo = get_tasa_actual(34)
tasa_badlar = get_tasa_actual(7)
tasa_tm20 = get_tasa_actual(8)
tasa_tamar = get_tasa_actual(160)

# Serie de reservas para calcular deltas
df_reservas = get_serie_bcra(1, dias=400)
reservas_actual = None
reservas_1m = None
reservas_1a = None
if not df_reservas.empty:
    reservas_actual = float(df_reservas["valor"].iloc[-1])
    # 1 mes atrás
    target_1m = df_reservas["fecha"].iloc[-1] - timedelta(days=30)
    sub_1m = df_reservas[df_reservas["fecha"] <= target_1m]
    if not sub_1m.empty:
        reservas_1m = float(sub_1m["valor"].iloc[-1])
    # 1 año atrás
    target_1a = df_reservas["fecha"].iloc[-1] - timedelta(days=365)
    sub_1a = df_reservas[df_reservas["fecha"] <= target_1a]
    if not sub_1a.empty:
        reservas_1a = float(sub_1a["valor"].iloc[-1])

reservas_delta_1m = pct_change(reservas_actual, reservas_1m) if reservas_actual and reservas_1m else None
reservas_delta_1a = pct_change(reservas_actual, reservas_1a) if reservas_actual and reservas_1a else None

print(f"  ✓ Plazo Fijo: {tasa_plazo_fijo}% | BADLAR: {tasa_badlar}% | TM20: {tasa_tm20}% | TAMAR: {tasa_tamar}%")
print(f"  ✓ Reservas: USD {reservas_actual}M | 1M: {reservas_delta_1m}% | 1A: {reservas_delta_1a}%")


def tna_a_retorno_periodo(tna, meses):
    if tna is None:
        return None
    tasa_mensual = tna / 100 / 12
    return round(((1 + tasa_mensual) ** meses - 1) * 100, 2)


# Estimaciones de tasas de financiamiento (basadas en BADLAR + spreads)
def estimar_tasa_financiamiento():
    if tasa_badlar is None:
        return {k: None for k in ["adelanto_cta_cte", "tarjeta_credito", "prestamo_personal",
                                   "hipotecario_uva", "sgr_cheque"]}
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
# 7. BENCHMARK + VALOR REAL
# ---------------------------------------------------------------
print("\n[7/10] Benchmark USD...")


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
    except Exception:
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
print("\n[8/10] Mercados yfinance...")

tickers = {
    "SP500": "^GSPC",
    "Merval": "^MERV",
    "BTC": "BTC-USD",
    "Oro": "GC=F",
    "Brent": "BZ=F",
    "AL30": "AL30.BA",
    "GGAL_ADR": "GGAL",
    "GGAL_LOC": "GGAL.BA",
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
# 9. MACRO GLOBAL (FRED + BCB Brasil + INE Chile)
# ---------------------------------------------------------------
print("\n[9/10] Macro global...")


def fetch_fred_api(serie_id):
    """FRED con API key."""
    if not FRED_API_KEY:
        # Fallback: CSV público
        url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={serie_id}"
        df = fetch_csv(url)
        if df.empty:
            return pd.DataFrame()
        df.columns = ["fecha"] + list(df.columns[1:])
        df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
        val_col = df.columns[1]
        df[val_col] = df[val_col].replace(".", pd.NA)
        df[val_col] = pd.to_numeric(df[val_col], errors="coerce")
        df = df.dropna().sort_values("fecha").reset_index(drop=True)
        df = df.rename(columns={val_col: "valor"})
        return df[["fecha", "valor"]]

    url = f"https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": serie_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "observation_start": (HOY - timedelta(days=400)).strftime("%Y-%m-%d"),
    }
    try:
        r = requests.get(url, params=params, timeout=20)
        if r.status_code != 200:
            print(f"  ⚠ FRED {serie_id}: HTTP {r.status_code}")
            return pd.DataFrame()
        data = r.json()
        obs = data.get("observations", [])
        if not obs:
            return pd.DataFrame()
        df = pd.DataFrame(obs)
        df["fecha"] = pd.to_datetime(df["date"], errors="coerce")
        df["valor"] = pd.to_numeric(df["value"], errors="coerce")
        df = df.dropna(subset=["fecha", "valor"]).sort_values("fecha").reset_index(drop=True)
        return df[["fecha", "valor"]]
    except Exception as e:
        print(f"  ⚠ FRED {serie_id}: {e}")
        return pd.DataFrame()


def ultimo_valor(df):
    if df.empty:
        return None
    try:
        return float(df["valor"].iloc[-1])
    except Exception:
        return None


# EEUU: Fed Funds + CPI
df_fedfunds = fetch_fred_api("FEDFUNDS")
df_cpi_us = fetch_fred_api("CPIAUCSL")
tasa_pm_us = ultimo_valor(df_fedfunds)
# Inflación YoY USA
inf_us = None
if not df_cpi_us.empty and len(df_cpi_us) >= 13:
    try:
        ult = float(df_cpi_us["valor"].iloc[-1])
        ant = float(df_cpi_us["valor"].iloc[-13])
        inf_us = round(((ult / ant) - 1) * 100, 2)
    except Exception:
        pass

print(f"  ✓ EEUU: Fed={tasa_pm_us}% | Inflación YoY={inf_us}%")


# Brasil: api.bcb.gov.br
def fetch_bcb_brasil(serie_id, ultimos=15):
    url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{serie_id}/dados/ultimos/{ultimos}?formato=json"
    data = fetch_json(url)
    if not data:
        return pd.DataFrame()
    try:
        df = pd.DataFrame(data)
        df["fecha"] = pd.to_datetime(df["data"], format="%d/%m/%Y", errors="coerce")
        df["valor"] = pd.to_numeric(df["valor"], errors="coerce")
        return df[["fecha", "valor"]].dropna().sort_values("fecha").reset_index(drop=True)
    except Exception:
        return pd.DataFrame()


# BCB 433 = IPCA (var % mensual), 432 = Selic meta (% anual)
df_ipca_br = fetch_bcb_brasil(433, ultimos=15)
df_selic_br = fetch_bcb_brasil(432, ultimos=5)

tasa_pm_br = ultimo_valor(df_selic_br)
# Inflación YoY Brasil: acumular 12 últimos meses
inf_br = None
if not df_ipca_br.empty and len(df_ipca_br) >= 12:
    try:
        ultimos_12 = df_ipca_br.tail(12)["valor"].astype(float) / 100
        inf_br = round(((1 + ultimos_12).prod() - 1) * 100, 2)
    except Exception:
        pass

print(f"  ✓ Brasil: Selic={tasa_pm_br}% | Inflación YoY={inf_br}%")


# Chile: INE con credenciales
def fetch_chile_ipc():
    """
    INE Chile tiene su API de estadísticas:
    https://stat.ine.cl/SDMX-JSON/data/IPC_CATEG_ORIG_POND/all/all?format=csv
    
    También hay una API con usuario/pass en el servicio de búsqueda.
    Usamos el endpoint público de estadísticas mensuales.
    """
    # Intento 1: API pública con credenciales
    if CHILE_USER and CHILE_PASS:
        url = "https://api.cmfchile.cl/api-sbifv3/recursos_api/ipc"
        params = {
            "apikey": CHILE_PASS,  # la clave suele ser el pass en API banco central
            "formato": "json",
        }
        data = fetch_json(url, headers={"user": CHILE_USER})
        if data and "IPCs" in data:
            try:
                rows = data["IPCs"]
                df = pd.DataFrame(rows)
                df["fecha"] = pd.to_datetime(df.get("Fecha", df.get("fecha")), errors="coerce")
                df["valor"] = pd.to_numeric(df.get("Valor", df.get("valor")).astype(str).str.replace(",", "."), errors="coerce")
                return df[["fecha", "valor"]].dropna().sort_values("fecha").reset_index(drop=True)
            except Exception as e:
                print(f"  ⚠ Parse Chile IPC: {e}")

    # Fallback: FRED Chile CPI
    df = fetch_fred_api("CHLCPIALLMINMEI")
    return df


df_ipc_cl = fetch_chile_ipc()
inf_cl = None
if not df_ipc_cl.empty and len(df_ipc_cl) >= 13:
    try:
        ult = float(df_ipc_cl["valor"].iloc[-1])
        ant = float(df_ipc_cl["valor"].iloc[-13])
        inf_cl = round(((ult / ant) - 1) * 100, 2)
    except Exception:
        pass

# Tasa política monetaria Chile: Banco Central Chile
# TPM actual via FRED IR3TIB01CLM156N
df_tpm_cl = fetch_fred_api("IR3TIB01CLM156N")
tasa_pm_cl = ultimo_valor(df_tpm_cl)

print(f"  ✓ Chile: TPM={tasa_pm_cl}% | Inflación YoY={inf_cl}%")


# Riesgo País Argentina
rp_arg = None
if not macro_dfs["rp"].empty:
    try:
        rp_arg = int(float(macro_dfs["rp"]["Riesgo_Pais"].iloc[-1]))
    except Exception:
        pass


# Tasa real = Tasa nominal - inflación
def tasa_real(tasa_nom, inf_yoy):
    if tasa_nom is None or inf_yoy is None:
        return None
    try:
        # Fórmula de Fisher: (1+nom)/(1+inf) - 1
        return round(((1 + tasa_nom / 100) / (1 + inf_yoy / 100) - 1) * 100, 2)
    except Exception:
        return None


# Para Argentina uso Plazo Fijo 30d como tasa nominal (lo que el ahorrista puede conseguir)
tasa_real_arg = tasa_real(tasa_plazo_fijo, ipc_yoy)
tasa_real_br = tasa_real(tasa_pm_br, inf_br)
tasa_real_cl = tasa_real(tasa_pm_cl, inf_cl)
tasa_real_us = tasa_real(tasa_pm_us, inf_us)

# Riesgo soberano estimado: Chile y Brasil con valores públicos típicos recientes
# En producción se podría conectar con bonos corporativos/sovereign CDS
cds_br = 200  # aprox
cds_cl = 65
cds_us = 30

macro_global = {
    "argentina": {
        "inflacion_yoy": ipc_yoy,
        "tasa_nominal": tasa_plazo_fijo,
        "tasa_real": tasa_real_arg,
        "riesgo_pais": rp_arg,
    },
    "brasil": {
        "inflacion_yoy": inf_br,
        "tasa_nominal": tasa_pm_br,
        "tasa_real": tasa_real_br,
        "riesgo_pais": cds_br,
    },
    "chile": {
        "inflacion_yoy": inf_cl,
        "tasa_nominal": tasa_pm_cl,
        "tasa_real": tasa_real_cl,
        "riesgo_pais": cds_cl,
    },
    "eeuu": {
        "inflacion_yoy": inf_us,
        "tasa_nominal": tasa_pm_us,
        "tasa_real": tasa_real_us,
        "riesgo_pais": cds_us,
    },
}

print(f"  ✓ Macro global:")
for pais, d in macro_global.items():
    print(f"    {pais}: {d}")


# ---------------------------------------------------------------
# 10. ROFEX + REM
# ---------------------------------------------------------------
print("\n[10/10] ROFEX + REM + noticias...")


def fetch_rofex_con_pyrofex():
    """
    Usa pyRofex con credenciales de ReMarkets.
    Requiere secrets ROFEX_USER y ROFEX_PASS.
    """
    if not ROFEX_USER or not ROFEX_PASS:
        print(f"  • ROFEX: sin credenciales, salto")
        return []

    try:
        import pyRofex

        pyRofex.initialize(
            user=ROFEX_USER,
            password=ROFEX_PASS,
            account="",
            environment=pyRofex.Environment.LIVE,
        )

        # Obtener instrumentos
        instruments = pyRofex.get_all_instruments()
        if not instruments or "instruments" not in instruments:
            print(f"  ⚠ ROFEX: sin instrumentos")
            return []

        futuros = []
        for ins in instruments["instruments"]:
            try:
                instrument_id = ins.get("instrumentId", {})
                symbol = instrument_id.get("symbol", "")
                # Solo futuros de dólar (símbolos DLR/)
                if not symbol.startswith("DLR/"):
                    continue
                md = pyRofex.get_market_data(
                    ticker=symbol,
                    entries=[pyRofex.MarketDataEntry.SETTLEMENT_PRICE,
                             pyRofex.MarketDataEntry.CLOSING_PRICE],
                )
                if md.get("status") == "OK":
                    data = md.get("marketData", {})
                    precio = None
                    if "SE" in data and data["SE"].get("price"):
                        precio = data["SE"]["price"]
                    elif "CL" in data and data["CL"].get("price"):
                        precio = data["CL"]["price"]
                    if precio:
                        venc = symbol.replace("DLR/", "")
                        futuros.append({"vencimiento": venc, "precio": float(precio)})
            except Exception:
                continue

        print(f"  ✓ ROFEX: {len(futuros)} contratos")
        return sorted(futuros, key=lambda x: x["vencimiento"])
    except ImportError:
        print(f"  ⚠ pyRofex no instalado, agregalo a requirements.txt")
        return []
    except Exception as e:
        print(f"  ⚠ ROFEX: {e}")
        return []


rofex_futuros = fetch_rofex_con_pyrofex()


# REM via API de Facundo Allia (verificada)
def fetch_rem_api():
    """API pública con REM parseado. Endpoints: ipc_general, tipo_cambio, tasa_interes, pbi."""
    base = "https://bcra-rem-api.facujallia.workers.dev/api"
    out = {
        "inflacion_mensual_proxima": None,
        "inflacion_12m": None,
        "tc_12m": None,
        "tasa_12m": None,
        "pbi_trim": None,
    }

    # Inflación
    data = fetch_json(f"{base}/ipc_general")
    if data and "datos" in data:
        for row in data["datos"]:
            ref = str(row.get("referencia", "")).lower()
            if "próx. 12 meses" in ref or "prox. 12 meses" in ref:
                out["inflacion_12m"] = row.get("mediana")
            # Primera proyección mensual (mes siguiente)
            if "mensual" in ref and out["inflacion_mensual_proxima"] is None:
                out["inflacion_mensual_proxima"] = row.get("mediana")

    # Tipo de cambio
    time.sleep(1)  # evitar rate limit
    data = fetch_json(f"{base}/tipo_cambio")
    if data and "datos" in data:
        for row in data["datos"]:
            ref = str(row.get("referencia", "")).lower()
            if "próx. 12 meses" in ref or "prox. 12 meses" in ref:
                out["tc_12m"] = row.get("mediana")

    # Tasa
    time.sleep(1)
    data = fetch_json(f"{base}/tasa_interes")
    if data and "datos" in data:
        for row in data["datos"]:
            ref = str(row.get("referencia", "")).lower()
            if "próx. 12 meses" in ref or "prox. 12 meses" in ref:
                out["tasa_12m"] = row.get("mediana")

    # PBI
    time.sleep(1)
    data = fetch_json(f"{base}/pbi")
    if data and "datos" in data:
        datos = data["datos"]
        if datos:
            # Tomar primer dato disponible
            out["pbi_trim"] = datos[0].get("mediana")

    return out


rem = fetch_rem_api()
print(f"  ✓ REM: {rem}")


# Ratio dólar futuro / spot
ratio_dolar_12m_spot = None
dev_anualizada_implicita = None
try:
    usd_spot = float(macro_dfs["oficial"]["USD_Oficial"].iloc[-1]) if not macro_dfs["oficial"].empty else None

    # Prioridad 1: ROFEX largo
    precio_12m = None
    if rofex_futuros and usd_spot:
        # Buscar contrato a ~12 meses
        for f in rofex_futuros[::-1]:
            venc_str = str(f.get("vencimiento", ""))
            if any(m in venc_str.upper() for m in ["DIC", "ENE", "NOV"]):
                precio_12m = float(f["precio"])
                break
        if precio_12m is None and rofex_futuros:
            precio_12m = float(rofex_futuros[-1]["precio"])

    # Prioridad 2: REM tipo de cambio
    if precio_12m is None and rem.get("tc_12m"):
        precio_12m = float(rem["tc_12m"])

    if precio_12m and usd_spot:
        ratio_dolar_12m_spot = round(precio_12m / usd_spot, 3)
        dev_anualizada_implicita = round((precio_12m / usd_spot - 1) * 100, 2)
except Exception as e:
    print(f"  ⚠ Ratio futuro: {e}")

print(f"  ✓ Ratio 12m/spot: {ratio_dolar_12m_spot}x | Dev implícita: {dev_anualizada_implicita}%")


# Inflación implícita 12m: usar REM si hay, si no fallback
inflacion_implicita_12m = rem.get("inflacion_12m") or ipc_yoy

# Tasa real esperada = plazo fijo nominal vs inflación esperada
tasa_real_esperada = None
if tasa_plazo_fijo and inflacion_implicita_12m:
    try:
        tasa_real_esperada = round(((1 + tasa_plazo_fijo / 100) / (1 + float(inflacion_implicita_12m) / 100) - 1) * 100, 2)
    except Exception:
        pass


# Vencimientos deuda (estimación basada en cronograma público AL/GD)
def vencimientos_deuda_publica():
    vencimientos = []
    for i in range(1, 13):
        mes = (HOY.replace(day=1) + timedelta(days=32 * i)).replace(day=1)
        # Cronograma típico AL/GD: cupones en enero y julio son los grandes
        if mes.month == 7:
            monto = 4500
        elif mes.month == 1:
            monto = 4000
        elif mes.month in [3, 9]:
            monto = 1200
        else:
            monto = 600
        vencimientos.append({
            "mes": mes.strftime("%b %y"),
            "monto_usd_mm": monto,
        })
    return vencimientos


vencimientos_deuda = vencimientos_deuda_publica()


# ---------------------------------------------------------------
# NOTICIAS (scoring estricto)
# ---------------------------------------------------------------
print("\n[Noticias] RSS + scoring...")

RSS_SOURCES = {
    "Ámbito": "https://www.ambito.com/rss/pages/economia.xml",
    "Infobae": "https://www.infobae.com/feeds/rss/economia/",
    "Cronista": "https://www.cronista.com/files/rss/economia.xml",
    "iProfesional": "https://www.iprofesional.com/rss",
    "El Economista": "https://eleconomista.com.ar/arc/outboundfeeds/rss/?outputType=xml",
    "Investing": "https://es.investing.com/rss/news_25.rss",
    "Perfil": "https://www.perfil.com/feed/economia",
}

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
    (["caputo", "tasa"], 6), (["fed", "tasa"], 6),
    (["caputo", "dolar"], 5), (["caputo", "dólar"], 5),
    (["bcra", "tasa"], 5), (["fmi", "reservas"], 5),
    (["riesgo pais", "bonos"], 4), (["riesgo país", "bonos"], 4),
    (["inflacion", "politica monetaria"], 4), (["inflación", "política monetaria"], 4),
    (["milei", "economia"], 3), (["milei", "economía"], 3),
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
    count_alta = sum(1 for kw in KEYWORDS_ALTA if kw in texto)
    count_media = sum(1 for kw in KEYWORDS_MEDIA if kw in texto)
    count_leve = sum(1 for kw in KEYWORDS_LEVE if kw in texto)

    score += count_alta * 4
    score += count_media * 2
    score += count_leve * 0.5

    for combo, bonus in COMBOS_BOOST:
        if all(kw in texto for kw in combo):
            score += bonus

    if len(titulo) < 40:
        score *= 0.7

    patterns_cotizacion = ["cotizó a", "cotizo a", "abrió a", "abrio a", "cerró a", "cerro a"]
    if any(p in texto for p in patterns_cotizacion) and count_alta == 0:
        score *= 0.5

    has_combo = any(all(kw in texto for kw in combo) for combo, _ in COMBOS_BOOST)
    if count_alta == 0 and not has_combo:
        score *= 0.6

    horas = max(0, (datetime.now() - fecha_pub).total_seconds() / 3600)
    score *= max(0.3, 1 - horas / 48)

    return round(score, 2)


noticias = []
for medio, url in RSS_SOURCES.items():
    try:
        feed = feedparser.parse(url)
        entries = feed.entries[:15]
        if not entries:
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
        print(f"  ✓ {medio}: {len(entries)}")
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


# ---------------------------------------------------------------
# SNAPSHOTS
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


# ---------------------------------------------------------------
# PROMPTS LLM
# ---------------------------------------------------------------
def build_prompt_resumen():
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

- "mundo": una línea que combine un dato del bloque DATOS MUNDO con tema concreto de noticia mundial. Máx 160 chars.

- "argentina": una línea que combine un dato del bloque DATOS ARGENTINA con tema concreto de noticia argentina. Máx 160 chars.

- "a_mirar": SIEMPRE indicar algo concreto. Elegí UNO:
  a) Si hay noticia con evento/dato próximo → mencionarlo.
  b) Si no → catalizador probable según los datos (ej: "riesgo país cerca de 700bps, atento a bonos").
  NUNCA devolver "Sin eventos destacados". Máx 160 chars.

JSON: {{"mundo": "...", "argentina": "...", "a_mirar": "..."}}

REGLAS: no inventar eventos, no recomendar comprar/vender, no predecir precios, rioplatense, sin emojis, JSON puro."""


def build_prompt_destacadas():
    noticias_list = "\n".join([
        f"{i+1}. [{n['medio']}] {n['titulo']} | URL: {n['url']}"
        for i, n in enumerate(destacadas_final)
    ])
    return f"""Analista financiero. 4 noticias pre-seleccionadas:

{noticias_list}

Para cada una, generá 'por_que_importa' (80 caracteres máx, concreto).

JSON: {{"destacadas": [
  {{"titular": "<exacto>", "medio": "<exacto>", "url": "<exacta>", "por_que_importa": "..."}},
  ...4 items
]}}

REGLAS: URLs y titulares exactos, no inventar, rioplatense, sin emojis, JSON puro."""


def build_prompt_valor_real(rendimientos, financiamiento, bench, periodo_label):
    def fmt_dict(d):
        return "\n".join([
            f"  {k}: {v:+.2f}%" if v is not None else f"  {k}: sin datos"
            for k, v in d.items()
        ])

    bench_str = f"{bench:+.2f}" if bench is not None else "0.00"

    return f"""Analista argentino. Rendimientos {periodo_label.lower()} en USD.

INVERSIONES (retorno USD):
{fmt_dict(rendimientos)}

FINANCIAMIENTO (costo USD, positivo=caro):
{fmt_dict(financiamiento)}

Benchmark dólares quietos: {bench_str}%

3 oraciones:
1. Qué pasó con dólares quietos.
2. Mejor y peor inversión vs benchmark.
3. Qué deuda quedó más cara/más licuada.

JSON: {{"analisis": "máx 500 chars"}}

REGLAS: no recomendar, no predecir, rioplatense, sin emojis, JSON puro."""


def build_prompt_lectura_macro():
    ipc_series = ", ".join([f"{f}:{v}%" for f, v in zip(ipc_fechas_12m, ipc_valores_12m)]) if ipc_valores_12m else "sin datos"
    emae_series = ", ".join([f"{f}:{v}" for f, v in zip(emae_fechas_12m, emae_valores_12m)]) if emae_valores_12m else "sin datos"
    sal_series = ", ".join([f"{f}:{v}" for f, v in zip(salario_real_fechas, salario_real_valores)]) if salario_real_valores else "sin datos"

    if emae_yoy is not None:
        emae_dir = "EXPANSIÓN (actividad crece)" if emae_yoy > 0 else "CONTRACCIÓN (actividad cae)" if emae_yoy < 0 else "ESTANCAMIENTO"
        emae_etiq = f"YoY={emae_yoy}% → {emae_dir}"
    else:
        emae_etiq = "sin datos"

    if ipc_accel is not None:
        ipc_dir = "ACELERA" if ipc_accel > 0.1 else "DESACELERA" if ipc_accel < -0.1 else "ESTABLE"
        ipc_etiq = f"aceleración={ipc_accel}pp → {ipc_dir}"
    else:
        ipc_etiq = ""

    if salario_real_yoy is not None:
        sal_dir = "GANAN poder de compra" if salario_real_yoy > 0 else "PIERDEN poder de compra" if salario_real_yoy < 0 else "sin cambios"
        sal_etiq = f"YoY={salario_real_yoy}% → salarios {sal_dir}"
    else:
        sal_etiq = "sin datos"

    return f"""Analista económico argentino. Datos macro 12m:

IPC mensual: {ipc_series}
  Último: {ipc_mes}% | {ipc_etiq}

EMAE: {emae_series}
  {emae_etiq}

SALARIO REAL (base 100): {sal_series}
  {sal_etiq}

IMPORTANTE: RESPETÁ las etiquetas entre paréntesis. No inviertas sentidos.
Si EMAE dice EXPANSIÓN → actividad SUBE (no digas "cae").
Si salario GANA → no digas "pierde".

3 oraciones:
1. Diagnóstico economía real combinando etiqueta EMAE + etiqueta IPC.
2. Poder adquisitivo: qué dice salario real vs dinámica anterior.
3. Escenario 1-3 meses según tendencia.

JSON: {{"lectura_macro": "3 oraciones, máx 450 chars"}}

REGLAS: respetar signos de etiquetas, no opinar política, rioplatense, sin emojis, JSON puro."""


def build_prompt_expectativas():
    fut_txt = ", ".join([f"{f['vencimiento']}:${f['precio']}" for f in rofex_futuros[:6]]) if rofex_futuros else "sin datos ROFEX"
    usd_spot = snapshots["usd_oficial"]["val"] if snapshots["usd_oficial"] else "?"

    return f"""Analista argentino. Datos expectativas de mercado:

Dólar spot: ${usd_spot}
Futuros ROFEX: {fut_txt}
Ratio 12m/spot: {ratio_dolar_12m_spot}x
Devaluación implícita 12m: {dev_anualizada_implicita}%
REM inflación 12m: {rem.get('inflacion_12m', 'N/D')}%
REM TC 12m esperado: ${rem.get('tc_12m', 'N/D')}
REM tasa 12m: {rem.get('tasa_12m', 'N/D')}%
Plazo fijo TNA: {tasa_plazo_fijo}%
Tasa real esperada: {tasa_real_esperada}%
Reservas BCRA: USD {reservas_actual}M (1M: {reservas_delta_1m}%, 1A: {reservas_delta_1a}%)
Riesgo país: {rp_arg} bps

3 oraciones:
1. Qué devaluación espera mercado (dev implícita vs inflación esperada → dev real).
2. Tasa real esperada positiva/negativa: qué implica para ahorro en pesos.
3. Evolución reservas + riesgo país: ¿mercado ve recesión, estancamiento o crecimiento?

JSON: {{"analisis_expectativas": "3 oraciones, máx 500 chars"}}

REGLAS: sin recomendaciones, interpretación objetiva, rioplatense, sin emojis, JSON puro."""


def build_prompt_macro_global():
    bloques = []
    for pais, d in macro_global.items():
        bloques.append(
            f"{pais.upper()}: inflación YoY={d.get('inflacion_yoy', 'N/D')}%, "
            f"tasa nominal={d.get('tasa_nominal', 'N/D')}%, "
            f"TASA REAL={d.get('tasa_real', 'N/D')}%, "
            f"riesgo={d.get('riesgo_pais', 'N/D')}bps"
        )

    return f"""Analista financiero. Comparación regional:

{chr(10).join(bloques)}

3 oraciones:
1. Posición de Argentina en TASA REAL vs región. Tasa real alta = atrae capital pero puede frenar economía.
2. Brasil vs Chile: cuál es más barato/caro en riesgo crediticio y por qué matters.
3. Diferencial de tasas con EEUU y implicancia para flujos a emergentes (carry trade).

JSON: {{"analisis_global": "3 oraciones, máx 500 chars"}}

REGLAS: sin recomendaciones, usar solo estos datos, rioplatense, sin emojis, JSON puro."""


def llamar_gemini(prompt, intentos=3, model=GEMINI_MODEL_FLASH):
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
                    content = cand.get("content", {})
                    parts = content.get("parts", [])
                    if parts and "text" in parts[0]:
                        return parts[0]["text"]
        except Exception as e:
            print(f"  ⚠ Intento {i+1} ({model}): {e}")
        time.sleep(3)
    print(f"  ✗ Gemini {model} falló ({prompt_size} chars)")
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


# Llamadas LLM (6 total)
print("\n  → Llamada 1: resumen (Gemini Pro, más robusto)...")
# Primero intentamos Pro (más confiable), si falla caemos a Flash
resp_resumen_txt = llamar_gemini(build_prompt_resumen(), intentos=2, model=GEMINI_MODEL_PRO)
if not resp_resumen_txt:
    print("  → Fallback a Flash...")
    resp_resumen_txt = llamar_gemini(build_prompt_resumen(), intentos=2, model=GEMINI_MODEL_FLASH)

resp_resumen = parsear_json(resp_resumen_txt) or {
    "mundo": "Análisis en procesamiento",
    "argentina": "Análisis en procesamiento",
    "a_mirar": "Datos macro en observación continua",
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

print("  → Llamada 6: macro global...")
resp_global = parsear_json(llamar_gemini(build_prompt_macro_global())) or {}
analisis_global = resp_global.get("analisis_global", "Sin análisis disponible")

print("  → Llamada 7: expectativas...")
resp_exp = parsear_json(llamar_gemini(build_prompt_expectativas())) or {}
analisis_expectativas = resp_exp.get("analisis_expectativas", "Sin análisis disponible")


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
    "reservas_actual_usd_mm": [reservas_actual if reservas_actual else ""],
    "reservas_delta_1m": [reservas_delta_1m if reservas_delta_1m else ""],
    "reservas_delta_1a": [reservas_delta_1a if reservas_delta_1a else ""],
    "vencimientos_deuda_json": [json.dumps(vencimientos_deuda, ensure_ascii=False)],
    "macro_global_json": [json.dumps(macro_global, ensure_ascii=False)],
})
write_ws("DB_Insights", insights_df)
print("  ✓ DB_Insights")

if noticias:
    df_news = pd.DataFrame(noticias)[["fecha", "medio", "titulo", "resumen", "url", "score"]]
    write_ws("DB_Noticias", df_news)
    print(f"  ✓ DB_Noticias: {len(df_news)} noticias")

print("\n" + "=" * 60)
print("Pipeline V21 - Completado")
print("=" * 60)
