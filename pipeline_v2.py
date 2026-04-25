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
print("Pipeline V22 - Iniciando")
print(f"Fecha corrida: {datetime.now(timezone.utc).isoformat()}")
print("=" * 60)

# ---------------------------------------------------------------
# CONFIG - SECRETS
# ---------------------------------------------------------------
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
GCP_JSON = os.environ.get("GCLOUD_SERVICE_ACCOUNT")
FRED_API_KEY = os.environ.get("FRED_API_KEY", "").strip()  # FIX: strip whitespace
CHILE_USER = os.environ.get("CHILE_USER", "").strip()
CHILE_PASS = os.environ.get("CHILE_PASS", "").strip()
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
HACE_400D = HOY - timedelta(days=400)  # margen para comparaciones YoY robustas

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


def get_historical_value(df, col, days_back, tolerancia_dias=20):
    """
    Devuelve el valor de la serie `col` correspondiente a hace `days_back` días.
    
    FIX V23: con tolerancia. Si pedís un valor de hace 365 días pero la serie
    arranca exactamente ahí (porque pedimos start=HACE_1A en yfinance), el
    filtro <= max-365 te puede dejar 0 filas. Tolerancia ±20 días: aceptamos
    el dato más cercano dentro de esa ventana.
    """
    try:
        serie = df[["fecha", col]].copy()
        serie[col] = pd.to_numeric(serie[col], errors="coerce")
        serie = serie.dropna(subset=[col]).drop_duplicates(subset=["fecha"]).sort_values("fecha")
        if serie.empty:
            return None
        target = serie["fecha"].max() - timedelta(days=days_back)
        # Buscar la observación más cercana al target dentro de la tolerancia
        serie["_d"] = (serie["fecha"] - target).abs()
        idx_min = serie["_d"].idxmin()
        delta_dias = serie.loc[idx_min, "_d"].days
        if delta_dias > tolerancia_dias:
            return None
        return serie.loc[idx_min, col]
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
# 2. EMAE (Excel oficial INDEC) + Índice de Salarios (CSV oficial INDEC)
# ---------------------------------------------------------------
print("\n[2/10] EMAE y Salarios desde fuentes INDEC oficiales...")

# === EMAE desde Excel oficial INDEC ===
# URL verificada: https://www.indec.gob.ar/ftp/cuadros/economia/sh_emae_mensual_base2004.xls
# Lag oficial: 50-60 días (publicación día 22 aprox del mes)
EMAE_XLS_URL = "https://www.indec.gob.ar/ftp/cuadros/economia/sh_emae_mensual_base2004.xls"


def fetch_emae_from_indec_xls():
    """
    Descarga el XLS oficial del EMAE base 2004 directo del INDEC.

    FIX V24: ahora trae AMBAS series (original + desestacionalizada).
    - Serie original (col 2): se usa para calcular YoY (es como reporta INDEC)
    - Serie desestacionalizada (col 4): se usa para nivel y tendencia mensual

    Estructura confirmada del Excel (validado en Colab abril 2026):
      col 0: Año (solo poblada en filas de enero)
      col 1: Mes en español ("Enero", "Febrero", ...)
      col 2: Índice Serie ORIGINAL (con estacionalidad)
      col 3: Var % i.a. de la serie original
      col 4: Índice Serie DESESTACIONALIZADA
      col 5: Var % m/m de la desestacionalizada
      col 6: Índice Serie Tendencia-Ciclo
      col 7: Var % m/m de la tendencia

    Devuelve DataFrame con columnas: fecha, EMAE_original, EMAE_desest.
    Mantiene también la columna EMAE como alias de EMAE_desest por compatibilidad
    con el resto del código (que usa "EMAE" como nombre canónico para nivel).
    """
    try:
        r = requests.get(EMAE_XLS_URL, headers=HEADERS, timeout=60, verify=False)
        if r.status_code != 200:
            print(f"  ⚠ EMAE XLS HTTP {r.status_code}")
            return pd.DataFrame()

        tmp_path = "/tmp/emae_indec.xls"
        with open(tmp_path, "wb") as f:
            f.write(r.content)

        df_raw = None
        for engine in ["xlrd", "openpyxl"]:
            try:
                df_raw = pd.read_excel(tmp_path, sheet_name=0, header=None, engine=engine)
                break
            except Exception:
                continue
        if df_raw is None:
            print(f"  ⚠ EMAE XLS: ningún engine pudo abrir el archivo")
            return pd.DataFrame()

        meses_map = {
            "enero": 1, "febrero": 2, "marzo": 3, "abril": 4,
            "mayo": 5, "junio": 6, "julio": 7, "agosto": 8,
            "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12,
        }

        rows = []
        anio_actual = None
        for idx in range(len(df_raw)):
            row = df_raw.iloc[idx]

            v_anio = row.iloc[0]
            if pd.notna(v_anio):
                try:
                    anio_int = int(float(str(v_anio).strip()))
                    if 2004 <= anio_int <= 2030:
                        anio_actual = anio_int
                except Exception:
                    pass

            v_mes = row.iloc[1] if df_raw.shape[1] > 1 else None
            if pd.isna(v_mes) or anio_actual is None:
                continue
            mes_str = str(v_mes).strip().lower()
            mes_num = meses_map.get(mes_str)
            if mes_num is None:
                continue

            if df_raw.shape[1] < 5:
                continue

            # Original (col 2) y desestacionalizada (col 4)
            try:
                v_orig = float(row.iloc[2])
                v_desest = float(row.iloc[4])
                if not (50 < v_orig < 300) or not (50 < v_desest < 300):
                    continue
                rows.append({
                    "fecha": pd.Timestamp(year=anio_actual, month=mes_num, day=1),
                    "EMAE_original": v_orig,
                    "EMAE_desest": v_desest,
                })
            except Exception:
                continue

        if not rows:
            print(f"  ⚠ EMAE XLS sin filas parseadas")
            return pd.DataFrame()

        df = pd.DataFrame(rows).drop_duplicates(subset=["fecha"]).sort_values("fecha").reset_index(drop=True)
        # Alias por compatibilidad: EMAE = serie desestacionalizada (lo que se usa para nivel)
        df["EMAE"] = df["EMAE_desest"]
        return df
    except Exception as e:
        print(f"  ⚠ EMAE XLS error: {str(e)[:200]}")
        return pd.DataFrame()


# Intento 1: Excel oficial INDEC
df_emae_raw = fetch_emae_from_indec_xls()

# Fallback: API datos.gob.ar (si Excel falla por algún motivo)
if df_emae_raw.empty:
    print(f"  → Fallback EMAE: datos.gob.ar")
    EMAE_SERIE_ID = "143.3_NO_PR_2004_A_21"
    df_emae_raw = fetch_indec_series(EMAE_SERIE_ID, "EMAE")

if not df_emae_raw.empty:
    print(f"  ✓ EMAE: {len(df_emae_raw)} filas, último {df_emae_raw['fecha'].max().date()}, "
          f"valor {df_emae_raw['EMAE'].iloc[-1]}")
macro_dfs["emae"] = df_emae_raw


# === Índice de Salarios desde CSV oficial INDEC ===
# URL verificada: https://www.indec.gob.ar/ftp/cuadros/sociedad/indice_salarios.csv
# Es el índice OFICIAL del INDEC (no RIPTE) - mide evolución salarial pura
SALARIOS_CSV_URL = "https://www.indec.gob.ar/ftp/cuadros/sociedad/indice_salarios.csv"


def fetch_salarios_indec():
    """
    CSV oficial INDEC con Índice de Salarios (base oct-2016 = 100).
    FIX V22: V7 estaba tomando la columna incorrecta (IS_total_registrado por el
    fallback "total") y parseaba fechas sin formato explícito, lo que en algunos
    runners interpretaba DD/MM/YYYY como MM/DD/YYYY.

    Estructura real del CSV (verificado 2026-04):
      periodo; IS_sector_privado_registrado; IS_sector_publico;
      IS_total_registrado; IS_sector_no_registrado; IS_indice_total

    Usamos IS_indice_total (Nivel General, dato oficial INDEC que citan
    todos los analistas).
    """
    try:
        r = requests.get(SALARIOS_CSV_URL, headers=HEADERS, timeout=30, verify=False)
        if r.status_code != 200:
            print(f"  ⚠ Salarios CSV HTTP {r.status_code}")
            return pd.DataFrame()

        # Decode tolerante (UTF-8 con o sin BOM)
        text = None
        for enc in ["utf-8-sig", "utf-8", "latin-1", "cp1252"]:
            try:
                text = r.content.decode(enc)
                break
            except Exception:
                continue
        if text is None:
            text = r.text

        # Separador confirmado: ;
        df_raw = pd.read_csv(StringIO(text), sep=";", dtype=str)

        if df_raw.empty or "periodo" not in df_raw.columns:
            print(f"  ⚠ Salarios CSV: columna 'periodo' no encontrada. Cols: {list(df_raw.columns)}")
            return pd.DataFrame()

        # Priorizar nombre exacto de nivel general
        col_is = None
        candidates_exact = ["IS_indice_total", "IS_indice_general", "indice_general", "nivel_general"]
        for c in df_raw.columns:
            if str(c).strip() in candidates_exact:
                col_is = c
                break

        # Fallback: buscar por contenido del nombre (sin matchear "total_registrado")
        if col_is is None:
            for c in df_raw.columns:
                cl = str(c).lower().strip()
                if "indice_total" in cl or "nivel_general" in cl or "indice_general" in cl:
                    col_is = c
                    break

        if col_is is None:
            print(f"  ⚠ Salarios CSV: no encuentro columna nivel general. Cols: {list(df_raw.columns)}")
            return pd.DataFrame()

        # Fecha con formato EXPLÍCITO dd/mm/yyyy (evita ambigüedad con locale US)
        df_raw["fecha"] = pd.to_datetime(df_raw["periodo"], format="%d/%m/%Y", errors="coerce")

        # Valor con coma decimal → punto
        df_raw["IS"] = pd.to_numeric(
            df_raw[col_is].astype(str).str.replace(",", ".").str.replace(" ", "").replace("NA", ""),
            errors="coerce"
        )

        df = df_raw[["fecha", "IS"]].dropna().sort_values("fecha").reset_index(drop=True)

        if df.empty:
            print(f"  ⚠ Salarios CSV: DataFrame vacío tras parseo (col usada: {col_is})")
            return pd.DataFrame()

        print(f"  • Salarios INDEC: columna usada = {col_is}")
        return df
    except Exception as e:
        print(f"  ⚠ Salarios CSV: {str(e)[:200]}")
        return pd.DataFrame()


df_is_raw = fetch_salarios_indec()

# RIPTE como fallback (la fuente anterior)
df_ripte_raw = pd.DataFrame()
if df_is_raw.empty:
    print(f"  → Fallback salarios: RIPTE CSV")
    RIPTE_CSV_URL = "https://infra.datos.gob.ar/catalog/sspm/dataset/158/distribution/158.1/download/remuneracion-imponible-promedio-trabajadores-estables-ripte-total-pais-pesos-serie-mensual.csv"
    try:
        df_raw = fetch_csv(RIPTE_CSV_URL)
        if not df_raw.empty and "indice_tiempo" in df_raw.columns and "ripte" in df_raw.columns:
            df_ripte_raw = df_raw.rename(columns={"indice_tiempo": "fecha", "ripte": "IS"})
            df_ripte_raw["fecha"] = pd.to_datetime(df_ripte_raw["fecha"], errors="coerce")
            df_ripte_raw["IS"] = pd.to_numeric(df_ripte_raw["IS"], errors="coerce")
            df_ripte_raw = df_ripte_raw.dropna().sort_values("fecha").reset_index(drop=True)
    except Exception:
        pass
    df_is_raw = df_ripte_raw

if not df_is_raw.empty:
    print(f"  ✓ Salarios: {len(df_is_raw)} filas, último {df_is_raw['fecha'].max().date()}, "
          f"valor {df_is_raw['IS'].iloc[-1]:.2f}")
else:
    print(f"  ✗ Salarios: SIN datos")

macro_dfs["salarios"] = df_is_raw


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
    # Nivel: usamos serie DESESTACIONALIZADA (alias EMAE)
    emae_val = float(df_emae_raw["EMAE"].iloc[-1])
    last_date = df_emae_raw["fecha"].iloc[-1]
    emae_age_days = (pd.Timestamp.now() - last_date).days

    # YoY: FIX V24 - se calcula sobre la serie ORIGINAL (alineado con INDEC oficial).
    # INDEC publica la "variación interanual" sobre la serie original. Hacerlo
    # sobre la desest da números distintos porque la desestacionalización elimina
    # parte del componente cíclico-anual.
    target = last_date - pd.DateOffset(months=12)
    ant = df_emae_raw[df_emae_raw["fecha"] <= target]
    if not ant.empty and "EMAE_original" in df_emae_raw.columns:
        try:
            v_actual_orig = float(df_emae_raw["EMAE_original"].iloc[-1])
            v_ant_orig = float(ant["EMAE_original"].iloc[-1])
            delta = pct_change(v_actual_orig, v_ant_orig)
            emae_yoy = round(delta, 2) if delta is not None else None
        except Exception:
            pass
    elif not ant.empty:
        # Fallback: si no tenemos serie original (caso raro), usar la genérica
        delta = pct_change(emae_val, ant["EMAE"].iloc[-1])
        emae_yoy = round(delta, 2) if delta is not None else None

    emae_fechas_12m, emae_valores_12m = serie_12m(df_emae_raw, "EMAE")

print(f"  ✓ EMAE: val={emae_val} | YoY={emae_yoy}% | age={emae_age_days}d")


# ---------------------------------------------------------------
# 5. SALARIO REAL — Índice de Salarios INDEC deflactado por IPC
# ---------------------------------------------------------------
print("\n[5/10] Salario real (Índice de Salarios INDEC / IPC)...")

salario_real_yoy = None
salario_real_fechas = []
salario_real_valores = []
salario_real_age_days = None

# Usamos df_is_raw (Índice de Salarios oficial INDEC)
if not df_is_raw.empty and not df_ipc.empty:
    df_s = df_is_raw.copy()
    df_s["ym"] = df_s["fecha"].dt.to_period("M")
    df_s = df_s.drop_duplicates(subset=["ym"], keep="last")

    df_i = df_ipc.copy()
    df_i["ym"] = df_i["fecha"].dt.to_period("M")
    df_i = df_i.drop_duplicates(subset=["ym"], keep="last")

    merged = df_s.merge(df_i[["ym", "IPC"]], on="ym", how="inner").sort_values("ym").reset_index(drop=True)
    print(f"  • Merge salarios+IPC: {len(merged)} filas")

    # Necesitamos 13 filas estrictas (mes 0 + 12 meses de IPC) para YoY correcto
    if len(merged) >= 13:
        # === YoY actual: ventana de 13 meses ===
        ventana = merged.tail(13).reset_index(drop=True)
        is_base = float(ventana.iloc[0]["IS"])
        is_actual = float(ventana.iloc[-1]["IS"])

        # Acumular inflación mes a mes (del mes 1 al 12)
        ipc_acum_yoy = 1.0
        for i in range(1, 13):
            ipc_acum_yoy *= (1 + float(ventana.iloc[i]["IPC"]) / 100)

        # Índice de salario real base 100 (12 meses atrás)
        salario_real_idx = (is_actual / is_base) / ipc_acum_yoy * 100
        salario_real_yoy = round(salario_real_idx - 100, 2)

        # Desglose para log (útil para verificar que los números tienen sentido)
        salario_nominal_yoy = round((is_actual / is_base - 1) * 100, 2)
        inflacion_yoy_ventana = round((ipc_acum_yoy - 1) * 100, 2)
        print(f"  • Desglose: nominal={salario_nominal_yoy}% | inflación={inflacion_yoy_ventana}% → real={salario_real_yoy}%")

        # === Trayectoria para gráfico: últimos 24 meses de índice real ===
        tray_n = min(25, len(merged))
        tray = merged.tail(tray_n).reset_index(drop=True)
        is_base_t = float(tray.iloc[0]["IS"])
        ipc_acum_t = 1.0
        for i in range(1, len(tray)):
            ipc_acum_t *= (1 + float(tray.iloc[i]["IPC"]) / 100)
            is_t = float(tray.iloc[i]["IS"])
            idx_real = (is_t / is_base_t) / ipc_acum_t * 100
            salario_real_valores.append(round(idx_real, 2))
            salario_real_fechas.append(tray.iloc[i]["ym"].strftime("%b %y"))

        salario_real_age_days = (pd.Timestamp.now() - df_is_raw["fecha"].iloc[-1]).days
    else:
        print(f"  ⚠ Merge salarios+IPC: solo {len(merged)} filas (necesito ≥13 para YoY)")

print(f"  ✓ Salario real: YoY={salario_real_yoy}% | age={salario_real_age_days}d")


# ---------------------------------------------------------------
# 6. BCRA v4.0 - TASAS + RESERVAS
# ---------------------------------------------------------------
print("\n[6/10] BCRA API v4.0...")

# BCRA v4.0 - URL en MINÚSCULA según doc oficial
BCRA_V4_BASE = "https://api.bcra.gob.ar/estadisticas/v4.0/monetarias"


def listar_variables_bcra():
    """Lista TODAS las variables disponibles en v4.0 con sus IDs.
    Esto nos permite buscar por descripción y no depender de IDs hardcodeados."""
    try:
        r = requests.get(BCRA_V4_BASE, headers=HEADERS, params={"limit": 1500}, timeout=45, verify=False)
        if r.status_code != 200:
            print(f"  ⚠ BCRA listado HTTP {r.status_code}")
            return []
        d = r.json()
        return d.get("results", [])
    except Exception as e:
        print(f"  ⚠ BCRA listado: {e}")
        return []


def buscar_id_bcra(catalogo, *keywords_must_have):
    """Busca un idVariable cuya descripción contenga todas las keywords."""
    keywords = [k.lower() for k in keywords_must_have]
    candidatos = []
    for var in catalogo:
        desc = str(var.get("descripcion", "")).lower()
        if all(k in desc for k in keywords):
            candidatos.append(var)
    if not candidatos:
        return None, None
    # Preferir el que tenga "ultValorInformado" no nulo
    candidatos_validos = [c for c in candidatos if c.get("ultValorInformado") is not None]
    if candidatos_validos:
        elegido = candidatos_validos[0]
    else:
        elegido = candidatos[0]
    return elegido.get("idVariable"), elegido.get("descripcion")


def fetch_bcra_v4(var_id, desde=None, hasta=None):
    """API BCRA v4.0 - estructura nueva: results[0].detalle[]"""
    if var_id is None:
        return []
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
            results = d.get("results", [])
            if results and isinstance(results, list):
                # Estructura v4.0: results[0]["detalle"] = lista de {fecha, valor}
                primer = results[0]
                if isinstance(primer, dict) and "detalle" in primer:
                    return primer.get("detalle", [])
                # Compatibilidad: a veces results es plano
                return results
            return []
        print(f"  ⚠ BCRA v4 var {var_id}: HTTP {r.status_code}")
        return []
    except Exception as e:
        print(f"  ⚠ BCRA v4 var {var_id}: {e}")
        return []


def get_tasa_actual(var_id):
    if var_id is None:
        return None
    res = fetch_bcra_v4(var_id, desde=(HOY - timedelta(days=30)).strftime("%Y-%m-%d"))
    if not res:
        return None
    try:
        return float(res[0]["valor"])  # v4 devuelve ordenado descendente
    except Exception:
        try:
            return float(res[-1]["valor"])
        except Exception:
            return None


def get_serie_bcra(var_id, dias=400):
    if var_id is None:
        return pd.DataFrame()
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


# Buscar IDs dinámicamente (más robusto que hardcodear)
print("  → Listando variables BCRA v4.0...")
catalogo_bcra = listar_variables_bcra()
print(f"  ✓ Variables BCRA disponibles: {len(catalogo_bcra)}")

if catalogo_bcra:
    id_reservas, desc_reservas = buscar_id_bcra(catalogo_bcra, "reservas", "internacional")
    id_pf, desc_pf = buscar_id_bcra(catalogo_bcra, "plazo fijo")
    id_badlar, desc_badlar = buscar_id_bcra(catalogo_bcra, "badlar", "privado")
    id_tamar, desc_tamar = buscar_id_bcra(catalogo_bcra, "tamar")
    id_tm20, desc_tm20 = buscar_id_bcra(catalogo_bcra, "tm20")

    print(f"  • Reservas: id={id_reservas} — {desc_reservas}")
    print(f"  • Plazo Fijo: id={id_pf} — {desc_pf}")
    print(f"  • BADLAR: id={id_badlar} — {desc_badlar}")
    print(f"  • TAMAR: id={id_tamar} — {desc_tamar}")
    print(f"  • TM20: id={id_tm20} — {desc_tm20}")
else:
    id_reservas = id_pf = id_badlar = id_tamar = id_tm20 = None

tasa_plazo_fijo = get_tasa_actual(id_pf)
tasa_badlar = get_tasa_actual(id_badlar)
tasa_tm20 = get_tasa_actual(id_tm20)
tasa_tamar = get_tasa_actual(id_tamar)

# Si plazo fijo es None, usar TAMAR como referencia (es la nueva tasa de referencia oficial)
if tasa_plazo_fijo is None and tasa_tamar is not None:
    print(f"  → Plazo Fijo no disponible, uso TAMAR como referencia: {tasa_tamar}%")
    tasa_plazo_fijo = tasa_tamar

# Serie de reservas para calcular deltas
df_reservas = get_serie_bcra(id_reservas, dias=400)
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


# FIX V24: tasas REALES del BCRA v4 (4 categorías).
# Sacamos "Adelanto Cta Cte" porque las series del BCRA (IDs 13, 145, 1199)
# reflejan promedios mayoristas que subestiman el costo retail real - mostrar
# 25-47% cuando un cuentacorrentista de a pie paga >100% TNA es engañoso.
# Mantener solo las 4 que sí reflejan realidad consumidor/PyME.
# IDs validados en abril 2026 — series mensuales (1 dato por mes, fin de mes).
TASAS_FIN_BCRA_IDS = {
    "tarjeta_credito":   1215,  # Financiaciones con tarjetas de crédito
    "prestamo_personal": 1219,  # Préstamos personales a tasa fija
    "sgr_cheque":        1216,  # Documentos a sola firma a tasa fija
}
# TNA REAL UVA hipotecario BNA (sobre capital ajustado por CER)
# Actualizar manual cada 6 meses. Última verificación: abril 2026
TASA_HIPOTECARIO_BNA_TNA_REAL = 4.5


def fetch_tasas_financiamiento_bcra():
    """
    Pide las 3 tasas BCRA + hipotecario hardcoded.
    Hipotecario UVA: la TNA "real" del BNA (4.5%) se convierte a TNA NOMINAL
    componiendo con la inflación esperada (más abajo, cuando esté `rem`).
    """
    out = {}
    for nombre, id_var in TASAS_FIN_BCRA_IDS.items():
        out[nombre] = get_tasa_actual(id_var)
    out["hipotecario_uva"] = None  # se setea después con rem
    out["_hipotecario_uva_real"] = TASA_HIPOTECARIO_BNA_TNA_REAL
    return out


tasas_fin = fetch_tasas_financiamiento_bcra()
print(f"  ✓ Financiamiento BCRA: {tasas_fin}")


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
print(f"  ✓ Bench 1M: {bench_1m}% | 1A (histórico): {bench_1a}%")

# FIX: si bench es None, usar 0 como fallback para que la tabla aparezca
# (mejor mostrar tabla con benchmark conservador que tabla vacía)
if bench_1m is None:
    bench_1m = 0.0
    print(f"  ⚠ bench_1m=None → fallback 0.0 para no romper tabla")
if bench_1a is None:
    bench_1a = 0.0
    print(f"  ⚠ bench_1a=None → fallback 0.0 para no romper tabla")

# bench_1a_esperado se calcula más adelante (cuando ya tengamos `rem`).
# Es el equivalente de bench_1a pero usando devaluación ESPERADA en lugar de
# histórica. Se usa en la tabla de financiamiento (que mira hacia adelante).
# Inicialización conservadora; el valor real se setea después.
bench_1a_esperado = bench_1a


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
    "AL30": "AL30D.BA",  # FIX: AL30D es la versión USD-LIQ que sí está disponible
    "GGAL_ADR": "GGAL",
    "GGAL_LOC": "GGAL.BA",
    "US10Y": "^TNX",
}

df_m = pd.DataFrame()
for col, tk in tickers.items():
    try:
        d = yf.download(tk, start=HACE_400D.strftime("%Y-%m-%d"), progress=False, auto_adjust=True)
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
    ccl_raw = (loc / (adr / 10)).round(2)
    # FIX: rolling 5 días para suavizar y evitar NaN cuando un ticker no tiene dato
    df_final["CCL"] = ccl_raw.rolling(window=5, min_periods=1).mean().round(2)
    oficial = pd.to_numeric(df_final.get("USD_Oficial", pd.Series()), errors="coerce")
    if not oficial.empty:
        df_final["Brecha_CCL"] = (((df_final["CCL"] / oficial) - 1) * 100).round(2)

df_final = df_final.ffill(limit=7)
print(f"  ✓ df_final: {len(df_final)} filas")


# ---------------------------------------------------------------
# 9. MACRO GLOBAL (FRED + BCB Brasil + INE Chile)
# ---------------------------------------------------------------
print("\n[9/10] Macro global...")


def fetch_fred_api(serie_id, max_intentos=3):
    """FRED con timeout largo, retry y API key obligatoria."""
    if not FRED_API_KEY:
        print(f"  ⚠ FRED {serie_id}: sin API key")
        return pd.DataFrame()

    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": serie_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "observation_start": (HOY - timedelta(days=400)).strftime("%Y-%m-%d"),
    }
    for intento in range(max_intentos):
        try:
            r = requests.get(url, params=params, timeout=60)  # FIX: 60s en vez de 20s
            if r.status_code != 200:
                print(f"  ⚠ FRED {serie_id} intento {intento+1}: HTTP {r.status_code}")
                time.sleep(3)
                continue
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
            print(f"  ⚠ FRED {serie_id} intento {intento+1}: {e}")
            time.sleep(5)
    return pd.DataFrame()


def ultimo_valor(df):
    if df.empty:
        return None
    try:
        return float(df["valor"].iloc[-1])
    except Exception:
        return None


# EEUU: Fed Funds + CPI - FIX V22: YoY robusto matcheado por fecha
df_fedfunds = fetch_fred_api("FEDFUNDS")
df_cpi_us = fetch_fred_api("CPIAUCSL")
tasa_pm_us = ultimo_valor(df_fedfunds)

inf_us = None
if not df_cpi_us.empty and len(df_cpi_us) >= 12:
    try:
        df_cpi_us = df_cpi_us.sort_values("fecha").reset_index(drop=True)
        fecha_ult = df_cpi_us["fecha"].iloc[-1]
        fecha_target = fecha_ult - pd.DateOffset(months=12)
        df_cpi_us["_d"] = (df_cpi_us["fecha"] - fecha_target).abs()
        idx_ant = df_cpi_us["_d"].idxmin()
        if df_cpi_us["_d"].iloc[idx_ant].days <= 45:
            ult = float(df_cpi_us["valor"].iloc[-1])
            ant = float(df_cpi_us["valor"].iloc[idx_ant])
            inf_us = round(((ult / ant) - 1) * 100, 2)
    except Exception as e:
        print(f"  ⚠ YoY EEUU: {e}")

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


# ===== Chile: Banco Central de Chile (BCCh) — API BDE =====
# Endpoint oficial: https://si3.bcentral.cl/SieteRestWS/SieteRestWS.ashx
# Series IDs (verificadas en doc oficial):
#   F022.TPM.TIN.D001.NO.Z.D = TPM diaria
#   F073.IPC.V12.Z.M         = IPC variación 12 meses
def fetch_bcch_serie(timeseries_id, dias=400):
    """Banco Central de Chile - API BDE."""
    if not CHILE_USER or not CHILE_PASS:
        return pd.DataFrame()
    fecha_desde = (HOY - timedelta(days=dias)).strftime("%Y-%m-%d")
    fecha_hasta = HOY.strftime("%Y-%m-%d")
    url = "https://si3.bcentral.cl/SieteRestWS/SieteRestWS.ashx"
    params = {
        "user": CHILE_USER,
        "pass": CHILE_PASS,
        "function": "GetSeries",
        "timeseries": timeseries_id,
        "firstdate": fecha_desde,
        "lastdate": fecha_hasta,
    }
    try:
        r = requests.get(url, params=params, timeout=30, verify=False)
        if r.status_code != 200:
            print(f"  ⚠ BCCh {timeseries_id}: HTTP {r.status_code}")
            return pd.DataFrame()
        data = r.json()
        # Estructura: data["Series"]["Obs"] = lista de {indexDateString, value, statusCode}
        obs = data.get("Series", {}).get("Obs", [])
        if not obs:
            print(f"  ⚠ BCCh {timeseries_id}: sin observaciones")
            return pd.DataFrame()
        rows = []
        for o in obs:
            try:
                # value puede ser "NaN", "NeuN", o número como string
                val_str = str(o.get("value", "")).replace(",", ".")
                if val_str.lower() in ("nan", "neun", "", "null"):
                    continue
                val = float(val_str)
                fecha = pd.to_datetime(o.get("indexDateString"), format="%d-%m-%Y", errors="coerce")
                if pd.isna(fecha):
                    continue
                rows.append({"fecha": fecha, "valor": val})
            except Exception:
                continue
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows).sort_values("fecha").reset_index(drop=True)
        return df
    except Exception as e:
        print(f"  ⚠ BCCh {timeseries_id}: {e}")
        return pd.DataFrame()


# IPC YoY Chile - FIX V22: serie G073.IPC.V12.2023.M (base 2023)
# La serie F073.IPC.V12.Z.M del V7 dejó de actualizarse al migrar el BCCh a base 2023.
# Backup: G073.IPC.IND.2023.M (índice) para calcular YoY manual.
# Fallback final: FRED CHLCPIALLMINMEI.
df_ipc_cl = fetch_bcch_serie("G073.IPC.V12.2023.M")
inf_cl = ultimo_valor(df_ipc_cl)

if inf_cl is None:
    # Backup 1: índice BCCh base 2023 → YoY manual
    print(f"  • IPC Chile YoY directo falló, probando índice base 2023...")
    df_ipc_cl_idx = fetch_bcch_serie("G073.IPC.IND.2023.M")
    if not df_ipc_cl_idx.empty and len(df_ipc_cl_idx) >= 13:
        try:
            df_ipc_cl_idx = df_ipc_cl_idx.sort_values("fecha").reset_index(drop=True)
            fecha_ult = df_ipc_cl_idx["fecha"].iloc[-1]
            fecha_target = fecha_ult - pd.DateOffset(months=12)
            df_ipc_cl_idx["_d"] = (df_ipc_cl_idx["fecha"] - fecha_target).abs()
            idx_ant = df_ipc_cl_idx["_d"].idxmin()
            if df_ipc_cl_idx["_d"].iloc[idx_ant].days <= 45:
                ult = float(df_ipc_cl_idx["valor"].iloc[-1])
                ant = float(df_ipc_cl_idx["valor"].iloc[idx_ant])
                inf_cl = round(((ult / ant) - 1) * 100, 2)
        except Exception as e:
            print(f"  ⚠ Chile IPC backup error: {e}")

if inf_cl is None:
    # Fallback final: FRED
    print(f"  • Backup BCCh falló, probando FRED CHLCPIALLMINMEI...")
    df_ipc_cl_fred = fetch_fred_api("CHLCPIALLMINMEI")
    if not df_ipc_cl_fred.empty and len(df_ipc_cl_fred) >= 13:
        try:
            df_ipc_cl_fred = df_ipc_cl_fred.sort_values("fecha").reset_index(drop=True)
            fecha_ult = df_ipc_cl_fred["fecha"].iloc[-1]
            fecha_target = fecha_ult - pd.DateOffset(months=12)
            df_ipc_cl_fred["_d"] = (df_ipc_cl_fred["fecha"] - fecha_target).abs()
            idx_ant = df_ipc_cl_fred["_d"].idxmin()
            if df_ipc_cl_fred["_d"].iloc[idx_ant].days <= 45:
                ult = float(df_ipc_cl_fred["valor"].iloc[-1])
                ant = float(df_ipc_cl_fred["valor"].iloc[idx_ant])
                inf_cl = round(((ult / ant) - 1) * 100, 2)
        except Exception:
            pass

# TPM Chile - serie activa confirmada F022.TPM.TIN.D001.NO.Z.D (sin cambios)
df_tpm_cl = fetch_bcch_serie("F022.TPM.TIN.D001.NO.Z.D")
tasa_pm_cl = ultimo_valor(df_tpm_cl)
if tasa_pm_cl is None:
    # Fallback: FRED serie mensual Chile
    print(f"  • TPM Chile directo falló, probando FRED IR3TIB01CLM156N...")
    df_tpm_cl_fallback = fetch_fred_api("IR3TIB01CLM156N")
    tasa_pm_cl = ultimo_valor(df_tpm_cl_fallback)

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


# REM via API de Facundo Allia - PARSER FIX V22
# Bug V7: filtraba por "referencia" pero el campo correcto es "período".
# Todos los valores REM venían None silenciosamente.
def fetch_rem_endpoint(endpoint, max_intentos=4):
    """Fetch con retry exponencial para 429: 15s, 30s, 60s, 90s."""
    url = f"https://bcra-rem-api.facujallia.workers.dev/api/{endpoint}"
    for i in range(max_intentos):
        try:
            r = requests.get(url, headers=HEADERS, timeout=30)
            if r.status_code == 200:
                return r.json()
            if r.status_code == 429:
                wait = [15, 30, 60, 90][i]
                print(f"  • REM {endpoint}: 429, esperando {wait}s (intento {i+1}/{max_intentos})")
                time.sleep(wait)
                continue
            print(f"  ⚠ REM {endpoint}: HTTP {r.status_code}")
            return None
        except Exception as e:
            print(f"  ⚠ REM {endpoint}: {str(e)[:100]}")
            time.sleep(5)
    print(f"  ✗ REM {endpoint}: agotados los intentos")
    return None


def _rem_parse_periodo(p):
    """
    Parsea el campo 'periodo' de una fila REM.

    FIX V25: la API de Allia (bcra-rem-api.facujallia.workers.dev) devuelve
    el campo como 'periodo' (sin tilde), formato "YYYY-MM" (largo 7) para
    proyecciones mensuales, "YYYY" (largo 4) para anuales, y strings descriptivos
    para horizontes ("próx. 12 meses", "próx. 24 meses").
    Bug previo: el parser pedía len >= 10 lo cual rechazaba el formato real.

    Devuelve ('anio', int) | ('fecha', Timestamp) | ('str', texto_lower).
    Orden CRÍTICO: int antes que to_datetime.
    """
    if isinstance(p, (int, float)) and not isinstance(p, bool):
        ival = int(p)
        if 2020 <= ival <= 2040:
            return ("anio", ival)
    if isinstance(p, str):
        s = p.strip()
        # YYYY → año
        if len(s) == 4 and s.isdigit():
            ival = int(s)
            if 2020 <= ival <= 2040:
                return ("anio", ival)
        # YYYY-MM o YYYY-MM-DD → fecha (acepta largos 7 y 10)
        if "-" in s and len(s) >= 7:
            try:
                # Si viene "YYYY-MM" lo normalizamos a primer día de mes
                if len(s) == 7:
                    return ("fecha", pd.to_datetime(s + "-01", errors="raise"))
                return ("fecha", pd.to_datetime(s, errors="raise"))
            except Exception:
                pass
    return ("str", str(p).strip().lower())


def _rem_extraer(datos, hoy=None):
    """
    De la lista 'datos' de un endpoint REM extrae las métricas agregadas.
    Filas tipo 'fecha' → forward curve mensual
    Filas tipo 'str' → "próx. 12 meses" / "próx. 24 meses"
    Filas tipo 'anio' → cierre dic-YYYY

    FIX V25: lee el campo 'periodo' (sin tilde) que es el real de la API.
    Antes leía 'período' y devolvía None silenciosamente.
    """
    if hoy is None:
        hoy = pd.Timestamp.now("UTC").tz_localize(None).normalize()
    anio_actual = hoy.year

    out = {
        "mensual_proxima": None,
        "a_12_meses": None,
        "a_24_meses": None,
        "cierre_anio": None,
        "cierre_anio_prox": None,
        "forward_curve": [],
    }
    if not datos:
        return out

    forward_rows = []
    for row in datos:
        # FIX V25: aceptar tanto 'periodo' (correcto) como 'período' (legacy)
        p = row.get("periodo")
        if p is None:
            p = row.get("período")
        mediana = row.get("mediana")
        if mediana is None:
            continue
        tipo, valor = _rem_parse_periodo(p)
        if tipo == "fecha":
            if valor >= hoy:
                forward_rows.append((valor, float(mediana)))
        elif tipo == "str":
            if "12 meses" in valor:
                out["a_12_meses"] = float(mediana)
            elif "24 meses" in valor:
                out["a_24_meses"] = float(mediana)
        elif tipo == "anio":
            if valor == anio_actual:
                out["cierre_anio"] = float(mediana)
            elif valor == anio_actual + 1:
                out["cierre_anio_prox"] = float(mediana)

    forward_rows.sort(key=lambda x: x[0])
    if forward_rows:
        out["mensual_proxima"] = forward_rows[0][1]
        out["forward_curve"] = [
            {"fecha": f.strftime("%Y-%m-%d"), "mediana": v}
            for f, v in forward_rows
        ]
    return out


def fetch_rem_api():
    """
    Devuelve dict con las claves que consume el resto del pipeline + dashboard.
    Mantenemos las claves legacy (inflacion_12m, tc_12m, etc.) por compatibilidad.
    Agregamos: inflacion_cierre_anio, inflacion_cierre_anio_prox, tc_cierre_*, tasa_cierre_*,
               forward curves (ipc, tc, tasa) para gráficos de expectativas.
    """
    out = {
        # Claves legacy (que consume el pipeline/dashboard actual)
        "inflacion_mensual_proxima": None,
        "inflacion_12m": None,
        "tc_12m": None,
        "tasa_12m": None,
        "pbi_trim": None,
        # Claves nuevas
        "inflacion_24m": None,
        "inflacion_cierre_anio": None,
        "inflacion_cierre_anio_prox": None,
        "tc_mensual_proxima": None,
        "tc_24m": None,
        "tc_cierre_anio": None,
        "tc_cierre_anio_prox": None,
        "tasa_mensual_proxima": None,
        "tasa_24m": None,
        "tasa_cierre_anio": None,
        "tasa_cierre_anio_prox": None,
        # Forward curves (listas de {fecha, mediana})
        "ipc_forward": [],
        "tc_forward": [],
        "tasa_forward": [],
    }

    # Inflación
    data = fetch_rem_endpoint("ipc_general")
    if data and "datos" in data:
        ext = _rem_extraer(data["datos"])
        out["inflacion_mensual_proxima"] = ext["mensual_proxima"]
        out["inflacion_12m"] = ext["a_12_meses"]
        out["inflacion_24m"] = ext["a_24_meses"]
        out["inflacion_cierre_anio"] = ext["cierre_anio"]
        out["inflacion_cierre_anio_prox"] = ext["cierre_anio_prox"]
        out["ipc_forward"] = ext["forward_curve"]

    # FIX V25: si la API solo devuelve forward mensual y NO el agregado 12m/24m,
    # lo calculamos componiendo las inflaciones mensuales (lo correcto).
    # Inflación acumulada N meses = (∏(1 + m_i) - 1) * 100, con m_i en %.
    if out["inflacion_12m"] is None and len(out["ipc_forward"]) >= 12:
        try:
            valores = [float(p["mediana"]) / 100 for p in out["ipc_forward"][:12]]
            acc = 1.0
            for v in valores:
                acc *= (1 + v)
            out["inflacion_12m"] = round((acc - 1) * 100, 2)
            print(f"  • inflacion_12m derivada de forward (12 meses): {out['inflacion_12m']}%")
        except Exception as e:
            print(f"  ⚠ No se pudo derivar inflacion_12m: {e}")
    if out["inflacion_24m"] is None and len(out["ipc_forward"]) >= 24:
        try:
            valores = [float(p["mediana"]) / 100 for p in out["ipc_forward"][:24]]
            acc = 1.0
            for v in valores:
                acc *= (1 + v)
            out["inflacion_24m"] = round((acc - 1) * 100, 2)
            print(f"  • inflacion_24m derivada de forward (24 meses): {out['inflacion_24m']}%")
        except Exception as e:
            print(f"  ⚠ No se pudo derivar inflacion_24m: {e}")

    time.sleep(8)

    # Tipo de cambio
    data = fetch_rem_endpoint("tipo_cambio")
    if data and "datos" in data:
        ext = _rem_extraer(data["datos"])
        out["tc_mensual_proxima"] = ext["mensual_proxima"]
        out["tc_12m"] = ext["a_12_meses"]
        out["tc_24m"] = ext["a_24_meses"]
        out["tc_cierre_anio"] = ext["cierre_anio"]
        out["tc_cierre_anio_prox"] = ext["cierre_anio_prox"]
        out["tc_forward"] = ext["forward_curve"]

    # FIX V25: para TC, los valores son en pesos por dólar (NO porcentaje).
    # tc_12m = el valor proyectado mensual ~12 meses adelante.
    # Si la API no lo expone como agregado, tomamos el punto 12 de la forward curve.
    if out["tc_12m"] is None and len(out["tc_forward"]) >= 12:
        try:
            out["tc_12m"] = float(out["tc_forward"][11]["mediana"])
            print(f"  • tc_12m derivado de forward (mes 12): ${out['tc_12m']}")
        except Exception:
            pass
    if out["tc_24m"] is None and len(out["tc_forward"]) >= 24:
        try:
            out["tc_24m"] = float(out["tc_forward"][23]["mediana"])
            print(f"  • tc_24m derivado de forward (mes 24): ${out['tc_24m']}")
        except Exception:
            pass

    time.sleep(8)

    # Tasa de interés
    data = fetch_rem_endpoint("tasa_interes")
    if data and "datos" in data:
        ext = _rem_extraer(data["datos"])
        out["tasa_mensual_proxima"] = ext["mensual_proxima"]
        out["tasa_12m"] = ext["a_12_meses"]
        out["tasa_24m"] = ext["a_24_meses"]
        out["tasa_cierre_anio"] = ext["cierre_anio"]
        out["tasa_cierre_anio_prox"] = ext["cierre_anio_prox"]
        out["tasa_forward"] = ext["forward_curve"]

    # FIX V25: tasa REM es TNA mensual proyectada. Si no hay agregado 12m,
    # tomamos el valor del mes 12 de la curva forward (TNA del mes 12).
    if out["tasa_12m"] is None and len(out["tasa_forward"]) >= 12:
        try:
            out["tasa_12m"] = float(out["tasa_forward"][11]["mediana"])
            print(f"  • tasa_12m derivada de forward (mes 12): {out['tasa_12m']}%")
        except Exception:
            pass

    time.sleep(8)

    # PBI trimestral (estructura distinta, lo dejamos heredado del V7)
    data = fetch_rem_endpoint("pbi")
    if data and "datos" in data and data["datos"]:
        out["pbi_trim"] = data["datos"][0].get("mediana")

    return out


rem = fetch_rem_api()
print(f"  ✓ REM inflación: mens={rem['inflacion_mensual_proxima']}% | 12m={rem['inflacion_12m']}% | dic-{datetime.now().year}={rem['inflacion_cierre_anio']}%")
print(f"  ✓ REM dólar: mens=${rem['tc_mensual_proxima']} | 12m=${rem['tc_12m']} | dic-{datetime.now().year}=${rem['tc_cierre_anio']}")
print(f"  ✓ REM tasa: 12m={rem['tasa_12m']}% | dic-{datetime.now().year}={rem['tasa_cierre_anio']}%")
print(f"  ✓ REM forward curves: ipc={len(rem['ipc_forward'])}pts | tc={len(rem['tc_forward'])}pts | tasa={len(rem['tasa_forward'])}pts")
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

# FIX V25: Infobae soporta múltiples URLs por medio (lista) con fallback automático.
# El feed antiguo /feeds/rss/economia/ devuelve 404 desde 2025. Probamos varias
# alternativas (Arc Publishing outboundfeeds + feed legacy + página de sección)
# y nos quedamos con la primera que devuelva entries.
RSS_SOURCES = {
    "Ámbito": ["https://www.ambito.com/rss/pages/economia.xml"],
    "Infobae": [
        "https://www.infobae.com/arc/outboundfeeds/rss/category/economia/?outputType=xml",
        "https://www.infobae.com/feeds/rss/economia/",
        "https://www.infobae.com/adjuntos/html/RSS/economia.xml",
    ],
    "Cronista": ["https://www.cronista.com/files/rss/economia.xml"],
    "iProfesional": ["https://www.iprofesional.com/rss"],
    "El Economista": ["https://eleconomista.com.ar/arc/outboundfeeds/rss/?outputType=xml"],
    "Investing": ["https://es.investing.com/rss/news_25.rss"],
    "Perfil": ["https://www.perfil.com/feed/economia"],
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
for medio, urls in RSS_SOURCES.items():
    # FIX V25: aceptar tanto string como lista de URLs (fallback automático)
    urls_list = urls if isinstance(urls, list) else [urls]
    feed = None
    url_usada = None
    for url in urls_list:
        try:
            feed_try = feedparser.parse(url)
            if feed_try.entries:
                feed = feed_try
                url_usada = url
                break
        except Exception:
            continue
    if feed is None or not feed.entries:
        print(f"  ✗ {medio}: ningún feed devolvió entries (probadas {len(urls_list)} URLs)")
        continue
    try:
        entries = feed.entries[:15]
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
        print(f"  ✓ {medio}: {len(entries)} (de {url_usada[:60]})")
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


def costo_fin_usd(tna, meses, bench, dev_esperada_pct=None):
    """
    Costo real en USD de financiamiento.
    
    FIX V23: para horizonte de 12 meses, usar devaluación ESPERADA (REM) en
    lugar de la histórica de los últimos 12 meses. La histórica subestima
    el costo real porque Argentina 2025-2026 tuvo devaluación moderada por
    cepo, pero el REM proyecta ~25% para los próximos 12 meses.
    
    Si se pasa `dev_esperada_pct` (en %, ej: 25.8 para 25.8%), se usa esa
    devaluación. Si no, se usa la histórica de los últimos `meses` meses.
    """
    if tna is None or bench is None:
        return None
    try:
        rend = tna_a_retorno_periodo(tna, meses) / 100
        
        if dev_esperada_pct is not None:
            # Usar devaluación esperada REM (ya viene como % anualizada)
            dev = dev_esperada_pct / 100
        else:
            # Devaluación histórica del período
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


# Devaluación esperada 12m del REM (mediana de analistas)
# Si REM no tiene dato, fallback a histórica
dev_esperada_12m = None
try:
    if rem.get("tc_12m") and not macro_dfs["oficial"].empty:
        usd_spot_actual = float(macro_dfs["oficial"]["USD_Oficial"].iloc[-1])
        dev_esperada_12m = ((float(rem["tc_12m"]) / usd_spot_actual) - 1) * 100
        print(f"  • Dev esperada 12m (REM): {dev_esperada_12m:.2f}%")
except Exception:
    pass

# Calcular bench_1a_esperado: equivalente de bench_1a pero con dev ESPERADA REM.
# Es el "rendimiento real en USD que tendrían los pesos quietos los próximos 12m
# si se cumple la inflación esperada y la devaluación esperada del REM".
# Usado en tabla de financiamiento para que el spread tenga sentido contra el costo proyectado.
try:
    inf_esp = rem.get("inflacion_12m")  # % esperado próximos 12m según REM
    if inf_esp is not None and dev_esperada_12m is not None:
        ipc_acc_esp = float(inf_esp) / 100
        dev_esp = dev_esperada_12m / 100
        bench_1a_esperado = round((((1 + ipc_acc_esp) / (1 + dev_esp)) - 1) * 100, 2)
        print(f"  • Bench 1A esperado (inflación REM vs dev REM): {bench_1a_esperado}%")
except Exception:
    pass

# FIX V25: bench_1m_esperado = equivalente mensual al 1A_esperado.
# Antes la tabla de financiamiento mensual usaba bench_1m (histórico, retroactivo),
# lo cual era inconsistente con el 1A que sí usaba bench_1a_esperado.
# Ahora ambas tablas de financiamiento usan benchmark forward-looking.
# Estrategia: tomar inflación REM 1m (si está) o desanualizar la 12m, idem dev.
bench_1m_esperado = bench_1m  # fallback por defecto al histórico
try:
    # 1) Inflación esperada 1m: priorizar dato directo REM, sino desanualizar 12m
    # FIX V25: la clave correcta es 'inflacion_mensual_proxima' (la primera fila de la
    # forward curve, que es el próximo mes calendario REM). Antes se buscaba 'inflacion_1m'
    # que nunca existió.
    inf_1m_esp = None
    if isinstance(rem, dict):
        inf_1m_esp = rem.get("inflacion_mensual_proxima") or rem.get("inflacion_1m")
    if inf_1m_esp is None and rem.get("inflacion_12m") is not None:
        # Desanualizar: (1+anual)^(1/12) - 1
        inf_12m = float(rem["inflacion_12m"]) / 100
        inf_1m_esp = ((1 + inf_12m) ** (1 / 12) - 1) * 100

    # 2) Devaluación esperada 1m: priorizar tc_forward primer punto, sino desanualizar 12m
    dev_1m_esp = None
    tc_forward_local = rem.get("tc_forward") if isinstance(rem, dict) else None
    if tc_forward_local and isinstance(tc_forward_local, list) and len(tc_forward_local) > 0:
        try:
            tc_1m = float(tc_forward_local[0].get("mediana", 0))
            usd_spot_actual_l = float(macro_dfs["oficial"]["USD_Oficial"].iloc[-1])
            if tc_1m > 0 and usd_spot_actual_l > 0:
                dev_1m_esp = ((tc_1m / usd_spot_actual_l) - 1) * 100
        except Exception:
            pass
    if dev_1m_esp is None and dev_esperada_12m is not None:
        # Desanualizar dev 12m → 1m
        dev_12m = dev_esperada_12m / 100
        dev_1m_esp = ((1 + dev_12m) ** (1 / 12) - 1) * 100

    if inf_1m_esp is not None and dev_1m_esp is not None:
        bench_1m_esperado = round((((1 + inf_1m_esp / 100) / (1 + dev_1m_esp / 100)) - 1) * 100, 2)
        print(f"  • Bench 1M esperado (inflación esp 1m vs dev esp 1m): {bench_1m_esperado}%")
except Exception as e:
    print(f"  ⚠ No se pudo calcular bench_1m_esperado: {e}")

# Convertir TNA real UVA a TNA nominal usando inflación esperada
# FIX V25: fórmula simple por pedido del usuario: TNA_nominal = 4.5 + inflación_esperada_12m
# Esta es la aproximación lineal estándar (no la composición Fisher), más conservadora
# y más legible. Para UVA mensual usamos inflación esperada / 12 (anualizada → mensual proxy).
inflacion_para_uva = rem.get("inflacion_12m") or ipc_yoy
if inflacion_para_uva and tasas_fin.get("_hipotecario_uva_real") is not None:
    tna_real_pct = tasas_fin["_hipotecario_uva_real"]  # 4.5
    inf_pct = float(inflacion_para_uva)
    tasas_fin["hipotecario_uva"] = round(tna_real_pct + inf_pct, 2)
    print(f"  • Hipotecario UVA: {tna_real_pct}% real + {inf_pct:.1f}% inf esperada = {tasas_fin['hipotecario_uva']}% TNA nominal")
else:
    # Fallback: si no tenemos inflación, asumimos 30% (proxy 2026)
    tna_real_pct = tasas_fin.get("_hipotecario_uva_real") or 4.5
    tasas_fin["hipotecario_uva"] = round(tna_real_pct + 30.0, 2)
    print(f"  ⚠ Hipotecario UVA: sin inflación esperada, uso fallback 30% → {tasas_fin['hipotecario_uva']}% TNA nominal")


# FIX V25: financiamiento_1m ahora usa bench_1m_esperado y dev esperada mensual.
# Antes inversiones y financiamiento mensuales compartían bench_1m (histórico),
# generando el "mismo número" en ambas tablas. Ahora son distintos:
#   - inversiones 1m → bench_1m (histórico, retorno realizado)
#   - financiamiento 1m → bench_1m_esperado (forward-looking, costo proyectado)
# Calculamos dev esperada mensual con la misma lógica del bench_1m_esperado.
dev_esperada_1m = None
try:
    tc_forward_local = rem.get("tc_forward") if isinstance(rem, dict) else None
    if tc_forward_local and isinstance(tc_forward_local, list) and len(tc_forward_local) > 0:
        try:
            tc_1m = float(tc_forward_local[0].get("mediana", 0))
            usd_spot_actual_l = float(macro_dfs["oficial"]["USD_Oficial"].iloc[-1])
            if tc_1m > 0 and usd_spot_actual_l > 0:
                dev_esperada_1m = ((tc_1m / usd_spot_actual_l) - 1) * 100
        except Exception:
            pass
    if dev_esperada_1m is None and dev_esperada_12m is not None:
        dev_12m = dev_esperada_12m / 100
        dev_esperada_1m = ((1 + dev_12m) ** (1 / 12) - 1) * 100
except Exception:
    pass

financiamiento_1m = {
    "Tarjeta crédito": costo_fin_usd(tasas_fin["tarjeta_credito"], 1, bench_1m_esperado, dev_esperada_1m),
    "Préstamo Personal": costo_fin_usd(tasas_fin["prestamo_personal"], 1, bench_1m_esperado, dev_esperada_1m),
    "Hipotecario UVA": costo_fin_usd(tasas_fin["hipotecario_uva"], 1, bench_1m_esperado, dev_esperada_1m),
    "Cheques SGR descuento": costo_fin_usd(tasas_fin["sgr_cheque"], 1, bench_1m_esperado, dev_esperada_1m),
}

# Para horizonte 12m: pasamos dev esperada REM si está disponible
financiamiento_1a = {
    "Tarjeta crédito": costo_fin_usd(tasas_fin["tarjeta_credito"], 12, bench_1a_esperado, dev_esperada_12m),
    "Préstamo Personal": costo_fin_usd(tasas_fin["prestamo_personal"], 12, bench_1a_esperado, dev_esperada_12m),
    "Hipotecario UVA": costo_fin_usd(tasas_fin["hipotecario_uva"], 12, bench_1a_esperado, dev_esperada_12m),
    "Cheques SGR descuento": costo_fin_usd(tasas_fin["sgr_cheque"], 12, bench_1a_esperado, dev_esperada_12m),
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
    # FIX V25: helpers None-safe para que None no se inyecte como string "None" en el prompt.
    def f_pct(v, dec=2):
        try:
            return f"{float(v):+.{dec}f}%" if v is not None and v != "" else "N/D"
        except (ValueError, TypeError):
            return "N/D"

    def f_num(v, dec=2):
        try:
            return f"{float(v):.{dec}f}" if v is not None and v != "" else "N/D"
        except (ValueError, TypeError):
            return "N/D"

    fut_txt = ", ".join([f"{f['vencimiento']}:${f['precio']}" for f in rofex_futuros[:6]]) if rofex_futuros else "sin datos ROFEX"
    usd_spot = snapshots["usd_oficial"]["val"] if snapshots.get("usd_oficial") else "N/D"
    rem_inf_12m = rem.get('inflacion_12m', None) if isinstance(rem, dict) else None
    rem_tc_12m = rem.get('tc_12m', None) if isinstance(rem, dict) else None
    rem_tasa_12m = rem.get('tasa_12m', None) if isinstance(rem, dict) else None

    return f"""Analista argentino. Datos expectativas de mercado:

Dólar spot: ${usd_spot}
Futuros ROFEX: {fut_txt}
Ratio 12m/spot: {f_num(ratio_dolar_12m_spot)}x
Devaluación implícita 12m: {f_pct(dev_anualizada_implicita, 1)}
REM inflación 12m: {f_pct(rem_inf_12m, 1)}
REM TC 12m esperado: ${f_num(rem_tc_12m, 0)}
REM tasa 12m: {f_pct(rem_tasa_12m, 1)}
Plazo fijo TNA: {f_pct(tasa_plazo_fijo, 1)}
Tasa real esperada: {f_pct(tasa_real_esperada, 2)}
Reservas BCRA: USD {f_num(reservas_actual, 0)}M (1M: {f_pct(reservas_delta_1m, 1)}, 1A: {f_pct(reservas_delta_1a, 1)})
Riesgo país: {rp_arg if rp_arg is not None else 'N/D'} bps

3 oraciones:
1. Qué devaluación espera mercado (dev implícita vs inflación esperada → dev real).
2. Tasa real esperada positiva/negativa: qué implica para ahorro en pesos.
3. Evolución reservas + riesgo país: ¿mercado ve recesión, estancamiento o crecimiento?

JSON: {{"analisis_expectativas": "3 oraciones, máx 500 chars"}}

REGLAS: sin recomendaciones, interpretación objetiva, rioplatense, sin emojis, JSON puro."""


def build_prompt_macro_global():
    def fmt_pct(v, suffix="%"):
        if v is None:
            return "N/D"
        try:
            return f"{float(v):.2f}{suffix}"
        except Exception:
            return "N/D"

    def fmt_int(v, suffix="bps"):
        if v is None:
            return "N/D"
        try:
            return f"{int(float(v))}{suffix}"
        except Exception:
            return "N/D"

    bloques = []
    for pais, d in macro_global.items():
        bloques.append(
            f"{pais.upper()}: inflación YoY={fmt_pct(d.get('inflacion_yoy'))}, "
            f"tasa nominal={fmt_pct(d.get('tasa_nominal'))}, "
            f"TASA REAL={fmt_pct(d.get('tasa_real'))}, "
            f"riesgo={fmt_int(d.get('riesgo_pais'))}"
        )

    return f"""Analista financiero. Comparación regional:

{chr(10).join(bloques)}

3 oraciones:
1. Posición de Argentina en TASA REAL vs región. Tasa real alta = atrae capital pero puede frenar economía.
2. Brasil vs Chile: cuál es más barato/caro en riesgo crediticio y por qué matters.
3. Diferencial de tasas con EEUU y implicancia para flujos a emergentes (carry trade).

REGLAS IMPORTANTES:
- Si algún dato dice "N/D", SIMPLEMENTE OMITILO sin mencionarlo.
- No digas "no hay datos" ni "información incompleta".
- Trabajá con los datos que SÍ están disponibles.

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
    last_error = None
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
                last_error = f"200 OK pero respuesta sin candidates: {str(data)[:200]}"
            else:
                # FIX: log detallado del error
                try:
                    err_body = r.json()
                    err_msg = err_body.get("error", {}).get("message", str(err_body)[:200])
                    err_status = err_body.get("error", {}).get("status", "")
                    last_error = f"HTTP {r.status_code} [{err_status}]: {err_msg[:200]}"
                except Exception:
                    last_error = f"HTTP {r.status_code}: {r.text[:200]}"
                # Si es error de auth, no reintentar
                if r.status_code in (401, 403):
                    print(f"  ✗ Gemini {model} ERROR AUTH: {last_error}")
                    return None
        except Exception as e:
            last_error = str(e)[:200]
        time.sleep(3)
    print(f"  ✗ Gemini {model} falló ({prompt_size} chars). Último error: {last_error}")
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
# FIX: validar que al menos 2 de 3 series tengan datos antes de llamar LLM
series_ok = sum([
    1 if ipc_yoy is not None else 0,
    1 if emae_yoy is not None else 0,
    1 if salario_real_yoy is not None else 0,
])
if series_ok >= 2:
    resp_macro = parsear_json(llamar_gemini(build_prompt_lectura_macro())) or {}
    lectura_macro = resp_macro.get("lectura_macro", "Análisis en proceso")
else:
    lectura_macro = "Datos macroeconómicos insuficientes en esta corrida. Esperando próxima publicación de INDEC."
    print(f"  ⚠ Solo {series_ok}/3 series con datos. Salto LLM.")

print("  → Llamada 6: macro global...")
resp_global = parsear_json(llamar_gemini(build_prompt_macro_global())) or {}
analisis_global = resp_global.get("analisis_global", "Sin análisis disponible")

print("  → Llamada 7: expectativas...")
# FIX V25: log del prompt size, fallback a Pro si Flash falla, y log de qué devolvió.
prompt_exp = build_prompt_expectativas()
print(f"     prompt size: {len(prompt_exp)} chars")
resp_exp_txt = llamar_gemini(prompt_exp, intentos=2, model=GEMINI_MODEL_FLASH)
if not resp_exp_txt:
    print("     → Fallback a Pro...")
    resp_exp_txt = llamar_gemini(prompt_exp, intentos=2, model=GEMINI_MODEL_PRO)
resp_exp = parsear_json(resp_exp_txt) or {}
analisis_expectativas = resp_exp.get("analisis_expectativas", "")
if not analisis_expectativas:
    # Último recurso: armar análisis determinístico mínimo con los datos disponibles
    partes = []
    if dev_anualizada_implicita is not None and rem.get("inflacion_12m"):
        try:
            dev_real_imp = dev_anualizada_implicita - float(rem["inflacion_12m"])
            partes.append(
                f"El mercado prevé una devaluación implícita 12m del {dev_anualizada_implicita:.1f}% "
                f"vs inflación REM {float(rem['inflacion_12m']):.1f}%, "
                f"lo que implica una devaluación real {'positiva' if dev_real_imp > 0 else 'negativa'} de {dev_real_imp:+.1f} pp."
            )
        except Exception:
            pass
    if tasa_real_esperada is not None:
        partes.append(
            f"La tasa real esperada del plazo fijo es {tasa_real_esperada:+.2f}%, "
            f"{'preservando' if tasa_real_esperada > 0 else 'erosionando'} poder de compra en pesos."
        )
    if reservas_delta_1a is not None and rp_arg is not None:
        partes.append(
            f"Reservas {('crecen' if reservas_delta_1a > 0 else 'caen')} {reservas_delta_1a:+.1f}% en 12m "
            f"con riesgo país en {rp_arg} bps."
        )
    analisis_expectativas = " ".join(partes) if partes else "Sin análisis disponible (LLM y datos insuficientes)."
    print(f"     ⚠ LLM falló, uso fallback determinístico ({len(analisis_expectativas)} chars)")
else:
    print(f"     ✓ análisis recibido ({len(analisis_expectativas)} chars)")


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
    "bench_1m_esperado": [bench_1m_esperado if bench_1m_esperado is not None else ""],
    "bench_1a": [bench_1a if bench_1a is not None else ""],
    "bench_1a_esperado": [bench_1a_esperado if bench_1a_esperado is not None else ""],
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
print("Pipeline V22 - Completado")
print("=" * 60)
