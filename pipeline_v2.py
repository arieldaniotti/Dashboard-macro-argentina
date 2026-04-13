
import os
import yfinance as yf
import requests
import gspread
from google.oauth2 import service_account
from datetime import datetime, timedelta

def get_precio(ticker):
    try:
        return round(yf.Ticker(ticker).fast_info["last_price"], 2)
    except:
        return None

def get_fred(serie):
    try:
        url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={serie}"
        lineas = requests.get(url).text.strip().split("\n")
        ultimo = lineas[-1].split(",")
        return float(ultimo[1])
    except:
        return None

def run():
    hoy = datetime.today()
    print(f"Iniciando — {hoy.strftime('%d/%m/%Y %H:%M')}")

    # Mercados globales
    sp500  = get_precio("^GSPC")
    nasdaq = get_precio("^NDX")
    oro    = get_precio("GC=F")
    brent  = get_precio("BZ=F")
    btc    = get_precio("BTC-USD")
    print("✓ Mercados globales")

    # Argentina
    data     = requests.get("https://api.bluelytics.com.ar/v2/latest").json()
    oficial  = data["oficial"]["value_sell"]
    blue     = data["blue"]["value_sell"]
    ggal_ars = yf.Ticker("GGAL.BA").fast_info["last_price"]
    ggal_usd = yf.Ticker("GGAL").fast_info["last_price"]
    ccl      = round((ggal_ars * 10) / ggal_usd, 2)
    brecha   = round((ccl / oficial - 1) * 100, 2)
    print("✓ Argentina")

    # Macro USA — directo desde FRED sin librería
    cpi_ahora = get_fred("CPIAUCSL")
    tasa      = get_fred("FEDFUNDS")
    gs10      = get_fred("GS10")
    gs2       = get_fred("GS2")

    # Para inflacion YoY necesitamos el valor de hace 12 meses
    # Usamos una aproximacion con los ultimos dos valores disponibles
    url_cpi = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=CPIAUCSL&vintage_date=" + (hoy - timedelta(days=365)).strftime("%Y-%m-%d")
    try:
        lineas   = requests.get(url_cpi).text.strip().split("\n")
        cpi_hace = float(lineas[-1].split(",")[1])
        infl     = round((cpi_ahora / cpi_hace - 1) * 100, 2)
    except:
        infl = None

    yc = round(gs10 - gs2, 2) if gs10 and gs2 else None
    tr = round(tasa - infl, 2) if tasa and infl else None
    print("✓ Macro USA")

    # Portafolio
    meli  = get_precio("MELI")
    nvda  = get_precio("NVDA")
    msft  = get_precio("MSFT")
    googl = get_precio("GOOGL")
    ypf   = get_precio("YPF")
    vist  = get_precio("VIST")
    pam   = get_precio("PAM")
    ggal  = get_precio("GGAL")
    print("✓ Portafolio")

    # Google Sheets
    credenciales = service_account.Credentials.from_service_account_file(
        "credentials.json",
        scopes=[
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    gc   = gspread.authorize(credenciales)
    sh   = gc.open(os.environ["SHEET_NAME"])
    hoja = sh.sheet1

    fila = [
        hoy.strftime("%d/%m/%Y"), hoy.strftime("%H:%M"),
        sp500, nasdaq, oro, brent, btc,
        oficial, blue, ccl, brecha,
        infl, tasa, tr, yc,
        meli, nvda, msft, googl,
        ypf, vist, pam, ggal
    ]
    hoja.append_row(fila)
    print(f"✓ Guardado — {hoy.strftime('%d/%m/%Y %H:%M')}")

run()
