
import os
import yfinance as yf
import requests
import pandas_datareader as pdr
import gspread
from google.oauth2 import service_account
from datetime import datetime, timedelta

def get_precio(ticker):
    try:
        return round(yf.Ticker(ticker).fast_info["last_price"], 2)
    except:
        return None

def run():
    hoy    = datetime.today()
    hace2a = hoy - timedelta(days=730)
    print(f"Iniciando — {hoy.strftime('%d/%m/%Y %H:%M')}")

    sp500  = get_precio("^GSPC")
    nasdaq = get_precio("^NDX")
    oro    = get_precio("GC=F")
    brent  = get_precio("BZ=F")
    btc    = get_precio("BTC-USD")
    print("✓ Mercados globales")

    data     = requests.get("https://api.bluelytics.com.ar/v2/latest").json()
    oficial  = data["oficial"]["value_sell"]
    blue     = data["blue"]["value_sell"]
    ggal_ars = yf.Ticker("GGAL.BA").fast_info["last_price"]
    ggal_usd = yf.Ticker("GGAL").fast_info["last_price"]
    ccl      = round((ggal_ars * 10) / ggal_usd, 2)
    brecha   = round((ccl / oficial - 1) * 100, 2)
    print("✓ Argentina")

    cpi  = pdr.get_data_fred("CPIAUCSL", start=hace2a).dropna()
    fed  = pdr.get_data_fred("FEDFUNDS", start=hace2a).dropna()
    gs10 = pdr.get_data_fred("GS10",     start=hace2a).dropna()
    gs2  = pdr.get_data_fred("GS2",      start=hace2a).dropna()
    infl = round((cpi.iloc[-1,0] / cpi.iloc[-13,0] - 1) * 100, 2)
    tasa = round(fed.iloc[-1,0], 2)
    yc   = round(gs10.iloc[-1,0] - gs2.iloc[-1,0], 2)
    tr   = round(tasa - infl, 2)
    print("✓ Macro USA")

    meli  = get_precio("MELI")
    nvda  = get_precio("NVDA")
    msft  = get_precio("MSFT")
    googl = get_precio("GOOGL")
    ypf   = get_precio("YPF")
    vist  = get_precio("VIST")
    pam   = get_precio("PAM")
    ggal  = get_precio("GGAL")
    print("✓ Portafolio")

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
