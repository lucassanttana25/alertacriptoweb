from fastapi import APIRouter
from typing import List
import yfinance as yf
import math
import pandas as pd

router = APIRouter()

@router.post("/prices")
async def get_current_prices(tickers: List[str]):
    """
    Recebe uma lista de tickers e retorna seus preços atuais do Yahoo Finance.
    """
    if not tickers:
        return {}

    # Formata os tickers para a API do Yahoo Finance
    yahoo_formatted_tickers = []
    for ticker in tickers:
        ticker = ticker.upper()
        if '-' in ticker:  # Assume par de cripto/moeda, como BTC-USD
            yahoo_formatted_tickers.append(ticker)
        else:  # Assume ação da B3
            yahoo_formatted_tickers.append(f"{ticker}.SA")
    
    latest_prices = {}
    try:
        # Usamos auto_adjust=True para seguir as boas práticas da biblioteca
        data = yf.download(tickers=yahoo_formatted_tickers, period='1d', progress=False, auto_adjust=True)
        if data.empty:
            return {}

        # --- CORREÇÃO: Lógica robusta para extrair preços ---
        close_prices = data['Close']
        
        if isinstance(close_prices, pd.Series):
            # Caso de um único ticker: o resultado é uma Series
            price = close_prices.iloc[-1]
            if price is not None and not math.isnan(price):
                original_ticker = yahoo_formatted_tickers[0].replace(".SA", "")
                latest_prices[original_ticker] = price
        else: # pd.DataFrame
            # Caso de múltiplos tickers: o resultado é um DataFrame
            last_prices_row = close_prices.iloc[-1]
            for ticker_col_name in last_prices_row.index:
                price = last_prices_row[ticker_col_name]
                if price is not None and not math.isnan(price):
                    original_ticker = ticker_col_name.replace(".SA", "")
                    latest_prices[original_ticker] = price

    except Exception as e:
        print(f"Erro ao buscar preços na API: {e}")
        # Retorna o que conseguiu encontrar, mesmo que alguns tickers falhem
    
    return latest_prices
