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
        # Usamos yf.Tickers para lidar com um ou múltiplos ativos de forma consistente
        data = yf.Tickers(" ".join(yahoo_formatted_tickers))
        
        for ticker_str in yahoo_formatted_tickers:
            # Para cada ticker, obtemos o histórico do último dia
            hist = data.tickers[ticker_str].history(period="1d", auto_adjust=True)
            if not hist.empty:
                # Pegamos o último preço de fecho disponível
                price = hist['Close'].iloc[-1]
                if price is not None and not math.isnan(price):
                    original_ticker = ticker_str.replace(".SA", "")
                    latest_prices[original_ticker] = price

    except Exception as e:
        print(f"Erro ao buscar preços na API: {e}")
    
    return latest_prices
