from fastapi import APIRouter, Depends, HTTPException
from typing import List, Dict, Any
import yfinance as yf
import numpy as np
from sklearn.linear_model import LinearRegression
from datetime import datetime, timedelta
import pandas as pd

import models
import security
from database import db

router = APIRouter()

class AnalysisData(models.BaseModel):
    historical_data: List[Dict[str, Any]]
    prediction: float

@router.get("/{ticker}", response_model=AnalysisData, tags=["Análise"])
async def get_asset_analysis(ticker: str, current_user: models.UserInDB = Depends(security.get_current_user)):
    """
    Busca o histórico de preços de um ativo e calcula uma previsão
    para o dia seguinte usando regressão linear.
    """
    try:
        # Formata o ticker para a API do Yahoo Finance
        yahoo_ticker = f"{ticker.upper()}.SA" if '-' not in ticker else ticker.upper()
        
        # 1. Busca os dados históricos dos últimos 90 dias
        end_date = datetime.now()
        start_date = end_date - timedelta(days=90)
        
        hist_data = yf.download(yahoo_ticker, start=start_date, end=end_date, progress=False)
        
        if hist_data.empty:
            raise HTTPException(status_code=404, detail="Não foi possível encontrar dados históricos para o ativo.")

        # Prepara os dados para a regressão
        hist_data = hist_data.reset_index()
        hist_data['days_since_start'] = (hist_data['Date'] - hist_data['Date'].min()).dt.days

        # 2. Calcula a Regressão Linear
        X = hist_data['days_since_start'].values.reshape(-1, 1)
        y = hist_data['Close'].values

        model = LinearRegression()
        model.fit(X, y)

        # 3. Faz a previsão para o próximo dia
        next_day_index = hist_data['days_since_start'].max() + 1
        prediction = model.predict([[next_day_index]])[0]

        # 4. Formata os dados históricos para enviar ao frontend (para o gráfico)
        historical_data_for_chart = []
        for index, row in hist_data.iterrows():
            historical_data_for_chart.append({
                "date": row['Date'].strftime('%Y-%m-%d'),
                "price": row['Close']
            })

        return {
            "historical_data": historical_data_for_chart,
            "prediction": prediction
        }

    except Exception as e:
        print(f"Erro na análise do ativo {ticker}: {e}")
        raise HTTPException(status_code=500, detail="Ocorreu um erro ao processar a análise do ativo.")
