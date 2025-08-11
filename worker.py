import asyncio
import yfinance as yf
import pandas as pd
import math
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime
import os
from dotenv import load_dotenv
# Importa a função de envio de e-mail de alerta
from email_service import send_alert_triggered_email

load_dotenv()

# --- Configuração do Banco de Dados ---
MONGO_URI = os.getenv("MONGO_URI")
DATABASE_NAME = os.getenv("DATABASE_NAME")

client = AsyncIOMotorClient(MONGO_URI)
db = client[DATABASE_NAME]
alerts_collection = db.alerts
users_collection = db.users # Adiciona referência à coleção de usuários

print("Worker iniciado. Conectado ao MongoDB.")

async def check_alerts():
    print(f"[{datetime.now()}] Verificando alertas...")

    active_alerts = await alerts_collection.find({"ativo": True}).to_list(length=None)

    if not active_alerts:
        print("Nenhum alerta ativo encontrado.")
        return

    tickers_to_fetch = {alert['assetTicker'] for alert in active_alerts}
    
    yahoo_formatted_tickers = []
    for ticker in tickers_to_fetch:
        if '-' in ticker:
            yahoo_formatted_tickers.append(ticker)
        else:
            yahoo_formatted_tickers.append(f"{ticker}.SA")

    if not yahoo_formatted_tickers:
        return

    print(f"Buscando cotações no Yahoo Finance para: {', '.join(yahoo_formatted_tickers)}")
    latest_prices = {}
    try:
        data = yf.download(tickers=yahoo_formatted_tickers, period='1d', progress=False, auto_adjust=True)
        
        if not data.empty:
            close_prices = data['Close']
            
            if isinstance(close_prices, pd.Series):
                price = close_prices.iloc[-1]
                if price is not None and not math.isnan(price):
                    original_ticker = yahoo_formatted_tickers[0].replace(".SA", "")
                    latest_prices[original_ticker] = price
            else: # pd.DataFrame
                last_prices_row = close_prices.iloc[-1]
                for ticker_col_name in last_prices_row.index:
                    price = last_prices_row[ticker_col_name]
                    if price is not None and not math.isnan(price):
                        original_ticker = ticker_col_name.replace(".SA", "")
                        latest_prices[original_ticker] = price

    except Exception as e:
        print(f"Erro ao buscar dados do yfinance: {e}")

    if not latest_prices:
        print("Não foi possível obter nenhum preço.")
        return
        
    print(f"Preços obtidos: {latest_prices}")

    # Verifica as condições para todos os alertas
    for alert in active_alerts:
        ticker = alert['assetTicker']
        current_price = latest_prices.get(ticker)

        if current_price is None:
            continue

        target_price = alert['preco_alvo']
        tipo = alert['tipo']
        alert_id = alert['_id']
        user_id = alert['userId']

        triggered = False
        if tipo == 'compra' and current_price < target_price:
            triggered = True
        elif tipo == 'venda' and current_price > target_price:
            triggered = True

        if triggered:
            print(f"--- ALERTA DISPARADO! ---")
            print(f"ID: {alert_id}, Ativo: {ticker}, Tipo: {tipo}, Preço Alvo: {target_price}, Preço Atual: {current_price}")
            
            # --- LÓGICA DE ENVIO DE E-MAIL ---
            # 1. Busca o e-mail do usuário no banco de dados
            user_doc = await users_collection.find_one({"_id": user_id})
            if user_doc and 'email' in user_doc:
                recipient_email = user_doc['email']
                alert_details = {
                    "ticker": ticker,
                    "tipo": tipo,
                    "target_price": target_price,
                    "current_price": current_price
                }
                # 2. Envia o e-mail de notificação
                try:
                    await send_alert_triggered_email(recipient_email, alert_details)
                except Exception as e:
                    print(f"ERRO CRÍTICO: Falha ao enviar e-mail de alerta para {recipient_email}. Erro: {e}")
            else:
                print(f"AVISO: Usuário com ID {user_id} não encontrado para envio de e-mail.")

            # 3. Desativa o alerta para não disparar novamente
            await alerts_collection.update_one(
                {"_id": alert_id},
                {"$set": {"ativo": False}}
            )

async def main():
    scheduler = AsyncIOScheduler()
    scheduler.add_job(check_alerts, 'interval', minutes=1)
    scheduler.start()
    print("Agendador iniciado. Verificando alertas a cada 1 minuto.")
    print("Pressione Ctrl+C para sair.")
    try:
        while True:
            await asyncio.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        print("Agendador encerrado.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except RuntimeError:
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        asyncio.run(main())
