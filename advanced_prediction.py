"""
Módulo de previsão avançada para preços de ativos.
Implementa múltiplos modelos de machine learning para melhorar a precisão das previsões.
"""

import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# Machine Learning
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import mean_absolute_error, mean_squared_error
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense, Dropout
from tensorflow.keras.optimizers import Adam

# Time Series
from statsmodels.tsa.arima.model import ARIMA
from statsmodels.tsa.seasonal import seasonal_decompose

# Technical Analysis
import talib

class AdvancedPricePredictor:
    """
    Classe para previsão avançada de preços usando múltiplos modelos de machine learning.
    """
    
    def __init__(self, ticker: str, days_history: int = 252):
        """
        Inicializa o preditor com o ticker e período de histórico.
        
        Args:
            ticker: Símbolo do ativo (ex: 'PETR4.SA')
            days_history: Número de dias de histórico para treinar (252 = 1 ano)
        """
        self.ticker = ticker
        self.days_history = days_history
        self.data = None
        self.features_df = None
        self.scaler = MinMaxScaler()
        self.models = {}
        self.predictions = {}
        self.metrics = {}
        
    def fetch_data(self):
        """
        Busca dados históricos do Yahoo Finance.
        """
        try:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=self.days_history + 50)  # Margem para indicadores
            
            asset = yf.Ticker(self.ticker)
            self.data = asset.history(start=start_date, end=end_date, auto_adjust=True)
            
            if self.data.empty:
                raise ValueError(f"Nenhum dado encontrado para {self.ticker}")
                
            print(f"Dados coletados: {len(self.data)} registros para {self.ticker}")
            return True
            
        except Exception as e:
            print(f"Erro ao buscar dados: {e}")
            return False
    
    def calculate_technical_indicators(self):
        """
        Calcula indicadores técnicos usando TA-Lib.
        """
        if self.data is None or self.data.empty:
            raise ValueError("Dados não carregados. Execute fetch_data() primeiro.")
        
        df = self.data.copy()
        
        # Médias Móveis
        df['SMA_5'] = talib.SMA(df['Close'], timeperiod=5)
        df['SMA_10'] = talib.SMA(df['Close'], timeperiod=10)
        df['SMA_20'] = talib.SMA(df['Close'], timeperiod=20)
        df['EMA_12'] = talib.EMA(df['Close'], timeperiod=12)
        df['EMA_26'] = talib.EMA(df['Close'], timeperiod=26)
        
        # Osciladores
        df['RSI'] = talib.RSI(df['Close'], timeperiod=14)
        df['MACD'], df['MACD_signal'], df['MACD_hist'] = talib.MACD(df['Close'])
        
        # Bollinger Bands
        df['BB_upper'], df['BB_middle'], df['BB_lower'] = talib.BBANDS(df['Close'])
        df['BB_width'] = df['BB_upper'] - df['BB_lower']
        df['BB_position'] = (df['Close'] - df['BB_lower']) / (df['BB_upper'] - df['BB_lower'])
        
        # Momentum
        df['MOM'] = talib.MOM(df['Close'], timeperiod=10)
        df['ROC'] = talib.ROC(df['Close'], timeperiod=10)
        
        # Volatilidade
        df['ATR'] = talib.ATR(df['High'], df['Low'], df['Close'], timeperiod=14)
        
        # Volume (se disponível)
        if 'Volume' in df.columns and df['Volume'].sum() > 0:
            df['Volume_SMA'] = talib.SMA(df['Volume'], timeperiod=20)
            df['Volume_ratio'] = df['Volume'] / df['Volume_SMA']
        else:
            df['Volume_ratio'] = 1.0
        
        # Features de preço
        df['Price_change'] = df['Close'].pct_change()
        df['High_Low_ratio'] = df['High'] / df['Low']
        df['Close_Open_ratio'] = df['Close'] / df['Open']
        
        # Lags (preços anteriores)
        for i in [1, 2, 3, 5, 7]:
            df[f'Close_lag_{i}'] = df['Close'].shift(i)
            df[f'Volume_lag_{i}'] = df['Volume'].shift(i) if 'Volume' in df.columns else 0
        
        # Remove linhas com NaN
        df = df.dropna()
        
        self.features_df = df
        print(f"Indicadores técnicos calculados. Shape: {df.shape}")
        return df
    
    def prepare_features(self):
        """
        Prepara as features para os modelos de machine learning.
        """
        if self.features_df is None:
            self.calculate_technical_indicators()
        
        # Seleciona as features relevantes
        feature_columns = [
            'Open', 'High', 'Low', 'Volume',
            'SMA_5', 'SMA_10', 'SMA_20', 'EMA_12', 'EMA_26',
            'RSI', 'MACD', 'MACD_signal', 'MACD_hist',
            'BB_width', 'BB_position', 'MOM', 'ROC', 'ATR',
            'Volume_ratio', 'Price_change', 'High_Low_ratio', 'Close_Open_ratio'
        ]
        
        # Adiciona lags
        for i in [1, 2, 3, 5, 7]:
            feature_columns.extend([f'Close_lag_{i}', f'Volume_lag_{i}'])
        
        # Filtra apenas colunas que existem
        available_features = [col for col in feature_columns if col in self.features_df.columns]
        
        X = self.features_df[available_features].values
        y = self.features_df['Close'].values
        
        return X, y, available_features
    
    def train_random_forest(self, X_train, y_train, X_test, y_test):
        """
        Treina modelo Random Forest.
        """
        print("Treinando Random Forest...")
        
        rf_model = RandomForestRegressor(
            n_estimators=100,
            max_depth=10,
            min_samples_split=5,
            min_samples_leaf=2,
            random_state=42,
            n_jobs=-1
        )
        
        rf_model.fit(X_train, y_train)
        
        # Previsões
        train_pred = rf_model.predict(X_train)
        test_pred = rf_model.predict(X_test)
        
        # Métricas
        train_mae = mean_absolute_error(y_train, train_pred)
        test_mae = mean_absolute_error(y_test, test_pred)
        train_rmse = np.sqrt(mean_squared_error(y_train, train_pred))
        test_rmse = np.sqrt(mean_squared_error(y_test, test_pred))
        
        self.models['random_forest'] = rf_model
        self.predictions['random_forest'] = test_pred[-1]  # Última previsão
        self.metrics['random_forest'] = {
            'train_mae': train_mae,
            'test_mae': test_mae,
            'train_rmse': train_rmse,
            'test_rmse': test_rmse
        }
        
        print(f"Random Forest - Test MAE: {test_mae:.4f}, Test RMSE: {test_rmse:.4f}")
        return rf_model
    
    def train_lstm(self, X_train, y_train, X_test, y_test, sequence_length=60):
        """
        Treina modelo LSTM (Long Short-Term Memory).
        """
        print("Treinando LSTM...")
        
        # Prepara dados para LSTM (sequências)
        def create_sequences(data, seq_length):
            X, y = [], []
            for i in range(seq_length, len(data)):
                X.append(data[i-seq_length:i, :])
                y.append(data[i, -1])  # Assume que Close é a última coluna
            return np.array(X), np.array(y)
        
        # Normaliza os dados
        X_scaled = self.scaler.fit_transform(np.column_stack([X_train, y_train.reshape(-1, 1)]))
        
        # Cria sequências
        X_seq, y_seq = create_sequences(X_scaled, sequence_length)
        
        if len(X_seq) < 10:  # Verifica se há dados suficientes
            print("Dados insuficientes para LSTM. Usando previsão simples.")
            self.predictions['lstm'] = y_test[-1]
            self.metrics['lstm'] = {'test_mae': float('inf'), 'test_rmse': float('inf')}
            return None
        
        # Divide em treino e validação
        split = int(0.8 * len(X_seq))
        X_train_seq, X_val_seq = X_seq[:split], X_seq[split:]
        y_train_seq, y_val_seq = y_seq[:split], y_seq[split:]
        
        # Constrói o modelo LSTM
        model = Sequential([
            LSTM(50, return_sequences=True, input_shape=(sequence_length, X_train.shape[1] + 1)),
            Dropout(0.2),
            LSTM(50, return_sequences=False),
            Dropout(0.2),
            Dense(25),
            Dense(1)
        ])
        
        model.compile(optimizer=Adam(learning_rate=0.001), loss='mse', metrics=['mae'])
        
        # Treina o modelo
        try:
            history = model.fit(
                X_train_seq, y_train_seq,
                batch_size=32,
                epochs=50,
                validation_data=(X_val_seq, y_val_seq),
                verbose=0,
                shuffle=False
            )
            
            # Faz previsão
            if len(X_val_seq) > 0:
                val_pred = model.predict(X_val_seq, verbose=0)
                val_mae = mean_absolute_error(y_val_seq, val_pred)
                val_rmse = np.sqrt(mean_squared_error(y_val_seq, val_pred))
                
                # Previsão para próximo período
                last_sequence = X_scaled[-sequence_length:].reshape(1, sequence_length, -1)
                next_pred_scaled = model.predict(last_sequence, verbose=0)[0][0]
                
                # Desnormaliza
                dummy_array = np.zeros((1, X_scaled.shape[1]))
                dummy_array[0, -1] = next_pred_scaled
                next_pred = self.scaler.inverse_transform(dummy_array)[0, -1]
                
                self.models['lstm'] = model
                self.predictions['lstm'] = next_pred
                self.metrics['lstm'] = {
                    'test_mae': val_mae,
                    'test_rmse': val_rmse
                }
                
                print(f"LSTM - Test MAE: {val_mae:.4f}, Test RMSE: {val_rmse:.4f}")
                return model
            
        except Exception as e:
            print(f"Erro no treinamento LSTM: {e}")
            self.predictions['lstm'] = y_test[-1]
            self.metrics['lstm'] = {'test_mae': float('inf'), 'test_rmse': float('inf')}
            return None
    
    def train_arima(self, y_data, test_size=20):
        """
        Treina modelo ARIMA para análise de séries temporais.
        """
        print("Treinando ARIMA...")
        
        try:
            # Divide os dados
            train_data = y_data[:-test_size] if test_size > 0 else y_data
            test_data = y_data[-test_size:] if test_size > 0 else []
            
            # Busca automaticamente os melhores parâmetros ARIMA
            best_aic = float('inf')
            best_order = None
            best_model = None
            
            for p in range(0, 3):
                for d in range(0, 2):
                    for q in range(0, 3):
                        try:
                            model = ARIMA(train_data, order=(p, d, q))
                            fitted_model = model.fit()
                            if fitted_model.aic < best_aic:
                                best_aic = fitted_model.aic
                                best_order = (p, d, q)
                                best_model = fitted_model
                        except:
                            continue
            
            if best_model is None:
                print("Não foi possível ajustar modelo ARIMA")
                self.predictions['arima'] = y_data[-1]
                self.metrics['arima'] = {'test_mae': float('inf'), 'test_rmse': float('inf')}
                return None
            
            # Faz previsões
            forecast = best_model.forecast(steps=1)[0]
            
            if test_size > 0 and len(test_data) > 0:
                test_forecast = best_model.forecast(steps=len(test_data))
                test_mae = mean_absolute_error(test_data, test_forecast)
                test_rmse = np.sqrt(mean_squared_error(test_data, test_forecast))
            else:
                test_mae = 0
                test_rmse = 0
            
            self.models['arima'] = best_model
            self.predictions['arima'] = forecast
            self.metrics['arima'] = {
                'test_mae': test_mae,
                'test_rmse': test_rmse,
                'aic': best_aic,
                'order': best_order
            }
            
            print(f"ARIMA{best_order} - AIC: {best_aic:.2f}, Test MAE: {test_mae:.4f}")
            return best_model
            
        except Exception as e:
            print(f"Erro no ARIMA: {e}")
            self.predictions['arima'] = y_data[-1]
            self.metrics['arima'] = {'test_mae': float('inf'), 'test_rmse': float('inf')}
            return None
    
    def create_ensemble_prediction(self):
        """
        Cria previsão ensemble combinando todos os modelos.
        """
        print("Criando previsão ensemble...")
        
        valid_predictions = []
        weights = []
        
        for model_name, prediction in self.predictions.items():
            if model_name == 'ensemble':
                continue
                
            metrics = self.metrics.get(model_name, {})
            test_mae = metrics.get('test_mae', float('inf'))
            
            if test_mae != float('inf') and not np.isnan(prediction):
                valid_predictions.append(prediction)
                # Peso inversamente proporcional ao erro
                weight = 1 / (test_mae + 1e-6)
                weights.append(weight)
        
        if not valid_predictions:
            # Fallback para último preço
            ensemble_pred = self.features_df['Close'].iloc[-1]
        else:
            # Média ponderada
            weights = np.array(weights)
            weights = weights / weights.sum()
            ensemble_pred = np.average(valid_predictions, weights=weights)
        
        self.predictions['ensemble'] = ensemble_pred
        
        # Calcula confiança baseada na concordância dos modelos
        if len(valid_predictions) > 1:
            std_dev = np.std(valid_predictions)
            confidence = max(0, 1 - (std_dev / np.mean(valid_predictions)))
        else:
            confidence = 0.5
        
        self.metrics['ensemble'] = {
            'confidence': confidence,
            'num_models': len(valid_predictions),
            'predictions_std': np.std(valid_predictions) if valid_predictions else 0
        }
        
        print(f"Ensemble - Previsão: {ensemble_pred:.2f}, Confiança: {confidence:.2f}")
        return ensemble_pred
    
    def train_all_models(self):
        """
        Treina todos os modelos e cria previsão ensemble.
        """
        if not self.fetch_data():
            return None
        
        # Prepara features
        X, y, feature_names = self.prepare_features()
        
        if len(X) < 50:
            print("Dados insuficientes para treinar modelos avançados")
            return None
        
        # Divide em treino e teste
        test_size = min(20, len(X) // 5)
        X_train, X_test = X[:-test_size], X[-test_size:]
        y_train, y_test = y[:-test_size], y[-test_size:]
        
        print(f"Treino: {len(X_train)} amostras, Teste: {len(X_test)} amostras")
        
        # Treina cada modelo
        self.train_random_forest(X_train, y_train, X_test, y_test)
        self.train_lstm(X_train, y_train, X_test, y_test)
        self.train_arima(y, test_size)
        
        # Cria ensemble
        ensemble_pred = self.create_ensemble_prediction()
        
        return {
            'predictions': self.predictions,
            'metrics': self.metrics,
            'ensemble_prediction': ensemble_pred,
            'current_price': self.features_df['Close'].iloc[-1],
            'historical_data': self.get_historical_data_for_chart()
        }
    
    def get_historical_data_for_chart(self, days=30):
        """
        Retorna dados históricos formatados para o gráfico.
        """
        if self.features_df is None:
            return []
        
        recent_data = self.features_df.tail(days)
        chart_data = []
        
        for index, row in recent_data.iterrows():
            chart_data.append({
                "date": index.strftime('%Y-%m-%d'),
                "price": float(row['Close'])
            })
        
        return chart_data
    
    def get_prediction_summary(self):
        """
        Retorna resumo das previsões para todos os modelos.
        """
        return {
            'ticker': self.ticker,
            'current_price': float(self.features_df['Close'].iloc[-1]) if self.features_df is not None else 0,
            'predictions': {k: float(v) for k, v in self.predictions.items()},
            'ensemble_confidence': self.metrics.get('ensemble', {}).get('confidence', 0),
            'models_used': list(self.predictions.keys()),
            'data_points': len(self.features_df) if self.features_df is not None else 0
        }


def predict_asset_price(ticker: str, days_history: int = 252):
    """
    Função principal para prever preço de um ativo usando múltiplos modelos.
    
    Args:
        ticker: Símbolo do ativo (ex: 'PETR4.SA')
        days_history: Dias de histórico para análise
    
    Returns:
        Dict com previsões e métricas
    """
    try:
        predictor = AdvancedPricePredictor(ticker, days_history)
        results = predictor.train_all_models()
        
        if results is None:
            return None
        
        return {
            'success': True,
            'prediction': results['ensemble_prediction'],
            'current_price': results['current_price'],
            'historical_data': results['historical_data'],
            'all_predictions': results['predictions'],
            'model_metrics': results['metrics'],
            'summary': predictor.get_prediction_summary()
        }
        
    except Exception as e:
        print(f"Erro na previsão para {ticker}: {e}")
        return {
            'success': False,
            'error': str(e),
            'prediction': None
        }


if __name__ == "__main__":
    # Teste do sistema
    result = predict_asset_price("PETR4.SA")
    if result and result['success']:
        print(f"\nPrevisão para PETR4:")
        print(f"Preço atual: R$ {result['current_price']:.2f}")
        print(f"Previsão ensemble: R$ {result['prediction']:.2f}")
        print(f"Modelos: {list(result['all_predictions'].keys())}")
    else:
        print("Erro na previsão de teste")