"""
Módulo de previsão avançada para preços de ativos.
Implementa múltiplos modelos de machine learning para melhorar a precisão das previsões.
"""

import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime, timedelta
import warnings
import os
import logging
import pickle
import hashlib
from pathlib import Path

# Configuração otimizada do TensorFlow para CPU-only
os.environ['CUDA_VISIBLE_DEVICES'] = '-1'  # Força CPU-only
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'  # Suprime todos os logs do TF
os.environ['TF_ENABLE_ONEDNN_OPTS'] = '0'  # Desabilita otimizações que podem causar problemas
os.environ['TF_FORCE_GPU_ALLOW_GROWTH'] = 'false'  # Não tenta usar GPU

# Detecta se está em ambiente de produção (Render, Heroku, etc.)
IS_PRODUCTION = any([
    os.getenv('RENDER'),
    os.getenv('HEROKU'),
    os.getenv('VERCEL'),
    os.getenv('RAILWAY'),
    'render.com' in os.getenv('HTTP_HOST', ''),
    'herokuapp.com' in os.getenv('HTTP_HOST', ''),
])

# Configurações de produção
USE_ADVANCED_MODELS = os.getenv('USE_ADVANCED_MODELS', 'true').lower() == 'true'
USE_TENSORFLOW = os.getenv('USE_TENSORFLOW', 'true').lower() == 'true'
CPU_ONLY_MODE = os.getenv('CPU_ONLY_MODE', str(IS_PRODUCTION)).lower() == 'true'

# Em produção, usa configuração mais conservadora por padrão
if IS_PRODUCTION and not os.getenv('USE_ADVANCED_MODELS'):
    USE_ADVANCED_MODELS = False
    print("🏭 Ambiente de produção detectado - usando configuração conservadora")

if CPU_ONLY_MODE:
    print("💻 Modo CPU-only ativado")

warnings.filterwarnings('ignore')
logging.getLogger('tensorflow').setLevel(logging.ERROR)

# Machine Learning
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.preprocessing import MinMaxScaler, StandardScaler
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import TimeSeriesSplit

# TensorFlow com configuração segura para produção
TENSORFLOW_AVAILABLE = False
if USE_TENSORFLOW:
    try:
        import tensorflow as tf
        
        # Configuração adicional para CPU-only
        tf.config.set_visible_devices([], 'GPU')  # Remove GPUs visíveis
        
        # Configuração de threading otimizada para CPU
        if IS_PRODUCTION or CPU_ONLY_MODE:
            # Em produção, usa configuração conservadora
            tf.config.threading.set_intra_op_parallelism_threads(2)
            tf.config.threading.set_inter_op_parallelism_threads(2)
        else:
            # Local, pode usar mais threads
            tf.config.threading.set_intra_op_parallelism_threads(0)
            tf.config.threading.set_inter_op_parallelism_threads(0)
        
        # Suprime logs
        tf.get_logger().setLevel('ERROR')
        
        from tensorflow.keras.models import Sequential
        from tensorflow.keras.layers import LSTM, Dense, Dropout, BatchNormalization
        from tensorflow.keras.optimizers import Adam
        from tensorflow.keras.callbacks import EarlyStopping, ReduceLROnPlateau
        
        TENSORFLOW_AVAILABLE = True
        print("✅ TensorFlow configurado para CPU-only")
        
    except Exception as e:
        print(f"⚠️  TensorFlow não disponível: {e}")
        print("   Usando apenas modelos tradicionais (Random Forest, ARIMA)")
        TENSORFLOW_AVAILABLE = False
else:
    print("⚠️  TensorFlow desabilitado via configuração")
    TENSORFLOW_AVAILABLE = False

# Time Series
from statsmodels.tsa.arima.model import ARIMA
from statsmodels.tsa.seasonal import seasonal_decompose

# Technical Analysis
import talib

class SimpleFallbackPredictor:
    """
    Preditor simples usando apenas pandas/numpy para casos onde modelos avançados falham.
    """
    
    def __init__(self, data):
        self.data = data
        
    def predict_next_price(self):
        """
        Previsão simples baseada em análise técnica básica.
        """
        if len(self.data) < 5:
            return float(self.data['Close'].iloc[-1])
            
        prices = self.data['Close']
        
        # 1. Tendência dos últimos 5 dias
        recent_trend = (prices.iloc[-1] - prices.iloc[-5]) / prices.iloc[-5]
        
        # 2. Média móvel simples (10 períodos ou disponível)
        sma_periods = min(10, len(prices) - 1)
        sma = prices.tail(sma_periods).mean()
        
        # 3. Volatilidade histórica
        returns = prices.pct_change().dropna()
        volatility = returns.std() if len(returns) > 1 else 0.02
        
        # 4. Suporte e resistência simples
        high_20 = prices.tail(min(20, len(prices))).max()
        low_20 = prices.tail(min(20, len(prices))).min()
        current_price = prices.iloc[-1]
        
        # Calcula posição relativa (0 = no mínimo, 1 = no máximo)
        price_position = (current_price - low_20) / (high_20 - low_20) if high_20 != low_20 else 0.5
        
        # 5. Momentum simples
        if len(prices) >= 3:
            momentum = (prices.iloc[-1] - prices.iloc[-3]) / prices.iloc[-3]
        else:
            momentum = 0
        
        # Combina os fatores
        trend_weight = 0.3
        sma_weight = 0.3
        momentum_weight = 0.2
        mean_reversion_weight = 0.2
        
        # Previsão baseada na tendência
        trend_pred = current_price * (1 + recent_trend * trend_weight)
        
        # Influência da média móvel
        sma_pred = sma * sma_weight + current_price * (1 - sma_weight)
        
        # Momentum
        momentum_pred = current_price * (1 + momentum * momentum_weight)
        
        # Reversão à média (se muito extremo)
        if price_position > 0.8:  # Próximo ao máximo
            mean_reversion_pred = current_price * 0.98  # Leve queda
        elif price_position < 0.2:  # Próximo ao mínimo
            mean_reversion_pred = current_price * 1.02  # Leve alta
        else:
            mean_reversion_pred = current_price
        
        # Média ponderada
        prediction = (trend_pred + sma_pred + momentum_pred + 
                     mean_reversion_pred * mean_reversion_weight) / 3
        
        # Limita a mudança pela volatilidade
        max_change = volatility * 1.5  # Máximo 1.5x a volatilidade
        change_ratio = abs((prediction - current_price) / current_price)
        
        if change_ratio > max_change:
            direction = 1 if prediction > current_price else -1
            prediction = current_price * (1 + direction * max_change)
        
        return float(prediction)
    
    def get_confidence(self):
        """
        Retorna confiança baseada na quantidade de dados e volatilidade.
        """
        if len(self.data) < 5:
            return 0.2
        elif len(self.data) < 20:
            return 0.4
        else:
            # Confiança baseada na estabilidade dos preços
            returns = self.data['Close'].pct_change().dropna()
            volatility = returns.std() if len(returns) > 1 else 0.02
            
            # Menor volatilidade = maior confiança
            confidence = max(0.3, min(0.7, 1 - volatility * 10))
            return confidence

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
        self.cv_scores = {}
        
        # Sistema de cache
        self.cache_dir = Path("cache/predictions")
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.cache_duration = timedelta(minutes=30)  # Cache válido por 30 minutos
        
    def _generate_cache_key(self, data_hash=None):
        """
        Gera uma chave única para o cache baseada no ticker, período e dados.
        """
        if data_hash is None:
            data_hash = "no_data"
        
        key_string = f"{self.ticker}_{self.days_history}_{data_hash}"
        return hashlib.md5(key_string.encode()).hexdigest()
    
    def _get_data_hash(self):
        """
        Gera hash dos dados para detectar mudanças.
        """
        if self.data is None or self.data.empty:
            return None
        
        # Usa os últimos 10 preços para gerar hash
        recent_data = self.data['Close'].tail(10).values
        return hashlib.md5(str(recent_data).encode()).hexdigest()[:8]
    
    def _save_to_cache(self, result):
        """
        Salva resultado no cache.
        """
        try:
            data_hash = self._get_data_hash()
            cache_key = self._generate_cache_key(data_hash)
            cache_file = self.cache_dir / f"{cache_key}.pkl"
            
            cache_data = {
                'result': result,
                'timestamp': datetime.now(),
                'ticker': self.ticker,
                'data_hash': data_hash
            }
            
            with open(cache_file, 'wb') as f:
                pickle.dump(cache_data, f)
                
            print(f"💾 Resultado salvo no cache: {cache_file.name}")
            
        except Exception as e:
            print(f"⚠️  Erro ao salvar cache: {e}")
    
    def _load_from_cache(self):
        """
        Carrega resultado do cache se válido.
        """
        try:
            data_hash = self._get_data_hash()
            cache_key = self._generate_cache_key(data_hash)
            cache_file = self.cache_dir / f"{cache_key}.pkl"
            
            if not cache_file.exists():
                return None
            
            with open(cache_file, 'rb') as f:
                cache_data = pickle.load(f)
            
            # Verifica se o cache ainda é válido
            cache_age = datetime.now() - cache_data['timestamp']
            if cache_age > self.cache_duration:
                print(f"🕒 Cache expirado ({cache_age}), recalculando...")
                cache_file.unlink()  # Remove cache expirado
                return None
            
            # Verifica se os dados mudaram
            if cache_data.get('data_hash') != data_hash:
                print("📊 Dados mudaram, recalculando...")
                cache_file.unlink()
                return None
            
            print(f"⚡ Cache válido encontrado (idade: {cache_age})")
            return cache_data['result']
            
        except Exception as e:
            print(f"⚠️  Erro ao carregar cache: {e}")
            return None
    
    def _clean_old_cache(self):
        """
        Remove arquivos de cache antigos.
        """
        try:
            now = datetime.now()
            for cache_file in self.cache_dir.glob("*.pkl"):
                try:
                    file_age = now - datetime.fromtimestamp(cache_file.stat().st_mtime)
                    if file_age > timedelta(hours=24):  # Remove caches > 24h
                        cache_file.unlink()
                except Exception:
                    continue
        except Exception as e:
            print(f"⚠️  Erro na limpeza do cache: {e}")
        
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
        Calcula indicadores técnicos avançados usando TA-Lib.
        """
        if self.data is None or self.data.empty:
            raise ValueError("Dados não carregados. Execute fetch_data() primeiro.")
        
        df = self.data.copy()
        
        # Médias Móveis Simples e Exponenciais
        df['SMA_5'] = talib.SMA(df['Close'], timeperiod=5)
        df['SMA_10'] = talib.SMA(df['Close'], timeperiod=10)
        df['SMA_20'] = talib.SMA(df['Close'], timeperiod=20)
        df['SMA_50'] = talib.SMA(df['Close'], timeperiod=50)
        df['EMA_8'] = talib.EMA(df['Close'], timeperiod=8)
        df['EMA_12'] = talib.EMA(df['Close'], timeperiod=12)
        df['EMA_26'] = talib.EMA(df['Close'], timeperiod=26)
        df['EMA_50'] = talib.EMA(df['Close'], timeperiod=50)
        
        # Médias Móveis Adaptativas
        df['KAMA'] = talib.KAMA(df['Close'], timeperiod=30)
        df['TEMA'] = talib.TEMA(df['Close'], timeperiod=30)
        
        # Osciladores de Momentum
        df['RSI_14'] = talib.RSI(df['Close'], timeperiod=14)
        df['RSI_7'] = talib.RSI(df['Close'], timeperiod=7)
        df['STOCH_K'], df['STOCH_D'] = talib.STOCH(df['High'], df['Low'], df['Close'])
        df['STOCHRSI_K'], df['STOCHRSI_D'] = talib.STOCHRSI(df['Close'])
        df['WILLR'] = talib.WILLR(df['High'], df['Low'], df['Close'], timeperiod=14)
        df['CCI'] = talib.CCI(df['High'], df['Low'], df['Close'], timeperiod=14)
        df['MFI'] = talib.MFI(df['High'], df['Low'], df['Close'], df['Volume'], timeperiod=14)
        
        # MACD e variações
        df['MACD'], df['MACD_signal'], df['MACD_hist'] = talib.MACD(df['Close'])
        df['MACD_fast'], df['MACD_fast_signal'], df['MACD_fast_hist'] = talib.MACD(df['Close'], fastperiod=8, slowperiod=17, signalperiod=9)
        
        # Bollinger Bands
        df['BB_upper'], df['BB_middle'], df['BB_lower'] = talib.BBANDS(df['Close'], timeperiod=20, nbdevup=2, nbdevdn=2)
        df['BB_width'] = (df['BB_upper'] - df['BB_lower']) / df['BB_middle']
        df['BB_position'] = (df['Close'] - df['BB_lower']) / (df['BB_upper'] - df['BB_lower'])
        df['BB_squeeze'] = df['BB_width'] < df['BB_width'].rolling(20).mean() * 0.8
        
        # Parabolic SAR
        df['SAR'] = talib.SAR(df['High'], df['Low'])
        df['SAR_signal'] = np.where(df['Close'] > df['SAR'], 1, -1)
        
        # Average Directional Movement Index
        df['ADX'] = talib.ADX(df['High'], df['Low'], df['Close'], timeperiod=14)
        df['PLUS_DI'] = talib.PLUS_DI(df['High'], df['Low'], df['Close'], timeperiod=14)
        df['MINUS_DI'] = talib.MINUS_DI(df['High'], df['Low'], df['Close'], timeperiod=14)
        
        # Momentum e Rate of Change
        df['MOM_10'] = talib.MOM(df['Close'], timeperiod=10)
        df['MOM_20'] = talib.MOM(df['Close'], timeperiod=20)
        df['ROC_10'] = talib.ROC(df['Close'], timeperiod=10)
        df['ROC_20'] = talib.ROC(df['Close'], timeperiod=20)
        
        # Volatilidade
        df['ATR_14'] = talib.ATR(df['High'], df['Low'], df['Close'], timeperiod=14)
        df['ATR_21'] = talib.ATR(df['High'], df['Low'], df['Close'], timeperiod=21)
        df['NATR'] = talib.NATR(df['High'], df['Low'], df['Close'], timeperiod=14)
        df['TRANGE'] = talib.TRANGE(df['High'], df['Low'], df['Close'])
        
        # Indicadores de Volume
        if 'Volume' in df.columns and df['Volume'].sum() > 0:
            df['OBV'] = talib.OBV(df['Close'], df['Volume'])
            df['AD'] = talib.AD(df['High'], df['Low'], df['Close'], df['Volume'])
            df['ADOSC'] = talib.ADOSC(df['High'], df['Low'], df['Close'], df['Volume'])
            df['Volume_SMA_10'] = talib.SMA(df['Volume'], timeperiod=10)
            df['Volume_SMA_20'] = talib.SMA(df['Volume'], timeperiod=20)
            df['Volume_ratio'] = df['Volume'] / df['Volume_SMA_20']
            df['VWAP'] = (df['Close'] * df['Volume']).cumsum() / df['Volume'].cumsum()
        else:
            # Valores padrão quando volume não está disponível
            df['OBV'] = 0
            df['AD'] = 0
            df['ADOSC'] = 0
            df['Volume_ratio'] = 1.0
            df['VWAP'] = df['Close']
        
        # Padrões de Candlestick (alguns dos mais importantes)
        df['DOJI'] = talib.CDLDOJI(df['Open'], df['High'], df['Low'], df['Close'])
        df['HAMMER'] = talib.CDLHAMMER(df['Open'], df['High'], df['Low'], df['Close'])
        df['ENGULFING'] = talib.CDLENGULFING(df['Open'], df['High'], df['Low'], df['Close'])
        df['MORNINGSTAR'] = talib.CDLMORNINGSTAR(df['Open'], df['High'], df['Low'], df['Close'])
        df['EVENINGSTAR'] = talib.CDLEVENINGSTAR(df['Open'], df['High'], df['Low'], df['Close'])
        
        # Features derivadas de preço
        df['Price_change'] = df['Close'].pct_change()
        df['Price_change_2'] = df['Close'].pct_change(2)
        df['Price_change_5'] = df['Close'].pct_change(5)
        df['High_Low_ratio'] = df['High'] / df['Low']
        df['Close_Open_ratio'] = df['Close'] / df['Open']
        df['Body_size'] = np.abs(df['Close'] - df['Open']) / df['Open']
        df['Upper_shadow'] = (df['High'] - np.maximum(df['Open'], df['Close'])) / df['Open']
        df['Lower_shadow'] = (np.minimum(df['Open'], df['Close']) - df['Low']) / df['Open']
        
        # Médias móveis cruzadas
        df['SMA_cross_5_10'] = np.where(df['SMA_5'] > df['SMA_10'], 1, 0)
        df['SMA_cross_10_20'] = np.where(df['SMA_10'] > df['SMA_20'], 1, 0)
        df['EMA_cross_12_26'] = np.where(df['EMA_12'] > df['EMA_26'], 1, 0)
        
        # Lags (preços anteriores) - importantes para séries temporais
        for i in [1, 2, 3, 5, 7, 10]:
            df[f'Close_lag_{i}'] = df['Close'].shift(i)
            df[f'RSI_lag_{i}'] = df['RSI_14'].shift(i)
            if 'Volume' in df.columns:
                df[f'Volume_lag_{i}'] = df['Volume'].shift(i)
            
        # Estatísticas móveis
        df['Close_std_20'] = df['Close'].rolling(20).std()
        df['Close_skew_20'] = df['Close'].rolling(20).skew()
        df['Close_kurt_20'] = df['Close'].rolling(20).kurt()
        df['Price_volatility'] = df['Price_change'].rolling(20).std() * np.sqrt(252)  # Volatilidade anualizada
        
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
        Treina modelo LSTM (Long Short-Term Memory) com arquitetura melhorada.
        """
        if not TENSORFLOW_AVAILABLE:
            print("⚠️  LSTM indisponível: TensorFlow não carregado")
            self.predictions['lstm'] = y_test[-1] if len(y_test) > 0 else 0
            self.metrics['lstm'] = {'test_mae': float('inf'), 'test_rmse': float('inf'), 'error': 'TensorFlow não disponível'}
            return None
            
        print("Treinando LSTM otimizado...")
        
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
        
        if len(X_seq) < 20:  # Verifica se há dados suficientes
            print("Dados insuficientes para LSTM. Usando previsão simples.")
            self.predictions['lstm'] = y_test[-1]
            self.metrics['lstm'] = {'test_mae': float('inf'), 'test_rmse': float('inf')}
            return None
        
        # Divide em treino e validação
        split = int(0.8 * len(X_seq))
        X_train_seq, X_val_seq = X_seq[:split], X_seq[split:]
        y_train_seq, y_val_seq = y_seq[:split], y_seq[split:]
        
        try:
            # Constrói o modelo LSTM melhorado com arquitetura mais profunda
            model = Sequential([
                # Primeira camada LSTM com mais neurônios
                LSTM(100, return_sequences=True, input_shape=(sequence_length, X_train.shape[1] + 1)),
                BatchNormalization(),
                Dropout(0.3),
                
                # Segunda camada LSTM
                LSTM(80, return_sequences=True),
                BatchNormalization(),
                Dropout(0.3),
                
                # Terceira camada LSTM
                LSTM(60, return_sequences=False),
                BatchNormalization(),
                Dropout(0.2),
                
                # Camadas densas com regularização
                Dense(50, activation='relu'),
                Dropout(0.2),
                Dense(25, activation='relu'),
                Dropout(0.1),
                Dense(1, activation='linear')
            ])
            
            # Compilação com otimizador melhorado
            model.compile(
                optimizer=Adam(learning_rate=0.001, decay=1e-6),
                loss='huber',  # Mais robusto a outliers que MSE
                metrics=['mae', 'mse']
            )
            
            # Callbacks para melhor treinamento
            early_stopping = EarlyStopping(
                monitor='val_loss',
                patience=10,
                restore_best_weights=True,
                verbose=0
            )
            
            reduce_lr = ReduceLROnPlateau(
                monitor='val_loss',
                factor=0.5,
                patience=5,
                min_lr=1e-7,
                verbose=0
            )
            
            # Treina o modelo com callbacks
            history = model.fit(
                X_train_seq, y_train_seq,
                batch_size=32,
                epochs=100,  # Mais épocas com early stopping
                validation_data=(X_val_seq, y_val_seq),
                callbacks=[early_stopping, reduce_lr],
                verbose=0,
                shuffle=False
            )
            
            # Faz previsão
            if len(X_val_seq) > 0:
                val_pred = model.predict(X_val_seq, verbose=0)
                val_mae = mean_absolute_error(y_val_seq, val_pred)
                val_rmse = np.sqrt(mean_squared_error(y_val_seq, val_pred))
                val_r2 = r2_score(y_val_seq, val_pred)
                
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
                    'test_rmse': val_rmse,
                    'test_r2': val_r2,
                    'epochs_trained': len(history.history['loss'])
                }
                
                print(f"LSTM - Test MAE: {val_mae:.4f}, Test RMSE: {val_rmse:.4f}")
                return model
            
        except Exception as e:
            print(f"Erro no treinamento LSTM: {e}")
            self.predictions['lstm'] = y_test[-1]
            self.metrics['lstm'] = {'test_mae': float('inf'), 'test_rmse': float('inf'), 'error': str(e)}
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
    
    def time_series_cross_validation(self, X, y, n_splits=5):
        """
        Implementa validação cruzada temporal para séries temporais.
        Usa janelas deslizantes respeitando a ordem temporal dos dados.
        """
        print(f"Executando validação cruzada temporal com {n_splits} splits...")
        
        tscv = TimeSeriesSplit(n_splits=n_splits)
        cv_scores = {
            'random_forest': {'mae': [], 'rmse': []},
            'gradient_boosting': {'mae': [], 'rmse': []}
        }
        
        for fold, (train_index, test_index) in enumerate(tscv.split(X)):
            X_train_cv, X_test_cv = X.iloc[train_index], X.iloc[test_index]
            y_train_cv, y_test_cv = y.iloc[train_index], y.iloc[test_index]
            
            # Random Forest
            rf = RandomForestRegressor(
                n_estimators=200,
                max_depth=15,
                min_samples_split=5,
                min_samples_leaf=2,
                random_state=42,
                n_jobs=-1
            )
            rf.fit(X_train_cv, y_train_cv)
            rf_pred = rf.predict(X_test_cv)
            
            rf_mae = mean_absolute_error(y_test_cv, rf_pred)
            rf_rmse = np.sqrt(mean_squared_error(y_test_cv, rf_pred))
            cv_scores['random_forest']['mae'].append(rf_mae)
            cv_scores['random_forest']['rmse'].append(rf_rmse)
            
            # Gradient Boosting
            gb = GradientBoostingRegressor(
                n_estimators=200,
                max_depth=8,
                learning_rate=0.05,
                random_state=42
            )
            gb.fit(X_train_cv, y_train_cv)
            gb_pred = gb.predict(X_test_cv)
            
            gb_mae = mean_absolute_error(y_test_cv, gb_pred)
            gb_rmse = np.sqrt(mean_squared_error(y_test_cv, gb_pred))
            cv_scores['gradient_boosting']['mae'].append(gb_mae)
            cv_scores['gradient_boosting']['rmse'].append(gb_rmse)
            
            print(f"Fold {fold+1}: RF MAE={rf_mae:.4f}, GB MAE={gb_mae:.4f}")
        
        # Calcula médias e desvios padrão
        for model in cv_scores:
            cv_scores[model]['mae_mean'] = np.mean(cv_scores[model]['mae'])
            cv_scores[model]['mae_std'] = np.std(cv_scores[model]['mae'])
            cv_scores[model]['rmse_mean'] = np.mean(cv_scores[model]['rmse'])
            cv_scores[model]['rmse_std'] = np.std(cv_scores[model]['rmse'])
            
            print(f"{model} - MAE: {cv_scores[model]['mae_mean']:.4f} ± {cv_scores[model]['mae_std']:.4f}")
        
        self.cv_scores = cv_scores
        return cv_scores
    
    def create_ensemble_prediction(self):
        """
        Cria previsão ensemble com pesos dinâmicos baseados na performance recente.
        """
        print("Criando previsão ensemble otimizada...")
        
        valid_predictions = []
        weights = []
        model_info = []
        
        for model_name, prediction in self.predictions.items():
            if model_name == 'ensemble':
                continue
                
            metrics = self.metrics.get(model_name, {})
            test_mae = metrics.get('test_mae', float('inf'))
            test_rmse = metrics.get('test_rmse', float('inf'))
            test_r2 = metrics.get('test_r2', -float('inf'))
            
            if test_mae != float('inf') and not np.isnan(prediction) and not np.isinf(prediction):
                valid_predictions.append(prediction)
                model_info.append(model_name)
                
                # Cálculo de pesos dinâmicos mais sofisticado
                # Combina MAE, RMSE e R² para um score mais robusto
                mae_score = 1 / (test_mae + 1e-6)  # Quanto menor o MAE, maior o peso
                rmse_score = 1 / (test_rmse + 1e-6)  # Quanto menor o RMSE, maior o peso
                r2_score = max(0, test_r2) + 0.1  # R² normalizado (mínimo 0.1)
                
                # Peso composto (média harmônica dos scores)
                if test_r2 != -float('inf'):
                    composite_weight = (3 * mae_score * rmse_score * r2_score) / (mae_score + rmse_score + r2_score)
                else:
                    composite_weight = (2 * mae_score * rmse_score) / (mae_score + rmse_score)
                
                # Aplica penalidade para modelos com alta variância
                if 'epochs_trained' in metrics and metrics['epochs_trained'] < 10:
                    composite_weight *= 0.7  # Penaliza modelos que pararam muito cedo
                
                # Bônus para modelos específicos baseado no histórico de performance
                if model_name == 'lstm' and test_mae < test_rmse * 0.8:
                    composite_weight *= 1.2  # LSTM funciona bem com tendências
                elif model_name == 'random_forest' and test_r2 > 0.6:
                    composite_weight *= 1.15  # RF bom para capturar não-linearidades
                elif model_name == 'arima' and test_mae < np.mean([m.get('test_mae', float('inf')) for m in self.metrics.values()]):
                    composite_weight *= 1.1  # ARIMA bom para séries estacionárias
                
                weights.append(composite_weight)
        
        if not valid_predictions:
            # Fallback robusto quando nenhum modelo avançado funciona
            print("⚠️  Nenhum modelo avançado válido - usando estratégia de fallback")
            
            last_price = self.features_df['Close'].iloc[-1]
            
            # Estratégia 1: Análise de tendência simples (últimos 5 dias)
            if len(self.features_df) >= 5:
                recent_prices = self.features_df['Close'].tail(5)
                price_trend = (recent_prices.iloc[-1] - recent_prices.iloc[0]) / recent_prices.iloc[0]
                
                # Estratégia 2: Média móvel simples
                if len(self.features_df) >= 10:
                    sma_10 = self.features_df['Close'].tail(10).mean()
                    sma_factor = last_price / sma_10
                else:
                    sma_factor = 1.0
                
                # Estratégia 3: Volatilidade adaptativa
                if 'Price_volatility' in self.features_df.columns:
                    volatility = self.features_df['Price_volatility'].iloc[-1] / 100
                else:
                    # Calcula volatilidade dos últimos 10 dias
                    if len(self.features_df) >= 10:
                        returns = self.features_df['Close'].pct_change().tail(10)
                        volatility = returns.std()
                    else:
                        volatility = 0.02  # 2% padrão
                
                # Combina as estratégias
                trend_pred = last_price * (1 + price_trend * 0.3)  # 30% da tendência
                sma_pred = last_price * (0.7 + sma_factor * 0.3)  # Influência da SMA
                
                # Média das estratégias com ajuste de volatilidade
                ensemble_pred = (trend_pred + sma_pred) / 2
                
                # Aplica limitação baseada na volatilidade (máximo 2x volatilidade)
                max_change = volatility * 2
                change_ratio = abs((ensemble_pred - last_price) / last_price)
                if change_ratio > max_change:
                    direction = 1 if ensemble_pred > last_price else -1
                    ensemble_pred = last_price * (1 + direction * max_change)
                
                print(f"   📊 Usando análise técnica simples (tendência: {price_trend:.1%}, SMA: {sma_factor:.3f})")
            else:
                # Não há dados suficientes - usa preço atual com mínima variação
                volatility = 0.01  # 1% padrão
                ensemble_pred = last_price * (1 + np.random.normal(0, volatility))
                print("   📈 Dados insuficientes - usando preço atual com variação mínima")
                
            # Garante que não é um valor inválido
            if np.isnan(ensemble_pred) or np.isinf(ensemble_pred) or ensemble_pred <= 0:
                ensemble_pred = last_price
                print("   🔧 Corrigindo previsão inválida para preço atual")
        else:
            # Normaliza os pesos
            weights = np.array(weights)
            weights = weights / weights.sum()
            
            # Calcula média ponderada
            ensemble_pred = np.average(valid_predictions, weights=weights)
            
            # Log dos pesos para debug
            for i, (model, weight) in enumerate(zip(model_info, weights)):
                print(f"  • {model}: peso {weight:.3f} (pred: {valid_predictions[i]:.2f})")
        
        self.predictions['ensemble'] = ensemble_pred
        
        # Calcula métricas de confiança mais sofisticadas
        if len(valid_predictions) > 1:
            # Concordância entre modelos (menor desvio padrão = maior confiança)
            predictions_array = np.array(valid_predictions)
            std_dev = np.std(predictions_array)
            mean_pred = np.mean(predictions_array)
            cv = std_dev / mean_pred if mean_pred != 0 else 1  # Coeficiente de variação
            
            # Confiança baseada na concordância e qualidade dos modelos
            agreement_confidence = max(0, 1 - cv)  # Quanto menor a variação, maior a confiança
            
            # Confiança baseada na qualidade média dos modelos
            avg_quality = np.mean(weights)
            quality_confidence = min(1, avg_quality / np.mean(weights) if len(weights) > 0 else 0)
            
            # Confiança final (média ponderada)
            final_confidence = 0.6 * agreement_confidence + 0.4 * quality_confidence
            
            # Penaliza se poucos modelos
            if len(valid_predictions) < 3:
                final_confidence *= 0.8
                
        else:
            final_confidence = 0.3  # Baixa confiança com apenas um modelo
        
        # Calcula intervalo de confiança
        if len(valid_predictions) > 1:
            sorted_preds = np.sort(valid_predictions)
            lower_bound = sorted_preds[len(sorted_preds)//4]  # Q1
            upper_bound = sorted_preds[3*len(sorted_preds)//4]  # Q3
        else:
            volatility = self.features_df.get('Price_volatility', [0.2]).iloc[-1] if len(self.features_df) > 0 else 0.2
            margin = ensemble_pred * volatility / 100
            lower_bound = ensemble_pred - margin
            upper_bound = ensemble_pred + margin
        
        self.metrics['ensemble'] = {
            'confidence': final_confidence,
            'num_models': len(valid_predictions),
            'predictions_std': np.std(valid_predictions) if valid_predictions else 0,
            'model_weights': dict(zip(model_info, weights)) if model_info else {},
            'prediction_range': {
                'lower': lower_bound,
                'upper': upper_bound,
                'ensemble': ensemble_pred
            }
        }
        
        print(f"✅ Ensemble - Previsão: R$ {ensemble_pred:.2f}")
        print(f"   📊 Confiança: {final_confidence:.1%}")
        print(f"   📈 Intervalo: R$ {lower_bound:.2f} - R$ {upper_bound:.2f}")
        
        return ensemble_pred
    
    def train_all_models(self):
        """
        Treina todos os modelos e cria previsão ensemble com cache inteligente.
        """
        # Limpa cache antigo primeiro
        self._clean_old_cache()
        
        if not self.fetch_data():
            return None
            
        # Tenta carregar do cache primeiro
        cached_result = self._load_from_cache()
        if cached_result is not None:
            print("🚀 Usando resultado do cache!")
            return cached_result
        
        print("🔄 Cache não encontrado ou inválido, calculando previsões...")

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
        print(f"Features utilizadas: {len(feature_names)} indicadores")
        
        # Executa validação cruzada se há dados suficientes
        if len(X) > 100:
            print("🔍 Executando validação cruzada...")
            self.time_series_cross_validation(
                pd.DataFrame(X_train, columns=feature_names), 
                pd.Series(y_train)
            )

        # Treina cada modelo
        print("\n🤖 Treinando modelos individuais...")
        
        # Verifica se deve usar modelos avançados
        if not USE_ADVANCED_MODELS:
            print("⚠️  Modelos avançados desabilitados via configuração")
            print("🔄 Usando sistema de fallback simples...")
            
            # Usa apenas o SimpleFallbackPredictor
            fallback = SimpleFallbackPredictor(self.data)
            fallback_pred = fallback.predict_next_price()
            fallback_confidence = fallback.get_confidence()
            
            self.predictions = {
                'simple_fallback': fallback_pred,
                'ensemble': fallback_pred
            }
            
            self.metrics = {
                'simple_fallback': {
                    'test_mae': 0.0,
                    'test_rmse': 0.0,
                    'confidence': fallback_confidence,
                    'model_type': 'Simple Technical Analysis'
                },
                'ensemble': {
                    'confidence': fallback_confidence,
                    'model_count': 1,
                    'method': 'Simple Fallback'
                }
            }
            
            print(f"✅ Previsão simples: ${fallback_pred:.2f} (confiança: {fallback_confidence:.1%})")
        else:
            # Treina modelos avançados normalmente
            models_trained = 0
            
            # Random Forest (sempre disponível)
            try:
                self.train_random_forest(X_train, y_train, X_test, y_test)
                models_trained += 1
            except Exception as e:
                print(f"⚠️  Random Forest falhou: {e}")
                
            # LSTM apenas se TensorFlow disponível
            if TENSORFLOW_AVAILABLE:
                try:
                    self.train_lstm(X_train, y_train, X_test, y_test)
                    models_trained += 1
                except Exception as e:
                    print(f"⚠️  LSTM falhou: {e}")
            else:
                print("⚠️  Pulando LSTM: TensorFlow não disponível")
                self.predictions['lstm'] = y_test[-1] if len(y_test) > 0 else 0
                self.metrics['lstm'] = {'test_mae': float('inf'), 'test_rmse': float('inf'), 'error': 'TensorFlow não disponível'}
                
            # ARIMA
            try:
                self.train_arima(y, test_size)
                models_trained += 1
            except Exception as e:
                print(f"⚠️  ARIMA falhou: {e}")
                
            # Se nenhum modelo avançado funcionou, usa fallback
            if models_trained == 0:
                print("🚨 Todos os modelos avançados falharam!")
                print("🔄 Usando sistema de fallback de emergência...")
                
                fallback = SimpleFallbackPredictor(self.data)
                fallback_pred = fallback.predict_next_price()
                fallback_confidence = fallback.get_confidence()
                
                self.predictions['emergency_fallback'] = fallback_pred
                self.metrics['emergency_fallback'] = {
                    'test_mae': 0.0,
                    'test_rmse': 0.0,
                    'confidence': fallback_confidence,
                    'model_type': 'Emergency Technical Analysis'
                }
                
                print(f"✅ Fallback de emergência: ${fallback_pred:.2f} (confiança: {fallback_confidence:.1%})")
        
        # Cria ensemble
        print("\n🎯 Criando ensemble...")
        ensemble_pred = self.create_ensemble_prediction()
        
        # Prepara resultado
        result = {
            'predictions': self.predictions.copy(),
            'metrics': self.metrics.copy(),
            'ensemble_prediction': ensemble_pred,
            'current_price': float(self.features_df['Close'].iloc[-1]),
            'historical_data': self.get_historical_data_for_chart(),
            'feature_count': len(feature_names),
            'data_points': len(self.features_df),
            'timestamp': datetime.now().isoformat()
        }
        
        # Salva no cache
        self._save_to_cache(result)
        
        return result
    
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