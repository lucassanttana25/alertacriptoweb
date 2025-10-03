"""
Arquivo de testes para o sistema avançado de previsão.
Execute este arquivo para testar as funcionalidades implementadas.
"""

import sys
import os
import asyncio
from datetime import datetime

# Adiciona o diretório atual ao path para importar módulos locais
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def test_basic_imports():
    """Testa se as dependências básicas estão instaladas."""
    print("🔍 Testando imports básicos...")
    
    try:
        import pandas as pd
        import numpy as np
        import yfinance as yf
        from sklearn.ensemble import RandomForestRegressor
        print("✅ Pandas, NumPy, yfinance, scikit-learn: OK")
    except ImportError as e:
        print(f"❌ Erro nos imports básicos: {e}")
        return False
    
    try:
        import tensorflow as tf
        print(f"✅ TensorFlow {tf.__version__}: OK")
    except ImportError as e:
        print(f"⚠️  TensorFlow não disponível: {e}")
        print("   Instale com: pip install tensorflow")
    
    try:
        import statsmodels.api as sm
        print("✅ Statsmodels: OK")
    except ImportError as e:
        print(f"⚠️  Statsmodels não disponível: {e}")
        print("   Instale com: pip install statsmodels")
    
    try:
        import talib
        print("✅ TA-Lib: OK")
    except ImportError as e:
        print(f"⚠️  TA-Lib não disponível: {e}")
        print("   Instale com: pip install TA-Lib")
        print("   Ou conda install -c conda-forge ta-lib")
    
    return True

def test_data_fetching():
    """Testa a coleta de dados do Yahoo Finance."""
    print("\n📡 Testando coleta de dados...")
    
    try:
        import yfinance as yf
        ticker = "PETR4.SA"
        asset = yf.Ticker(ticker)
        data = asset.history(period="1mo")
        
        if not data.empty:
            print(f"✅ Dados coletados para {ticker}: {len(data)} registros")
            print(f"   Último preço: R$ {data['Close'].iloc[-1]:.2f}")
            return True
        else:
            print(f"❌ Nenhum dado encontrado para {ticker}")
            return False
            
    except Exception as e:
        print(f"❌ Erro na coleta de dados: {e}")
        return False

def test_advanced_prediction():
    """Testa o sistema avançado de previsão."""
    print("\n🤖 Testando sistema avançado de previsão...")
    
    try:
        from advanced_prediction import predict_asset_price
        
        ticker = "PETR4.SA"
        print(f"Executando previsão para {ticker}...")
        
        result = predict_asset_price(ticker, days_history=100)
        
        if result and result.get('success', False):
            print("✅ Previsão avançada executada com sucesso!")
            print(f"   Preço atual: R$ {result['current_price']:.2f}")
            print(f"   Previsão ensemble: R$ {result['prediction']:.2f}")
            print(f"   Modelos utilizados: {list(result['all_predictions'].keys())}")
            print(f"   Confiança: {result['summary']['ensemble_confidence']:.2%}")
            
            # Testa diferentes previsões
            for model, pred in result['all_predictions'].items():
                variation = ((pred / result['current_price']) - 1) * 100
                print(f"   • {model}: R$ {pred:.2f} ({variation:+.1f}%)")
                
            return True
        else:
            print(f"❌ Erro na previsão: {result.get('error', 'Erro desconhecido')}")
            return False
            
    except ImportError as e:
        print(f"❌ Sistema avançado não disponível: {e}")
        return False
    except Exception as e:
        print(f"❌ Erro na previsão avançada: {e}")
        return False

def test_api_compatibility():
    """Testa a compatibilidade com a API existente."""
    print("\n🔗 Testando compatibilidade com API...")
    
    try:
        # Simula importação do router
        import sys
        sys.path.append('routers')
        
        from routers.analysis import get_asset_analysis
        print("✅ Router de análise importado com sucesso")
        
        # Testa estrutura de resposta
        from routers.analysis import AnalysisData
        
        # Cria dados de teste
        test_data = AnalysisData(
            historical_data=[{"date": "2024-01-01", "price": 30.0}],
            prediction=31.5,
            confidence=0.75,
            model_used="ensemble_advanced",
            all_predictions={"rf": 31.2, "lstm": 31.8},
            current_price=30.5
        )
        
        print("✅ Estrutura de dados da API compatível")
        print(f"   Modelo: {test_data.model_used}")
        print(f"   Confiança: {test_data.confidence:.2%}")
        
        return True
        
    except Exception as e:
        print(f"❌ Erro na compatibilidade da API: {e}")
        return False

def test_performance_comparison():
    """Compara a performance dos diferentes modelos."""
    print("\n📊 Testando performance dos modelos...")
    
    try:
        from advanced_prediction import AdvancedPricePredictor
        
        ticker = "VALE3.SA"
        predictor = AdvancedPricePredictor(ticker, days_history=100)
        
        if predictor.fetch_data():
            predictor.calculate_technical_indicators()
            X, y, features = predictor.prepare_features()
            
            print(f"✅ Dados preparados para {ticker}")
            print(f"   Features: {len(features)} indicadores")
            print(f"   Amostras: {len(X)} registros")
            print(f"   Período: {predictor.features_df.index[0].strftime('%Y-%m-%d')} a {predictor.features_df.index[-1].strftime('%Y-%m-%d')}")
            
            # Lista algumas features importantes
            important_features = ['RSI', 'MACD', 'BB_position', 'SMA_20', 'Volume_ratio']
            available_important = [f for f in important_features if f in features]
            print(f"   Indicadores técnicos: {', '.join(available_important)}")
            
            return True
        else:
            print(f"❌ Falha ao buscar dados para {ticker}")
            return False
            
    except Exception as e:
        print(f"❌ Erro no teste de performance: {e}")
        return False

def run_comprehensive_test():
    """Executa todos os testes de forma abrangente."""
    print("🚀 INICIANDO TESTES DO SISTEMA AVANÇADO DE PREVISÃO")
    print("=" * 60)
    
    tests = [
        ("Imports Básicos", test_basic_imports),
        ("Coleta de Dados", test_data_fetching),
        ("Previsão Avançada", test_advanced_prediction),
        ("Compatibilidade API", test_api_compatibility),
        ("Performance dos Modelos", test_performance_comparison)
    ]
    
    results = []
    
    for test_name, test_func in tests:
        print(f"\n{'='*20} {test_name} {'='*20}")
        try:
            success = test_func()
            results.append((test_name, success))
        except Exception as e:
            print(f"❌ Erro inesperado em {test_name}: {e}")
            results.append((test_name, False))
    
    # Relatório final
    print(f"\n{'='*60}")
    print("📋 RELATÓRIO FINAL DOS TESTES")
    print("=" * 60)
    
    passed = sum(1 for _, success in results if success)
    total = len(results)
    
    for test_name, success in results:
        status = "✅ PASSOU" if success else "❌ FALHOU"
        print(f"{test_name}: {status}")
    
    print(f"\n🎯 Resultado: {passed}/{total} testes passaram ({passed/total*100:.1f}%)")
    
    if passed == total:
        print("🎉 TODOS OS TESTES PASSARAM! Sistema pronto para uso.")
    elif passed >= total * 0.7:
        print("⚠️  MAIORIA DOS TESTES PASSARAM. Sistema funcional com limitações.")
    else:
        print("❌ VÁRIOS TESTES FALHARAM. Verifique as dependências.")
    
    print("\n💡 Dicas para melhorar os resultados:")
    print("• Instale todas as dependências: pip install -r requirements.txt")
    print("• Para TA-Lib: conda install -c conda-forge ta-lib")
    print("• Para TensorFlow: pip install tensorflow")
    print("• Verifique conexão com internet para yfinance")
    
    return passed, total

if __name__ == "__main__":
    passed, total = run_comprehensive_test()
    exit(0 if passed == total else 1)