#!/usr/bin/env python3
"""
Teste rápido do sistema de previsão com fallback.
"""
import os
import sys

# Simula ambiente de produção para testar
os.environ['CPU_ONLY_MODE'] = 'true'
os.environ['USE_TENSORFLOW'] = 'false'  # Força fallback
os.environ['USE_ADVANCED_MODELS'] = 'true'

# Adiciona o diretório atual ao path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def test_prediction_system():
    print("🧪 Testando sistema de previsão com fallback...")
    
    try:
        from advanced_prediction import predict_asset_price, SimpleFallbackPredictor
        import pandas as pd
        import numpy as np
        
        print("✅ Importações bem-sucedidas")
        
        # Teste 1: Sistema completo
        print("\n📊 Teste 1: Sistema completo com PETR4.SA")
        try:
            result = predict_asset_price('PETR4.SA', days_history=30)
            print(f"✅ Previsão: ${result['predictions']['ensemble']:.2f}")
            print(f"   Confiança: {result['metrics']['ensemble']['confidence']:.1%}")
            print(f"   Modelos ativos: {len(result['predictions'])}")
        except Exception as e:
            print(f"❌ Erro no sistema completo: {e}")
        
        # Teste 2: SimpleFallbackPredictor direto
        print("\n🔧 Teste 2: SimpleFallbackPredictor direto")
        try:
            # Cria dados de teste
            dates = pd.date_range('2024-01-01', periods=50, freq='D')
            prices = 100 + np.cumsum(np.random.normal(0, 0.5, 50))
            data = pd.DataFrame({
                'Close': prices,
                'Date': dates
            })
            
            fallback = SimpleFallbackPredictor(data)
            pred = fallback.predict_next_price()
            conf = fallback.get_confidence()
            
            print(f"✅ Fallback: ${pred:.2f} (confiança: {conf:.1%})")
        except Exception as e:
            print(f"❌ Erro no fallback: {e}")
        
        # Teste 3: Configurações extremas
        print("\n⚠️  Teste 3: Modo de emergência")
        os.environ['USE_ADVANCED_MODELS'] = 'false'
        os.environ['USE_TENSORFLOW'] = 'false'
        
        try:
            result = predict_asset_price('PETR4.SA', days_history=20)
            print(f"✅ Modo emergência: ${result['predictions']['ensemble']:.2f}")
        except Exception as e:
            print(f"❌ Erro no modo emergência: {e}")
        
        print("\n🎉 Testes concluídos!")
        
    except ImportError as e:
        print(f"❌ Erro de importação: {e}")
        print("   Isso é esperado se algumas dependências não estiverem instaladas")
    except Exception as e:
        print(f"❌ Erro geral: {e}")

if __name__ == "__main__":
    test_prediction_system()