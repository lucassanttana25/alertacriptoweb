"""
Sistema de previsão melhorado para ativos financeiros.
Este arquivo demonstra o uso do novo sistema avançado de previsão.
"""

import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
import matplotlib.pyplot as plt

# Importa o novo sistema avançado
try:
    from advanced_prediction import predict_asset_price
    print("✅ Sistema avançado de previsão carregado!")
    
    # Demonstração com dados reais
    print("\n🔄 Testando previsão avançada com PETR4...")
    result = predict_asset_price("PETR4.SA")
    
    if result and result['success']:
        print(f"\n📊 Resultados para PETR4:")
        print(f"💰 Preço atual: R$ {result['current_price']:.2f}")
        print(f"🎯 Previsão ensemble: R$ {result['prediction']:.2f}")
        print(f"📈 Variação estimada: {((result['prediction'] / result['current_price']) - 1) * 100:.2f}%")
        print(f"🤖 Modelos utilizados: {list(result['all_predictions'].keys())}")
        print(f"🎓 Confiança: {result['summary']['ensemble_confidence']:.2%}")
        
        print("\n📋 Previsões individuais:")
        for model, pred in result['all_predictions'].items():
            print(f"  • {model}: R$ {pred:.2f}")
            
    else:
        print("❌ Erro na previsão avançada")
        
except ImportError:
    print("⚠️  Sistema avançado não disponível. Executando exemplo básico...")
    
    # Exemplo básico original (mantido para compatibilidade)
    dias = np.array([1, 2, 3, 4, 5, 6, 7])
    valores = np.array([30, 25, 32, 28, 33, 27, 31])

    df = pd.DataFrame({'Dia': dias, 'Valor': valores})

    modelo = LinearRegression()
    modelo.fit(df[['Dia']], df['Valor'])

    dia_seguinte = np.array([[8]])
    previsao = modelo.predict(dia_seguinte)

    print(f'Previsão básica para o dia 8: {previsao[0]:.2f}')

    dias = np.append(dias, 8)
    valores = np.append(valores, previsao[0])

    plt.figure(figsize=(10, 6))
    plt.scatter(dias[:-1], valores[:-1], color='blue', label='Dados históricos')
    plt.scatter(dias[-1], valores[-1], color='red', label='Previsão', s=100)
    plt.plot(dias, valores, color='gray', alpha=0.7, linestyle='--')
    plt.xlabel('Dia')
    plt.ylabel('Valor')
    plt.title('Previsão de Valores - Método Básico')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.show()

print("\n" + "="*60)
print("🚀 SISTEMA DE PREVISÃO MELHORADO IMPLEMENTADO!")
print("="*60)
print("✨ Principais melhorias:")
print("• 🧠 LSTM (Redes Neurais) para padrões complexos")
print("• 🌲 Random Forest para robustez")
print("• 📈 ARIMA para análise de séries temporais")
print("• 🎯 Ensemble que combina todos os modelos")
print("• 📊 Indicadores técnicos (RSI, MACD, Bollinger Bands)")
print("• 🎓 Métricas de confiança e validação")
print("• 📉 Análise de volume e volatilidade")
print("\n💡 Use a API /analysis/{ticker} para previsões avançadas!")
print("="*60)