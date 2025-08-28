import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
import matplotlib.pyplot as plt

dias = np.array([1, 2, 3, 4, 5, 6, 7])
valores = np.array([30, 25, 32, 28, 33, 27, 31])

df = pd.DataFrame({'Dia': dias, 'Valor': valores})

modelo = LinearRegression()
modelo.fit(df[['Dia']], df['Valor'])

dia_seguinte = np.array([[8]])
previsao = modelo.predict(dia_seguinte)

print(f'Previsão para o dia 8: {previsao[0]}')

dias = np.append(dias, 8)
valores = np.append(valores, previsao[0])

plt.scatter(dias, valores, color='blue')
plt.plot(dias,valores,color='red')
plt.xlabel('Dia')
plt.ylabel('Valor')
plt.title('Previsão de Valores')
plt.show()