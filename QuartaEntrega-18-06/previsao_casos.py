from keras.models import Sequential
from keras.layers import Dense, Dropout, LSTM
from sklearn.preprocessing import MinMaxScaler
from sklearn.model_selection import train_test_split
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from keras.callbacks import EarlyStopping, ReduceLROnPlateau, ModelCheckpoint

dataset = pd.read_csv("SegundaEntrega-04-06/dataframe_final.csv", sep=";", decimal=',')
dataset = dataset.sort_values(['cidade'])

dataset_treinamento = dataset[["casos",
                               "precipitacao_total_mensal(mm)",
                               "temperatura_maxima_media_mensal(°c)",
                               "temperatura_media_compensada_mensal(°c)",
                               "temperatura_minima_media_mensal(°c)",
                               "umidade_relativa_do_ar_media_mensal(%)", "pop"]]

normalizador = MinMaxScaler(feature_range=(0, 1))
dataset_treinamento_normalizada = normalizador.fit_transform(
    dataset_treinamento)

cols = list(dataset_treinamento)[0:7]
prev = dataset_treinamento_normalizada[:, 1:7]
target = dataset_treinamento_normalizada[:, 0]

X_train, X_test, y_train, y_test = train_test_split(prev, target, test_size=0.15, random_state=42, shuffle=True)

model = Sequential()
model.add(Dense(100, activation='relu', input_shape=(X_train.shape[1], )))
model.add(Dense(50, activation='relu'))
model.add(Dense(50, activation='relu'))
model.add(Dense(1))

model.compile(optimizer='adam', loss='mse', metrics=['mae','accuracy'])
model.fit(X_train, y_train, epochs=250, batch_size=32)

predict = model.predict(X_test)

plt.plot(y_test, color='red', label='Preço real')
plt.plot(predict, color='blue', label='Previsões')
plt.title('Previsão casos')
plt.xlabel('Tempo')
plt.ylabel('Numero de casos')
plt.legend()
plt.show()
