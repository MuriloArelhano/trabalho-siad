# %%
import datetime
from sklearn import preprocessing
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import psycopg2 as psql
from psycopg2.extensions import register_adapter, AsIs
psql.extensions.register_adapter(np.int64, psql._psycopg.AsIs)

# %%
# Coletando os dados do database
try:
    conn = psql.connect(database="dwdengue",
                        user='root', password='root',
                        host='localhost', port='5434')
    cursor = conn.cursor()
    db_dataframe = pd.read_sql_query("""select data.mes, data.ano, cidade.nome as cidade, cidade.pop, cidade.estado, dengue.n_casos, clima.temp_max_celsius, clima.temp_med_celsius, clima.temp_min_celsius, clima.umidade_relativa, clima.precipitacao_mensal_mm
        from dengue
        inner join clima on clima.id = dengue.clima_id
        inner join data on data.id = dengue.data_id
        inner join cidade on cidade.id = dengue.cidade_id """, conn)
    db_dataframe['data'] = pd.to_datetime(
        dict(year=db_dataframe.ano, month=db_dataframe.mes, day='1'), format='%Y-%m-%d')
    cols = list(db_dataframe.columns)
    cols = [cols[-1]] + cols[:-1]
    db_dataframe = db_dataframe[cols]
    # db_dataframe.set_index('data', inplace=True)
except Exception as e:
    print(e)

# %%
db_dataframe.head(n=5)

# %%
# Normalização dos dados usados no gráfico
final_dataframe = db_dataframe.loc[db_dataframe['cidade'].isin([
                                                               'rio_de_janeiro'])]
min_max = preprocessing.StandardScaler()

n_casos_norm = final_dataframe['n_casos'].values.copy()
n_casos_norm.shape = (len(n_casos_norm), 1)
n_casos_norm = min_max.fit_transform(n_casos_norm)

temp_med_celsius_norm = final_dataframe['temp_med_celsius'].values.copy()
temp_med_celsius_norm.shape = (len(temp_med_celsius_norm), 1)
temp_med_celsius_norm = min_max.fit_transform(temp_med_celsius_norm)

precipitacao_mensal_mm_norm = final_dataframe['precipitacao_mensal_mm'].values.copy(
)
precipitacao_mensal_mm_norm.shape = (len(precipitacao_mensal_mm_norm), 1)
precipitacao_mensal_mm_norm = min_max.fit_transform(
    precipitacao_mensal_mm_norm)


# %%
# Colocando os grafico dentro de um dataframe final
final_dataframe['n_casos_norm'] = n_casos_norm

final_dataframe['precipitacao_mensal_mm_norm'] = precipitacao_mensal_mm_norm

final_dataframe['temp_med_celsius_norm'] = temp_med_celsius_norm

final_dataframe['pop_casos_percent'] = (
    db_dataframe['n_casos']*100)/db_dataframe['pop']
final_dataframe

# %%
db_dataframe_graph = db_dataframe.pivot(
    index='data', columns='cidade', values='n_casos')
db_dataframe_graph.plot(figsize=(15, 7), title='Numero de casos por cidade', xlim=[
    datetime.date(2012, 1, 1), datetime.date(2016, 2, 1)], style='-o')

# %%
final_dataframe_graph = final_dataframe.pivot(index='data', columns='cidade', values=[
    'n_casos_norm', 'precipitacao_mensal_mm_norm'])

final_dataframe_graph.plot(figsize=(15, 7), title="Relação crescimento dos casos com precipitação", xlim=[
    datetime.date(2012, 1, 1), datetime.date(2016, 2, 1)], style='-o')


# %%
final_dataframe_graph = final_dataframe.pivot(index='data', columns='cidade', values=[
    'n_casos_norm', 'temp_med_celsius_norm'])

final_dataframe_graph.plot(figsize=(15, 7), title="Relação crescimento dos casos temperatura média", xlim=[
                           datetime.date(2012, 1, 1), datetime.date(2016, 5, 1)], style='-o')

# %%

# %%
