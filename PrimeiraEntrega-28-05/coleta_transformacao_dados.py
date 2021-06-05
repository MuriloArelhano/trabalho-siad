# %%
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np


class City:
    def __init__(self, name, state):
        self.name = name
        self.state = state


cities = [
    City('fortaleza', 'CE'),
    City('rio_de_janeiro', 'RJ'),
    City('curitiba', 'PR'),
    City('maringa', 'PR'),
    City('belo_horizonte', 'MG'),
    City('divinopolis', 'MG'),
    City('vitoria', 'ES'),
    City('barbacena', 'MG')
]

# %%


def normalize_date(data, column_date_name):
    data[column_date_name] = pd.to_datetime(data[column_date_name])
    data[column_date_name] = data[column_date_name].dt.strftime('%Y-%m')
    return data
# %%


def transformDengueData(citiesNameArray):
    df_dengue_list = []

    for city in citiesNameArray:
        df_dengue = pd.read_csv(
            f'./datasets_dengue/{city.name}_dengue.csv', delimiter=',')
        df_dengue['cidade'] = city.name
        df_dengue['estado'] = city.state
        df_dengue_list.append(df_dengue)

    df_dengue_full = pd.concat(df_dengue_list, axis=0, ignore_index=True)
    df_dengue_full = df_dengue_full.drop(columns=['casos_est', 'casos_est_min', 'casos_est_max', 'SE', 'Localidade_id', 'receptivo',	'transmissao',
                                                  'nivel_inc', 'notif_accum_year', 'p_rt1', 'p_inc100k', 'nivel', 'id', 'versao_modelo', 'tweet', 'Rt', 'tempmin', 'umidmax'])
    df_dengue_full = normalize_date(df_dengue_full, 'data_iniSE')
    df_dengue_full = df_dengue_full.groupby(['cidade', 'data_iniSE'], as_index=False).agg({
        'casos': sum, 'pop': min})

    return df_dengue_full


df_dengue = transformDengueData(cities)
print(df_dengue.shape)
df_dengue.head()
# %%


def transformClimaData(citiesNameArray):
    df_clima_list = []
    for city in citiesNameArray:
        df_clima = pd.read_csv(
            f'./datasets_clima/{city.name}_clima.csv', delimiter=';')
        df_clima = df_clima.iloc[:, :-1]
        df_clima['cidade'] = city.name
        df_clima['estado'] = city.state
        df_clima_list.append(df_clima)

    df_clima_full = pd.concat(df_clima_list, axis=0, ignore_index=True)
    df_clima_full = df_clima_full.dropna()

    df_clima_full.columns = df_clima_full.columns.str.replace('[#,@,&]', '')
    df_clima_full.columns = df_clima_full.columns.str.replace(' ', '_')
    df_clima_full.columns = map(str.lower, df_clima_full.columns)

    df_clima_full = normalize_date(df_clima_full, 'data_medicao')
    return df_clima_full


df_clima = transformClimaData(cities)
print(df_clima.shape)
df_clima.head()
# %%
df_final = pd.merge(df_clima, df_dengue, left_on=['data_medicao', 'cidade'],
                    right_on=['data_iniSE', 'cidade'], how='inner', suffixes=('_y', '_x'))
df_final = df_final.drop(columns=['data_iniSE'])
df_final['data_medicao'] = pd.to_datetime(df_final['data_medicao'])
df_final["year"] = df_final["data_medicao"].dt.year
df_final["month"] = df_final["data_medicao"].dt.month


df_final = df_final.sort_values(by=['data_medicao'])


df_final.to_csv("../SegundaEntrega-04-06/dataframe_final.csv",
                index=False, sep=";")

# %%
df_final_graph = df_final.pivot(
    index='data_medicao', columns='cidade', values='casos')
df_final_graph.plot(figsize=(15, 7))

