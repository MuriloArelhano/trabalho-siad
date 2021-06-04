# %%
import pandas as pd
import psycopg2 as psql
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
df_final = df_final.sort_values(by=['data_medicao'])

# %%
df_final_graph = df_final.pivot(
    index='data_medicao', columns='cidade', values='casos')
df_final_graph.plot(figsize=(15, 7))

# %%
df_final_datas_no_duplicates = df_final['data_medicao'].drop_duplicates()
df_final_datas_splitted = df_final_datas_no_duplicates.str.split("-", expand=True)
df_final_cidades_estados = df_final[['cidade', 'estado']].copy()
df_final_cidades_estados_no_duplicates = df_final_cidades_estados.drop_duplicates()

#%%
try:
    conn = psql.connect(database="postgres", user='postgres', password='example', host='localhost', port= '5432')
    cursor = conn.cursor()

    data_dict = {}
    for ind in df_final_datas_splitted.index:
        ano = df_final_datas_splitted[0][ind]
        mes = df_final_datas_splitted[1][ind]
        key = ano + "-" + mes
        cursor.execute("INSERT INTO data (mes, ano) VALUES (%s, %s) RETURNING id", (mes, ano))
        data_id = cursor.fetchone()[0]
        conn.commit()
        data_dict[key] = data_id
    print(data_dict)

    cidade_dict = {}
    for ind in df_final_cidades_estados_no_duplicates.index:
        cidade = df_final_cidades_estados_no_duplicates['cidade'][ind]
        estado = df_final_cidades_estados_no_duplicates['estado'][ind]
        key = cidade + "-" + estado
        cursor.execute("INSERT INTO cidade (nome, estado) VALUES (%s, %s) RETURNING id", (cidade, estado))
        cidade_id = cursor.fetchone()[0]
        conn.commit()
        cidade_dict[key] = cidade_id
    print(cidade_dict)  

    cursor.close()
    conn.close()
except Exception as e:
    print(e)
