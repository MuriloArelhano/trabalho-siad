# %%
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

cities = []


# %%
def normalize_date(data, column_date_name):
    data[column_date_name] = pd.to_datetime(data[column_date_name])
    data[column_date_name] = data[column_date_name].dt.strftime('%Y-%m')
    return data


# %%
df_dengue = pd.read_csv('./info_dengue.mat.csv', delimiter=',')
df_dengue = df_dengue.drop(columns=['casos_est', 'casos_est_min', 'casos_est_max', 'SE', 'Localidade_id', 'receptivo',	'transmissao',
                                    'nivel_inc', 'notif_accum_year', 'p_rt1', 'p_inc100k', 'nivel', 'id', 'versao_modelo', 'tweet', 'Rt', 'tempmin', 'umidmax'])

df_dengue = normalize_date(df_dengue, 'data_iniSE')
print(df_dengue)


# %%
df_dengue = df_dengue.groupby('data_iniSE', as_index=False).agg({
    'casos': sum, 'pop': 'mean'})
print(df_dengue)


# %%
df_clima = pd.read_csv('./dados_clima.csv', delimiter=';')
df_clima = df_clima.iloc[:, :-1]
df_clima = df_clima.dropna()

df_clima.columns = df_clima.columns.str.replace('[#,@,&]', '')
df_clima.columns = df_clima.columns.str.replace(' ', '_')
df_clima.columns = map(str.lower, df_clima.columns)

df_clima = normalize_date(df_clima, 'data_medicao')


print(df_clima)


# %%

df_final = pd.merge(df_clima, df_dengue, left_on='data_medicao',
                    right_on='data_iniSE', how='inner')
df_final = df_final.drop(columns=['data_iniSE'])
df_final = df_final.set_index('data_medicao')
print(df_final)


# %%
plt.plot(df_final['data_medicao'], df_final['casos'])

# %%
