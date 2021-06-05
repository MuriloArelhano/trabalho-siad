# %%
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import psycopg2 as psql
from psycopg2.extensions import register_adapter, AsIs
psql.extensions.register_adapter(np.int64, psql._psycopg.AsIs)


# %%
df_final = pd.read_csv("./dataframe_final.csv", sep=";", decimal=',')
print(df_final.shape)
df_final.head()


# %%


df_final_datas_no_duplicates = df_final['data_medicao'].drop_duplicates()
df_final_datas_no_duplicates = df_final_datas_no_duplicates.str.split(
    "-", expand=True)

df_final_cidades_estados = df_final[['cidade', 'estado']].copy()
df_final_cidades_estados_no_duplicates = df_final_cidades_estados.drop_duplicates()

# %%
# Criando estrutura do banco de dados
try:
    conn = psql.connect(database="teste",
                        user='postgres', password='teste',
                        host='localhost', port='5432')
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS data(
        id serial PRIMARY KEY,
        mes INT NOT NULL,
        ano INT NOT NULL
    );""")
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS cidade (
        id serial PRIMARY KEY,
        nome VARCHAR NOT NULL,
        estado VARCHAR NOT NULL);
    """)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS clima (
        id serial PRIMARY KEY,
        temp_med_celsius double precision NOT NULL,
        temp_min_celsius double precision NOT NULL,
        temp_max_celsius double precision NOT NULL,
        precipitacao_mensal_mm double precision NOT NULL,
        umidade_relativa double precision NOT NULL,
        data_id INT NOT NULL,
        cidade_id INT NOT NULL,
        FOREIGN KEY (data_id) REFERENCES data (id),
        FOREIGN KEY (cidade_id) REFERENCES cidade (id)
        );
    """)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS dengue (
        id serial PRIMARY KEY,
        n_casos double precision NOT NULL,
        data_id INT NOT NULL,
        cidade_id INT NOT NULL,
        clima_id INT NOT NULL,
        FOREIGN KEY (data_id) REFERENCES data (id),
        FOREIGN KEY (cidade_id) REFERENCES cidade (id),
        FOREIGN KEY (clima_id) REFERENCES clima (id)
        );
   """)
    conn.commit()

except Exception as e:
    print(e)


# %%
try:
    for ind in df_final_datas_no_duplicates.index:
        ano = df_final_datas_no_duplicates[0][ind]
        mes = df_final_datas_no_duplicates[1][ind]
        cursor.execute(
            "INSERT INTO data (mes, ano) VALUES (%s, %s) RETURNING id", (mes, ano))
        data_id = cursor.fetchone()[0]
        conn.commit()

    for ind in df_final_cidades_estados_no_duplicates.index:
        cidade = df_final_cidades_estados_no_duplicates['cidade'][ind]
        estado = df_final_cidades_estados_no_duplicates['estado'][ind]
        cursor.execute(
            "INSERT INTO cidade (nome, estado) VALUES (%s, %s) RETURNING id", (cidade, estado))
        cidade_id = cursor.fetchone()[0]
        conn.commit()

    for ind in df_final.index:
        cursor.execute(
            "SELECT * FROM data WHERE mes = %s and ano = %s", (df_final['month'][ind], df_final['year'][ind]))
        current_date = cursor.fetchone()
        print("Data: ", current_date)

        cursor.execute(
            "SELECT * FROM cidade WHERE nome = %s and estado = %s", (df_final['cidade'][ind], df_final['estado'][ind]))
        current_city = cursor.fetchone()
        print("Cidade: ", current_city)

        cursor.execute(
            "INSERT INTO clima (temp_med_celsius, temp_min_celsius, temp_max_celsius, precipitacao_mensal_mm, umidade_relativa, data_id, cidade_id) VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id", (df_final['temperatura_media_compensada_mensal(°c)'][ind], df_final['temperatura_minima_media_mensal(°c)'][ind], df_final['temperatura_maxima_media_mensal(°c)'][ind], df_final['precipitacao_total_mensal(mm)'][ind], df_final['umidade_relativa_do_ar_media_mensal(%)'][ind], current_date[0], current_city[0]))

        current_clima = cursor.fetchone()
        print("Clima Id: ", current_clima)

        cursor.execute(
            "INSERT INTO dengue (n_casos, data_id, cidade_id, clima_id) VALUES (%s, %s, %s, %s) RETURNING id", (df_final['casos'][ind], current_date[0], current_city[0], current_clima[0]))

        conn.commit()

    cursor.close()
    conn.close()
except Exception as e:
    print(e)

