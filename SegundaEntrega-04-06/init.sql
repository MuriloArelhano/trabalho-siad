CREATE TABLE IF NOT EXISTS data (
   id serial PRIMARY KEY,
   mes SMALLINT NOT NULL,
   ano SMALLINT NOT NULL
);

CREATE TABLE IF NOT EXISTS cidade (
   id serial PRIMARY KEY,
   nome VARCHAR (100) NOT NULL,
   estado VARCHAR (2) NOT NULL
);

CREATE TABLE IF NOT EXISTS clima (
   id serial PRIMARY KEY,
   temp_med_celsius float(8) NOT NULL,
   temp_min_celsius float(8) NOT NULL,
   temp_max_celsius float(8) NOT NULL,
   precipitacao_mensal_mm INT NOT NULL,
   data_id INT NOT NULL,
   cidade_id INT NOT NULL,
   FOREIGN KEY (data_id) REFERENCES data (id),
   FOREIGN KEY (cidade_id) REFERENCES cidade (id)
);

CREATE TABLE IF NOT EXISTS dengue (
   id serial PRIMARY KEY,
   n_casos INT NOT NULL,
   data_id INT NOT NULL,
   cidade_id INT NOT NULL,
   FOREIGN KEY (data_id) REFERENCES data (id),
   FOREIGN KEY (cidade_id) REFERENCES cidade (id)
);