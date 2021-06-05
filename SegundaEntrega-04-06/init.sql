CREATE TABLE IF NOT EXISTS data (
   id serial PRIMARY KEY,
   mes INT NOT NULL,
   ano INT NOT NULL
);

CREATE TABLE IF NOT EXISTS cidade (
   id serial PRIMARY KEY,
   nome VARCHAR NOT NULL,
   estado VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS clima (
   id serial PRIMARY KEY,
   temp_med_celsius double NOT NULL,
   temp_min_celsius double NOT NULL,
   temp_max_celsius double NOT NULL,
   precipitacao_mensal_mm double NOT NULL,
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
   clima_id INT NOT NULL, 
   FOREIGN KEY (data_id) REFERENCES data (id),
   FOREIGN KEY (cidade_id) REFERENCES cidade (id),
   FOREIGN KEY (clima_id) REFERENCES clima (id)
);