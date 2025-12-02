
FROM apache/airflow:2.9.1
RUN pip install --no-cache-dir apache-airflow-providers-mysql

FROM apache/airflow:2.9.1

# Define o diretório de trabalho
WORKDIR /opt/airflow

# Copia o arquivo de dependências
COPY requirements.txt .

# Instala as dependências do requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

