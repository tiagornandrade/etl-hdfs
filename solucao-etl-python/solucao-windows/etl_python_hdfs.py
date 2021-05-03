import pandas as pd
import pyodbc
import pyarrow as pa
import pyarrow.parquet as pq
import os
import subprocess
import shutil
import glob
from datetime import date, datetime
from time import time
from subprocess import Popen, PIPE

print('Importação das bibliotecas', '                 -------- OK')

# Variáveis de data e hora
ref_data = date.today()
ref_year = date.today().year
ref_time = datetime.now().strftime("%H%M%S")

print('Definição das variáveis de tempo', '                 -------- OK')

# Parametros para a criação da string de conexão ao banco sql
server = 'localhost'
database = 'db_datasus'
username = 'sa'
password = 'Ribeiro83'

# String de conexão ao banco sql
conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+password)

query = """SELECT [date] 
                ,[state] 
                ,SUM([confirmed]) AS [confirmed] 
                ,SUM([deaths]) AS [deaths] 
                ,[estimated_population_2019] 
                ,[estimated_population] 
                ,[city_ibge_code] 
                ,[confirmed_per_100k_inhabitants] 
                ,[death_rate] 
            FROM [db_datasus].[dbo].[COVID] 
            WHERE city_ibge_code < 100 
            GROUP BY  
                  [date] 
                  ,[state] 
                  ,[estimated_population_2019] 
                  ,[estimated_population] 
                  ,[city_ibge_code] 
                  ,[confirmed_per_100k_inhabitants] 
                  ,[death_rate] """

sql_query = pd.read_sql_query(query, conn)

print('Consulta sql no banco', '                 -------- OK')

# Cria o dataset pandas a partir da consulta sql
df = pd.DataFrame(sql_query)

print('Criação do dataset pandas', '                 -------- OK')

# Realiza a conversa do dataset em pandas para uma table
table = pa.Table.from_pandas(df)

print('Conversão do dataset pandas em table parquet', '          -------- OK')

# Declara as variáveis de dia e hora corrente para montar o nome do arquivo
date = datetime.now().strftime('%Y%m%d')
time = str(ref_time)

# Pasta e nome do arquivo no diretório temporário
filename = 'C:/Temp/stage/etl_hdfs'+'_'+date +'_'+ time

# Convert a table para o arquivo .parquet
pq.write_table(table, filename+'.parquet')

print('Conversão da table em arquivo parquet', '                 -------- OK')

# Variáveis para criação do Caminho de Destino no HDFS
dir_ano = str(ref_year)
dir_mes = datetime.now().strftime('%m')
dir_dia = datetime.now().strftime('%d')

# Criação da pasta raw no hdfs
raw = 'hadoop fs -mkdir /user/datalake/raw'
os.system(raw)
print('Criação da pasta: ', '/user/datalake/raw', '                 -------- OK')

# Criação da pasta ano no hdfs
ano = 'hadoop fs -mkdir /user/datalake/raw/'+str(dir_ano)
os.system(ano)
print('Criação da pasta: ', '/user/datalake/raw/'+str(dir_ano), '            -------- OK')

# Criação da pasta ano no hdfs
mes = 'hadoop fs -mkdir /user/datalake/raw/'+str(dir_ano)+'/'+str(dir_mes)
os.system(mes)
print('Criação da pasta: ', '/user/datalake/raw/'+str(dir_ano)+'/'+str(dir_mes), '         -------- OK')

# Criação da pasta ano no hdfs
dia = 'hadoop fs -mkdir /user/datalake/raw/'+str(dir_ano)+'/'+str(dir_mes)+'/'+str(dir_dia)
os.system(dia)
print('Criação da pasta: ', '/user/datalake/raw/'+str(dir_ano)+'/'+str(dir_mes)+'/'+str(dir_dia), '      -------- OK')

# Variáveis para criação do Caminho de Destino no HDFS
dir_ano = str(ref_year)
dir_mes = datetime.now().strftime('%m')
dir_dia = datetime.now().strftime('%d')

# Caminho de origem do arquivo .parquet
source_dir = 'C:\Temp\stage'
sources = glob.glob(os.path.join(source_dir,"*.parquet"))

# Criação do comando shell para realização do put
cmd = 'hadoop'+' fs'+' -put '+sources[0]+' /user/datalake/raw/'+str(dir_ano)+'/'+str(dir_mes)+'/'+str(dir_dia)

# Comando put no hdfs
os.system(cmd)

print('Realização do PUT no HDFS', '                                  -------- OK')

# Rotina para limpar a pasta Temp após o input no HDFS
source_dir = 'C:\Temp\stage'
sources = glob.glob(os.path.join(source_dir,"*.parquet"))

for f in sources:
    os.remove(f)

print('Limpeza da pasta temporária', '                       -------- OK')