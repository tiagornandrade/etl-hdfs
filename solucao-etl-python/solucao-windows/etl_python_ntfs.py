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

print(datetime.now(), ' | [Inicio da carga]', '                                          --      OK')
print(datetime.now(), ' | [Importação das bibliotecas]', '                               --      OK')

# Variáveis de data e hora
ref_data = date.today()
ref_year = date.today().year
ref_time = datetime.now().strftime("%H%M%S")

print(datetime.now(), ' | [Definição das variáveis de tempo]', '                         --      OK')

# Parametros para a criação da string de conexão ao banco sql
server = 'localhost'
database = 'db_datasus'
username = 'dev'
password = 'SenhaDev1234'

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
            WHERE city_ibge_code < 100 AND CONVERT(DATE, [date]) = CONVERT(DATE,GETDATE()-1)  
            GROUP BY  
                  [date] 
                  ,[state] 
                  ,[estimated_population_2019] 
                  ,[estimated_population] 
                  ,[city_ibge_code] 
                  ,[confirmed_per_100k_inhabitants] 
                  ,[death_rate] """

sql_query = pd.read_sql_query(query, conn)

print(datetime.now(), ' | [Consulta sql no banco]', '                                    --      OK')

# Cria o dataset pandas a partir da consulta sql
df = pd.DataFrame(sql_query)

print(datetime.now(), ' | [Criação do dataset pandas]', '                                --      OK')

# Realiza a conversa do dataset em pandas para uma table
table = pa.Table.from_pandas(df)

print(datetime.now(), ' | [Conversão do dataset pandas em table]', '                     --      OK')

# Declara as variáveis de dia e hora corrente para montar o nome do arquivo
date = datetime.now().strftime('%Y%m%d')
time = str(ref_time)

# Pasta e nome do arquivo no diretório temporário
filename = 'C:/Temp/stage/etl_hdfs'+'_'+date +'_'+ time

# Convert a table para o arquivo .parquet
pq.write_table(table, filename+'.snappy.parquet', compression='snappy')

print(datetime.now(), ' | [Conversão do table em arquivo parquet]', '                    --      OK')


### Criação da Pasta RAW ###
# Diretorio do ano corrente que será criado
dir_raw = 'raw'
  
# Caminho do diretório pai
dir_pai = "C:/datalake/"
  
# Caminho a ser criado
path = os.path.join(dir_pai, dir_raw)
  
# Criar o diretorio 'RAW' em 'C:/datalake/'
if not os.path.exists(path):
    os.makedirs(path)
print(datetime.now(), ' | [Criação da pasta no NTFS]: ', 'C:/datalake/'+dir_raw, '               --      OK')


### Criação da Pasta ANO ###
# Diretorio do ano corrente que será criado
dir_ano = str(ref_year)
  
# Caminho do diretório pai
dir_pai = "C:/datalake/"+dir_raw
  
# Caminho a ser criado
path = os.path.join(dir_pai, dir_ano)
  
# Criar o diretorio 'ANO' em 'C:/datalake/'
if not os.path.exists(path):
    os.makedirs(path)
print(datetime.now(), ' | [Criação da pasta no NTFS]: ', 'C:/datalake/'+dir_raw+'/'+str(dir_ano), '          --      OK')


### Criação da Pasta MÊS ###
# Diretorio do mês corrente que será criado
dir_mes = datetime.now().strftime('%m')
  
# Caminho do diretório pai
dir_pai = "C:/datalake/"+dir_raw+'/'+dir_ano
  
# Caminho a ser criado
path = os.path.join(dir_pai, dir_mes)
  
# Criar o diretorio 'MÊS' em 'C:/datalake/'
if not os.path.exists(path):
    os.makedirs(path)
print(datetime.now(), ' | [Criação da pasta no NTFS]: ', 'C:/datalake/'+dir_raw+'/'+str(dir_ano)+'/'+str(dir_mes), '       --      OK')


### Criação da Pasta DIA ###
# Diretorio do dia corrente que será criado
dir_dia = datetime.now().strftime('%d')
  
# Caminho do diretório pai
dir_pai = "C:/datalake/"+dir_raw+'/'+dir_ano+'/'+dir_mes

# Caminho a ser criado
path = os.path.join(dir_pai, dir_dia)
  
# Criar o diretorio 'DIA' em 'C:/datalake/'
if not os.path.exists(path):
    os.makedirs(path)
print(datetime.now(), ' | [Criação da pasta no NTFS]: ', 'C:/datalake/'+dir_raw+'/'+str(dir_ano)+'/'+str(dir_mes)+'/'+str(dir_dia), '    --      OK')


# Variáveis para criação do Caminho de Destino no HDFS
dir_ano = str(ref_year)
dir_mes = datetime.now().strftime('%m')
dir_dia = datetime.now().strftime('%d')

# Caminho de origem do arquivo .parquet
source_dir = 'C:/Temp/stage'
sources = glob.glob(os.path.join(source_dir,"*.parquet"))

# Caminho de destino do arquivo .parquet
destination = 'C:/datalake/'+dir_raw+'/'+str(dir_ano)+'/'+str(dir_mes)+'/'+str(dir_dia)
# Move o arquivo .parquet do caminho de origem para o de destino
for source in sources:
    if os.path.isfile(source):
        shutil.move(source,destination)

print(datetime.now(), ' | [Realização do PUT no NTFS]', '                                --      OK')

# Rotina para limpar a pasta Temp após o input no HDFS
source_dir = 'C:\Temp\stage'
sources = glob.glob(os.path.join(source_dir,"*.parquet"))

for f in sources:
    os.remove(f)

print(datetime.now(), ' | [Limpeza da pasta temporária]', '                              --      OK')


print(datetime.now(), ' | [Fim da carga]', '                                             --      OK')