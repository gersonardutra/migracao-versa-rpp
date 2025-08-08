#Funções para conexão das bases de origem e destino
#Autor: Gerson Dutra
#Data: 21/12/2024
import mysql.connector
import psycopg2
 #======================================================Configurações de conexão MySQL=======================================================
def conectar_ao_mysql(alerta = False):
   
    config_mysql = {
        'user': 'gerson',
        'password': 'gerson@111_*',
        'host': '192.168.254.39',
        'database': 'homologacao_luisburgo'
    }
    if alerta:
        print(f"""Banco destino - {config_mysql['database']} em {config_mysql['host']}""")

    #Conecta ao banco de dados MySQL
    conexao_mysql = mysql.connector.connect(**config_mysql)
    return conexao_mysql
 #===========================================================================================================================================

 #====================================================Configurações de conexão Postgres======================================================
def conectar_ao_postgres(alerta = False):
    # Configurações de conexão PostgreSQL
    config_postgres = {
        'user': 'postgres',
        'password': '1234',
        'host': 'localhost',
        'database': 'LUISBURGO'
    }
    if alerta:
        print(f"""Banco origem - {config_postgres['database']} em {config_postgres['host']}""")
    

    # Conecta ao banco de dados PostgreSQL
    conexao_postgres = psycopg2.connect(**config_postgres)
    return conexao_postgres
#===========================================================================================================================================