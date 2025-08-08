#Funções para execução de querys
#Autor: Gerson Dutra
#Data: 21/12/2024

import psycopg2
import psycopg2.extras
import fn_001_conexoes
import mysql.connector
from mysql.connector import Error

#==========================================================Execução de Querys MySQL=========================================================

def query_mysql(conexao, query, valores=None, is_write=False):
    cursor = conexao.cursor(dictionary=True)

    while True:
        try:
            # Executa a consulta
            cursor.execute(query, valores)
            
            if is_write:
                # Faz o commit da transação se for update, insert ou delete
                conexao.commit()
                resultado = cursor.rowcount  # Retorna o número de linhas afetadas
            else:
                # Grava o resultado em uma variável
                resultado = cursor.fetchall()
            
            break  # Sai do loop se a consulta for bem-sucedida

        except mysql.connector.Error as err:
            #trata erro de registro duplicado
            if err.errno == 1062:
                return # Código de erro para entrada duplicada no MySQL
                print("Erro 1062: Entrada duplicada. Tentando novamente...")
                print(query)
                continue  # Tenta novamente
            else:
                raise err  # Relevanta outros erros

    # Fecha o cursor
    cursor.close()
    
    return resultado
#===========================================================================================================================================
#========================================================Execução de Querys Postgres========================================================

def query_postgres(conexao, query, valores=None):
    cursor = conexao.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    
    while True:
        try:
            # Executa a consulta
            cursor.execute(query, valores)
            
            # Verifica se a consulta é uma seleção ou uma inserção/atualização
            if query.strip().lower().startswith("select"):
                # Grava o resultado em uma variável
                resultado = cursor.fetchall()
            else:
                # Confirma a transação para inserções/atualizações
                conexao.commit()
                resultado = cursor.rowcount  # Retorna o número de linhas afetadas
            
            break  # Sai do loop se a consulta for bem-sucedida

        except psycopg2.Error as err:
            if err.pgcode == '23505':  # Código de erro para entrada duplicada no PostgreSQL
                print("Erro 23505: Entrada duplicada. Tentando novamente...")
                print(query)
                continue  # Tenta novamente
            else:
                raise err  # Relevanta outros erros

    # Fecha o cursor
    cursor.close()
    
    return resultado
#===========================================================================================================================================

