import os
import pandas as pd
import psycopg2
import chardet

# Função para mapear tipos de dados do pandas para tipos do PostgreSQL
def infer_postgresql_type(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return "BIGINT"
    elif pd.api.types.is_float_dtype(dtype):
        return "REAL"
    elif pd.api.types.is_bool_dtype(dtype):
        return "BOOLEAN"
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return "TIMESTAMP"
    else:
        return "TEXT"

# Função para criar conexão com o banco de dados PostgreSQL
def connect_to_db():
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="LUISBURGO",
            user="postgres",
            password="1234"
        )
        return conn
    except Exception as e:
        print("Erro ao conectar ao banco de dados:", e)
        return None

# Função para detectar a codificação do arquivo
def detect_encoding(file_path):
    with open(file_path, "rb") as f:
        result = chardet.detect(f.read())
    return result["encoding"]

# Função para criar tabela e preencher dados
def create_table_from_file(conn, file_path, separator=","):
    try:
        # Detectar a codificação do arquivo
        encoding = detect_encoding(file_path)

        # Ler o arquivo usando pandas
        df = pd.read_csv(file_path, sep=separator, encoding=encoding)
        
        # Gerar nome da tabela baseado no nome do arquivo (sem extensão)
        table_name = os.path.splitext(os.path.basename(file_path))[0]
        
        # Gerar comando SQL para criar tabela com tipos inferidos
        create_table_query = f"CREATE TABLE {table_name} ("
        for col in df.columns:
            col_type = infer_postgresql_type(df[col].dtype)
            if col in ["cnae","item_lista_servico"]:
                col_type = "VARCHAR(20)"
            create_table_query += f"{col} {col_type},"
        create_table_query = create_table_query.rstrip(",") + ");"
        
        # Executar comando de criação de tabela
        cursor = conn.cursor()
        cursor.execute(create_table_query)
        conn.commit()
        print(f"Tabela '{table_name}' criada com sucesso.")
        
        # Inserir dados na tabela
        for index, row in df.iterrows():
            insert_query = f"INSERT INTO {table_name} VALUES ({', '.join(['%s'] * len(row))});"
            cursor.execute(insert_query, tuple(row))
        conn.commit()
        print(f"Dados inseridos na tabela '{table_name}' com sucesso.")
    except Exception as e:
        print(f"Erro ao processar o arquivo {file_path}:", e)

# Função para processar todos os arquivos em uma pasta
def process_folder(conn, folder_path, separator=","):
    try:
        # Iterar sobre todos os arquivos na pasta
        for file_name in os.listdir(folder_path):
            file_path = os.path.join(folder_path, file_name)
            # Verificar se o arquivo é CSV ou TXT
            if file_name.endswith(".csv") or file_name.endswith(".txt"):
                print(f"Processando arquivo: {file_name}")
                create_table_from_file(conn, file_path, separator)
            else:
                print(f"Arquivo ignorado (não é CSV ou TXT): {file_name}")
    except Exception as e:
        print("Erro ao processar a pasta:", e)

# Uso do script
if __name__ == "__main__":
    pasta_arquivos = r"C:\Users\Gerson\Migração\RPP\Backup"  # Caminho para a pasta com os arquivos
    separador = "|"  # Altere para o separador dos seus arquivos
    
    conexao = connect_to_db()
    if conexao:
        process_folder(conexao, pasta_arquivos, separador)
        conexao.close()
