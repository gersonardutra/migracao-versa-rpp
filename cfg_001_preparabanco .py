import time
import sys
import threading
import fn_002_query
import fn_001_conexoes
import fn_003_funcoes


def main_task():

    #===========================================================Seta Conexões===============================================================
    tabela_estado =True
    tabela_municipio = True

    # Conecta ao banco de dados MySQL
    conexao_mysql = fn_001_conexoes.conectar_ao_mysql()

    # Conecta ao banco de dados PostgreSQL
    conexao_postgres = fn_001_conexoes.conectar_ao_postgres()
    #=======================================================================================================================================

    #===========================================================Ajusta Tabelas==============================================================

    query_postgres = """
               DO $$ 
        DECLARE
            prefix TEXT := 'luisburgomg_20250731_'; --prefixo a remover
            tbl RECORD;
            new_name TEXT;
        BEGIN
            FOR tbl IN 
                SELECT tablename 
                FROM pg_tables 
                WHERE schemaname = 'public' AND tablename LIKE prefix || '%'
            LOOP
                new_name := REPLACE(tbl.tablename, prefix, '');
                EXECUTE 'ALTER TABLE ' || quote_ident(tbl.tablename) || ' RENAME TO ' || quote_ident(new_name);
            END LOOP;
        END $$;;
    """
    
    # Realiza o SELECT no PostgreSQL e grava o resultado
    resultado_postgres = fn_002_query.query_postgres(conexao_postgres, query_postgres)

    print(f'Criando view desif no banco de dados PostgreSQL')
    query_postgres = """
                CREATE VIEW public.view_desif (
            pessoa,
            codigo,
            ano,
            competencia,
            situacao,
            base_calculo,
            aliquota,
            valor_issqn,
            valor_issqn_retido,
            valor_servicos,
            item_lista_servico)
        AS
        SELECT cd.pessoa,
            desif.codigo,
            substr(desif.competencia::text, 1, 4) AS ano,
            concat(substr(desif.competencia::text, 1, 4), '-', substr(desif.competencia::text, 5, 2)) AS competencia,
            desif.situacao,
            desif.base_calculo,
            desif.aliquota,
            desif.valor_issqn,
            desif.valor_issqn_retido,
            desif.valor_servicos,
                CASE
                    WHEN length(desif.item_lista_servico::text) < 3 THEN lpad(concat(lpad(desif.item_lista_servico::text, 2, ''::text), '.01'), 5, '0'::text)
                    WHEN desif.item_lista_servico::text = 'NaN'::text THEN '15.01'::text
                    WHEN length(desif.item_lista_servico::text) = 3 THEN concat('0', replace(desif.item_lista_servico::text, ','::text, '.'::text), '0')
                    WHEN desif.item_lista_servico::text = '15.1'::text THEN '15.01'::text
                    ELSE '15.01'::text
                END AS item_lista_servico
        FROM declaracao_servicos_prestados_banco desif
            JOIN cadastro cd ON cd.cpf_cnpj = desif.prestador_cpf_cnpj;

        ALTER VIEW public.view_desif
        OWNER TO postgres;
    """
    
    # Realiza o SELECT no PostgreSQL e grava o resultado
    resultado_postgres = fn_002_query.query_postgres(conexao_postgres, query_postgres)

    print(f'Criando view nfse no banco de dados PostgreSQL')
    query_postgres = """
                CREATE OR REPLACE VIEW public.view_nfse(
    id,
    item_lista_servico,
    cnae,
    codigo_tributacao_municipio,
    base_calculo,
    aliquota_servicos,
    valor_issqn,
    valor_liquido_nota,
    issqn_retido,
    valor_issqn_retido,
    discriminacao)
AS
  SELECT notas_fiscais.id,
         CASE
           WHEN length(notas_fiscais.item_lista_servico::text) < 3 THEN lpad(concat(lpad(notas_fiscais.item_lista_servico::text, 2, ''::text), '.01'),
             5, '0'::text)
           WHEN length(notas_fiscais.item_lista_servico::text) = 3 THEN concat('0', replace (notas_fiscais.item_lista_servico::text, ','::text, '.'::
             text), '0')
           ELSE lpad(replace (notas_fiscais.item_lista_servico::text, ','::text, '.'::text), 5, '0'::text)
         END AS item_lista_servico,
         CASE
           WHEN notas_fiscais.cnae::text = 'NaN'::text THEN NULL::character varying
           ELSE notas_fiscais.cnae
         END AS cnae,
         CASE
           WHEN notas_fiscais.codigo_tributacao_municipio = '\\N'::text THEN NULL::text
           ELSE notas_fiscais.codigo_tributacao_municipio
         END AS codigo_tributacao_municipio,
         notas_fiscais.base_calculo,
         notas_fiscais.aliquota_servicos,
         notas_fiscais.valor_issqn,
         notas_fiscais.valor_liquido_nota,
         notas_fiscais.issqn_retido,
         notas_fiscais.valor_issqn_retido,
         notas_fiscais.discriminacao
  FROM notas_fiscais;
    """
    
    # Realiza o SELECT no PostgreSQL e grava o resultado
    resultado_postgres = fn_002_query.query_postgres(conexao_postgres, query_postgres)

   
