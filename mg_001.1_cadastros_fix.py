# Script de migração da base cadastral do sistema RPP para Versa Nota
# Autor: Gerson Dutra
# Data: 05/05/2025

import re
import fn_002_query
import fn_001_conexoes
import fn_003_funcoes
from tqdm import tqdm
from datetime import datetime

def main():
    # Define município migrado pelo código
    # municipio_migracao = fn_003_funcoes.municipio()

    # Conecta ao banco de dados MySQL
    conexao_mysql = fn_001_conexoes.conectar_ao_mysql()

    # Conecta ao banco de dados PostgreSQL
    conexao_postgres = fn_001_conexoes.conectar_ao_postgres()

    delete_flag = True  # Se True, executa o delete dos registros antigos
    delete_only = False  # Se True, executa o delete apenas se delete_flag for True

    # ==========================================================
    # Trata Delete
    # ==========================================================

    if delete_flag:
        query_delete = """DELETE FROM cadastro WHERE justificativa_inativo = 'MIGRACAO_FIX';"""
        registros_deletados = fn_002_query.query_mysql(conexao_mysql, query_delete, is_write=True)
        
        if registros_deletados > 0:
            print(f'{registros_deletados} registros deletados.')
        else:
            print('Nenhum registro deletado.')
        if delete_only:
            print('Delete executado. Encerrando script.')
            return
    else:
        print('DELETE está desativado.')
    
    print('Migrando cadastros!')

    # ==========================================================
    # Início - Migra Cadastros
    # ==========================================================

    query_postgres = """
        SELECT 
    
            notas_fiscais.cadastro_economico,
            cadastro_economico_pessoa,
            case
            when cadastro_economico_inscricao_municipal = 'NaN' then null
            when cadastro_economico_inscricao_municipal = '\\N' then null
            else cadastro_economico_inscricao_municipal 
            end as cadastro_economico_inscricao_municipal,
            case
            when cadastro_economico_inscricao_estadual = 'NaN' then null
            when cadastro_economico_inscricao_estadual = '\\N' then null
            else cadastro_economico_inscricao_estadual 
            end as cadastro_economico_inscricao_estadual,
            cadastro_economico_razao_social,
            case
            when cadastro_economico_nome_fantasia = 'NaN' then null
            when cadastro_economico_nome_fantasia = '\\N' then null
            else cadastro_economico_nome_fantasia
            end as cadastro_economico_nome_fantasia,
            
            cadastro_economico_cpf_cnpj,
            cadastro_economico_endereco,
            cadastro_economico_email
            
            FROM 
            public.notas_fiscais
            left JOIN cidade mun_tom ON mun_tom.codibge::bigint = notas_fiscais.tomador_municipio
            left join cidade local_prest ON local_prest.codibge::bigint = notas_fiscais.municipio_execucao_servico
            left join cadastro cadastro ON cadastro.pessoa = notas_fiscais.cadastro_economico_pessoa
            where cadastro.pessoa is null
            order by id
            
    """
    
    # Executa a consulta no PostgreSQL e obtém os resultados
    resultado_postgres = fn_002_query.query_postgres(conexao_postgres, query_postgres)
    
    v_cnt = 0
    for v_rec in tqdm(resultado_postgres, desc="Migrando Cadastros"):
        datainicio = '0000-00-00'
        dataopcaosimples = '0000-00-00'
        endereco =fn_003_funcoes.extrair_endereco(v_rec['cadastro_economico_endereco'])
        nfe = 'S'
        codtipo = 1

        cnpj = v_rec['cadastro_economico_cpf_cnpj']
        if len(cnpj) > 14:
            pessoa = 'J'
        else:
            pessoa = 'F'
            codtipo = 11
        
  
        nome = v_rec['cadastro_economico_nome_fantasia']
        razao = v_rec['cadastro_economico_razao_social'] 
        email = v_rec['cadastro_economico_email']
        fone = ""
        inscricao_municipal = v_rec['cadastro_economico_inscricao_municipal']
        inscricao_estadual = v_rec['cadastro_economico_inscricao_estadual']
   
        logradouro = endereco['Logradouro']
        bairro = endereco['Bairro']
        numero = endereco['Número']
        complemento = endereco['Complemento']
        municipio = endereco['Cidade']
        uf = endereco['Estado']
        cep = endereco['CEP']

        if codtipo == 1 and municipio.lower() != 'luisburgo' and pessoa == 'J':
            codtipo = 20    

        insert = """INSERT INTO `cadastro` (
            `codigo`, 
            `sequencial_empresa`, 
            `classificacao`, 
            `codtipo`, 
            `codtipodeclaracao`, 
            `nome`, 
            `razaosocial`, 
            `cnpj`, 
            `cpf`, 
            `senha`, 
            `inscrmunicipal`, 
            `inscrestadual`, 
            `isentoiss`, 
            `logradouro`, 
            `numero`, 
            `complemento`, 
            `bairro`, 
            `cep`, 
            `municipio`, 
            `uf`, 
            `logo`, 
            `email`, 
            `ultimanota`, 
            `ultimocupom`, 
            `notalimite`, 
            `notalimiteperiodo`, 
            `ultima_solicitacao_notalimite`, 
            `estado`, 
            `credito`, 
            `nfe`, 
            `fonecomercial`, 
            `fonecelular`, 
            `pispasep`, 
            `datainicio`, 
            `data_consolidacao`, 
            `datafim`, 
            `dt_atualizacao`, 
            `dt_simples`, 
            `justificativa_inativo`, 
            `tipo_pessoa`
        ) VALUES (
            %s, 
            %s, 
            %s, 
            %s, 
            %s, 
            %s, 
            %s, 
            %s, 
            %s, 
            %s, 
            %s, 
            %s, 
            %s, 
            %s, 
            %s, 
            %s, 
            %s, 
            %s, 
            %s, 
            %s, 
            %s, 
            %s, 
            %s, 
            %s, 
            %s, 
            %s, 
            %s, 
            %s, 
            %s, 
            %s, 
            %s, 
            %s, 
            %s, 
            %s, 
            %s,
            %s, 
            %s, 
            %s, 
            %s, 
            %s
        );"""


       
        valores =(
            v_rec['cadastro_economico_pessoa'],
            1,
            0,  # Classificação
            codtipo,  # Tipo de cadastro
            0,
            nome,
            razao,
            cnpj,
            '',
            None,
           v_rec['cadastro_economico_inscricao_municipal'],
            '',
            'N',
            logradouro,
            numero,
            complemento,
            bairro,
            cep,
            municipio,
            uf,
            '',
            email,
            '0',
            '0',
            None,
            '0000-00-00',
            None,
            'A',
            None,
            nfe,
            fone,
            None,
            '',
            datainicio,
            '0000-00-00',
            '0000-00-00',
            datetime.now(),
            dataopcaosimples,
            'MIGRACAO_FIX',
            pessoa
        )
        #print (insert)

        #print(endereco)
        fn_002_query.query_mysql(conexao_mysql, insert,valores, is_write=True)
        #fn_002_query.query_mysql(conexao_mysql, insert, is_write=True)
        v_cnt += 1

        #if v_cnt % 100 == 0:
            #print(f'{v_cnt} cadastros migrados.')

    query_postgres = """
        SELECT distinct
            prestador_cpf_cnpj,
            prestador_nome,
            prestador_endereco,
            cp.pessoa
            FROM 
            public.guias g
            left JOIN cadastro cp ON cp.cpf_cnpj = g.prestador_cpf_cnpj
            where cp.pessoa is null
            
    """
    
    # Executa a consulta no PostgreSQL e obtém os resultados
    resultado_postgres = fn_002_query.query_postgres(conexao_postgres, query_postgres)
    
    v_cnt = 0
    for v_rec in tqdm(resultado_postgres, desc="Migrando Cadastros Guias"):
        # Define valores padrão para as colunas
        datainicio = '0000-00-00'
        dataopcaosimples = '0000-00-00'
        endereco =fn_003_funcoes.extrair_endereco(v_rec['prestador_endereco'])
        nfe = 'N'
        codtipo = 11

        cnpj = v_rec['prestador_cpf_cnpj']

        pessoa = 'J'
  
        nome = ''
        razao = v_rec['prestador_nome'] 
        email = ''
        fone = ""
        inscricao_municipal = ''
        inscricao_estadual = ''
   
        logradouro = endereco['Logradouro']
        bairro = endereco['Bairro']
        numero = endereco['Número']
        complemento = endereco['Complemento']
        municipio = endereco['Cidade']
        uf = endereco['Estado']
        cep = endereco['CEP']
        check = fn_002_query.query_mysql(conexao_mysql, "SELECT COUNT(*) as cnt FROM cadastro WHERE cnpj = %s", (cnpj,))
        if check[0]['cnt'] > 0:
            print(f'Cadastro com CNPJ {cnpj} já existe, pulando inserção.')
            continue
        else:
            insert = """INSERT INTO `cadastro` (
                `sequencial_empresa`, 
                `classificacao`, 
                `codtipo`, 
                `codtipodeclaracao`, 
                `nome`, 
                `razaosocial`, 
                `cnpj`, 
                `cpf`, 
                `senha`, 
                `inscrmunicipal`, 
                `inscrestadual`, 
                `isentoiss`, 
                `logradouro`, 
                `numero`, 
                `complemento`, 
                `bairro`, 
                `cep`, 
                `municipio`, 
                `uf`, 
                `logo`, 
                `email`, 
                `ultimanota`, 
                `ultimocupom`, 
                `notalimite`, 
                `notalimiteperiodo`, 
                `ultima_solicitacao_notalimite`, 
                `estado`, 
                `credito`, 
                `nfe`, 
                `fonecomercial`, 
                `fonecelular`, 
                `pispasep`, 
                `datainicio`, 
                `data_consolidacao`, 
                `datafim`, 
                `dt_atualizacao`, 
                `dt_simples`, 
                `justificativa_inativo`, 
                `tipo_pessoa`
            ) VALUES (
                %s, 
                %s, 
                %s, 
                %s, 
                %s, 
                %s, 
                %s, 
                %s, 
                %s, 
                %s, 
                %s, 
                %s, 
                %s, 
                %s, 
                %s, 
                %s, 
                %s, 
                %s, 
                %s, 
                %s, 
                %s, 
                %s, 
                %s, 
                %s, 
                %s, 
                %s, 
                %s, 
                %s, 
                %s, 
                %s, 
                %s, 
                %s, 
                %s, 
                %s,
                %s, 
                %s, 
                %s, 
                %s, 
                %s
            );"""


        
            valores =(
                1,
                0,  # Classificação
                codtipo,  # Tipo de cadastro
                0,
                nome,
                razao,
                cnpj,
                '',
                None,
            '',
                '',
                'N',
                logradouro,
                numero,
                complemento,
                bairro,
                cep,
                municipio,
                uf,
                '',
                email,
                '0',
                '0',
                None,
                '0000-00-00',
                None,
                'A',
                None,
                nfe,
                fone,
                None,
                '',
                datainicio,
                '0000-00-00',
                '0000-00-00',
                datetime.now(),
                dataopcaosimples,
                'MIGRACAO_FIX',
                pessoa
            )
        #print (insert)
  


        #print(endereco)
        fn_002_query.query_mysql(conexao_mysql, insert,valores, is_write=True)
        fn_002_query.query_mysql(conexao_mysql, """update `cadastro_servicos` set cnaexlc116 = '' where cnaexlc116 IN ('None','NONE')""", is_write=True)
        #fn_002_query.query_mysql(conexao_mysql, insert, is_write=True)
        v_cnt += 1

        #if v_cnt % 100 == 0:
            #print(f'{v_cnt} cadastros migrados.')

    query_postgres = """
        SELECT distinct
        prestador_cpf_cnpj,
        prestador_nome,
        prestador_uf,
        prestador_endereco
        FROM 
        public.psene
        left JOIN cadastro ON cadastro.cpf_cnpj = psene.prestador_cpf_cnpj
        where cadastro.cpf_cnpj is null
            
    """
    
    # Executa a consulta no PostgreSQL e obtém os resultados
    resultado_postgres = fn_002_query.query_postgres(conexao_postgres, query_postgres)
    
    v_cnt = 0
    for v_rec in tqdm(resultado_postgres, desc="Migrando Cadastros Externos"):
        # Define valores padrão para as colunas
        datainicio = '0000-00-00'
        dataopcaosimples = '0000-00-00'
        endereco =fn_003_funcoes.extrair_endereco(v_rec['prestador_endereco'])
        nfe = 'N'
        codtipo = 11

        cnpj = v_rec['prestador_cpf_cnpj']

        pessoa = 'J'
  
        nome = ''
        razao = v_rec['prestador_nome'] 
        email = ''
        fone = ""
        inscricao_municipal = ''
        inscricao_estadual = ''
   
        logradouro = endereco['Logradouro']
        bairro = endereco['Bairro']
        numero = endereco['Número']
        complemento = endereco['Complemento']
        municipio = endereco['Cidade']
        uf = endereco['Estado']
        cep = endereco['CEP']
        check = fn_002_query.query_mysql(conexao_mysql, "SELECT COUNT(*) as cnt FROM cadastro WHERE cnpj = %s", (cnpj,))
        if check[0]['cnt'] > 0:
            print(f'Cadastro com CNPJ {cnpj} já existe, pulando inserção.')
            continue
        else:
            insert = """INSERT INTO `cadastro` (
                `sequencial_empresa`, 
                `classificacao`, 
                `codtipo`, 
                `codtipodeclaracao`, 
                `nome`, 
                `razaosocial`, 
                `cnpj`, 
                `cpf`, 
                `senha`, 
                `inscrmunicipal`, 
                `inscrestadual`, 
                `isentoiss`, 
                `logradouro`, 
                `numero`, 
                `complemento`, 
                `bairro`, 
                `cep`, 
                `municipio`, 
                `uf`, 
                `logo`, 
                `email`, 
                `ultimanota`, 
                `ultimocupom`, 
                `notalimite`, 
                `notalimiteperiodo`, 
                `ultima_solicitacao_notalimite`, 
                `estado`, 
                `credito`, 
                `nfe`, 
                `fonecomercial`, 
                `fonecelular`, 
                `pispasep`, 
                `datainicio`, 
                `data_consolidacao`, 
                `datafim`, 
                `dt_atualizacao`, 
                `dt_simples`, 
                `justificativa_inativo`, 
                `tipo_pessoa`
            ) VALUES (
                %s, 
                %s, 
                %s, 
                %s, 
                %s, 
                %s, 
                %s, 
                %s, 
                %s, 
                %s, 
                %s, 
                %s, 
                %s, 
                %s, 
                %s, 
                %s, 
                %s, 
                %s, 
                %s, 
                %s, 
                %s, 
                %s, 
                %s, 
                %s, 
                %s, 
                %s, 
                %s, 
                %s, 
                %s, 
                %s, 
                %s, 
                %s, 
                %s, 
                %s,
                %s, 
                %s, 
                %s, 
                %s, 
                %s
            );"""


        
            valores =(
                1,
                0,  # Classificação
                codtipo,  # Tipo de cadastro
                0,
                nome,
                razao,
                cnpj,
                '',
                None,
            '',
                '',
                'N',
                logradouro,
                numero,
                complemento,
                bairro,
                cep,
                municipio,
                uf,
                '',
                email,
                '0',
                '0',
                None,
                '0000-00-00',
                None,
                'A',
                None,
                nfe,
                fone,
                None,
                '',
                datainicio,
                '0000-00-00',
                '0000-00-00',
                datetime.now(),
                dataopcaosimples,
                'MIGRACAO_FIX',
                pessoa
            )
        #print (insert)
  


        #print(endereco)
        fn_002_query.query_mysql(conexao_mysql, insert,valores, is_write=True)
        #fn_002_query.query_mysql(conexao_mysql, insert, is_write=True)
        v_cnt += 1

        #if v_cnt % 100 == 0:
            #print(f'{v_cnt} cadastros migrados.')

    
if __name__ == "__main__":
    main()
