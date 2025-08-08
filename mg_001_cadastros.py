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

    delete_flag = True

    buscarfb = False  # Define se deve buscar dados da RFB (Receita Federal do Brasil)
    # Se for False, não busca dados da RFB, apenas migra os dados já existentes na tabela cadastro do PostgreSQL

    # ==========================================================
    # Trata Delete
    # ==========================================================

    if delete_flag:
        query_delete = """DELETE FROM cadastro WHERE justificativa_inativo = 'MIGRACAO';"""
        registros_deletados = fn_002_query.query_mysql(conexao_mysql, query_delete, is_write=True)
        query_delete = """DELETE FROM cadastro_dados;"""
        registros_deletados = fn_002_query.query_mysql(conexao_mysql, query_delete, is_write=True)
        query_delete = """DELETE FROM cadastro_servicos;"""
        registros_deletados = fn_002_query.query_mysql(conexao_mysql, query_delete, is_write=True)
        
        if registros_deletados > 0:
            print(f'{registros_deletados} registros deletados.')
        else:
            print('Nenhum registro deletado.')
    else:
        print('DELETE está desativado.')
    
    print('Migrando cadastros!')

    # ==========================================================
    # Início - Migra Cadastros
    # ==========================================================

    query_postgres = """
        SELECT DISTINCT
            cpf_cnpj,
            pessoa,
            cadastro_economico,
            CASE
                WHEN inscricao_municipal = 'NaN' THEN NULL
                ELSE inscricao_municipal
            END AS inscricao_municipal,
            endereco_completo,
            emissor_nota,
            declarante,
            substituto_tributario,
            incentivo_fiscal,
            CASE
                WHEN natureza_juridica = 'NaN' THEN NULL
                ELSE natureza_juridica
            END AS natureza_juridica,
            regime_especial_tributacao,
            regime_recolhimento,
            tipo_cadastro
        FROM public.cadastro
        ORDER BY pessoa
    """
    
    # Executa a consulta no PostgreSQL e obtém os resultados
    resultado_postgres = fn_002_query.query_postgres(conexao_postgres, query_postgres)
    
    v_cnt = 0
    for v_rec in tqdm(resultado_postgres, desc="Migrando Cadastros"):
        datainicio = '0000-00-00'
        dataopcaosimples = '0000-00-00'
        endereco =fn_003_funcoes.extrair_endereco(v_rec['endereco_completo'])
        logradouro = endereco['Logradouro']
        bairro = endereco['Bairro']
        numero = endereco['Número']
        complemento = endereco['Complemento']
        municipio = endereco['Cidade']
        uf = endereco['Estado']
        cep = endereco['CEP']
        
        nfe = 'N'

        cnpj = v_rec['cpf_cnpj']

        if len(cnpj) <= 14 :
            cnpj = ''
            cpf = v_rec['cpf_cnpj']
            pessoa = 'F'
        else:
            cpf = ''
            pessoa = 'J'
        
        codtipo = 1
        
        if pessoa == 'F':
            codtipo = 11

        if v_rec['tipo_cadastro'] == "BANCO":
            codtipo = 12
        if v_rec['tipo_cadastro'] == "CARTORIO":
            codtipo = 13

        



        cnpj_busca = re.sub(r'[^0-9]', '', cnpj)
            # Busca dados da RFB
        if buscarfb:
            empresa = f"""SELECT 
                            cnpj,
                            matrizfilial,
                            nomerazao,
                            nomefantasia,
                            situacaocadastral,
                            datasituacaocadastral,
                            motivosituacao,
                            nomecidadeexterior,
                            codpais,
                            nomepais,
                            codnaturezajuridica,
                            CONCAT(substr(datainiatv,1,4),'-',substr(datainiatv,5,2),'-',substr(datainiatv,7,2)) datainiatv,
                            cnaefiscal,
                            tipologradouro,
                            logradouro,
                            numero,
                            complemento,
                            bairro,
                            cep,
                            uf,
                            codmunicipio,
                            dddfone1,
                            fone1,
                            dddfone2,
                            fone2,
                            dddfax,
                            fax,
                            email,
                            qualifresponsavel,
                            capitalsocial,
                            porte,
                            opcaosimples,
                            CONCAT(substr(dataopcaosimples,1,4),'-',substr(dataopcaosimples,5,2),'-',substr(dataopcaosimples,7,2)) dataopcaosimples,
                            dataexclusaosimples,
                            opcaomei,
                            situacaoespecial,
                            datasituacaoespecial,
                            municipio
                            FROM 
                            public.cnpj where cnpj='{cnpj_busca}' limit 1;"""

            empresa = fn_002_query.query_postgres(conexao_postgres, empresa)

            if empresa:
                datainicio = empresa[0]['datainiatv']
                dataopcaosimples = empresa[0]['dataopcaosimples']

                cidade = f"""SELECT 
                    lpad(codtom::text,4,'0')codtom,
                    codibge,
                    nome,
                    nome_sub,
                    uf
                    FROM 
                    public.cidade where lpad(codtom::text,4,'0') = '{empresa[0]['codmunicipio']}'limit 1;"""
                cidade = fn_002_query.query_postgres(conexao_postgres, cidade)
                

                nome = fn_003_funcoes.coalesce(empresa[0]['nomefantasia'],'')
                razao = fn_003_funcoes.coalesce(empresa[0]['nomerazao'],'')
                fone = fn_003_funcoes.coalesce(empresa[0]['dddfone1']) + fn_003_funcoes.coalesce(empresa[0]['fone1'],'')
                email = fn_003_funcoes.coalesce(empresa[0]['email'],'')
                logradouro = fn_003_funcoes.coalesce(empresa[0]['tipologradouro'],'') + ' ' + fn_003_funcoes.coalesce(empresa[0]['logradouro'],'')
                bairro = fn_003_funcoes.coalesce(empresa[0]['bairro'],'')
                numero = fn_003_funcoes.coalesce(empresa[0]['numero'],'')
                complemento = fn_003_funcoes.coalesce(empresa[0]['complemento'],'')
                municipio = fn_003_funcoes.coalesce(cidade[0]['nome'],'')
                uf = fn_003_funcoes.coalesce(cidade[0]['uf'],'')
                cep = fn_003_funcoes.coalesce(empresa[0]['cep'],'')
                
            else:
                empresa = fn_003_funcoes.extrair_cadastro_nf(v_rec['cpf_cnpj'],conexao_postgres)
                endereco = fn_003_funcoes.extrair_endereco(empresa['endereco'])
                
                logradouro = endereco['Logradouro']
                bairro = endereco['Bairro']
                numero = endereco['Número']
                complemento = endereco['Complemento']
                municipio = endereco['Cidade']
                uf = endereco['Estado']
                cep = endereco['CEP']
        empresa = fn_003_funcoes.extrair_cadastro_nf(v_rec['cpf_cnpj'],conexao_postgres)
        nome = empresa['nome_fantasia']
        razao = empresa['razao_social']
        email =  empresa['email']
        fone = ''
        if not endereco:
            endereco = fn_003_funcoes.extrair_endereco(empresa['endereco'])
            logradouro = endereco['Logradouro']
            bairro = endereco['Bairro']
            numero = endereco['Número']
            complemento = endereco['Complemento']
            municipio = endereco['Cidade']
            uf = endereco['Estado']
            
        if codtipo in (1,12) and municipio.lower() != 'luisburgo':
            codtipo = 20

        if codtipo == 20 and municipio.lower() == 'luisburgo':   
            codtipo = 11

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
            v_rec['pessoa'],
            1,
            0,  # Classificação
            codtipo,  # Tipo de cadastro
            v_rec['regime_recolhimento'],
            nome,
            razao,
            cnpj,
            cpf,
            None,
           v_rec['inscricao_municipal'],
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
            'MIGRACAO',
            pessoa
        )
        #print (insert)

        #print(endereco)
        fn_002_query.query_mysql(conexao_mysql, insert,valores, is_write=True)
        #fn_002_query.query_mysql(conexao_mysql, insert, is_write=True)
        v_cnt += 1

        #if v_cnt % 100 == 0:
            #print(f'{v_cnt} cadastros migrados.')

    query_postgres = """SELECT 
        pessoa,
        lpad (replace(cnae,'.0',''),7,'0') cnae,
        case
            when item_lista = 'NaN' then NULL
            else lpad(item_lista::text,5,'0')::text
        end as item_lista,
        tipo_cnae
        FROM 
        public.cadastro
        where cnae != 'NaN'
        order by pessoa,tipo_cnae"""
    
    resultado_postgres = fn_002_query.query_postgres(conexao_postgres, query_postgres)
    count = 0
    for v_rec in tqdm(resultado_postgres, desc="Vinculando Serviços"):
        servico = f"""select * from servicos where codservico = {v_rec['cnae']}"""
        servico = fn_002_query.query_mysql(conexao_mysql, servico)
        #print(servico)
            

        if v_rec['tipo_cnae'] == 'PRINCIPAL':
            primario = 'S'
        else:
            primario = 'N'
            
        if v_rec['item_lista']:    
            if v_rec['item_lista'].startswith('08'):
                classificacao = 6
            elif v_rec['item_lista'].startswith('07'):
                classificacao = 1
            else:
                classificacao = 0
        else:
            classificacao = 0

        fn_002_query.query_mysql(conexao_mysql, f"update cadastro set classificacao = {classificacao} where codigo = {v_rec['pessoa']}",is_write=True)

        if v_rec['item_lista'] != ('NaN'):
            fn_002_query.query_mysql(conexao_mysql, f"update cadastro set codtipo = 1 where codigo = {v_rec['pessoa']} and codtipo != 20 and tipo_pessoa = 'J'",is_write=True)
        else:
            fn_002_query.query_mysql(conexao_mysql, f"update cadastro set codtipo = 11 where codigo = {v_rec['pessoa']} and codtipo != 20",is_write=True)

        if v_rec['item_lista'] == ('17.19'):
            fn_002_query.query_mysql(conexao_mysql, f"update cadastro set codtipo = 10 where codigo = {v_rec['pessoa']}",is_write=True)

        
        if servico:
            
            insert = f"""INSERT INTO `cadastro_servicos` (
                `codservico`, 
                `codemissor`, 
                `cnaexlc116`, 
                `isento`, 
                `primario`, 
                `data_inclusao`
            ) VALUES (
                {servico[0]['codigo']}, 
                {v_rec['pessoa']}, 
                '{v_rec['item_lista']}', 
                'N', 
                '{primario}', 
                CURRENT_TIMESTAMP
            );
            """
            count += 1
            print(count)
            fn_002_query.query_mysql(conexao_mysql, insert,is_write=True)
            fn_002_query.query_mysql(conexao_mysql, f"update cadastro set classificacao = {classificacao} where codigo = {v_rec['pessoa']}",is_write=True)
        else:
            print(f"""select * from servicos where codservico = {v_rec['cnae']}""")
            #raise Exception(f"Serviço não encontrado para o CNPJ {v_rec['cnae']} e Item Lista {v_rec['item_lista']}")
        
    # ==========================================================
    # Fim - Migra Cadastros
    # ==========================================================
    query_postgres = """SELECT 
        pessoa,
        cnae,
        case
            when item_lista = 'NaN' then NULL
            else lpad(item_lista::text,5,'0')::text
        end as item_lista,
        tipo_cnae
        FROM 
        public.cadastro
        where cnae = 'NaN'
        order by pessoa,tipo_cnae"""
    
    resultado_postgres = fn_002_query.query_postgres(conexao_postgres, query_postgres)

    for v_rec in tqdm(resultado_postgres, desc="Ajustando Cadastros"):
        
        fn_002_query.query_mysql(conexao_mysql, f"update cadastro set codtipo = 11 where codigo = {v_rec['pessoa']} and codtipo = 20",is_write=True)

if __name__ == "__main__":
    main()
