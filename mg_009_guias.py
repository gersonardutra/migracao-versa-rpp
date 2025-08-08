import fn_002_query
import fn_001_conexoes
import fn_003_funcoes
from datetime import datetime
import re
import threading
from tqdm import tqdm

def main():
    deletar = True
    

    # Conecta ao banco de dados MySQL
    conexao_mysql = fn_001_conexoes.conectar_ao_mysql(alerta=True)

    # Conecta ao banco de dados PostgreSQL
    conexao_postgres = fn_001_conexoes.conectar_ao_postgres(alerta=True)

    if deletar:
        delete = """delete from guia_notas;"""
        delete = fn_002_query.query_mysql(conexao_mysql, delete, is_write=True)
        if delete > 0:
            print(f'{delete} Registros Deletados de guias notas')
        else:
            print(f'Nenhum registro deletado')
        print("Consultando NFSE da base origem")

        delete = """delete from guia_prestadas;"""
        delete = fn_002_query.query_mysql(conexao_mysql, delete, is_write=True)
        if delete > 0:
            print(f'{delete} Registros Deletados de guias prestadas')
        else:
            print(f'Nenhum registro deletado')

        delete = """delete from guia_tomadas;"""
        delete = fn_002_query.query_mysql(conexao_mysql, delete, is_write=True)
        if delete > 0:
            print(f'{delete} Registros Deletados de guias tomadas')
        else:
            print(f'Nenhum registro deletado')
        
        delete = """delete from desif_guia_declaracoes;"""
        delete = fn_002_query.query_mysql(conexao_mysql, delete, is_write=True)
        if delete > 0:
            print(f'{delete} Registros Deletados de guias DESIF')
        else:
            print(f'Nenhum registro deletado')

        delete = """delete from cartorio_guia_declaracoes;"""
        delete = fn_002_query.query_mysql(conexao_mysql, delete, is_write=True)
        if delete > 0:
            print(f'{delete} Registros Deletados de guias Cartorio')
        else:
            print(f'Nenhum registro deletado')

        

        delete = """truncate guias;"""
        delete = fn_002_query.query_mysql(conexao_mysql, delete, is_write=True)
        if delete > 0:
            print(f'{delete} Registros Deletados')
        else:
            print(f'Nenhum registro deletado')
        print("Consultando NFSE da base origem")

        
    else:
        print("DELETE está desativado")
    #Monta query de consulta ao banco da HLH
    query_postgres = """
        SELECT distinct
            prestador_cpf_cnpj,
            prestador_nome,
            prestador_endereco,
            substr(competencia::text,1,4)||'-'||substr(competencia::text,5,2) competencia,
            recolhimento,
            situacao_guia,
            situacao_pagamento,
            guia_numero,
            parcela_numero::text as parcela_numero,
            valor_principal,
            valor_juros,
            valor_multa,
            valor_correcao,
            valor_cobrado,
            replace(vencimento,'/','-') as vencimento,
            valor_pago,
            case
            when dt_pagamento = 'NaN' then '00-00-0000'
            else replace(dt_pagamento,'/','-')
            end as dt_pagamento,
            case
            when dt_contabilizacao = 'NaN' then '00-00-0000'
            else replace(dt_contabilizacao,'/','-')
            end as dt_contabilizacao,
            case
            when dt_credito = 'NaN' then '00-00-0000'
            else replace(dt_credito,'/','-')
            end as dt_credito,
            case
            when banco_codigo = 'NaN' then null
            else banco_codigo
            end as banco_codigo
            FROM 
            public.guias g
    """
    
    # Realiza o SELECT com a consulta montada em query_postgres
    resultado_postgres = fn_002_query.query_postgres(conexao_postgres, query_postgres)

    #seta o contador de resultados de migração pra a contagem obtida
    v_cnt = 0
    print(f"Migrando {len(resultado_postgres)} Guias")
    #percorre a consulta
    for v_rec in tqdm(resultado_postgres, desc="Inserindo Guias"):

        #converte os ids do prestador e tomador para o novo id registrado na base da versa
        

        #trata nossonumero
        nossonumero = v_rec['parcela_numero']

        #trata datas

        dataemissao = v_rec['vencimento']
        #dataemissao = dataemissao.strftime("%d-%m-%Y")
        dataemissao = datetime.strptime(dataemissao, "%d-%m-%Y")

        horaemissao = '00:00:00'

        data_vazia =  '0000-00-00'

        if v_rec['dt_pagamento'] is None:
            databaixa = data_vazia
        else:
            databaixa = v_rec['dt_pagamento']
        
        if v_rec['valor_correcao'] is None:
            correcao = 0
        else:
            correcao = v_rec['valor_correcao']

        if v_rec['valor_multa'] is None:
            multa = 0
        else:
            multa = v_rec['valor_multa']

        if v_rec['valor_juros'] is None:
            juros = 0
        else:
            juros = v_rec['valor_juros']

        acrescimos = juros + multa + correcao
        total = v_rec['valor_cobrado']
        #define o status da guia e trata valor pago
        if v_rec["situacao_pagamento"] in ('QUITADA'):
            status = 'P'
            valorpago = total
        elif v_rec["situacao_guia"] != 'ATIVA':
            status = 'C'
            valorpago = 0
        else:
            status = 'N'
            valorpago = 0
        
        pessoa = fn_002_query.query_mysql(conexao_mysql,f"""select codigo from cadastro where cnpj = '{v_rec['prestador_cpf_cnpj']}'""")
        if not pessoa:
            pessoa = fn_002_query.query_mysql(conexao_mysql,f"""select codigo from cadastro where cpf = '{v_rec['prestador_cpf_cnpj']}'""")
        
        codpessoa = pessoa[0]['codigo'] if pessoa else 0

        query = f"""
            INSERT INTO 
                `guias`
                (
                `id`,
                `codprestador`,
                `incidencia`,
                `tipo`,
                `emissao`,
                `vencimento`,
                `alteracao`,
                `cancelamento`,
                `pagamento`,
                `nossonumero`,
                `estado`,
                `motivo_cancelamento`,
                `valor`,
                `acrescimos`,
                `creditos`,
                `deducao`,
                `vlr_pago`,
                `codbanco`,
                `multa`,
                `juros`,
                `baixa_manual_justificativa`,
                `justificativa_deducao`,
                `credito_conta`,
                `guia_pai`,
                `remessa`,
                `controle`,
                `hora_emissao`,
                `correcao_monetaria`,
                `valortaxa`,
                `tipotaxa`,
                `vencimento_tributo`,
                `guia_substituida`,
                `observacao`,
                `regime_tributacao`,
                `incidencia_inicial`) 
                VALUE (
                {v_rec['guia_numero']},
                {codpessoa},
                '{v_rec['competencia']}',
                'HOMOLOGACAO',
                '{dataemissao}',
                '{dataemissao}',
                '{data_vazia}',
                '{data_vazia}',
                '{databaixa}',
                '{nossonumero}',
                '{status}',
                '',
                {v_rec['valor_principal']},
                {acrescimos},
                0,
                0,
                {valorpago},
                null,
                {multa},
                {juros},
                '',
                '',
                '0000-00-00',
                null,
                0,
                999,
                '{horaemissao}',
                {correcao},
                0,
                'N',
                '0000-00-00',
                null,
                '',
                null,
                null);
            """
        

        

       
        try:
            resultado = fn_002_query.query_mysql(conexao_mysql, query, is_write=True)
        except:
            
            print(query)
            #print(f"Valor ISS: {v_rec['valoriss']}") 
            
            raise resultado.get_mysql_exception

        v_cnt = v_cnt + 1
    query_postgres = f"""SELECT 
                        g.guia_numero as id_guia,
                        nf.id as id_nota,
                        g.situacao_guia,
                        g.situacao_pagamento
                        FROM 
                        public.guias g
                        INNER JOIN notas_fiscais nf ON nf.cadastro_economico_cpf_cnpj = g.prestador_cpf_cnpj and nf.competencia = g.competencia and nf.valor_issqn = g.valor_principal
                        where situacao_guia = 'ATIVA'
                        UNION
                        SELECT 
                        g.guia_numero as id_guia,
                        nf.id as id_nota,
                        g.situacao_guia,
                        g.situacao_pagamento
                        FROM 
                        public.guias g
                        INNER JOIN notas_fiscais nf ON nf.tomador_cpf_cnpj = g.prestador_cpf_cnpj and nf.competencia = g.competencia and nf.valor_issqn_retido = g.valor_principal
                        where situacao_guia = 'ATIVA'"""
    
    resultado_postgres = fn_002_query.query_postgres(conexao_postgres, query_postgres)
    print("Vinculando Guias a notas")

    v_cnt = 0
    #percorre a consulta
    for v_rec in tqdm(resultado_postgres, desc="Vinculando NFS'e"):


        insert = f"""INSERT INTO 
                    `guia_notas`
                    (
                    `codguia`,
                    `codnota`) 
                    VALUE (
                    {v_rec['id_guia']},
                    {v_rec['id_nota']});"""
        fn_002_query.query_mysql(conexao_mysql, insert, is_write=True)
        if v_rec['situacao_pagamento'] == 'PENDENTE':
            fn_002_query.query_mysql(conexao_mysql, f"UPDATE notas SET estado = 'B' WHERE codigo = {v_rec['id_nota']} and estado = 'N'" , is_write=True)
        elif v_rec['situacao_pagamento'] == 'QUITADA':
            fn_002_query.query_mysql(conexao_mysql, f"UPDATE notas SET estado = 'E' WHERE codigo = {v_rec['id_nota']} and estado = 'N'", is_write=True)
    query_postgres = f"""SELECT distinct
                    g.prestador_cpf_cnpj,
                    substr(g.competencia::text,1,4)||'-'||substr(g.competencia::text,5,2) competencia,
                    g.valor_principal,
                    g.valor_juros,
                    g.valor_multa,
                    g.valor_correcao,
                    g.valor_cobrado,
                    g.guia_numero id
                    FROM 
                    public.guias g
                    INNER JOIN
                    declaracao_servicos_prestados_banco des ON des.prestador_cpf_cnpj = g.prestador_cpf_cnpj"""
    
    resultado_postgres = fn_002_query.query_postgres(conexao_postgres, query_postgres)
    print("Vinculando Guias a DESIF")
    #percorre a consulta
    v_cnt = 0
    for v_rec in tqdm(resultado_postgres, desc="Vinculando DESIF"):

        query_mysql = f"""select * from cadastro where cnpj = '{v_rec['prestador_cpf_cnpj']}'"""
        cadastro = fn_002_query.query_mysql(conexao_mysql, query_mysql)
        if not cadastro:
            query_mysql = f"""select * from cadastro where cpf = '{v_rec['prestador_cpf_cnpj']}'"""
            cadastro = fn_002_query.query_mysql(conexao_mysql, query_mysql)
        
        query_mysql = f"""SELECT declaracao_id FROM `desif_declaracao` where cadastro_id = {cadastro[0]['codigo']} and periodo = '{v_rec['competencia']}' and imposto_devido = {v_rec['valor_principal']} """
        declaracao = fn_002_query.query_mysql(conexao_mysql, query_mysql)
        if declaracao:
            insert = f"""INSERT INTO 
                        `desif_guia_declaracoes`
                        (
                        `codguia`,
                        `coddeclaracao`) 
                        VALUE (
                        {v_rec['id']},
                        {declaracao[0]['declaracao_id']});"""
            fn_002_query.query_mysql(conexao_mysql, insert, is_write=True)


    query_postgres = f"""SELECT distinct
                        g.prestador_cpf_cnpj,
                        substr(g.competencia::text,1,4)||'-'||substr(g.competencia::text,5,2) competencia,
                        g.valor_principal,
                        g.valor_juros,
                        g.valor_multa,
                        g.valor_correcao,
                        g.valor_cobrado,
                        g.guia_numero as id
                        FROM 
                        public.guias g
                        INNER JOIN
                        declaracao_servicos_prestados_cartorio des ON des.prestador_cpf_cnpj = g.prestador_cpf_cnpj"""
    
    resultado_postgres = fn_002_query.query_postgres(conexao_postgres, query_postgres)
    print("Vinculando Guias a Cartórios")
    #percorre a consulta
    v_cnt = 0
    for v_rec in tqdm(resultado_postgres, desc="Vinculando Cartórios"):

        query_mysql = f"""select * from cadastro where cnpj = '{v_rec['prestador_cpf_cnpj']}'"""
        cadastro = fn_002_query.query_mysql(conexao_mysql, query_mysql)
        if not cadastro:
            query_mysql = f"""select * from cadastro where cpf = '{v_rec['prestador_cpf_cnpj']}'"""
            cadastro = fn_002_query.query_mysql(conexao_mysql, query_mysql)
        
        query_mysql = f"""SELECT id FROM `cartorio_declaracoes` where cadastro_id = {cadastro[0]['codigo']} and periodo = '{v_rec['competencia']}' AND (imposto = {v_rec['valor_principal']} OR imposto = {v_rec['valor_principal']}-0.01 OR imposto = {v_rec['valor_principal']}+0.01) """
        declaracao = fn_002_query.query_mysql(conexao_mysql, query_mysql)
        if declaracao:
            insert = f"""INSERT INTO 
                        `cartorio_guia_declaracoes`
                        (
                        `codguia`,
                        `coddeclaracao`) 
                        VALUE (
                        {v_rec['id']},
                        {declaracao[0]['id']});"""
            fn_002_query.query_mysql(conexao_mysql, insert, is_write=True)


    query_postgres = f"""SELECT distinct
        sp.id id_dec,
        g.guia_numero id_guia,
        g.situacao_pagamento,
        g.situacao_guia
        FROM 
        public.psene sp
        INNER JOIN guias g ON g.competencia::text = concat(sp.ano::text,lpad(sp.mes::text,2,'0')) and g.prestador_cpf_cnpj = sp.prestador_cpf_cnpj and g.valor_principal = sp.valor_issqn
        UNION
        SELECT distinct
        sp.id id_dec,
        g.guia_numero id_guia,
        g.situacao_pagamento,
        g.situacao_guia
        FROM 
        public.psene sp
        INNER JOIN guias g ON g.competencia::text = concat(sp.ano::text,lpad(sp.mes::text,2,'0')) and g.prestador_cpf_cnpj = sp.prestador_cpf_cnpj and g.valor_principal = sp.valor_issqn_retido"""
    
    resultado_postgres = fn_002_query.query_postgres(conexao_postgres, query_postgres)
    print("Vinculando Guias a Cartórios")
    #percorre a consulta
    v_cnt = 0
    for v_rec in tqdm(resultado_postgres, desc="Vinculando Notas Prestadas"):


        insert = f"""INSERT INTO 
                        `guia_prestadas`
                        (
                        `codguia`,
                        `codnota`) 
                        VALUE (
                        {v_rec['id_guia']},
                        {v_rec['id_dec']});"""
        fn_002_query.query_mysql(conexao_mysql, insert, is_write=True)

        if v_rec['situacao_pagamento'] == 'PENDENTE':
            fn_002_query.query_mysql(conexao_mysql, f"UPDATE notas_prestadas SET estado = 'N' WHERE codigo = {v_rec['id_dec']}" , is_write=True)
       
    query_postgres = f"""SELECT distinct
        sp.id id_dec,
        g.guia_numero id_guia
        FROM 
        public.declaracao_servicos_tomados sp
        INNER JOIN guias g ON g.competencia::text = sp.competencia::text and g.prestador_cpf_cnpj = sp.tomador_cpf_cpnj and g.valor_principal = sp.valor_issqn
        """
    
    resultado_postgres = fn_002_query.query_postgres(conexao_postgres, query_postgres)
    print("Vinculando Guias a Declaracoes Tomadas")
    #percorre a consulta
    v_cnt = 0
    for v_rec in tqdm(resultado_postgres, desc="Vinculando Tomadas"):

        insert = f"""INSERT INTO 
                        `guia_tomadas`
                        (
                        `codguia`,
                        `codnota`) 
                        VALUE (
                        {v_rec['id_guia']},
                        {v_rec['id_dec']});"""
        print(insert)
        fn_002_query.query_mysql(conexao_mysql, insert, is_write=True)

    query_postgres = f"""  SELECT 
                            competencia,
                            sum(valor_issqn)valor_issqn,
                            cadastro_economico_cpf_cnpj
                            FROM 
                            public.notas_fiscais
                            where valor_issqn> 0
                            group by 
                            competencia,
                            cadastro_economico_cpf_cnpj"""
    
    resultado_postgres = fn_002_query.query_postgres(conexao_postgres, query_postgres)
    print("Vinculando Guias a Notas na Competencia")
    #percorre a consulta
    v_cnt = 0
    for v_rec in tqdm(resultado_postgres, desc="Vinculando Notas Prestadas Competencia"):
        query_guias = f"""SELECT guia_numero, competencia, prestador_cpf_cnpj FROM guias WHERE competencia = '{v_rec['competencia']}' AND prestador_cpf_cnpj = '{v_rec['cadastro_economico_cpf_cnpj']}' AND valor_principal = {v_rec['valor_issqn']}"""
        guias = fn_002_query.query_postgres(conexao_postgres, query_guias)
        if guias:
            query_notas = f"""  SELECT 
                            competencia,
                            cadastro_economico_cpf_cnpj,
                            id
                            FROM 
                            public.notas_fiscais
                            WHERE
                            competencia = '{v_rec['competencia']}' AND
                            cadastro_economico_cpf_cnpj = '{v_rec['cadastro_economico_cpf_cnpj']}'"""
            notas = fn_002_query.query_postgres(conexao_postgres, query_notas)
            for v_rec_notas in notas:

                insert = f"""INSERT INTO 
                    `guia_notas`
                    (
                    `codguia`,
                    `codnota`) 
                    VALUE (
                    {guias[0]['guia_numero']},
                    {v_rec_notas['id']});"""
                
                fn_002_query.query_mysql(conexao_mysql, insert, is_write=True)


    #Vincula as notas fiscais a guia de acordo com a competencia
    query_postgres = f"""  SELECT 
                            concat(ano::text,lpad(mes::text,2,'0'))competencia,
                            prestador_cpf_cnpj,
                            sum(valor_issqn)valor_issqn
                            FROM 
                            public.psene
                            group by
                            concat(ano::text,lpad(mes::text,2,'0')),
                            prestador_cpf_cnpj"""
                
    resultado_postgres = fn_002_query.query_postgres(conexao_postgres, query_postgres)
    print("Vinculando Guias a Notas na Competencia")
    #percorre a consulta
    v_cnt = 0
    for v_rec in tqdm(resultado_postgres, desc="Vinculando Notas Prestadas Competencia"):
        query_guias = f"""SELECT guia_numero, competencia, prestador_cpf_cnpj FROM guias WHERE competencia = '{v_rec['competencia']}' AND prestador_cpf_cnpj = '{v_rec['prestador_cpf_cnpj']}' AND valor_principal = {v_rec['valor_issqn']}"""
        guias = fn_002_query.query_postgres(conexao_postgres, query_guias)
        if guias:
            query_notas = f"""  SELECT 
                            concat(ano::text,lpad(mes::text,2,'0'))competencia,
                            prestador_cpf_cnpj,
                            id
                            FROM 
                            public.psene
                            WHERE
                            concat(ano::text,lpad(mes::text,2,'0')) = '{v_rec['competencia']}' AND
                            prestador_cpf_cnpj = '{v_rec['prestador_cpf_cnpj']}'"""
            notas = fn_002_query.query_postgres(conexao_postgres, query_notas)
            for v_rec_notas in notas:
                print(f"Vinculando Guia {guias[0]['guia_numero']} a Nota {v_rec_notas['id']}")
                insert = f"""INSERT INTO 
                    `guia_prestadas`
                    (
                    `codguia`,
                    `codnota`) 
                    VALUE (
                    {guias[0]['guia_numero']},
                    {v_rec_notas['id']});"""
                
                fn_002_query.query_mysql(conexao_mysql, insert, is_write=True)
if __name__ == "__main__":
    main()
