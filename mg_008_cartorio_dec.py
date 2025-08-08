import fn_002_query
import fn_001_conexoes
import fn_003_funcoes
from datetime import datetime
import threading
from tqdm import tqdm
import calendar

def main():
    deletar = True
    

    # Conecta ao banco de dados MySQL
    conexao_mysql = fn_001_conexoes.conectar_ao_mysql()

    # Conecta ao banco de dados PostgreSQL
    conexao_postgres = fn_001_conexoes.conectar_ao_postgres()

    if deletar:
        
        delete = """DELETE from cartorio_declaracao_faixas;"""
        delete = fn_002_query.query_mysql(conexao_mysql, delete, is_write=True)
        if delete > 0:
            print(f'{delete} Registros Deletados de declaração de cartorios')
        else:
            print(f'Nenhum registro deletado de declaração de cartorios')

        delete = """DELETE from cartorio_declaracoes;"""
        delete = fn_002_query.query_mysql(conexao_mysql, delete, is_write=True)
        if delete > 0:
            print(f'{delete} Registros Deletados de declaração de cartorios')
        else:
            print(f'Nenhum registro deletado de declaração de cartorios')

        
    else:
        print("DELETE está desativado")

    #Inicio: Migra Declaracoes cartorarias
    #Monta query de consulta ao banco da HLH
    print("Consultando PGC da base origem")
    query_postgres = f"""SELECT 
                            prestador_cpf_cnpj,
                            cp.pessoa,
                            substr(competencia::text,1,4)||'-'||substr(competencia::text,5,2) competencia,
                            situacao,
                            sum(base_calculo) base_calculo,
                            sum(valor_issqn) valor_issqn,
                            sum(valor_servicos) valor_servicos
                            FROM 
                            public.declaracao_servicos_prestados_cartorio sp
                            INNER JOIN cadastro cp ON cp.cpf_cnpj = sp.prestador_cpf_cnpj
                            group by
                            prestador_cpf_cnpj,
                            cp.pessoa,
                            competencia,
                            situacao"""
    
    resultado_postgres = fn_002_query.query_postgres(conexao_postgres, query_postgres)
    print("Migrando PGC")


    v_cnt = 0
    #percorre a consulta
    for v_rec in tqdm(resultado_postgres, desc="Inserindo Declaracoes"):
    

        if v_rec['situacao'] == 'ENCERRADA':
            status = 'E'
        else:
            status = 'N'




        insert = f"""INSERT INTO `cartorio_declaracoes` 
            (
                `cadastro_id`, 
                `periodo`, 
                `dia`, 
                `estado`, 
                `emolumentos`, 
                `imposto`, 
                `tipo`
            ) 
            VALUES 
            (
                {v_rec['pessoa']}, 
                '{v_rec['competencia']}', 
                NULL, 
                '{status}', 
                {v_rec['base_calculo']}, 
                {v_rec['valor_issqn']}, 
                1
            );
"""
        v_cnt = v_cnt + 1

        try:
            resultado = fn_002_query.query_mysql(conexao_mysql, insert, is_write=True)
        except:
            raise Exception(insert)

       

    print (f'{v_cnt} PGC migrados no total')
    #Fim: Migra PGC
    
    #Inicio: Migra Declarações PGC
    query_postgres = f"""SELECT 
                        prestador_cpf_cnpj,
                        cp.pessoa,
                        prestador_nome,
                        prestador_endereco,
                        substr(competencia::text,1,4)||'-'||substr(competencia::text,5,2) competencia,
                        substr(competencia::text,1,4)||'-01' precos_data,
                        substr(competencia::text,1,4) precos_ano,
                        situacao,
                        base_calculo,
                        aliquota,
                        valor_issqn,
                        valor_issqn_retido,
                        valor_servicos,
                        codigo,
                        quantidade,
                        tipo_tributacao
                        FROM 
                        public.declaracao_servicos_prestados_cartorio sp
                        INNER JOIN cadastro cp ON cp.cpf_cnpj = sp.prestador_cpf_cnpj
                        --where prestador_cpf_cnpj = '21.299.441/0001-06' and substr(competencia::text,1,4) = '2025'
                        """
                        
    
    resultado_postgres = fn_002_query.query_postgres(conexao_postgres, query_postgres)
    print("Migrando declarações Cartório")


    v_cnt = 0
    #percorre a consulta
    for v_rec in tqdm(resultado_postgres, desc="Inserindo declarações de Cartório"):

        select = f"""SELECT id FROM `cartorio_declaracoes`  where cadastro_id = {v_rec['pessoa']} and periodo = '{v_rec['competencia']}';"""
        select = fn_002_query.query_mysql(conexao_mysql, select)

        select_servico = f"""SELECT cart_servico_id FROM `cartorio_servico` WHERE cart_servico_codigo = '{v_rec['codigo']}' and estado = 'A'"""
        select_servico = fn_002_query.query_mysql(conexao_mysql, select_servico)
        if not select_servico:
            select_servico = f"""SELECT cart_servico_id FROM `cartorio_servico` WHERE cart_servico_codigo = '{v_rec['codigo']}'"""
            select_servico = fn_002_query.query_mysql(conexao_mysql, select_servico)

        select_precos = f"""SELECT precos_id FROM `cartorio_precos` where estado = 'A' and precos_data = '{v_rec['competencia']}'"""
        select_precos = fn_002_query.query_mysql(conexao_mysql, select_precos)
        if not select_precos:
            select_precos = f"""SELECT precos_id FROM `cartorio_precos` where estado = 'A' and precos_data = '{v_rec['precos_data']}'"""
            select_precos = fn_002_query.query_mysql(conexao_mysql, select_precos)
            if not select_precos:
                select_precos = f"""SELECT precos_id FROM `cartorio_precos` where estado = 'A' and substring(precos_data,1,4) = '{v_rec['precos_ano']}'"""
                select_precos = fn_002_query.query_mysql(conexao_mysql, select_precos)
        
        select_faixa = f"""SELECT faixas_id FROM `cartorio_faixas` where precos_id = {select_precos[0]['precos_id']} and cart_servico_id = {select_servico[0]['cart_servico_id']}"""
        debug_faixa = select_faixa
        #print(f"Debug faixa: {debug_faixa}")
        select_faixa = fn_002_query.query_mysql(conexao_mysql, select_faixa)
        if not select_faixa:
            print(f"Não foi possível encontrar faixa para o serviço {v_rec['codigo']} na competência {v_rec['competencia']}")
            print(f"Debug faixa: {debug_faixa}")
            


        insert = f"""INSERT INTO `cartorio_declaracao_faixas` 
                        (
                            `declaracoes_id`, 
                            `faixas_id`, 
                            `emolumentos_unitario`, 
                            `quantidade`, 
                            `quantidade_desconto_50`, 
                            `quantidade_desconto_75`, 
                            `quantidade_desconto_80`, 
                            `quantidade_desconto_90`, 
                            `quantidade_sem_imposto`, 
                            `justificativa_isencao`, 
                            `quantidade_total`, 
                            `emolumentos_total`, 
                            `aliquota`, 
                            `imposto`
                        ) 
                        VALUES 
                            (
                            {select[0]['id']}, 
                            {select_faixa[0]['faixas_id']}, 
                            {v_rec['base_calculo']}, 
                            {v_rec['quantidade']}, 
                            0, 
                            0, 
                            0, 
                            0, 
                            0, 
                            '', 
                            {v_rec['quantidade']}, 
                            {v_rec['quantidade']*v_rec['base_calculo']}, 
                            {v_rec['aliquota']}, 
                            {v_rec['valor_issqn']}
                        );
"""
        v_cnt = v_cnt + 1
        try:
            resultado = fn_002_query.query_mysql(conexao_mysql, insert, is_write=True)
        except:
            raise Exception(insert)
            return False


    print (f'{v_cnt} Declarações de PGC migrados no total')
    #Fim: Migra declarações PGC
if __name__ == "__main__":
    main()
