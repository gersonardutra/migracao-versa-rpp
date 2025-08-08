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
        
        delete = """DELETE from desif_balancete;"""
        delete = fn_002_query.query_mysql(conexao_mysql, delete, is_write=True)
        if delete > 0:
            print(f'{delete} Registros Deletados de declaração de balancete')
        else:
            print(f'Nenhum registro deletado de declaração de balancete')

        delete = """DELETE from desif_declaracao;"""
        delete = fn_002_query.query_mysql(conexao_mysql, delete, is_write=True)
        if delete > 0:
            print(f'{delete} Registros Deletados de Balancetes')
        else:
            print(f'Nenhum registro deletado de Balancetes')

        delete = """DELETE from desif_contas;"""
        delete = fn_002_query.query_mysql(conexao_mysql, delete, is_write=True)
        if delete > 0:
            print(f'{delete} Registros Deletados de declaração de PGC')
        else:
            print(f'Nenhum registro deletado de declaração de PGC')

        delete = """DELETE from desif_planos;"""
        delete = fn_002_query.query_mysql(conexao_mysql, delete, is_write=True)
        if delete > 0:
            print(f'{delete} Registros Deletados de PGC')
        else:
            print(f'Nenhum registro deletado de PGC')
    else:
        print("DELETE está desativado")

    #Inicio: Migra PGC
    #Monta query de consulta ao banco da HLH
    print("Consultando PGC da base origem")
    query_postgres = f"""SELECT distinct
                        prestador_cpf_cnpj,
                        substr(competencia::text,1,4)::bigint ano,
                        cd.pessoa,
                        CONCAT('01/',substr(competencia::text,1,4)) anomesiniciocompetencia,
                        CONCAT('12/',substr(competencia::text,1,4)) anomesfincompetencia
                        
                        FROM 
                        public.declaracao_servicos_prestados_banco dec
                        INNER JOIN cadastro cd ON dec.prestador_cpf_cnpj = cd.cpf_cnpj ;"""
    
    resultado_postgres = fn_002_query.query_postgres(conexao_postgres, query_postgres)
    print("Migrando PGC")


    v_cnt = 0
    #percorre a consulta
    for v_rec in tqdm(resultado_postgres, desc="Inserindo Planos de contas"):
    


        status = 'A'
        comp_fim = '31/12' + v_rec['anomesiniciocompetencia']
        comp_ini = '01/01' + v_rec['anomesiniciocompetencia']


        insert = f"""INSERT INTO 
                `desif_planos`
                (
                `cadastro_id`,
                `ano`,
                `periodo`,
                `inicio`,
                `fim`,
                `estado`,
                `desif_tipo`) 
                VALUE (
                {v_rec['pessoa']},
                {v_rec['ano']},
                'anual',
                '{comp_ini}',
                '{comp_fim}',
                '{status}',
                '1.0');"""
        v_cnt = v_cnt + 1

        try:
            resultado = fn_002_query.query_mysql(conexao_mysql, insert, is_write=True)
        except:
            raise Exception(insert)

       

    print (f'{v_cnt} PGC migrados no total')
    #Fim: Migra PGC

    #Inicio: Migra Declarações PGC
    query_postgres = f"""SELECT distinct
                        prestador_cpf_cnpj,
                        cd.pessoa,
                        prestador_nome,
                        substr(competencia::text,1,4) ano,
                        situacao,
                        codigo::text,
                        descricao,
                        case
                            when length(item_lista_servico::text) < 3 then lpad(concat(lpad(item_lista_servico::text,2,''),'.01'),5,'0')
                            when item_lista_servico::text = 'NaN' then '15.01'
                            when length(item_lista_servico::text) = 3 then concat('0',replace(item_lista_servico::TEXT,',','.'),'0')
                            when item_lista_servico::text = '15.1' then '15.01'
                            else '15.01'
                        end as item_lista_servico
                        FROM 
                        public.declaracao_servicos_prestados_banco desif
                        INNER JOIN public.cadastro cd On cd.cpf_cnpj = desif.prestador_cpf_cnpj"""
    
    resultado_postgres = fn_002_query.query_postgres(conexao_postgres, query_postgres)
    print("Migrando declarações PGC")


    v_cnt = 0
    #percorre a consulta
    for v_rec in tqdm(resultado_postgres, desc="Inserindo declarações de PGCs"):

        cosif = f"""SELECT 
                    `cosif_id`,
                    `cosif_conta`,
                    `cosif_subconta`,
                    `cosif_nomeconta`,
                    `cosif_anomes_ini`,
                    `cosif_anomes_fim`,
                    `cosif_natureza`,
                    `cosif_atributos`,
                    `cosif_grupo`
                    FROM 
                    `desif_cosif_subcontas` 
                    WHERE cosif_subconta = '{v_rec['codigo']}'; """
        cosif = fn_002_query.query_mysql(conexao_mysql, cosif)

        #if not cosif:
           # raise Exception(f"Conta {v_rec['codigo']} não encontrada na tabela desif_cosif_subcontas")

        if v_rec['item_lista_servico'] == '':
            servico = '15.01'
        else:
            servico = v_rec['item_lista_servico']
            
        select_servico =f"""SELECT 
                `id`,
                `lc116`,
                `descricao`,
                `aliquota`
                FROM 
                `desif_servicos`Where lc116 = {servico};"""
        servico = fn_002_query.query_mysql(conexao_mysql, select_servico)

        descricao = v_rec['descricao']
        descricao = descricao.replace("'","\\'")

        try:
            cosif_conta = cosif[0]['cosif_conta']
        except IndexError:
            cosif_conta = ''
        
        conta = fn_002_query.query_mysql(conexao_mysql, f"""select * from desif_planos where cadastro_id = {v_rec['pessoa']} and ano = {v_rec['ano']}""")

        insert = f"""INSERT INTO 
                `desif_contas`
                (
                `planos_id`,
                `cosif_conta`,
                `cosif_subconta`,
                `contas_codigo`,
                `descricao`,
                `planos_tributado`,
                `planos_servico`,
                `conta_arq`,
                `conta_compl`,
                `nome`,
                `planos_tributacao`,
                `conta_supe`,
                `conta_supe_id`) 
                VALUE (
                {conta[0]['planos_id']},
                '{v_rec['codigo']}',
                '{v_rec['codigo']}',
                '{v_rec['codigo']}',
                '{descricao}',
                'sim',
                '{servico[0]['id']}',
                '',
                '',
                '',
                NULL,
                '',
                NULL);"""
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
