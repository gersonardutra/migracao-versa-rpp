import fn_002_query
import fn_001_conexoes
import fn_003_funcoes
from datetime import datetime
import threading
from tqdm import tqdm

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

        
        
    else:
        print("DELETE está desativado")

    #===========================================================Inicio: Migra Balancetes===================================================
    #Monta query de consulta ao banco da HLH
    print("Consultando Balancetes da base origem")
    query_postgres = f"""SELECT 
                            pessoa,
                            ano,
                            competencia,
                            situacao,
                            sum(base_calculo)base_calculo,
                            sum(valor_issqn)valor_issqn,
                            sum(valor_issqn_retido)valor_issqn_retido,
                            sum(valor_servicos)valor_servicos,
                            item_lista_servico
                            FROM 
                            public.view_desif
                            GROUP BY
                            pessoa,
                            ano,
                            competencia,
                            situacao,
                            item_lista_servico
                        """
    
    resultado_postgres = fn_002_query.query_postgres(conexao_postgres, query_postgres)
    print("Migrando Balancetes")


    v_cnt = 0
    #percorre a consulta
    for v_rec in tqdm(resultado_postgres, desc="Inserindo Balancetes"):
        if v_rec['situacao'] == 'ENCERRADA':
            situacao = 'E'
        else:
            situacao = 'C'
        pgc = f"""SELECT 
                    planos_id,
                    cadastro_id,
                    ano 
                    FROM `desif_planos` 
                    WHERE 
                    cadastro_id = {v_rec['pessoa']} 
                    AND ano = {v_rec['ano']} 
                    AND estado = 'A' LIMIT 1; """
        pgc = fn_002_query.query_mysql(conexao_mysql,pgc)
        if pgc:
            insert = f"""INSERT INTO 
                            `desif_declaracao`
                            (
                            `cadastro_id`,
                            `dec_saldo_inicial`,
                            `dec_saldo_final`,
                            `dec_creditos`,
                            `dec_debitos`,
                            `dec_basecalculo`,
                            `dec_imposto`,
                            `periodo`,
                            `estado`,
                            `desif_tipo`,
                            `receita`,
                            `deducoes_subtitulos`,
                            `deducoes_consolidado`,
                            `imposto_devido`,
                            `imposto_retido`,
                            `incentivo_subtitulos`,
                            `incentivo_consolidado`,
                            `credito_abater`,
                            `imposto_recolhido`,
                            `tipo_consolidado`,
                            `tipo_arredondamento`,
                            `desif_planos_id`) 
                            VALUE (
                            {v_rec['pessoa']},
                            0,
                            0,
                            0,
                            0,
                            {v_rec['base_calculo']},
                            {v_rec['valor_issqn']},
                            '{v_rec['competencia']}',
                            '{situacao}',
                            1,
                            0,
                            0,
                            0,
                            {v_rec['valor_issqn']},
                            0,
                            0,
                            0,
                            0,
                            0,
                            null,
                            null,
                            {pgc[0]['planos_id']});"""
            v_cnt = v_cnt + 1

            try:
                resultado = fn_002_query.query_mysql(conexao_mysql, insert, is_write=True)
            except:
                raise Exception(insert)



    #print (f'{v_cnt} Balancetes migrados no total')
    #============================================================Fim: Migra Balancete=======================================================


    #======================================================Início: Migra Balancete Detalhes=================================================
    #Monta query de consulta ao banco da HLH
    print("Consultando Balancetes da base origem")
    query_postgres = f"""SELECT
                            cd.pessoa,
                            codigo,
                            substr(competencia::text,1,4) ano,
                            concat(substr(competencia::text,1,4),'-',substr(competencia::text,5,2))competencia,
                            situacao,
                            base_calculo,
                            aliquota,
                            valor_issqn,
                            valor_issqn_retido,
                            valor_servicos,
                            case
                                when length(item_lista_servico::text) < 3 then lpad(concat(lpad(item_lista_servico::text,2,''),'.01'),5,'0')
                                when item_lista_servico::text = 'NaN' then '15.01'
                                when length(item_lista_servico::text) = 3 then concat('0',replace(item_lista_servico::TEXT,',','.'),'0')
                                when item_lista_servico::text = '15.1' then '15.01'
                                else '15.01'
                            end as item_lista_servico
                            FROM 
                            public.declaracao_servicos_prestados_banco desif
                            INNER JOIN public.cadastro cd On cd.cpf_cnpj = desif.prestador_cpf_cnpj

                        """
    
    resultado_postgres = fn_002_query.query_postgres(conexao_postgres, query_postgres)
    print("Migrando Detalhes de Balancetes")


    v_cnt = 0
    #percorre a consulta
    for v_rec in tqdm(resultado_postgres, desc="Inserindo Declarações de balancetes"):
        
            declaracao = fn_002_query.query_mysql(conexao_mysql,f"""SELECT declaracao_id FROM `desif_declaracao` where periodo = '{v_rec['competencia']}' and cadastro_id = {v_rec['pessoa']} limit 1""")
            declaracao = declaracao[0]['declaracao_id']

            conta = fn_002_query.query_mysql(conexao_mysql, f"""select * from desif_planos where cadastro_id = {v_rec['pessoa']} and ano = {v_rec['ano']}""")
            #print (f"""SELECT contas_id FROM `desif_contas` WHERE contas_codigo ='{v_rec['codigo']}' and planos_id = {conta[0]['planos_id']}""")
            conta = fn_002_query.query_mysql(conexao_mysql, f"""SELECT contas_id FROM `desif_contas` WHERE contas_codigo ='{v_rec['codigo']}' and planos_id = {conta[0]['planos_id']}""")
            datafim = fn_003_funcoes.ultimo_dia_do_mes(v_rec['competencia'])
            #print(datafim)
            insert = f"""INSERT INTO `desif_balancete` (
                            `declaracao_id`, 
                            `contas_id`, 
                            `balancete_saldo_inicial`, 
                            `balancete_saldo_final`, 
                            `balancete_creditos`, 
                            `balancete_debitos`, 
                            `balancete_basecalculo`, 
                            `balancete_aliquota`, 
                            `balancete_imposto`, 
                            `balancete_data_fim`, 
                            `dependencia`, 
                            `codtributacao`, 
                            `balancete_receita`, 
                            `balancete_deducoes`, 
                            `discriminacao_deducoes`, 
                            `balancete_incentivo`, 
                            `discriminacao_incentivo`, 
                            `motivo_nao_exigencia`, 
                            `processo_nao_exigencia`, 
                            `valor_issqn_retido`
                        ) VALUES (
                            {declaracao}, 
                            {conta[0]['contas_id']}, 
                            0, 
                            0,
                        0, 
                            0, 
                            {v_rec['base_calculo']}, 
                            {v_rec['aliquota']}, 
                            {v_rec['valor_issqn']}, 
                            {datafim}, 
                            null, 
                            {v_rec['item_lista_servico']}, 
                            0, 
                            0, 
                            '', 
                            0, 
                            '', 
                            '', 
                            '', 
                            ''
                        );
    """
            v_cnt = v_cnt + 1

            try:
                resultado = fn_002_query.query_mysql(conexao_mysql, insert, is_write=True)
            except:
                raise Exception(insert)
            


            print (f'{v_cnt} Balancetes migrados no total')


        
    #============================================================Fim: Migra Balancete=======================================================

if __name__ == "__main__":
    main()
