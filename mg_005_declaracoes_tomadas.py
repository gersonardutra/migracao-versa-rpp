import fn_002_query
import fn_001_conexoes
import fn_003_funcoes
import threading
from datetime import datetime
from tqdm import tqdm

def main():
    deletar = True
    formatacnae = False
    municipio = fn_003_funcoes.municipio() #configura municipio migrado com o codigo do ibge

    # Conecta ao banco de dados MySQL
    conexao_mysql = fn_001_conexoes.conectar_ao_mysql()

    # Conecta ao banco de dados PostgreSQL
    conexao_postgres = fn_001_conexoes.conectar_ao_postgres()

    if deletar:

        delete = """DELETE from notas_tomadas_servicos where 1;"""
        delete = fn_002_query.query_mysql(conexao_mysql, delete, is_write=True)
        if delete > 0:
            print(f'{delete} Registros Deletados de notas_tomadas_servicos')
        else:
            print(f'Nenhum registro deletado')
        print("Consultando NFSE da base origem")

        delete = """DELETE from notas_tomadas where 1;"""
        delete = fn_002_query.query_mysql(conexao_mysql, delete, is_write=True)
        if delete > 0:
            print(f'{delete} Registros Deletados de notas_tomadas')
        else:
            print(f'Nenhum registro deletado')
        print("Consultando NFSE da base origem")
    else:
        print("DELETE está desativado")
    #Monta query de consulta ao banco da HLH
    query_postgres = """
        SELECT distinct
            id,
            case
            when prestador_cpf_cnpj = 'NaN' then ''
            else prestador_cpf_cnpj
            end as prestador_cpf_cnpj,
            cp.pessoa as codigo_prestador,
            prestador_nome,
            tomador_cpf_cpnj,
            ct.pessoa as codigo_tomador,
            tomador_nome,
            tomador_endereco,
            substr(competencia::text,1,4)||'-'||substr(competencia::text,5,2) competencia,
            (dia::text || '/' || substr(competencia::text,5,2) || '/' || substr(competencia::text,1,4))::date dataemissao,
            situacao_nfse,
            situacao_des,
            item_lista_servico as serv,
            case
            when length(item_lista_servico::text) < 3 then lpad(concat(lpad(item_lista_servico::text,2,''),'.01'),5,'0')
            when length(item_lista_servico::text) = 3 then concat('0',replace(item_lista_servico::TEXT,',','.'),'0')
            when item_lista_servico::text = 'NaN' then '99.99'
            else lpad(replace(item_lista_servico::TEXT,',','.'),5,'0')
            end as item_lista_servico,
            base_calculo,
            aliquota,
            case
            when descricao_servico = 'NaN' then ''
            else descricao_servico
            end as descricao_servico,
            valor_issqn,
            valor_issqn_retido,
            case
            when valor_deducao_material = 'NaN' then 0
            else valor_deducao_material::double precision
            end as valor_deducao_material,
            valor_servicos,
            dia,
            numero,
            serie,
            exigibilidade_issqn,
            cp.endereco_completo,
            case
            when cp.inscricao_municipal = 'NaN' then ''
            else cp.inscricao_municipal
            end as inscricao_municipal
            FROM 
            public.declaracao_servicos_tomados sp
            LEFT JOIN cadastro cp ON cp.cpf_cnpj = sp.prestador_cpf_cnpj
            LEFT JOIN cadastro ct ON ct.cpf_cnpj = sp.tomador_cpf_cpnj
    """
    
    resultado_postgres = fn_002_query.query_postgres(conexao_postgres, query_postgres)

    #seta o contador de resultados de migração pra a contagem obtida
    v_cnt = 0
    #print("Migrando NFSE")
    #percorre a consulta
    for v_rec in tqdm(resultado_postgres, desc="Inserindo Notas Prestadas"):

        #converte os ids do prestador e tomador para o novo id registrado na base da versa
        prestador_id = v_rec['codigo_prestador']
        tomador_id = v_rec['codigo_tomador']


        #define o status da NFSE
        #if v_rec["status"] in (0,4,6,7,8,10,12):
            #status = 'E'
        #elif v_rec["status"] in (1,3,11):
            #status = 'N'
        #elif v_rec["status"] in (2,9,13):
            #status = 'C'
        #else:
            #status = 'E'

        if v_rec["situacao_nfse"] in ('VALIDA'):
            status = 'E'
        elif v_rec["situacao_nfse"] in ('CANCELADA'):
            status = 'C'
        else:
            status = 'N'


        tributacao = 'TM'
        

        #trata os dados de endereços, tenta separar a string do endereço para preencher os campos
        #enderecotom = fn_003_funcoes.extrair_endereco(v_rec['tomador_endereco'])
        if v_rec['endereco_completo']:
            enderecoprestador = fn_003_funcoes.extrair_endereco(v_rec['endereco_completo'])
        else:
            enderecoprestador = {
            "Logradouro": "",
            "Número": "",
            "Complemento": "",
            "Bairro": "",
            "Cidade": "",
            "Estado": "",
            "CEP": ""
        }
        #nometomador = v_rec["tomador_nome"]
        #nometomador = nometomador.replace("'", "\\'", 1)
        
        if v_rec['aliquota'] == 0:
            if v_rec['valor_issqn_retido'] > 0:
                aliquota = (v_rec['valor_issqn'] / v_rec['valor_servicos']) * 100
                v_rec['valor_issqn'] = 0
            elif v_rec['valor_issqn'] > 0:
                aliquota = (v_rec['valor_issqn'] / v_rec['valor_servicos']) * 100
            else:
                aliquota = 0
        else:
            aliquota = v_rec['aliquota']
        
        if v_rec['valor_issqn_retido'] > 0:
            v_rec['valor_issqn'] = 0
        #query_nota = f"""SELECT * FROM `notas` WHERE emissor_cgc = '{v_rec['prestador_cpf_cnpj']}' and numero = '{ v_rec['numero']}' limit 1"""
        #print(query_nota)
        query_nota =  fn_002_query.query_mysql(conexao_mysql, f"""SELECT * FROM `notas` WHERE emissor_cgc = '{v_rec['prestador_cpf_cnpj']}' and numero = '{ v_rec['numero']}' limit 1""")
        if query_nota:
            codnota = query_nota[0]['codigo']
        else:
            codnota = 'NULL'
        query = f"""
            INSERT INTO `notas_tomadas`(
                `id`, 
                `codcadastro`, 
                `cpfcnpj_prestador`, 
                `codnota`,
                `numero`, 
                `periodo`, 
                `estado`, 
                `valorservicos`, 
                `valoriss`, 
                `valorissretido`, 
                `guia_avulsa`, 
                `codservico`, 
                `razao`, 
                `simples`, 
                `deducao`, 
                `desconto`, 
                `base`, 
                `aliq`, 
                `aliq_aproximada`, 
                `inscricao`, 
                `uf_prestador`, 
                `municipio_prestador`, 
                `data_emissao`, 
                `data_liquidacao`, 
                `periodo_liquidacao`, 
                `local_tributacao`, 
                `nacionalidade_pais`, 
                `codigo_verificacao`, 
                `exigibilidade`, 
                `serie`, 
                `obra`, 
                `codigo_obra`, 
                `descricao_obra`, 
                `data_declaracao`, 
                `data_remocao`, 
                `prest_codtipo`, 
                `prest_inscrestadual`, 
                `prest_email`, 
                `prest_telefone`, 
                `prest_cep`, 
                `prest_logradouro`, 
                `prest_numero`, 
                `prest_complemento`, 
                `prest_bairro`, 
                `cnaexlc116`, 
                `nfse_nacional`)
            VALUES
            (
                {v_rec['id']}, 
                {v_rec['codigo_tomador']}, 
                '{v_rec['prestador_cpf_cnpj']}',
                {codnota},
                {v_rec['numero']}, 
                '{v_rec['competencia']}', 
                '{status}', 
                {v_rec['base_calculo']}, 
                {v_rec['valor_issqn']}, 
                {v_rec['valor_issqn_retido']}, 
                'N', 
                '', 
                '{fn_003_funcoes.coalesce(v_rec['prestador_nome'],'')}', 
                'N', 
                {v_rec['valor_deducao_material']}, 
                0, 
                {v_rec['valor_servicos']}, 
                {v_rec['aliquota']}, 
                {v_rec['aliquota']}, 
                '{fn_003_funcoes.coalesce(v_rec['inscricao_municipal'],'')}',
                '{enderecoprestador['Estado']}', 
                '{enderecoprestador['Cidade']}', 
                '{v_rec['dataemissao']}', 
                '{v_rec['dataemissao']}',
                '{v_rec['competencia']}', 
                'TM', 
                'N', 
                '', 
                '', 
                '{v_rec['serie']}', 
                'N', 
                NULL, 
                '', 
                '{v_rec['dataemissao']}', 
                NULL, 
                11, 
                '', 
                '',
                '', 
                '{enderecoprestador['CEP']}', 
                '{enderecoprestador['Logradouro']}', 
                '{enderecoprestador['Número']}',  
                "{enderecoprestador['Complemento']}", 
                '{enderecoprestador['Bairro']}',
                '', 
                'N')"""
        

       
        try:
            resultado = fn_002_query.query_mysql(conexao_mysql, query, is_write=True)
        except:
            
            print(query)
 
            resultado = fn_002_query.query_mysql(conexao_mysql, query, is_write=True)
            raise resultado.get_mysql_exception

        v_cnt = v_cnt + 1

    print(f'{v_cnt} Notas migradas')


    select = f"""SELECT distinct
                    id,
                    case
                    when prestador_cpf_cnpj = 'NaN' then ''
                    else prestador_cpf_cnpj
                    end as prestador_cpf_cnpj,
                    cp.pessoa as codigo_prestador,
                    prestador_nome,
                    tomador_cpf_cpnj,
                    ct.pessoa as codigo_tomador,
                    tomador_nome,
                    tomador_endereco,
                    substr(competencia::text,1,4)||'-'||substr(competencia::text,5,2) competencia,
                    (dia::text || '/' || substr(competencia::text,5,2) || '/' || substr(competencia::text,1,4))::date dataemissao,
                    situacao_nfse,
                    situacao_des,
                    item_lista_servico as serv,
                    case
                    when item_lista_servico::text = 'NaN' then '99.99'
                    when length(item_lista_servico::text) < 3 then lpad(concat(lpad(item_lista_servico::text,2,''),'.01'),5,'0')
                    when length(item_lista_servico::text) = 3 then concat('0',replace(item_lista_servico::TEXT,',','.'),'0')
                    else lpad(replace(item_lista_servico::TEXT,',','.'),5,'0')
                    end as item_lista_servico,
                    base_calculo,
                    aliquota,
                    case
                    when descricao_servico = 'NaN' then ''
                    else descricao_servico
                    end as descricao_servico,
                    valor_issqn,
                    valor_issqn_retido,
                    case
                    when valor_deducao_material = 'NaN' then 0
                    else valor_deducao_material::double precision
                    end as valor_deducao_material,
                    valor_servicos,
                    dia,
                    numero,
                    serie,
                    exigibilidade_issqn,
                    cp.endereco_completo,
                    case
                    when cp.inscricao_municipal = 'NaN' then ''
                    else cp.inscricao_municipal
                    end as inscricao_municipal
                    FROM 
                    public.declaracao_servicos_tomados sp
                    LEFT JOIN cadastro cp ON cp.cpf_cnpj = sp.prestador_cpf_cnpj
                    LEFT JOIN cadastro ct ON ct.cpf_cnpj = sp.tomador_cpf_cpnj;"""
                        
    #trata os dados da discriminação
    resultado_postgres = fn_002_query.query_postgres(conexao_postgres, select)
    v_cnt = 0
    for v_rec in tqdm(resultado_postgres, desc="Inserindo Notas Serviços"):

        if v_rec['aliquota'] == 0:
            if v_rec['valor_issqn_retido'] > 0:
                aliquota = (v_rec['valor_issqn'] / v_rec['valor_servicos']) * 100
                v_rec['valor_issqn'] = 0
            elif v_rec['valor_issqn'] > 0:
                aliquota = (v_rec['valor_issqn'] / v_rec['valor_servicos']) * 100
            else:
                aliquota = 0
        else:
            aliquota = v_rec['aliquota']
        
        if v_rec['valor_issqn_retido'] > 0:
            v_rec['valor_issqn'] = 0
        #print(aliquota)
        #monta a descrição com base nos itens da tabela nfe_nfeitem
        

        discriminacao = v_rec['descricao_servico']
        discriminacao = discriminacao.replace("'", "\\'", 1)
        item_servico = v_rec['item_lista_servico']
        item_servico = item_servico.zfill(5)
        cnae = fn_002_query.query_mysql(conexao_mysql,f'SELECT cnae FROM `integ_cnaexlc116` where lc116 = "{item_servico}" order by id limit 1')

        try:
            cnae = cnae[0]['cnae']
                #print(cnae)
        except:
            print(v_rec['id'],"---",v_rec['item_lista_servico'])
            #print(v_rec['codserv'])




        #print(f"""{v_rec['cnae']}----{cnae}""")
        codcnae = f"""SELECT 
                    `codigo`,
                    `codcategoria`,
                    `codservico`
                    FROM 
                    `servicos`
                    WHERE codservico = '{cnae}'"""
        codcnae = fn_002_query.query_mysql(conexao_mysql,codcnae)
        
        try:
            codcnae = codcnae[0]['codigo']
        except:
            codcnae = 'NULL'
        insert = f""" INSERT INTO 
                        `notas_tomadas_servicos`
                        (
                        `codnota_tomada`,
                        `codservico`,
                        `basecalculo`,
                        `issretido`,
                        `iss`,
                        `discriminacao`
                        ) VALUES (%s, %s, %s, %s, %s, %s)
    """

        values = (
            v_rec['id'],
            codcnae,
            v_rec['valor_servicos'],
            v_rec['valor_issqn_retido'],
            v_rec['valor_issqn'],
            discriminacao)
        conexao_mysql = fn_001_conexoes.conectar_ao_mysql()
        fn_002_query.query_mysql(conexao_mysql, """UPDATE notas_tomadas_servicos SET codservico = 540 WHERE codservico = 0""", is_write=True)
        fn_002_query.query_mysql(conexao_mysql, """UPDATE notas_tomadas SET codservico = 540 WHERE codservico = 0""", is_write=True)
        fn_002_query.query_mysql(conexao_mysql, """UPDATE notas_tomadas ns LEFT JOIN integ_cnaexlc116 sv ON sv.lc116 = ns.cnaexlc116 SET ns.cnaexlc116 = '99.99' WHERE sv.lc116 IS NULL OR lc116 = ''""", is_write=True)


        try:
            resultado = fn_002_query.query_mysql(conexao_mysql, insert,values, is_write=True)
            fn_002_query.query_mysql(conexao_mysql, f"update notas_tomadas set codservico = {codcnae},cnaexlc116 = '{v_rec['item_lista_servico']}' where id = {v_rec['id']}", is_write=True)
        except:
            print(f"update notas_tomadas set codservico = {codcnae} where id = {v_rec['id']}")
            return 0

        v_cnt = v_cnt + 1
            
    print(f'{v_cnt} Serviços vinculados a notas')
if __name__ == "__main__":
    main()