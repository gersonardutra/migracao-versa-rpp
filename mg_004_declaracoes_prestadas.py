import fn_002_query
import fn_001_conexoes
import fn_003_funcoes
import threading
from datetime import datetime
from tqdm import tqdm

def main():
    deletar = True
    deleteonly = False
    formatacnae = False
    municipio = fn_003_funcoes.municipio() #configura municipio migrado com o codigo do ibge

    # Conecta ao banco de dados MySQL
    conexao_mysql = fn_001_conexoes.conectar_ao_mysql()

    # Conecta ao banco de dados PostgreSQL
    conexao_postgres = fn_001_conexoes.conectar_ao_postgres()

    if deletar:

        delete = """DELETE from notas_prestadas_servicos where 1;"""
        delete = fn_002_query.query_mysql(conexao_mysql, delete, is_write=True)
        if delete > 0:
            print(f'{delete} Registros Deletados de notas_prestadas_servicos')
        else:
            print(f'Nenhum registro deletado')
        print("Consultando NFSE da base origem")

        delete = """DELETE from notas_prestadas where 1;"""
        delete = fn_002_query.query_mysql(conexao_mysql, delete, is_write=True)
        if delete > 0:
            print(f'{delete} Registros Deletados de notas_prestadas')
        else:
            print(f'Nenhum registro deletado')
        print("Consultando NFSE da base origem")
        if deleteonly:
            return 0
    else:
        print("DELETE está desativado")
    #Monta query de consulta ao banco da HLH
    query_postgres = """
        SELECT distinct
        replace(codigo,'/','')codigo,
        concat(ano::text,'-',lpad(mes::text,2,'0'))competencia,
        concat(lpad(dia::text,2,'0'),'-',lpad(mes::text,2,'0'),'-',ano::text)::date emissao,
        case
        when verificacao = 'NaN' then ''
        else verificacao
        end as verificacao,
        mes,
        ano,
        prestador_cpf_cnpj,
        prestador_nome,
        prestador_uf,
        prestador_municipio,
        case
        when prestador_endereco = 'NaN' then ''
        else prestador_endereco
        end as prestador_endereco,
        tomador_cpf_cnpj,
        tomador_nome,
        case
        when tomador_endereco = 'NaN' then ''
        else tomador_endereco
        end as tomador_endereco,
        tomador_municipio,
        tomador_email,
        exigibilidade,
        serie,
        incidencia,
        item,
        dia,
        numero_documento,
        case
        when descricao_servico = 'NaN' then ''
        else descricao_servico
        end as descricao_servico,
        valor_servicos,
        valor_liquido,
        base,
        aliquota,
        valor_issqn,
        valor_issqn_retido,
        estado,
        case
        when data_estado = 'NaN' then null
        else data_estado::date
        end as data_estado,
        id,
        cp.pessoa id_prestador,
        ct.pessoa id_tomador
        FROM 
        public.psene sp
        left JOIN cadastro cp ON cp.cpf_cnpj = sp.prestador_cpf_cnpj
        LEFT JOIN cadastro ct ON ct.cpf_cnpj = sp.tomador_cpf_cnpj
    """
    
    resultado_postgres = fn_002_query.query_postgres(conexao_postgres, query_postgres)

    #seta o contador de resultados de migração pra a contagem obtida
    v_cnt = 0
    #print("Migrando NFSE")
    #percorre a consulta
    for v_rec in tqdm(resultado_postgres, desc="Inserindo Notas Prestadas"):

        #converte os ids do prestador e tomador para o novo id registrado na base da versa
        prestador_id = v_rec['id_prestador']
        if not prestador_id:
            cadastro = fn_002_query.query_mysql(conexao_mysql, f"select codigo from cadastro where cnpj = '{v_rec['prestador_cpf_cnpj']}'")
            if cadastro:
                prestador_id = cadastro[0]['codigo']
            else:
                prestador_id = 'NULL'
                raise Exception(f"Prestador {v_rec['prestador_cpf_cnpj']} não encontrado na tabela cadastro")
        tomador_id = v_rec['id_tomador']


        #define o status da NFSE
        #if v_rec["status"] in (0,4,6,7,8,10,12):
            #status = 'E'
        #elif v_rec["status"] in (1,3,11):
            #status = 'N'
        #elif v_rec["status"] in (2,9,13):
            #status = 'C'
        #else:
            #status = 'E'

        if v_rec["estado"] in ('DECLARADO'):
            status = 'E'
        elif v_rec["estado"] in ('LANCADO'):
            status = 'N'
        else:
            status = 'C'


        tributacao = 'TM'
        

        #trata os dados de endereços, tenta separar a string do endereço para preencher os campos
        enderecotom = fn_003_funcoes.extrair_endereco(v_rec['tomador_endereco'])
        enderecoprestador = fn_003_funcoes.extrair_endereco(v_rec['prestador_endereco'])
        nometomador = v_rec["tomador_nome"]
        nometomador = nometomador.replace("'", "\\'", 1)
        


        query = f"""
            INSERT INTO 
            `notas_prestadas`
            (
            `codigo`,
            `codemissor`,
            `tomador_cnpjcpf`,
            `nome_tomador`,
            `numero`,
            `data`,
            `estado`,
            `valortotal`,
            `valoriss`,
            `valordeducoes`,
            `valoracrescimos`,
            `basecalculo`,
            `credito`,
            `aliqinss`,
            `valorirrf`,
            `aliqirrf`,
            `total_retencao`,
            `cofins`,
            `contribuicaosocial`,
            `aliq_percentual`,
            `natureza_operacao`,
            `desconto_condicionado`,
            `desconto_incondicionado`,
            `outras_retencoes`,
            `periodo_tributacao`,
            `valorinss`,
            `guia_avulsa`,
            `data_declaracao`) 
            VALUE (
            '{v_rec['id']}',
            {prestador_id},
            '{v_rec['tomador_cpf_cnpj']}',
            '{nometomador}',
            {v_rec['numero_documento']},
            '{v_rec['emissao']}',
            '{status}',
            {v_rec['valor_servicos']},
            {v_rec['valor_issqn']},
            0,
            0,
            {v_rec['valor_servicos']},
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            {v_rec['aliquota']},
            '{tributacao}',
            0,
            0,
            0,
            '{v_rec['competencia']}',
           0,
            'N',
            '{v_rec['data_estado']}');
            """
        

       
        try:
            resultado = fn_002_query.query_mysql(conexao_mysql, query, is_write=True)
        except:
            
            print(query)
 
            resultado = fn_002_query.query_mysql(conexao_mysql, query, is_write=True)
            raise resultado.get_mysql_exception

        v_cnt = v_cnt + 1

    print(f'{v_cnt} Notas migradas')


    select = f"""SELECT distinct
                replace(codigo,'/','')codigo,
                concat(ano::text,'-',lpad(mes::text,2,'0'))competencia,
                case
                when verificacao = 'NaN' then ''
                else verificacao
                end as verificacao,
                mes,
                ano,
                prestador_cpf_cnpj,
                prestador_nome,
                prestador_uf,
                prestador_municipio,
                case
                when prestador_endereco = 'NaN' then ''
                else prestador_endereco
                end as prestador_endereco,
                tomador_cpf_cnpj,
                tomador_nome,
                case
                when tomador_endereco = 'NaN' then ''
                else tomador_endereco
                end as tomador_endereco,
                tomador_municipio,
                tomador_email,
                exigibilidade,
                serie,
                incidencia,
                case
                when length(item::text) < 3 then lpad(concat(lpad(item::text,2,''),'.01'),5,'0')
                when length(item::text) = 3 then concat('0',replace(item::TEXT,',','.'),'0')
                when item::text = 'NaN' then '99.99'
                else lpad(replace(item::TEXT,',','.'),5,'0')
                end as item,
                dia,
                numero_documento,
                case
                when descricao_servico = 'NaN' then ''
                else descricao_servico
                end as descricao_servico,
                valor_servicos,
                valor_liquido,
                base,
                aliquota,
                valor_issqn,
                valor_issqn_retido,
                estado,
                case
                    when data_estado = 'NaN' then null
                    else data_estado::date
                    end as data_estado,
                id,
                cp.pessoa id_prestador,
                ct.pessoa id_tomador
                FROM 
                public.psene sp
                left JOIN cadastro cp ON cp.cpf_cnpj = sp.prestador_cpf_cnpj
                LEFT JOIN cadastro ct ON ct.cpf_cnpj = sp.tomador_cpf_cnpj"""
                        
    #trata os dados da discriminação
    resultado_postgres = fn_002_query.query_postgres(conexao_postgres, select)
    v_cnt = 0
    for v_rec in tqdm(resultado_postgres, desc="Inserindo Notas Serviços"):

       

        
        #trata os dados da discriminação
        discriminacao = v_rec['descricao_servico']
        try:
                discriminacao = discriminacao.replace('"','')
                discriminacao = discriminacao.replace("'","")
        except IndexError:
                discriminacao = ''

        discriminacao = discriminacao[:2000]

        cnae = fn_002_query.query_mysql(conexao_mysql,f"""SELECT cnae FROM `integ_cnaexlc116` where lc116 = '{v_rec['item']}' order by id limit 1""")
        try:
            cnae = cnae[0]['cnae']
        except:
            cnae = '9999999'
            v_rec['item'] = '99.99'



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
        if v_rec['valor_issqn_retido'] > 0:
            v_rec['valor_issqn'] = 0
        
        insert = f"""INSERT INTO 
                        `notas_prestadas_servicos`
                        (
                        `codnota`,
                        `codservico`,
                        `basecalculo`,
                        `deducoes`,
                        `acrescimos`,
                        `desconto_condicionado`,
                        `desconto_incondicionado`,
                        `issretido`,
                        `iss`,
                        `discriminacao`,
                        `aliquota`,
                        `cnaexlc116`) 
                        VALUE (
                        {v_rec['id']},
                        {codcnae},
                        {v_rec['valor_servicos']},
                        0,
                        0,
                        0,
                        0,
                        {v_rec['valor_issqn_retido']},
                        {v_rec['valor_issqn']},
                        '{discriminacao}',
                        {v_rec['aliquota']},
                        '{v_rec['item']}');"""
           # Conecta ao banco de dados MySQL
        conexao_mysql = fn_001_conexoes.conectar_ao_mysql()
        try:
            resultado = fn_002_query.query_mysql(conexao_mysql, insert, is_write=True)
        except:
            print(insert)
            return 0

        v_cnt = v_cnt + 1
            
    print(f'{v_cnt} Serviços vinculados a notas')
if __name__ == "__main__":
    main()