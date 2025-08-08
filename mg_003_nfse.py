import fn_002_query
import fn_001_conexoes
import fn_003_funcoes
from tqdm import tqdm
from datetime import datetime

def main():
    #======================================================== Configurações ================================================================
    deletar = True #Se Habilitado, deleta os dados migrados
    delete_batch = 5000 #Tamanho do batch para delete
    batch = 300 #Tamanho do batch para insert
    formatacnae = False #formata ou não o codigo cnae
    municipio = fn_003_funcoes.municipio() #configura municipio migrado com o codigo do ibge
    #=======================================================================================================================================

    #======================================================== Seta Conexões ================================================================
    # Conecta ao banco de dados PostgreSQL
    conexao_postgres = fn_001_conexoes.conectar_ao_postgres(True)
    # Conecta ao banco de dados MySQL
    conexao_mysql = fn_001_conexoes.conectar_ao_mysql(True)
    #=======================================================================================================================================
    
    #==================================================== Início - Trata Delete ============================================================
    #Limpa a base destino e o controle
    if deletar:
        if delete_batch > 0:
            print(f'Delete Batch ativado!')
            delete_total = delete_batch
            total = 0
            with tqdm(total=delete_total, desc="Deletando registros") as pbar:
                while delete_total > 0:
                    delete = f"""DELETE FROM notas WHERE tipoemissao = 'migracao' LIMIT {delete_batch};"""
                    delete_total = fn_002_query.query_mysql(conexao_mysql, delete, is_write=True)
                    total = total + delete_total
                    pbar.update(delete_total)
            print(f'{total} Registros Deletados')
        else:
            delete = f"""DELETE FROM notas WHERE tipoemissao = 'migracao' LIMIT {delete_batch};"""
            delete_total = fn_002_query.query_mysql(conexao_mysql, delete, is_write=True)
            fn_002_query.query_postgres(conexao_postgres,"truncate migracao.controle_nfse")
            if delete_total > 0:
                print(f'{delete_total} Registros Deletados')
            else:
                print(f'Nenhum registro deletado')

        delete = """DELETE FROM notas_servicos;"""
        delete = fn_002_query.query_mysql(conexao_mysql, delete, is_write=True)
        if delete > 0:
            print(f'{delete} Registros Deletados de notas_servicos')
        else:
            print(f'Nenhum registro deletado de notas_servicos')
        print("Consultando NFSE da base origem")

        delete = """DELETE FROM notas_dados;"""
        delete = fn_002_query.query_mysql(conexao_mysql, delete, is_write=True)
        if delete > 0:
            print(f'{delete} Registros Deletados de notas_dados')
        else:
            print(f'Nenhum registro deletado de notas_dados')
        print("Consultando NFSE da base origem")
    else:
        print("DELETE está desativado")
    #===================================================== Fim - Trata Delete ==============================================================


    #=================================================== Início - Migra NFS'e ==============================================================

    #conta as notas migradas no controle para retomar a migração


    #Monta query de consulta ao banco da HLH
    query_postgres = """
        SELECT 
            id,
            numero,
            codigo_verificacao,
            dt_emissao::date,
            dt_hr_emissao::timestamp,
            substr(competencia::text,1,4)||'-'||substr(competencia::text,5,2) competencia,
            case
            when nf_substituida_id = '\\N' then null
            else nf_substituida_id 
            end as nf_substituida_id,
            case
            when nf_substituida_numero = '\\N' then null
            else nf_substituida_numero 
            end as nf_substituida_numero,
            exigibilidade_issqn,
            ce_regime_especial_tributacao,
            optante_simples_nacional,
            incentivo_fiscal,
            case
            when nf_rps_numero = '\\N' then null
            else nf_rps_numero 
            end as nf_rps_numero,
            case
            when nf_substituida_numero = '\\N' then null
            else nf_substituida_numero 
            end as nf_substituida_numero,
            case
            when nf_rps_codigo = '\\N' then null
            else nf_rps_codigo 
            end as nf_rps_codigo,
            case
            when nf_rps_serie = '\\N' then null
            else nf_rps_serie 
            end as nf_rps_serie,
            case
            when nf_rps_tipo = '\\N' then null
            else nf_rps_tipo 
            end as nf_rps_tipo,
            case
            when nf_rps_dt_emissao = '\\N' then null
            else nf_rps_dt_emissao 
            end as nf_rps_dt_emissao,
            case
            when outras_informacoes = '\\N' then null
            else outras_informacoes 
            end as outras_informacoes,
            valor_servicos,
            valor_deducoes,
            case
            when length(item_lista_servico::text) < 3 then lpad(concat(lpad(item_lista_servico::text,2,''),'.01'),5,'0')
            when length(item_lista_servico::text) = 3 then concat('0',replace(item_lista_servico::TEXT,',','.'),'0')
            else lpad(replace(item_lista_servico::TEXT,',','.'),5,'0')
            end as item_lista_servico,
            case
            when cnae = 'NaN' then null
            else cnae 
            end as cnae,
            case
            when codigo_tributacao_municipio = '\\N' then null
            else codigo_tributacao_municipio 
            end as codigo_tributacao_municipio,
            base_calculo,
            aliquota_servicos,
            valor_issqn,
            valor_liquido_nota,
            case
            when outras_retencoes = '\\N' then 0
            else outras_retencoes::double precision 
            end as outras_retencoes,
            case
            when valor_credito = '\\N' then 0
            else valor_credito::double precision 
            end as valor_credito,
            issqn_retido,
            valor_issqn_retido,
            case
            when valor_desconto_incondicionado = '\\N' then 0
            else valor_desconto_incondicionado::double precision 
            end as valor_desconto_incondicionado,
            case
            when valor_desconto_condicionado = '\\N' then 0
            else valor_desconto_condicionado::double precision 
            end as valor_desconto_condicionado,
            discriminacao,
            municipio_prestacao_servico,
            local_prest.nome as local_prestacao,
            local_prest.uf as local_prestacao_uf,
            pais_prestacao_servico,
            municipio_execucao_servico,
            pais_execucao_servico,
            cadastro_economico,
            cadastro_economico_pessoa,
            case
            when cadastro_economico_inscricao_municipal = 'NaN' then null
            else cadastro_economico_inscricao_municipal 
            end as cadastro_economico_inscricao_municipal,
            case
            when cadastro_economico_inscricao_estadual = 'NaN' then null
            else cadastro_economico_inscricao_estadual 
            end as cadastro_economico_inscricao_estadual,
            cadastro_economico_razao_social,
            cadastro_economico_nome_fantasia,
            cadastro_economico_cpf_cnpj,
            cadastro_economico_endereco,
            cadastro_economico_email,
            tomador,
            tomador_pessoa,
            case
            when tomador_inscricao_municipal = '\\N' then null
            when tomador_inscricao_municipal = 'NaN' then null
            else tomador_inscricao_municipal
            end as tomador_inscricao_municipal,
            case
            when tomador_inscricao_estadual = '\\N' then null
            when tomador_inscricao_estadual = 'NaN' then null
            else tomador_inscricao_estadual
            end as tomador_inscricao_estadual,
            tomador_razao_social,
            tomador_cpf_cnpj,
            tomador_endereco,
            tomador_pais,
            mun_tom.nome as tomador_municipio,
            tomador_email,
            tomador_substituto_tributario,
            tomador_optante_sn,
            case
            when tomador_estrangeiro_documento = '\\N' then null
            else tomador_estrangeiro_documento
            end as tomador_estrangeiro_documento,
            case
            when tomador_endereco_endereco = '\\N' then null
            else tomador_endereco_endereco
            end as tomador_endereco_endereco,
            case
            when tomador_endereco_numero = '\\N' then null
            else tomador_endereco_numero 
            end as tomador_endereco_numero,
            case
            when tomador_endereco_complemento = '\\N' then null
            else tomador_endereco_complemento
            end as tomador_endereco_complemento,
            case
            when tomador_endereco_bairro = '\\N' then null
            else tomador_endereco_bairro 
            end as tomador_endereco_bairro,
            case
            when tomador_endereco_uf = '\\N' then null
            else tomador_endereco_uf 
            end as tomador_endereco_uf,
            case
            when tomador_endereco_cep = '\\N' then null
            else tomador_endereco_cep 
            end as tomador_endereco_cep,
            case
            when tomador_telefone = '\\N' then null
            else tomador_telefone 
            end as tomador_telefone,
            case
            when intermediario_servico_razao_social = '\\N' then null
            else intermediario_servico_razao_social 
            end as intermediario_servico_razao_social,
            case
            when intermediario_servico_insricao_municipal = '\\N' then null
            else intermediario_servico_insricao_municipal 
            end as intermediario_servico_insricao_municipal,
            case
            when intermediario_servico_cnpj = '\\N' then null
            else intermediario_servico_cnpj 
            end as intermediario_servico_cnpj,
            case
            when obra = '\\N' then null
            else obra 
            end as obra,
            case
            when obra_matricula_cei = '\\N' then null
            else obra_matricula_cei 
            end as obra_matricula_cei,
            case
            when obra_art = '\\N' then null
            else obra_art 
            end as obra_art,
            case
            when obra_valor = '\\N' then 0
            else obra_valor:: double precision
            end as obra_valor,
            estado,
            case
            when estado_motivo = 'NaN' then null
            else estado_motivo
            end as estado_motivo,
            case
            when nf_motivo_cancelamento = '\\N' then null
            else nf_motivo_cancelamento
            end as nf_motivo_cancelamento,
            ce_regime_recolhimento,
            case
            when informacao_complementar = 'NaN' then null
            else informacao_complementar
            end as informacao_complementar,
            pis::double precision,
            cofins::double precision,
            inss::double precision,
            ir::double precision,
            csll::double precision,
            icms_lei_12741,
            iss_lei_12741,
            ipi_lei_12741,
            iof_lei_12741,
            pis_lei_12741,
            pasep_lei_12741,
            cofins_lei_12741,
            cide_lei_12741,
            xml
            FROM 
            public.notas_fiscais
            left JOIN cidade mun_tom ON mun_tom.codibge::bigint = notas_fiscais.tomador_municipio
            left join cidade local_prest ON local_prest.codibge::bigint = notas_fiscais.municipio_execucao_servico
            order by id
            
    """
    
    # Realiza o SELECT com a consulta montada em query_postgres
    resultado_postgres = fn_002_query.query_postgres(conexao_postgres, query_postgres)

    #seta o contador de resultados de migração pra a contagem obtida
    values = ''
    values_dados = ''
    commit = 0
    print("NFSE")
    #percorre a consulta
    v_cnt = 0
    for v_rec in tqdm(resultado_postgres, desc="Migrando NFSE"):

        if v_rec['estado'] == 'CANCELADA':
            status = 'C'
        elif v_rec['estado'] == 'VALIDA':
            status ='N'
        else:
            status = 'N'
        
        if len(v_rec['cadastro_economico_cpf_cnpj'])<14:
            avulsa = 'S'
        else:
            avulsa = 'N'
        
        if v_rec['item_lista_servico'] in('03.05','07.02','07.04','07.05','07.09','07.10','07.11','07.12','07.16','07.17','07.18','11.01','11.02','11.04','12.01','12.02','12.03','12.04','12.05','12.06','12.07','12.08','12.09','12.10','12.11','12.12'):
            if v_rec['municipio_execucao_servico'] != '3138674':
                tributacao = 'TF'
            else:
                tributacao = 'TM'
        else:
            tributacao = 'TM'
        if v_rec['optante_simples_nacional'] == 'SIM':
            declaracao_prestador = '3'
        else:
            declaracao_prestador = '1'

        if v_rec["valor_issqn_retido"] > 0:
            v_rec["valor_issqn"] = 0

        #monta o batch
        tomador_nome = fn_003_funcoes.coalesce(v_rec["tomador_razao_social"],'')
        tomador_nome = tomador_nome.replace("'", "\\'", 1)

        endereco_tomador = fn_003_funcoes.extrair_endereco(v_rec['tomador_endereco'])
        tomador_cidade = endereco_tomador["Cidade"]
        tomador_cidade = tomador_cidade.replace("'", "\\'", 1)
        values = values + f"""(
            {v_rec["id"]},
            '{fn_003_funcoes.coalesce(v_rec["numero"],'')}',
            '{v_rec["codigo_verificacao"]}',
            '{v_rec["dt_hr_emissao"]}',
            {v_rec["cadastro_economico_pessoa"]},
            {v_rec["tomador_pessoa"]},
            '{v_rec["nf_rps_numero"]}',
            '{v_rec["nf_rps_dt_emissao"]}',
            '{tomador_nome}',
            '{v_rec["tomador_cpf_cnpj"]}',
            '{fn_003_funcoes.coalesce(v_rec["tomador_inscricao_municipal"],'')}',
            '{fn_003_funcoes.coalesce(v_rec["tomador_inscricao_estadual"],'')}',
            Null,
            '{endereco_tomador["Logradouro"]}',
            '{endereco_tomador["Número"]}',
            '{endereco_tomador["Complemento"]}',
            '{endereco_tomador["Bairro"]}',
            '{endereco_tomador["CEP"]}',
            '{tomador_cidade}',
            '{endereco_tomador["Estado"]}',
            '{v_rec["tomador_email"]}',
            '',
            '{v_rec["outras_informacoes"]}',
            {v_rec["valor_servicos"]},
            {v_rec["valor_deducoes"]},
            0,
            {v_rec["base_calculo"]},
            {v_rec["valor_issqn"]},
            {v_rec["valor_issqn_retido"]},
            {v_rec["inss"]},
            {v_rec["cofins"]},
            {v_rec["csll"]},
            0,
            {v_rec["ir"]},
            0,
            0,
            {v_rec['outras_retencoes'] +v_rec['pis']+v_rec['inss']+v_rec['cofins']+v_rec['csll']+v_rec['ir']+v_rec['valor_issqn_retido']},
            {v_rec['valor_credito']},
            {v_rec["pis"]},
            '{status}',
            'migracao',
            '{v_rec["nf_motivo_cancelamento"]}',
            {v_rec["aliquota_servicos"]},
            '{tributacao}',
            {v_rec["valor_desconto_condicionado"]},
            {v_rec["valor_desconto_incondicionado"]},
            {v_rec["outras_retencoes"]},
            '{v_rec["local_prestacao_uf"]}',
            '{v_rec["local_prestacao"]}',
            NULL,
            '{v_rec['competencia']}',
            'N',
            6,
            'N',
            'Brasil',
            'Brasil',
            'NAO',
            '{avulsa}',
            '{v_rec["cadastro_economico_cpf_cnpj"]}',
            '{v_rec["cadastro_economico_inscricao_municipal"]}',
            '{v_rec["cadastro_economico_inscricao_estadual"]}',
            '',
            '{v_rec["cadastro_economico_razao_social"]}',
            '{v_rec["cadastro_economico_nome_fantasia"]}',
            '{v_rec["cadastro_economico_endereco"]}',
            '',
            '1',
             {declaracao_prestador},
            '',
            NULL,
            'N',
            '',
            0,
            '',
            '',
            '',
            '',
            '',
            '{v_rec["cadastro_economico_email"]}'
            ),"""
        if {v_rec['tomador_telefone']} is not None and {v_rec['tomador_telefone']} != '':
            telefone_tomador = fn_003_funcoes.formatar_telefone(v_rec['tomador_telefone'])
            values_dados = values_dados + f"""(
                                {v_rec["id"]},
                                '{telefone_tomador}'
                                ),"""
            

        v_cnt = v_cnt + 1

        if v_cnt % batch == 0:
            
            query = f"""
                INSERT INTO `notas`
                (
                `codigo`,                        -- nfe.id
                `numero`,                        -- nfe.id
                `codverificacao`,                -- nfe.codverificador
                `datahoraemissao`,               -- nfe.dataemissao
                `codemissor`,                    -- prestador.pessoa (id_prestador)
                `codtomador`,                    -- tomador.pessoa (id_tomador)
                `rps_numero`,                    -- nfe.codrps
                `rps_data`,                      -- nfe.dataemissao
                `tomador_nome`,                  -- nfe.nometomador
                `tomador_cnpjcpf`,               -- None
                `tomador_inscrmunicipal`,        -- nfe.inscricaomunicipaltomador
                `tomador_inscrestadual`,         -- nfe.inscricaoestadualtomador
                `tomador_endereco`,              -- nfe.enderecotomador
                `tomador_logradouro`,            -- nfe.enderecotomador
                `tomador_numero`,                -- nfe.enderecotomador
                `tomador_complemento`,           -- nfe.enderecotomador
                `tomador_bairro`,                -- nfe.enderecotomador
                `tomador_cep`,                   -- nfe.enderecotomador
                `tomador_municipio`,             -- nfe.enderecotomador
                `tomador_uf`,                    -- nfe.enderecotomador
                `tomador_email`,                 -- nfe.emailtomador
                `discriminacao`,                 -- nfe.informacoesadicionais
                `observacao`,                    -- nfe.informacoesadicionais
                `valortotal`,                    -- nfe.valorservico
                `valordeducoes`,                 -- nfe.valordeducao
                `valoracrescimos`,               -- nfe.valordesccondicionado
                `basecalculo`,                   -- nfe.valorbase
                `valoriss`,                      -- nfe.valoriss
                `issretido`,                     -- nfe.valorissretido
                `valorinss`,                     -- nfe.valorinss
                `cofins`,                        -- nfe.valorcofins
                `contribuicaosocial`,            -- nfe.valorcsll
                `aliqinss`,                      -- nfe.aliquotainss
                `valorirrf`,                     -- nfe.valorir
                `aliqirrf`,                      -- nfe.aliquotairrf
                `deducao_irrf`,                  -- nfe.baseirrf
                `total_retencao`,                -- nfe.valortotalretencoes
                `credito`,                       -- nfe.valoroutro1
                `pispasep`,                      -- nfe.valorpis
                `estado`,                        -- nfe.status
                `tipoemissao`,                   -- None
                `motivo_cancelamento`,           -- nfe.datacancelamento
                `aliq_percentual`,               -- nfe.aliquotaiss
                `natureza_operacao`,             -- nfe.natureza
                `desconto_condicionado`,         -- nfe.valordesccondicionado
                `desconto_incondicionado`,       -- nfe.valordescincondicionado
                `outras_retencoes`,              -- nfe.valoroutraretencao
                `natureza_operacao_uf`,          -- nfe.municipioprestacao
                `natureza_operacao_municipio`,   -- nfe.municipiotributacao
                `nota_substituida`,              -- None
                `periodo_tributacao`,            -- nfe.competencia
                `guia_avulsa`,                   -- nfe.guiaemitida
                `layout`,                        -- None
                `tomador_nacionalidade_tipo`,    -- None
                `tomador_nacionalidade_pais`,    -- nfe.paisconsumoexportacao
                `natureza_operacao_pais`,        -- nfe.paisprestacao
                `processo_administrativo`,       -- None
                `nota_avulsa`,                   -- nfe.notaavulsa
                `emissor_cgc`,                   -- None
                `emissor_inscrmunicipal`,        -- nfe.inscricaomunicipalprestador
                `emissor_inscrestadual`,         -- nfe.inscricaoestadualprestador
                `emissor_pispasep`,              -- None
                `emissor_razaosocial`,           -- nfe.nomeprestador
                `emissor_nome`,                  -- nfe.nomeprestador
                `emissor_endereco`,              -- nfe.enderecoprestador
                `emissor_telefone`,              -- nfe.telefoneprestador
                `emissor_codtipo`,               -- nfe.tipoemitente
                `emissor_codtipodeclaracao`,     -- None
                `emissor_regime_tributacao`,     -- nfe.simplesprestador
                `emissor_classificacao`,         -- None
                `emissor_isentoiss`,             -- nfe.naodemonstrarinss
                `emissor_logradouro`,            -- nfe.enderecoprestador
                `emissor_numero`,                -- nfe.enderecoprestador
                `emissor_complemento`,           -- nfe.enderecoprestador
                `emissor_bairro`,                -- nfe.enderecoprestador
                `emissor_cep`,                   -- nfe.enderecoprestador
                `emissor_municipio`,             -- nfe.enderecoprestador
                `emissor_uf`,                    -- nfe.enderecoprestador
                `emissor_email`                  -- nfe.emailprestador
                ) VALUES"""
            
            values = values[:-1] #tratamento para remover a ultima virgula dos values
            query = query + values #monta batch
            
            query_dados = f"""INSERT INTO 
                                `notas_dados`
                                (
                                `codigo`,
                                `tomador_telefone`
                                ) 
                                VALUES"""
            values_dados = values_dados[:-1]
            query_dados = query_dados + values_dados


            conexao_mysql = fn_001_conexoes.conectar_ao_mysql()
            conexao_postgres = fn_001_conexoes.conectar_ao_postgres()
            try:
                fn_002_query.query_mysql(conexao_mysql, query, is_write=True)
                fn_002_query.query_mysql(conexao_mysql, query_dados, is_write=True)
            except:
                print(query)
                return 0
            values_dados = ''
            values_controle = ''
            values = ''
            
            if (len(resultado_postgres) - commit) > batch:
                commit = commit + batch

            if (len(resultado_postgres) - commit) < batch:
                batch = (len(resultado_postgres) - commit)
                v_cnt = 0 
    #===================================================== Fim - Migra NFS'e ===============================================================
     
    #============================================= Início - Migra Notas Serviços ===========================================================

    select = """SELECT 
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
                    discriminacao
                    FROM 
                    public.view_nfse ;
            """
                        
    #trata os dados da discriminação
    resultado_postgres = fn_002_query.query_postgres(conexao_postgres, select)
    v_cnt = 0
    values = ''
    commit = 0
    for v_rec in tqdm(resultado_postgres, desc="Migrando Notas Serviços"):
    


        
        #trata os dados da discriminação
        discriminacao = v_rec["discriminacao"]
        discriminacao = discriminacao.replace('"','')
        discriminacao = discriminacao.replace("'","")
        discriminacao = discriminacao[:2000]

        #trata cnae
        if v_rec['cnae'] is not None:
            if formatacnae:
                cnae = fn_003_funcoes.formata_cnae(v_rec['cnae'])
            else:
                cnae = v_rec['cnae']
                cnae = cnae.replace('.0','')
                cnae = cnae.replace('.1','')
                cnae = cnae.replace('.2','')
                
        else:
            cnae = fn_002_query.query_mysql(conexao_mysql,f'SELECT cnae FROM `integ_cnaexlc116` where lc116 = "{v_rec['item_lista_servico']}" order by id limit 1')

            try:
                cnae = cnae[0]['cnae']
                #print(cnae)
            except:
                print(v_rec['id'],"---",v_rec['item_lista_servico'])
            #print(v_rec['codserv'])

        codcnae_query = f"""SELECT 
                    `codigo`,
                    `codcategoria`,
                    `codservico`
                    FROM 
                    `servicos`
                    WHERE codservico = '{cnae}'"""
        codcnae = fn_002_query.query_mysql(conexao_mysql,codcnae_query)
        if not codcnae:
            cnae = fn_002_query.query_mysql(conexao_mysql,f'SELECT cnae FROM `integ_cnaexlc116` where lc116 = "{v_rec['item_lista_servico']}" order by id limit 1')
            try:
                #print(cnae)
                cnae = cnae[0]['cnae']
                cnae = cnae[:7]
            except:
                print(v_rec['id'],"---",v_rec['item_lista_servico'])
            codcnae_query = f"""SELECT 
                    `codigo`,
                    `codcategoria`,
                    `codservico`
                    FROM 
                    `servicos`
                    WHERE codservico = '{cnae}'"""
            codcnae = fn_002_query.query_mysql(conexao_mysql,codcnae_query)
            if not codcnae:
                print(f"""cnae não encontrado 2 {cnae}""")
        try:
            codcnae = codcnae[0]['codigo']
        except:
            codcnae = 'NULL'


        if v_rec['valor_issqn_retido'] > 0:
            v_rec['valor_issqn'] = 0
        
        if v_rec['aliquota_servicos'] == 0 and v_rec['valor_issqn'] > 0:
            v_rec['aliquota_servicos'] = round((v_rec['valor_issqn'] / v_rec['base_calculo']) * 100,2)
        elif v_rec['aliquota_servicos'] == 0 and v_rec['valor_issqn_retido'] > 0:
            v_rec['aliquota_servicos'] = round((v_rec['valor_issqn_retido'] / v_rec['base_calculo']) * 100,2)

        #se não tiver cnae, seta o padrao
        #prepara valores do insert
        values = values + f"""(
                        {v_rec['id']},
                        {codcnae},
                        {v_rec['base_calculo']},
                        0,
                        0,
                        0,
                        0,
                        {v_rec['valor_issqn_retido']},
                        {v_rec['valor_issqn']},
                        '{discriminacao}',
                        {v_rec['aliquota_servicos']},
                        '{v_rec['item_lista_servico']}'),"""
        
        v_cnt = v_cnt + 1

        #monta insert
        if v_cnt % batch == 0:
            insert = f"""INSERT INTO 
                        `notas_servicos`
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
                        VALUE"""
            
            values = values[:-1]#tratamento para remover a ultima virgula dos values

            insert = insert + values #monta batch
            
            conexao_mysql = fn_001_conexoes.conectar_ao_mysql()
            try:
                resultado = fn_002_query.query_mysql(conexao_mysql, insert, is_write=True)
            except:
                print(insert)
                return 0
            #print(f'{v_cnt} Balancetes migrados')
            values = ''
            commit = commit + batch
            if (len(resultado_postgres) - commit) < batch:
                batch = (len(resultado_postgres) - commit)
                v_cnt = 0 
    #=============================================== Fim - Migra Notas Serviços ============================================================

    #Ajusta notas que não tem serviço vinculado para receber a aliquota
    query = """INSERT INTO `notas_servicos`
            (`codnota`, 
            `codservico`, 
            `basecalculo`, 
            `iss`, 
            `aliquota`, 
            `cnaexlc116`)
            SELECT 
            n.codigo,
            540,
            n.basecalculo,
            round((n.basecalculo * (n.aliq_percentual/100)),2),
            n.aliq_percentual,
            '99.99' 
            FROM `notas` n left join notas_servicos ns ON ns.codnota = n.codigo 
            WHERE ns.codnota is null"""
    fn_002_query.query_mysql(conexao_mysql,query)
    fn_002_query.query_mysql(conexao_mysql, 'UPDATE notas set rps_numero = NULL, rps_data = NULL where rps_numero = 0', is_write=True)
    fn_002_query.query_mysql(conexao_mysql, """UPDATE notas_servicos ns LEFT JOIN integ_cnaexlc116 sv ON sv.lc116 = ns.cnaexlc116 SET ns.cnaexlc116 = '99.99' WHERE sv.lc116 IS NULL OR lc116 = ''""", is_write=True)
    fn_002_query.query_mysql(conexao_mysql, """UPDATE notas_servicos SET codservico = 540 WHERE codservico IS NULL""", is_write=True)


    print(f'{v_cnt} Total Serviços vinculados a notas')
if __name__ == "__main__":
    main()
