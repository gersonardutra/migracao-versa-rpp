#Funções auxiliares de migração
#Autor: Gerson Dutra
#Data: 21/12/2024

import re
import fn_002_query
import time
import sys
from datetime import datetime, timedelta
import calendar


#======================================================== Formata CFF / CNPJ ================================================================
def formatar_cgc(cgc):
    # Remove qualquer caractere não numérico
    cgc = ''.join(filter(str.isdigit, cgc))
    
    if len(cgc) == 11:
        # Formata CPF
        return f"{cgc[:3]}.{cgc[3:6]}.{cgc[6:9]}-{cgc[9:]}"
    elif len(cgc) == 14:
        # Formata CNPJ
        return f"{cgc[:2]}.{cgc[2:5]}.{cgc[5:8]}/{cgc[8:12]}-{cgc[12:]}"
    else:
        return "000.000.000-00"
#===========================================================================================================================================

#========================================================== Valida E-mail ==================================================================

def verificaemail(email):
    # Define o padrão regex para um email válido
    padrao = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    if email is None:
        return ""
    else:
        # Usa a função match para verificar se o email corresponde ao padrão
        if re.match(padrao, email):
            return email
        else:
            return "" 

#===========================================================================================================================================

#======================================================== Obtem id Pessoa ==================================================================

#função para converter o id de cadastro da HLH para o id do banco da Versa
def obtemidpessoa(id, conexao_postgres, conexao_mysql):
    select = f"""SELECT cnpj FROM nfe_pessoa WHERE codigo = {id};"""
    #print(select)
    cadastro = fn_002_query.query_postgres(conexao_postgres, select)
    #print(cadastro['cnpj'])
    if not cadastro:
        raise ValueError(f"Cadastro não encontrado = {id}")
    
    cgc = formatar_cgc(cadastro[0]['cnpj'])

    select = f"SELECT codigo FROM cadastro WHERE cnpj = '{cgc}';"
    cadastro = fn_002_query.query_mysql(conexao_mysql, select)
    
    if not cadastro:
        select = f"SELECT codigo FROM cadastro WHERE cpf = '{cgc}';"
        cadastro = fn_002_query.query_mysql(conexao_mysql, select)
        
        if not cadastro:
            raise ValueError(f"Cadastro não encontrado = {cgc}")
        
    return cadastro[0]['codigo']  # Acessando o primeiro elemento da lista de resultados
#===========================================================================================================================================

#============================================================ Coalesce =====================================================================
#encontra o primeiro valor nao nulo passado nos argumentos
def coalesce(*args):
    return next((arg for arg in args if arg is not None), '')


#===========================================================================================================================================

#========================================================= Separa endereço =================================================================
def separar_endereco(endereco):
    # Expressão para tentar separar os componentes do endereço
    padrao = r'([\w\s]+),\s*(\w+)\s*-\s*([\w\s]+)\s*-\s*([\w\s]+)\s*([\w\s]+)\s*-\s*(\w+)\s*\|\s*(\d+)(?:\s*\|\s*([\w\s]+))?'
    correspondencia = re.match(padrao, endereco)
    if not correspondencia:
        padrao= r'([\w\s]+),\s*(\d+)\s*-\s*([\w\s]+)\s*([\w\s]+)\s*-\s*(\w+)\s*\|\s*(\d+)'
        correspondencia2 = re.match(padrao, endereco)

    #print(f'Esse é o endereço recebido: {endereco}')
    if correspondencia:
        rua = coalesce(correspondencia.group(1))
        numero = coalesce(correspondencia.group(2))
        ponto_referencia = coalesce(correspondencia.group(3))
        bairro = coalesce(correspondencia.group(4))
        cidade = coalesce(correspondencia.group(5))
        estado = coalesce(correspondencia.group(6))
        cep = coalesce(correspondencia.group(7))
        complemento = coalesce(correspondencia.group(8))
        
        return {
            'Rua': rua,
            'Numero': numero,
            'Ponto de Referência': ponto_referencia,
            'Bairro': bairro,
            'Cidade': cidade,
            'Estado': estado,
            'CEP': cep,
            'Complemento': complemento
        }
    else:
        if correspondencia2:
            rua = correspondencia2.group(1)
            numero = correspondencia2.group(2)
            bairro = correspondencia2.group(3).split("       ")[0]
            cidade = correspondencia2.group(3).split("       ")[1]
            estado = correspondencia2.group(5)
            cep = correspondencia2.group(6)
        
            return {
                'Rua': rua,
                'Numero': numero,
                'Bairro': bairro,
                'Cidade': cidade,
                'Estado': estado,
                'CEP': cep
            }
        else:
            return {
                'Rua': '',
                'Numero': '',
                'Bairro': '',
                'Cidade': '',
                'Estado': '',
                'CEP': ''
            }
#===========================================================================================================================================

#================================================= Remove Caracteres Não Numéricos =========================================================
def remove_non_numeric(input_string):
    # Remove tudo que não for numérico
    numeric_string = ''.join(filter(str.isdigit, input_string))
    
    # Se a string resultante estiver vazia, substitua por '0'
    if not numeric_string:
        numeric_string = '0'
    
    return numeric_string

#===========================================================================================================================================

#================================================== Separar String por primeira virgula=====================================================
def separar_string(string):
    # Substituir apenas a primeira vírgula encontrada por um hífen
    string = string.replace(',', '-', 1)
    
    # Separar a string em duas partes pelo espaçamento
    partes = string.split("       ")
    
    # Função para identificar campos de informações separados por - ou |
    def identificar_campos(parte):
        return [campo.strip() for campo in parte.replace('|', '-').split('-')]
    
    # Identificar campos em cada parte
    parte1_campos = identificar_campos(partes[0])
    parte2_campos = identificar_campos(partes[1])
    
    return parte1_campos, parte2_campos
#===========================================================================================================================================

#===================================================== Barra de Progresso ==================================================================

def progress_bar(total, current, bar_length=50):
    progress = current / total
    block = int(bar_length * progress)
    bar = "#" * block + "-" * (bar_length - block)
    percentage = progress * 100
    sys.stdout.write(f"\r[{bar}] {percentage:.2f}% ({current}/{total})")
    sys.stdout.flush()

def run_task(total, task_function):
    for i in range(total + 1):
        progress_bar(total, i)
        time.sleep(0.1)
    task_function()
    print("\nCarregamento completo!")
#===========================================================================================================================================

#=================================================== Obtem ultimo dia do mes ===============================================================
def ultimo_dia_do_mes(data):
    ano, mes = map(int, data.split('/'))
    ultimo_dia = calendar.monthrange(ano, mes)[1]
    nova_data = datetime(ano, mes, ultimo_dia)
    return nova_data.strftime('%d/%m/%Y')
#===========================================================================================================================================

#=================================================== Retorna município migrado =============================================================
def municipio():
    
    #virgem da lapa
    #return 3171600 
    #santana do riacho
    #return 3159001
    #Rubelita
    return 3156502
#===========================================================================================================================================

#========================================================== Formata Cnae ===================================================================
def formata_cnae(codigo):
    # Converte o código para string e preenche com zeros à esquerda, se necessário
    codigo_str = str(codigo).zfill(7)
    
    # Formata o código no formato desejado
    codigo_formatado = f"{codigo_str[:4]}-{codigo_str[4]}/{codigo_str[5:]}"
    
    return codigo_formatado
#===========================================================================================================================================

#======================================================= Formata Telefone ==================================================================
def formatar_telefone(numero):
    if numero is not None:
        numero = ''.join(filter(str.isdigit, numero))  # Remove caracteres não numéricos
        if len(numero) == 10:  # Número fixo
            return f"({numero[:2]}) {numero[2:6]}-{numero[6:]}"
        elif len(numero) == 11:  # Número celular
            return f"({numero[:2]}) {numero[2:7]}-{numero[7:]}"
        else:
            return ""
    else: 
        return ''
    
#===========================================================================================================================================

from datetime import datetime, timedelta

def ultimo_dia_do_mes(data):
    # Converter a string para um objeto datetime
    data = datetime.strptime(data, "%Y-%m")
    # Adicionar um mês e subtrair um dia para obter o último dia do mês
    proximo_mes = data.replace(day=28) + timedelta(days=4)
    ultimo_dia = proximo_mes - timedelta(days=proximo_mes.day)
    return ultimo_dia.strftime("%Y-%m-%d")


import re

def extrair_endereco(endereco):
    # Padrão para endereços com complemento
    padrao_complemento = re.compile(r'^(.*?),\s*(\d+),\s*(.*?),\s*(.*?)\s*-\s*(.*?)\s*-\s*(.*?)\s*-\s*CEP:\s*([\d.-]+)$')
    
    # Padrão para endereços sem complemento
    padrao_sem_complemento = re.compile(r'^(.*?),\s*(\d+),\s*(.*?)\s*-\s*(.*?)\s*-\s*(.*?)\s*-\s*CEP:\s*([\d.-]+)$')
    
    # Padrão para endereços sem número, apenas logradouro e bairro
    padrao_sem_numero = re.compile(r'^(.*?),\s*(.*?)\s*-\s*(.*?)\s*-\s*(.*?)\s*-\s*CEP:\s*([\d.-]+)$')
    
    # Padrão para endereços sem CEP
    padrao_sem_cep = re.compile(r'^(.*?),\s*(\d+),\s*(.*?),\s*(.*?)\s*-\s*(.*?)\s*-\s*(.*?)$')

    # Padrão para endereços sem CEP
    padrao_log_cidade = re.compile(r'^(.*?),\s*(.*?)\s*-\s*(.*?)\s*-\s*(.*?)$')  # Novo padrão para logradouro, bairro, cidade e estado
    #re.compile(r'^(.*?),\s*(.*?),\s*(.*?),\s*(.*?)\s*-\s*(.*?)$')
    correspondencia = padrao_complemento.match(endereco)
    if correspondencia:
        return {
            "Logradouro": correspondencia.group(1),
            "Número": correspondencia.group(2),
            "Complemento": correspondencia.group(3),
            "Bairro": correspondencia.group(4),
            "Cidade": correspondencia.group(5),
            "Estado": correspondencia.group(6),
            "CEP": correspondencia.group(7)
        }

    correspondencia = padrao_sem_complemento.match(endereco)
    if correspondencia:
        return {
            "Logradouro": correspondencia.group(1),
            "Número": correspondencia.group(2),
            "Complemento": "",
            "Bairro": correspondencia.group(3),
            "Cidade": correspondencia.group(4),
            "Estado": correspondencia.group(5),
            "CEP": correspondencia.group(6)
        }
    
    correspondencia = padrao_sem_numero.match(endereco)
    if correspondencia:
        return {
            "Logradouro": correspondencia.group(1),
            "Número": "",
            "Complemento": "",
            "Bairro": correspondencia.group(2),
            "Cidade": correspondencia.group(3),
            "Estado": correspondencia.group(4),
            "CEP": correspondencia.group(5)
        }
    
    correspondencia = padrao_sem_cep.match(endereco)
    if correspondencia:
        return {
            "Logradouro": correspondencia.group(1),
            "Número": correspondencia.group(2),
            "Complemento": correspondencia.group(3),
            "Bairro": correspondencia.group(4),
            "Cidade": correspondencia.group(5),
            "Estado": correspondencia.group(6),
            "CEP": ""
            
        }
    
    correspondencia = padrao_log_cidade.match(endereco)
    if correspondencia:
        return {
            "Logradouro": correspondencia.group(1),
            "Número": "",
            "Complemento": "",
            "Bairro": correspondencia.group(2),
            "Cidade": correspondencia.group(3),
            "Estado": correspondencia.group(4),
            "CEP": ""
        }
    
    return {
            "Logradouro": "",
            "Número": "",
            "Complemento": "",
            "Bairro": "",
            "Cidade": "",
            "Estado": "",
            "CEP": ""
        }

def extrair_enderecov2(endereco):
    # Padrões para diferentes formatos de endereço
    padroes = [
        re.compile(r'^(.*?),\s*(\d+),\s*(.*?),\s*(.*?)\s*-\s*(.*?)\s*-\s*(.*?)\s*-\s*CEP:\s*([\d.-]+)$'),
        re.compile(r'^(.*?),\s*(\d+),\s*(.*?)\s*-\s*(.*?)\s*-\s*(.*?)\s*-\s*CEP:\s*([\d.-]+)$'),
        re.compile(r'^(.*?),\s*(.*?)\s*-\s*(.*?)\s*-\s*(.*?)\s*-\s*CEP:\s*([\d.-]+)$'),
        re.compile(r'^(.*?),\s*(\d+),\s*(.*?),\s*(.*?)\s*-\s*(.*?)\s*-\s*(.*?)$'),
        re.compile(r'^(.*?),\s*(.*?),\s*(.*?),\s*(.*?)\s*-\s*(.*?)$')  # Novo padrão para logradouro, bairro, cidade e estado
    ]

    for padrao in padroes:
        correspondencia = padrao.match(endereco)
        if correspondencia:
            grupos = correspondencia.groups()
            chaves = ["Logradouro", "Número", "Complemento", "Bairro", "Cidade", "Estado", "CEP"][:len(grupos)]
            return dict(zip(chaves, grupos))
    
    return f"Endereço no formato inesperado: {endereco}"

def extrair_cadastro_nf(cgc,conexao_postgres):
    cadastro = f"""SELECT distinct
                    cadastro_economico_pessoa,
                    cadastro_economico_inscricao_municipal,
                    cadastro_economico_inscricao_estadual,
                    cadastro_economico_razao_social,
                    cadastro_economico_nome_fantasia,
                    cadastro_economico_cpf_cnpj,
                    cadastro_economico_endereco,
                    cadastro_economico_email
                        
                FROM 
                    public.notas_fiscais
                    WHERE cadastro_economico_cpf_cnpj = '{cgc}'
                    order by cadastro_economico_inscricao_municipal limit 1"""

    cadastro = fn_002_query.query_postgres(conexao_postgres, cadastro)
    if cadastro:
        cnpj = cadastro[0]['cadastro_economico_cpf_cnpj']
        inscricao_municipal = cadastro[0]['cadastro_economico_inscricao_municipal']
        inscricao_estadual = cadastro[0]['cadastro_economico_inscricao_estadual']
        razao_social = cadastro[0]['cadastro_economico_razao_social']
        nome_fantasia = cadastro[0]['cadastro_economico_nome_fantasia']
        endereco = cadastro[0]['cadastro_economico_endereco']
        email = cadastro[0]['cadastro_economico_email']
        if not email:
            email = ""

  
    else:
        cadastro = f"""SELECT 

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
                tomador_endereco
                
                FROM 
                public.notas_fiscais
                WHERE tomador_cpf_cnpj = '{cgc}'
                order by tomador_inscricao_municipal limit 1"""

        cadastro = fn_002_query.query_postgres(conexao_postgres, cadastro)
        if cadastro:
            cnpj = cadastro[0]['tomador_cpf_cnpj']
            inscricao_municipal = cadastro[0]['tomador_inscricao_municipal']
            inscricao_estadual = cadastro[0]['tomador_inscricao_estadual']
            razao_social = cadastro[0]['tomador_razao_social']
            nome_fantasia = cadastro[0]['tomador_razao_social']
            endereco = cadastro[0]['tomador_endereco']
            email = ""
        else:
            
            cadastro = f"""SELECT 
                            prestador_cpf_cnpj,
                            prestador_nome,
                            prestador_uf,
                            prestador_municipio,
                            prestador_endereco
                            FROM 
                            public.psene
                WHERE prestador_cpf_cnpj = '{cgc}'
                limit 1"""
            
            cadastro = fn_002_query.query_postgres(conexao_postgres, cadastro)
            if cadastro:
                cnpj = cadastro[0]['prestador_cpf_cnpj']
                inscricao_municipal = ''
                inscricao_estadual = ''
                razao_social = cadastro[0]['prestador_nome']
                nome_fantasia = cadastro[0]['prestador_nome']
                endereco = cadastro[0]['prestador_endereco']
                email = ''
            else:
                cadastro = f"""SELECT 
                            tomador_cpf_cnpj,
                            tomador_nome,
                            tomador_endereco,
                            tomador_municipio,
                            tomador_email
                            
                            FROM 
                            public.psene
                WHERE tomador_cpf_cnpj = '{cgc}'
                limit 1"""
            
                cadastro = fn_002_query.query_postgres(conexao_postgres, cadastro)
                if cadastro:
                    cnpj = cadastro[0]['tomador_cpf_cnpj']
                    inscricao_municipal = ''
                    inscricao_estadual = ''
                    razao_social = cadastro[0]['tomador_nome']
                    nome_fantasia = cadastro[0]['tomador_nome']
                    endereco = cadastro[0]['tomador_endereco']
                    email = cadastro[0]['tomador_email']
                else:
                    cadastro = f"""SELECT distinct
                            prestador_cpf_cnpj,
                            prestador_nome,
                            prestador_endereco

                            FROM 
                            public.declaracao_servicos_prestados_cartorio
                            WHERE prestador_cpf_cnpj = '{cgc}'
                            limit 1"""
                
                    cadastro = fn_002_query.query_postgres(conexao_postgres, cadastro)
                    if cadastro:
                        cnpj = cadastro[0]['prestador_cpf_cnpj']
                        inscricao_municipal = ''
                        inscricao_estadual = ''
                        razao_social = cadastro[0]['prestador_nome']
                        nome_fantasia = cadastro[0]['prestador_nome']
                        endereco = cadastro[0]['prestador_endereco']
                        email = ''
                    else:
                        cadastro = f"""SELECT distinct
                                    prestador_cpf_cnpj,
                                    prestador_nome,
                                    prestador_endereco
                                    FROM 
                                    public.declaracao_servicos_prestados_banco 
                                    WHERE prestador_cpf_cnpj = '{cgc}'
                                    limit 1"""
                
                        cadastro = fn_002_query.query_postgres(conexao_postgres, cadastro)
                        if cadastro:
                            cnpj = cadastro[0]['prestador_cpf_cnpj']
                            inscricao_municipal = ''
                            inscricao_estadual = ''
                            razao_social = cadastro[0]['prestador_nome']
                            nome_fantasia = cadastro[0]['prestador_nome']
                            endereco = cadastro[0]['prestador_endereco']
                            email = ''
                        else:
                            cadastro = f"""SELECT distinct
                                    prestador_cpf_cnpj,
                                    prestador_nome,
                                    prestador_endereco
                                    FROM 
                                    public.declaracao_servicos_prestados_demais
                                    WHERE prestador_cpf_cnpj = '{cgc}'
                                    limit 1"""
                
                            cadastro = fn_002_query.query_postgres(conexao_postgres, cadastro)
                            if cadastro:
                                cnpj = cadastro[0]['prestador_cpf_cnpj']
                                inscricao_municipal = ''
                                inscricao_estadual = ''
                                razao_social = cadastro[0]['prestador_nome']
                                nome_fantasia = cadastro[0]['prestador_nome']
                                endereco = cadastro[0]['prestador_endereco']
                                email = ''
                            else:
                                cadastro = f"""SELECT distinct
 
                                        tomador_cpf_cpnj,
                                        tomador_nome,
                                        tomador_endereco
                                        
                                        FROM 
                                        public.declaracao_servicos_prestados_demais
                                    WHERE tomador_cpf_cpnj = '{cgc}'
                                    limit 1"""
                
                                cadastro = fn_002_query.query_postgres(conexao_postgres, cadastro)
                                if cadastro:
                                    cnpj = cadastro[0]['prestador_cpf_cnpj']
                                    inscricao_municipal = ''
                                    inscricao_estadual = ''
                                    razao_social = cadastro[0]['prestador_nome']
                                    nome_fantasia = cadastro[0]['prestador_nome']
                                    endereco = cadastro[0]['prestador_endereco']
                                    email = ''
                                else:
                                    cnpj = ''
                                    inscricao_municipal = ''
                                    inscricao_estadual = ''
                                    razao_social = ''
                                    nome_fantasia = ''
                                    endereco = ''
                                    email = ''


    return{
        'cnpj': cnpj,
        'inscricao_municipal': inscricao_municipal,
        'inscricao_estadual': inscricao_estadual,
        'razao_social': razao_social,
        'nome_fantasia': nome_fantasia,
        'endereco': endereco,
        'email': email
    }






