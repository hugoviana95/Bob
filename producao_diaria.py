import gspread
import pandas as pd
import os.path
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from read_sheets import read_sheets


def busca_codigos_servicos(contrato, cookie):

    url = 'https://sirtecba.gpm.srv.br/gpm/geral/consulta_servico.php'
    header = {
        'cookie': cookie
        }
    data = {
        "data_inicial_b": datetime.strptime('10/08/2022 00:00', '%d/%m/%Y %H:%M'),
        "data_final_b": datetime.strptime('10/08/2022 23:59', '%d/%m/%Y %H:%M'),
        'submit': 'Pesquisar',
        'contrato': contrato,
        'avancaTodos': 'Todas',
        'h_tot': '99999'
        }

    resposta = requests.post(url, headers = header, data = data)
    conteudo = resposta.content
    site = BeautifulSoup(conteudo,'html.parser') #Captura o html da página
    lista_codigos = []
    try:
        tabela = site.find('table', attrs={'class': 'tbl_100_c_5p_0h_lin zebra'})
        cod = tabela.findAll('font', attrs={'color': 'blue'})
        for i in cod:
            lista_codigos.append(i.text)
    except:
        lista_codigos.append('sem registro de atividades')

    return(lista_codigos)


def busca_relatorio_servico(codigo_serv, cookie):
    url = 'https://sirtecba.gpm.srv.br/gpm/geral/relatorio_servico.php'
    header = {
        'cookie': cookie
    }

    data = {
        'cod_srv': codigo_serv
    }

    resposta = requests.post(url, headers=header, data=data)

    conteudo = resposta.content
    site = BeautifulSoup(conteudo,'html.parser') #Captura o html da página
    lista_atividades = []

    tabela = site.find('table', attrs={'class': 'tbl_head'})  # REGISTRA A TURMA QUE REALIZOU A ATIVIDADE
    linhas = tabela.findAll('tr')
    coluna = linhas[3].findAll('td')
    turma = coluna[3].text
    if turma[0:9] == 'BAR - KIT':
        turma = turma[0:12]
    elif turma[0:3] == 'BAR':
        turma = turma[0:11]
    elif turma[0:3] == 'KIT':
        turma = turma[0:6]
    elif turma[0:9] == 'SIR009527':
        turma = turma[0:13]
    elif turma[0:9] == 'SIR009527':
        turma = turma[0:13]
    elif turma[0:9] == 'SIR009387':
        turma = turma[0:18]
    elif turma[0:9] == 'SIR009199':
        turma = turma[0:20]
    elif turma[0:9] == 'SIR009504':
        turma = turma[0:17]
    elif turma[0:9] == 'SIR009094':
        turma = turma[0:20]
    elif turma[0:9] == 'SIR009400':
        turma = turma[0:20]
    elif turma[0:9] == 'SIR009089':
        turma = turma[0:18]
    else:
        turma = turma[0:5]

    coluna = linhas[2].findAll('td') # REGISTRA O SUPERVISOR
    supervisor = coluna[3].text

    coluna = linhas[11].findAll('td') # REGISTRA A DATA
    data = coluna[3].text
    data = data[0:10]

    coluna = linhas[14].findAll('td') # REGISTRA O NÚMERO DA OBRA
    obra = coluna[1].text
    if obra[0] == 'B':
        obra = obra[0:9]


    tabelas = site.findAll('table', attrs={'class': 'tbl_itens'}) # REGISTRA AS ATIVIDADES
    tabela = tabelas[2]
    linhas = tabela.findAll('tr')
    if len(linhas) == 2: # REGISTRO EM CASO DE NENHUMA ATIVIDADE CADASTRADA PELA TURMA
        lista_atividades.append([turma,  obra, data[0:10], int(codigo_serv), '', 'sem atividades cadastradas', 0.0, 0.0, 0.0])
        return(lista_atividades)
    else:
        linhas.pop(0)
        linhas.pop(-1)
        for i in linhas:
            coluna = i.findAll('td')
            atividade = coluna[0].text
            if atividade[0:3] == 'SIR':
                codigo_ativ = atividade[0:10]
                atividade = atividade[13:]
            else:
                codigo_ativ = atividade[0:11]
                atividade = atividade[14:]
            quantidade = coluna[2].text
            valor_unit = coluna[3].text
            valor_tot = coluna[4].text
            lista_atividades.append([turma, obra, data, int(codigo_serv), codigo_ativ, atividade, float(quantidade.replace('.','').replace(',','.')), float(valor_unit.replace('.','').replace(',','.')), float(valor_tot.replace('.','').replace(',','.'))])
    return(lista_atividades)


def atualiza_servicos():

    cookie = 'PHPSESSID=lfadonfifjj9inmnfkg5308d6c'
    lista_serv = []
    lista_serv.append(busca_codigos_servicos('11', cookie))  # CONTRATO SUDOESTE
    lista_serv.append(busca_codigos_servicos('12', cookie))  # CONTRATO OESTE

    print(lista_serv)


    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

    """Shows basic usage of the Sheets API.
    Prints values from a sample spreadsheet.
    """
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token_sheets.json'):
        creds = Credentials.from_authorized_user_file('token_sheets.json', SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token_sheets.json', 'w') as token:
            token.write(creds.to_json())
    
    gc = gspread.authorize(creds)
    planilha = gc.open_by_key('1kthDa5_Ed7mfpnye-xpnTzZXkFnBB4veVyxeM9Ti8ds')
    producao = planilha.worksheet("produção")
    dados = producao.get_all_records()
    df = pd.DataFrame(dados)

    dados = read_sheets('1kthDa5_Ed7mfpnye-xpnTzZXkFnBB4veVyxeM9Ti8ds', 'produção', 'UNFORMATTED_VALUE')
    df = pd.DataFrame(dados, columns = dados.pop(0))

    linhas_excluir = []
    for i in lista_serv:
        for n in i:
            if int(n) in df['Nº do Serviço'].tolist():
                linhas_excluir.append(df.index[df['Nº do Serviço'] == int(n)].tolist())
    print(linhas_excluir)

    for i in linhas_excluir:
        producao.delete_rows(i[0]+2, i[-1]+2)
        for n in range(len(linhas_excluir)):
            if i[0] < linhas_excluir[n][0]:
                for j in range(len(linhas_excluir[n])):
                    linhas_excluir[n][j] = linhas_excluir[n][j] - len(i)
                


    planilha = pd.DataFrame([], columns=['TURMA', 'OBRA', 'DATA', 'Nº DO SERVIÇO', 'COD. ATIVIDADE', 'ATIVIDADE', 'QUANTIDADE', 'VALOR UNITÁRIO', 'VALOR TOTAL'])
    for i in lista_serv:
        for n in i:
            if n != 'sem registro de atividades':
                    print(n)
                    df_atividades = pd.DataFrame(busca_relatorio_servico(n, cookie), columns=['TURMA', 'OBRA', 'DATA', 'Nº DO SERVIÇO', 'COD. ATIVIDADE', 'ATIVIDADE', 'QUANTIDADE', 'VALOR UNITÁRIO', 'VALOR TOTAL'])
                    print(df_atividades)
                    planilha = pd.merge(planilha, df_atividades, how = 'outer')

    planilha = planilha.values.tolist()
    producao.append_rows(planilha, value_input_option = 'USER_ENTERED')



if __name__ == "__main__":
    atualiza_servicos()