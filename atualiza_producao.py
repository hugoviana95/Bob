from datetime import datetime, timedelta
import requests
import pandas as pd
from bs4 import BeautifulSoup
from read_sheets import read_sheets
from write_sheets import write_sheets

def buscaCodigoServico(contrato, cookie):
    dia = datetime.now()
    dia_menos_sete = dia - timedelta(15)
    dia_menos_sete = dia_menos_sete.strftime("%d/%m/%Y")
    dia = dia.strftime("%d/%m/%Y")
    url = 'https://sirtecba.gpm.srv.br/gpm/geral/consulta_servico.php'
    header = {
        'cookie': cookie
        }
    data = {
        "data_inicial": str(dia_menos_sete),
        "data_final": str(dia),
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


def buscaRelatorioServico(codigo_serv, cookie):
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
        lista_atividades.append([turma, obra, data[0:10], int(codigo_serv),'', 'sem atividades cadastradas', '', '', ''])
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
            lista_atividades.append([turma, obra, data[0:10], int(codigo_serv), codigo_ativ, atividade, quantidade, valor_unit, valor_tot])

    return(lista_atividades)

def atualiza_producao():
    hora = datetime.now()
    hora = hora.strftime('%H:%M')
    print('[' + hora + '] ', "Atualizando produção...")

    servicos = read_sheets('1kthDa5_Ed7mfpnye-xpnTzZXkFnBB4veVyxeM9Ti8ds', 'produção!D2:D', 'FORMATTED_VALUE')
    range = len(servicos) + 2
    cookie = 'PHPSESSID=3is9mgb0lsojrt1nv6acd0ino6'

    # BUSCA TODOS OS CÓDIGOS DE SERVIÇOS REALIZADOS
    lista_cod_servicos = []
    lista_cod_servicos.append(buscaCodigoServico('11', cookie))
    lista_cod_servicos.append(buscaCodigoServico('12', cookie))


    # GERA A PLANILHA FINAL DAS ATIVIDADES REALIZADAS
    atividades = []
    for i in lista_cod_servicos:
        for n in i:
            if n != 'sem registro de atividades':
                if [n] in servicos:
                    continue
                else:
                    atividades += buscaRelatorioServico(n, cookie)
    write_sheets('1kthDa5_Ed7mfpnye-xpnTzZXkFnBB4veVyxeM9Ti8ds', 'produção!A'+str(range), atividades)


    hora = datetime.now()
    hora = hora.strftime('%H:%M')
    print('[' + hora + '] ', len(atividades), " atividades foram adicionadas")


if __name__ == '__main__':
    atualiza_producao()