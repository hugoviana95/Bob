from datetime import datetime, timedelta
import requests
import pandas as pd
from bs4 import BeautifulSoup
from sqlalchemy import create_engine

def buscaCodigoServico(contrato, cookie, dta_inicio, dta_fim):
    url = 'https://sirtecba.gpm.srv.br/gpm/geral/consulta_servico.php'
    header = {
        'cookie': cookie
        }
    data = {
        "data_inicial": str(dta_inicio),
        "data_final": str(dta_fim),
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
    elif turma[0:5] == 'EH170':
        turma = turma[0:12]
    else:
        turma = turma[0:5]


    coluna = linhas[2].findAll('td') # REGISTRA O SUPERVISOR
    supervisor = coluna[3].text

    coluna = linhas[11].findAll('td') # REGISTRA A DATA
    data = coluna[3].text
    data = data[0:10]
    data = datetime.strptime(data, '%d/%m/%Y')

    coluna = linhas[14].findAll('td') # REGISTRA O NÚMERO DA OBRA
    obra = coluna[1].text
    if obra[0] == 'B':
        obra = obra[0:9]


    tabelas = site.findAll('table', attrs={'class': 'tbl_itens'}) # REGISTRA AS ATIVIDADES
    tabela = tabelas[2]
    linhas = tabela.findAll('tr')
    if len(linhas) == 2: # REGISTRO EM CASO DE NENHUMA ATIVIDADE CADASTRADA PELA TURMA
        lista_atividades.append([turma, obra, data, int(codigo_serv),'', 'sem atividades cadastradas', '', '', ''])
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
            quantidade = (quantidade.replace('.','')).replace(',','.')
            quantidade = float(quantidade)
            valor_unit = coluna[3].text
            valor_unit = (valor_unit.replace('.','')).replace(',','.')
            valor_unit = float(valor_unit)
            valor_tot = coluna[4].text
            valor_tot = (valor_tot.replace('.','')).replace(',','.')
            valor_tot = float(valor_tot)
            lista_atividades.append([turma, obra, data, int(codigo_serv), codigo_ativ, atividade, quantidade, valor_unit, valor_tot])

    return(lista_atividades)    

def atualiza_producao_db():
    cookie = 'PHPSESSID=e9cm2oe6t5ueqt3vj2em2eptmk'

    agora = datetime.now()
    dia = agora.date()

    hora = agora.strftime('%H:%M')
    dia_menos_sete = dia - timedelta(3)
    dta_inicio = dia_menos_sete.strftime("%d/%m/%Y")
    dta_fim = dia.strftime("%d/%m/%Y")

    print('[' + hora + '] ', "Atualizando produção no banco de dados...")

    # BUSCA TODOS OS CÓDIGOS DE SERVIÇOS REALIZADOS
    lista_cod_servicos = []
    lista_cod_servicos.append(buscaCodigoServico('11', cookie, dta_inicio, dta_fim))
    lista_cod_servicos.append(buscaCodigoServico('12', cookie, dta_inicio, dta_fim))

    # GERA A PLANILHA FINAL DAS ATIVIDADES REALIZADAS
    atividades = []
    for i in lista_cod_servicos:
        for cont, serv in enumerate(i):
            print('Coletando serviços lançados [%s de %s]...' % (str(cont), str(len(i))))
            print(serv)
            if serv != 'sem registro de atividades':
                    atividades += buscaRelatorioServico(serv, cookie)

    atividades = pd.DataFrame(atividades, columns=['turma', 'obra', 'data', 'n_servico', 'codigo_atividade', 'atividade', 'quantidade', 'valor_unit', 'valor_tot'])
    atividades['data'] = pd.to_datetime(atividades['data'], format='%d/%m/%Y %H:%M:%S')


    engine = create_engine("mysql+pymysql://u369946143_pcpBahia:#Energia26#90@31.220.16.3/u369946143_pcpBahia", echo=False)
    con = engine.connect()
    con.execute("DELETE FROM producao_gpm WHERE data between %s and %s", dia_menos_sete.strftime("%Y-%m-%d"), dia.strftime("%Y-%m-%d"))
    atividades.to_sql('producao_gpm', if_exists='append', index=False, con=con)
    con.close()

    agora = datetime.now()
    print(agora.strftime('%H:%M'), 'Producão GPM atualizada no DB')


if __name__ == '__main__':
    atualiza_producao_db()