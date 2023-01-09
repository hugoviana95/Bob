import requests
import pandas as pd
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from sqlalchemy import create_engine

global planilha
planilha = []

def busca_n_checklist(cod_turno, cookie):
    url = "https://sirtecba.gpm.srv.br/gpm/geral/relatorio_turno.php"
    headers = {
        'cookie': cookie,
    }
    data = {
        "cod_tur": cod_turno,
    }

    resposta = requests.post(url, headers=headers, data=data) 
    conteudo = resposta.content
    site = BeautifulSoup(conteudo, "html.parser") #Captura o html da página

    tabela = site.find('table', attrs={'class': 'tbl_itens'})
    linhas = tabela.findAll('tr')
    coluna = linhas[1].findAll('td')

    n_checklist = coluna[2].text

    return(int(n_checklist[:10]))

def possui_camera(cod_turno, cookie):
    n_checklist = busca_n_checklist(cod_turno, cookie)
    
    url = "https://sirtecba.gpm.srv.br/gpm/geral/checklists_relatorio.php"
    headers = {
        'cookie': cookie,
    }
    data = {
        "rel_final": 3,
        "rel_check": n_checklist
    }

    resposta = requests.post(url, headers=headers, data=data) 
    conteudo = resposta.content
    site = BeautifulSoup(conteudo, "html.parser") #Captura o html da página

    head = site.find("div", attrs={'class': 'div_head'})
    head = head.text

    if (head.find("Check List Diário - CCM") == 1) or (head.find("Check List Diário - Linha Viva") == 1) or (head.find("Check List Diário - STC") == 1):
        table = site.find("table", attrs={'class': 'tbl_itens'})
        linhas = table.findAll("tr")
        try:
            questao = linhas[18].findAll("td")
            pergunta = questao[0].text
            if pergunta == 'Equipe possui câmera funcional?:':
                resposta = questao[1].text
            else:
                resposta = '-'
        except:
            resposta = '-'
    
        return(resposta)
    else:
        return('checklist incorreto')

def coleta_infos(cod_contrato, planilha):
    engine = create_engine("mysql+pymysql://u369946143_pcpBahia:#Energia26#90@31.220.16.3/u369946143_pcpBahia", echo=False)
    abertura_turnos = pd.read_sql_table('abertura_turnos', con=engine)
    cookie = "PHPSESSID=26qak1aargp7f8pmbu7gjev40k;"
    turnos_registrados = abertura_turnos['cod_turno_tur']
    url = "https://sirtecba.gpm.srv.br/gpm/geral/consulta_turno.php?tip=C"
    headers = {
        'cookie': cookie,
        'upgrade-insecure-requests': '1'
    }
    dia = datetime.now()
    dia = dia.strftime("%d/%m/%Y")
    data = {
        "data_inicial": '08/01/2023',#str(dia),
        "data_final": '08/01/2023',#str(dia),
        "submit": "Pesquisar",
        "contrato": cod_contrato,
        "h_tot": "999",
        "avancaTodos": "Todas",
    }

    #Busca turnos do contrato final 70
    try:
        response = requests.post(url, headers=headers, data=data) 
        conteudo = response.content
        site = BeautifulSoup(conteudo, "html.parser") #Captura o html da página
    except:
        print('Não foi possível fazer conexão com o GPM.')
    try:    
        tabela = site.find('table', attrs = {"class": "tbl_100_c_5p_0h_lin zebra"})
        linhas = tabela.findAll('tr')
        linhas.pop(0)
    except:
        print('Nenhum turno aberto.')


    agora = datetime.now()
    oito_e_dez = agora.replace(hour=8, minute=10, second=0, microsecond=0)
    try:
        for i in linhas:
            colunas = i.findAll('td')
            if colunas[3].text in turnos_registrados.values:
                continue
            else:
                hora_abertura = datetime.strptime(colunas[12].text, '%d/%m/%Y %H:%M:%S')
                if hora_abertura > oito_e_dez:
                    pontualidade = 0
                elif hora_abertura < oito_e_dez:
                    pontualidade = 1

                resposta = possui_camera(colunas[3].text, cookie)
                planilha.append([colunas[3].text, colunas[7].text, colunas[12].text, pontualidade, resposta, colunas[11].text])
    except:
        print('Nenhum turno encontratdo')

def atualiza_turno():
    
    coleta_infos("12", planilha)
    coleta_infos("11", planilha)
    coleta_infos("16", planilha)
    coleta_infos("15", planilha)
    coleta_infos("13", planilha)

    df = pd.DataFrame(planilha, columns=['cod_turno_tur','des_equipe', 'dta_solicitacao', 'pontualidade', 'possui_camera', 'placa_veiculo'])
    df['dta_solicitacao'] = pd.to_datetime(df['dta_solicitacao'], format='%d/%m/%Y %H:%M:%S')

    print(df)

    engine = create_engine("mysql+pymysql://u369946143_pcpBahia:#Energia26#90@31.220.16.3/u369946143_pcpBahia", echo=False)

    df.to_sql('abertura_turnos', index=False, if_exists='append', con=engine)


if __name__ == '__main__':
    atualiza_turno()