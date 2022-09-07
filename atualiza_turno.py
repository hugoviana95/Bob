import requests
import pandas as pd

from datetime import datetime
from bs4 import BeautifulSoup
from read_sheets import read_sheets
from write_sheets import write_sheets


def atualiza_turno():
    hora = datetime.now()
    hora = hora.strftime('%H:%M')
    print('[' + hora + ']', "Atualizando abertura de turnos...")

    id='1kthDa5_Ed7mfpnye-xpnTzZXkFnBB4veVyxeM9Ti8ds'
    range="turnos!A:A"
    result = read_sheets(id, range, 'FORMATTED_VALUE')
    df = pd.DataFrame(result, columns = result.pop(0))
    tamanhoTabela = len(df['cod_turno_tur'])

    # COLETA INFORMAÇÕES DO GPM
    url = "https://sirtecba.gpm.srv.br/gpm/geral/consulta_turno.php?tip=C"
    headers = {
        'cookie': "PHPSESSID=elnm3leifj74rb8v6i6is47oi9"
    }
    dia = datetime.now()
    diaehora = dia.strftime("%d/%m/%Y %H:%M")
    dia = dia.strftime("%d/%m/%Y")
    data = {
        "data_inicial": str(dia),
        "data_final": str(dia),
        "submit": "Pesquisar",
        "contrato": "11",
        "h_tot": "999",
        "avancaTodos": "Todas",
    }

    #Busca turnos do contrato final 70
    try:
        resposta = requests.post(url, headers=headers, data=data) 
        conteudo = resposta.content
        site = BeautifulSoup(conteudo, "html.parser") #Captura o html da página
    except:
        print('Não foi possível fazer conexão com o GPM.')
    try:    
        tabela = site.find('table', attrs = {"class": "tbl_100_c_5p_0h_lin zebra"})
        linhas = tabela.findAll('tr')
        linhas.pop(0)
    except:
        print('Nenhum turno aberto.')
        return
    

    planilha = []

    for i in linhas:
        cont = 0
        colunas = i.findAll('td')
        for n in df['cod_turno_tur']:
            if colunas[3].text == n:
                cont += 1
        if cont == 0:
            planilha.append([colunas[3].text, colunas[7].text, colunas[12].text])

    #Busca turnos do contrato final 69
    try:
        data["contrato"]= "12"
        resposta = requests.post(url, headers=headers, data=data) 
        conteudo = resposta.content
        site = BeautifulSoup(conteudo, "html.parser") #Captura o html da página
    except:
        print('Não foi possível fazer conexão com o GPM.')
    try:
        tabela = site.find('table', attrs = {"class": "tbl_100_c_5p_0h_lin zebra"})
        linhas = tabela.findAll('tr')
        linhas.pop(0)
    except:
        print('Nenhum turno aberto.')
        return

    for i in linhas:
        cont = 0
        colunas = i.findAll('td')
        for n in df['cod_turno_tur']:
            if colunas[3].text == n:
                cont += 1
        if cont == 0:
            planilha.append([colunas[3].text, colunas[7].text, colunas[12].text])



    # PREENCHIMENTO DA PLANILHA ONLINE

    id='1kthDa5_Ed7mfpnye-xpnTzZXkFnBB4veVyxeM9Ti8ds'
    range = ("turnos!A"+str(tamanhoTabela + 2))
    valores = planilha
    write_sheets(id, range, valores)

    range = ("turnos!B3")
    valores = [[str(diaehora)]]
    write_sheets(id, range, valores)
    hora = datetime.now()
    hora = hora.strftime('%H:%M')
    print('[' + hora + ']' , len(planilha), "aberturas de turno foram adicionadas")


if __name__ == "__main__":
    atualiza_turno()