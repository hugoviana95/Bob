import pandas as pd
import requests
from time import strftime
from read_sheets import read_sheets
from write_sheets import write_sheets
from datetime import datetime


def atualiza_asbuilt():
    agora = datetime.now().strftime('%H:%M')
    print('[' + str(agora) + '] Atualizando planilha de as-built...')

    ####################### LENDO CARTEIRAS ONLINE
    carteira_vtc = read_sheets('1hIaJaiPm2JNl7ogPcAu8-172flUK2uLaDPs3hyeMQ6g', 'CARTEIRA!A:AS', 'FORMATTED_VALUE')
    carteira_vtc = pd.DataFrame(carteira_vtc, columns = carteira_vtc.pop(0))
    carteira_vtc = carteira_vtc.query("PROJETO != ''")

    carteira_jeq = read_sheets('1EruOLu5kNzq3Vn7Xj4XmgXBOlXXAFhEdmjBWS-rgvWk', 'CARTEIRA!A:AS', 'FORMATTED_VALUE')
    carteira_jeq = pd.DataFrame(carteira_jeq, columns = carteira_jeq.pop(0))
    carteira_jeq = carteira_jeq.query("PROJETO != ''")

    carteira_gbi = read_sheets('1OGQse2IeSjxfZ-MRtXFarssQegqlAsZ8ruZaKwar6rY', 'CARTEIRA!A:AS', 'FORMATTED_VALUE')
    carteira_gbi = pd.DataFrame(carteira_gbi, columns = carteira_gbi.pop(0))
    carteira_gbi = carteira_gbi.query("PROJETO != ''")

    carteira_bjl = read_sheets('1HLsZcMjsKiqqsKZnU_osIOmKFytkNqBTxKPhdqBzcmg', 'CARTEIRA!A:AS', 'FORMATTED_VALUE')
    carteira_bjl = pd.DataFrame(carteira_bjl, columns = carteira_bjl.pop(0))
    carteira_bjl = carteira_bjl.query("PROJETO != ''")

    carteira_brr = read_sheets('1-xNWYwTWVl9w-eHYGgMzFiV2TPlDCx88y82HnL2wx4U', 'CARTEIRA!A:AS', 'FORMATTED_VALUE')
    carteira_brr = pd.DataFrame(carteira_brr, columns = carteira_brr.pop(0))
    carteira_brr = carteira_brr.query("PROJETO != ''")

    carteira_ire = read_sheets('1gFStrS82U7PRX5gBd9jVvYVdkUDAYNFamNxoFATL2kc', 'CARTEIRA!A:AS', 'FORMATTED_VALUE')
    carteira_ire = pd.DataFrame(carteira_ire, columns = carteira_ire.pop(0))
    carteira_ire = carteira_ire.query("PROJETO != ''")

    carteira_ibt = read_sheets('1Fo2obLTZObf33d2vA_1GbzPzxCU1egJdX9okIF06oDo', 'CARTEIRA!A:AS', 'FORMATTED_VALUE')
    carteira_ibt = pd.DataFrame(carteira_ibt, columns = carteira_ibt.pop(0))
    carteira_ibt = carteira_ibt.query("PROJETO != ''")

    carteira_geral = pd.concat([carteira_ire, carteira_vtc, carteira_jeq, carteira_brr, carteira_bjl, carteira_gbi, carteira_ibt], ignore_index = True)


    ####################### LENDO PLANILHA DO FECHAMENTO
    obras_recepcionadas_resolucao = read_sheets('1OGcmrWmbZs0ouApHKaVfYEJEhzfEfBIa7eMezkLvk84', 'Obras em resolução de problema!A:I', 'FORMATTED_VALUE')
    obras_recepcionadas_resolucao = pd.DataFrame(obras_recepcionadas_resolucao, columns = obras_recepcionadas_resolucao.pop(0))
    obras_recepcionadas_resolucao = obras_recepcionadas_resolucao.query("PROJETO != ''")

    obras_recepcionadas_vtc = read_sheets('1OGcmrWmbZs0ouApHKaVfYEJEhzfEfBIa7eMezkLvk84', 'OBRAS CONQUISTA!A:I', 'FORMATTED_VALUE')
    obras_recepcionadas_vtc = pd.DataFrame(obras_recepcionadas_vtc, columns = obras_recepcionadas_vtc.pop(0))
    obras_recepcionadas_vtc = obras_recepcionadas_vtc.query("PROJETO != ''")

    obras_recepcionadas_jeq = read_sheets('1OGcmrWmbZs0ouApHKaVfYEJEhzfEfBIa7eMezkLvk84', 'OBRAS JEQUIE!A:I', 'FORMATTED_VALUE')
    obras_recepcionadas_jeq = pd.DataFrame(obras_recepcionadas_jeq, columns = obras_recepcionadas_jeq.pop(0))
    obras_recepcionadas_jeq = obras_recepcionadas_jeq.query("PROJETO != ''")

    obras_recepcionadas_bjl = read_sheets('1OGcmrWmbZs0ouApHKaVfYEJEhzfEfBIa7eMezkLvk84', 'OBRAS LAPA!A:I', 'FORMATTED_VALUE')
    obras_recepcionadas_bjl = pd.DataFrame(obras_recepcionadas_bjl, columns = obras_recepcionadas_bjl.pop(0))
    obras_recepcionadas_bjl = obras_recepcionadas_bjl.query("PROJETO != ''")

    obras_recepcionadas_ire = read_sheets('1OGcmrWmbZs0ouApHKaVfYEJEhzfEfBIa7eMezkLvk84', 'OBRAS IRECE!A:I', 'FORMATTED_VALUE')
    obras_recepcionadas_ire = pd.DataFrame(obras_recepcionadas_ire, columns = obras_recepcionadas_ire.pop(0))
    obras_recepcionadas_ire = obras_recepcionadas_ire.query("PROJETO != ''")

    obras_recepcionadas_gbi = read_sheets('1OGcmrWmbZs0ouApHKaVfYEJEhzfEfBIa7eMezkLvk84', 'OBRAS GUANAMBI!A:I', 'FORMATTED_VALUE')
    obras_recepcionadas_gbi = pd.DataFrame(obras_recepcionadas_gbi, columns = obras_recepcionadas_gbi.pop(0))
    obras_recepcionadas_gbi = obras_recepcionadas_gbi.query("PROJETO != ''")

    obras_recepcionadas_brr = read_sheets('1OGcmrWmbZs0ouApHKaVfYEJEhzfEfBIa7eMezkLvk84', 'OBRAS BARREIRAS!A:I', 'FORMATTED_VALUE')
    obras_recepcionadas_brr = pd.DataFrame(obras_recepcionadas_brr, columns = obras_recepcionadas_brr.pop(0))
    obras_recepcionadas_brr = obras_recepcionadas_brr.query("PROJETO != ''")

    obras_recepcionadas_ibt = read_sheets('1OGcmrWmbZs0ouApHKaVfYEJEhzfEfBIa7eMezkLvk84', 'OBRAS IBOTIRAMA!A:I', 'FORMATTED_VALUE')
    obras_recepcionadas_ibt = pd.DataFrame(obras_recepcionadas_ibt, columns = obras_recepcionadas_ibt.pop(0))
    obras_recepcionadas_ibt = obras_recepcionadas_ibt.query("PROJETO != ''")

    obras_recepcionadas_geral = pd.concat([obras_recepcionadas_resolucao, obras_recepcionadas_vtc, obras_recepcionadas_jeq , obras_recepcionadas_brr, obras_recepcionadas_gbi, obras_recepcionadas_bjl, obras_recepcionadas_ire, obras_recepcionadas_ibt], ignore_index = True)


    ####################### PROCURA POR OBRAS CONCLUIÍDAS NAS CARTEIRAS
    obras_concluidas_completo = carteira_geral.query("`STATUS GERAL` == 'CONCLUÍDA'")
    # obras_concluidas_completo = obras_concluidas_completo.loc[obras_concluidas_completo['CARTEIRA'] != 'JANEIRO']
    # obras_concluidas_completo = obras_concluidas_completo.loc[obras_concluidas_completo['CARTEIRA'] != 'FEVEREIRO']
    # obras_concluidas_completo = obras_concluidas_completo.loc[obras_concluidas_completo['CARTEIRA'] != 'MARÇO']
    # obras_concluidas_completo = obras_concluidas_completo.loc[obras_concluidas_completo['CARTEIRA'] != 'ABRIL']
    # obras_concluidas_completo = obras_concluidas_completo.loc[obras_concluidas_completo['CARTEIRA'] != 'MAIO']
    # obras_concluidas_completo = obras_concluidas_completo.loc[obras_concluidas_completo['CARTEIRA'] != 'JUNHO']
    obras_concluidas = obras_concluidas_completo['PROJETO']
    obras_concluidas = obras_concluidas.astype(int)


    obras_recepcionadas_geral = obras_recepcionadas_geral['PROJETO']

    cont = 0
    for i in obras_recepcionadas_geral:
        if ((i != None) and (i != '')):
            obras_recepcionadas_geral[cont] = int(i[2:9])
        cont += 1

    obras_concluidas_sem_pasta_no_fechamento = []
    obras_concluidas = obras_concluidas.tolist()
    obras_recepcionadas_geral = obras_recepcionadas_geral.tolist()


    ####################### CONFERE QUAIS OBRAS JÁ ESTÃO NA PLANILHA DO FECHAMENTO
    cont = 0
    for i in obras_concluidas: 
        if i in obras_recepcionadas_geral:
            pass
        else:
            obras_concluidas_sem_pasta_no_fechamento.append(obras_concluidas[cont])
        cont += 1

    ####################### CONFERE QUAIS OBRAS ESTÃO PENDENTES DE ENVIO DA PASTA NO GEOEX
    projetos_pendente_asbuilt = []
    url = 'https://geoex.com.br/EPS/ConsultarProjeto/Item'
    header = {
        'cookie': '_ga=GA1.1.1408546827.1679344756; TemaEscuro=true; ASP.NET_SessionId=okg3jb253fykgd13kxtodv0x; _ga_ZBQMHFHTL8=GS1.1.1679395496.2.0.1679395496.0.0.0; .ASPXAUTH=9CCD03C8D413CAFDA79DBE2A6C5AB2FC9052F77CE232866B7A82B6D6AAA808C074E756774B4E08B0826CA747019AD9C319711153931228848B8CF7E36DEEAF1A49192FD96B6836E7B7CCBD1DFA567C9AD0AD3BF42A8B17244F92C32CCF574723FD1116917CF5E38D1AEBC137C1039C0B72C825AE6456B13653B08CA682BB84CC03ECFD66CE8400131080898D89BFFD4840856E2683B712B83486D9CBE89B26E54E8298EEACFC17FE69B826E3014217E8881E1969791D30768928995B40BF8FCC17CEE6F78EE90CAA035572591E613DDD; Home.Buscar.Texto='
    }
    for i in obras_concluidas_sem_pasta_no_fechamento:
        resposta = requests.post(url, headers=header, json = {'id': i}).json()
        try:
            status_pasta = resposta['Content']['EnvioPastaStatus']
            if status_pasta != "PASTA ACEITA" and status_pasta != "PASTA REJEITADA" and status_pasta != "PASTA ACEITA COM RESTRIÇÃO" and status_pasta != "PASTA REPOSTADA" and status_pasta != "PASTA POSTADA PARA RECEPÇÃO":
                try:
                    vl_projeto = resposta['Content']['VlProjeto']
                except:
                    vl_projeto = ''
                try:
                    titulo = resposta['Content']['Titulo']
                except:
                    titulo = ''
                try:
                    data_energ = resposta['Content']['DtZps09'][0:10]
                    data_energ = datetime.strptime(data_energ, "%Y-%m-%d")
                    data_energ = data_energ.strftime("%d/%m/%y")
                except:
                    data_energ = ''

                unidade = obras_concluidas_completo.loc[obras_concluidas_completo['PROJETO'] == str(i)]['UNIDADE'].values
                if unidade.size > 0:
                    unidade = unidade[-1]
                else:
                    unidade = ''

                supervisor = obras_concluidas_completo.loc[obras_concluidas_completo['PROJETO'] == str(i)]['SUPERVISOR'].values
                if supervisor.size > 0:
                    supervisor = supervisor[-1]
                else:
                    supervisor = ''
                if supervisor[0:3] == 'SUP':
                    supervisor = supervisor[8:]

                municipio = obras_concluidas_completo.loc[obras_concluidas_completo['PROJETO'] == str(i)]['MUNICÍPIO'].values
                if municipio.size > 0:
                    municipio = municipio[-1]
                else:
                    municipio = ''
                
                projetos_pendente_asbuilt.append([unidade, i, titulo, vl_projeto, data_energ, supervisor, municipio])
        except:
            print('sem acesso ao projeto ', i)



    planilha_michelle = read_sheets('1WyjUGW3IP_or21BDUmi8QSf_Moa8RG53CgOyT5L_0OU','PENDENTE AS BUILT!A:E', 'FORMATTED_VALUE')
    df_pendencias = pd.DataFrame(planilha_michelle, columns = planilha_michelle.pop(0))
    projetos = df_pendencias['PROJETO']
    for i in projetos:
        resposta = requests.post(url, headers=header, json = {'id': i}).json()
        try:
            vl_projeto = resposta['Content']['VlProjeto']
        except:
            vl_projeto = ''
        try:
            titulo = resposta['Content']['Titulo']
        except:
            titulo = ''
        try:
            data_energ = resposta['Content']['DtZps09'][0:10]
            data_energ = datetime.strptime(data_energ, "%Y-%m-%d")
            data_energ = data_energ.strftime("%d/%m/%y")
        except:
            data_energ = ''
        
        unidade = df_pendencias.loc[df_pendencias['PROJETO'] == i]['UNIDADE'].values
        unidade = unidade[-1]
        supervisor = df_pendencias.loc[df_pendencias['PROJETO'] == i]['COORDENADOR'].values
        supervisor = supervisor[-1]
        projetos_pendente_asbuilt.append([unidade, i, titulo, vl_projeto, data_energ, supervisor])


    write_sheets('18-AoLupeaUIOdkW89o6SLK6Z9d8X0dKXgdjft_daMBk', 'pastas pendentes!A2',[['']*7]*1000) # LIMPA A PLANILHA
    write_sheets('18-AoLupeaUIOdkW89o6SLK6Z9d8X0dKXgdjft_daMBk', 'pastas pendentes!A2', projetos_pendente_asbuilt)
    write_sheets('18-AoLupeaUIOdkW89o6SLK6Z9d8X0dKXgdjft_daMBk', 'Quadro Geral!F2', [[datetime.now().strftime("%d/%m/%Y %H:%M")]])
    agora = datetime.now().strftime('%H:%M')
    print('[' + agora + "] As-builts atualizados!")



if __name__ == '__main__':
    atualiza_asbuilt()