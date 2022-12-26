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
    carteira_vtc = read_sheets('1ffjjOM5iTu6JmOuqcM_rHNIvgL3N0wsLRzPbVJ5St0M', 'Carteira Geral!A:AW', 'FORMATTED_VALUE')
    carteira_vtc = pd.DataFrame(carteira_vtc, columns = carteira_vtc.pop(0))
    carteira_vtc = carteira_vtc.query("PROJETO != ''")

    carteira_jeq = read_sheets('1FaoZZfOJe1ipGFCcd1TjTkW1gaFFiFHbYkEYrR2ILcw', 'Acomp. Carteira!A:W', 'FORMATTED_VALUE')
    carteira_jeq = pd.DataFrame(carteira_jeq, columns = carteira_jeq.pop(0))
    carteira_jeq = carteira_jeq.query("PROJETO != ''")

    carteira_gbi = read_sheets('1frwOdWK89EKyAKgNIcxvA457NeeGbBQoWKOCaVdC9zE', 'Acomp. Carteira!A:W', 'FORMATTED_VALUE')
    carteira_gbi = pd.DataFrame(carteira_gbi, columns = carteira_gbi.pop(0))
    carteira_gbi = carteira_gbi.query("PROJETO != ''")

    carteira_bjl = read_sheets('1o8JAPPUqPF5rVH66XVtwkzjfLZGGA2QecyXAQ1lohpQ', 'Acomp. Carteira!A:W', 'FORMATTED_VALUE')
    carteira_bjl = pd.DataFrame(carteira_bjl, columns = carteira_bjl.pop(0))
    carteira_bjl = carteira_bjl.query("PROJETO != ''")

    carteira_brr = read_sheets('1HodsFNkVEy8PGpMDvqxwGod182LmiEZFuP8HwBBGCus', 'Acomp. Carteira!A:W', 'FORMATTED_VALUE')
    carteira_brr = pd.DataFrame(carteira_brr, columns = carteira_brr.pop(0))
    carteira_brr = carteira_brr.query("PROJETO != ''")

    carteira_ire = read_sheets('1q9k3TB_vi1IcIvg5FD4Crpe6teqbHvDx1sIEszzAA1U', 'Acomp. Carteira!A:W', 'FORMATTED_VALUE')
    carteira_ire = pd.DataFrame(carteira_ire, columns = carteira_ire.pop(0))
    carteira_ire = carteira_ire.query("PROJETO != ''")

    carteira_ibt = read_sheets('1eAdIlj44V7HWELC9rht3FXsf5GQWg9SHSt6KMIPkCi0', 'Acomp. Carteira!A:W', 'FORMATTED_VALUE')
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
    obras_concluidas_completo = obras_concluidas_completo.loc[obras_concluidas_completo['CARTEIRA'] != 'JANEIRO']
    obras_concluidas_completo = obras_concluidas_completo.loc[obras_concluidas_completo['CARTEIRA'] != 'FEVEREIRO']
    obras_concluidas_completo = obras_concluidas_completo.loc[obras_concluidas_completo['CARTEIRA'] != 'MARÇO']
    obras_concluidas_completo = obras_concluidas_completo.loc[obras_concluidas_completo['CARTEIRA'] != 'ABRIL']
    obras_concluidas_completo = obras_concluidas_completo.loc[obras_concluidas_completo['CARTEIRA'] != 'MAIO']
    obras_concluidas_completo = obras_concluidas_completo.loc[obras_concluidas_completo['CARTEIRA'] != 'JUNHO']
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
        'cookie': '_ga=GA1.1.628028127.1657558275; TemaEscuro=true; BaseConhecimento.Informativos.ModoLista=true; Home.Buscar.Texto=; ConsultarContrato.Contrato=4600053669; ConsultarNota.Numero=9101268118; ASP.NET_SessionId=nrn1liphf415ibelpmy4luwu; ConsultarProjeto.Numero=B-1056104; _ga_ZBQMHFHTL8=GS1.1.1672084692.467.0.1672084692.0.0.0; .ASPXAUTH=11F1929B69C86D95AF5F85B8696AA8158BF9A445845CE0A8AE31F4B83B2CB7859757CEDAD2AEED747A460D2C85BDE0628C371B5B88691853E251A40D015F703D2F7A7CF1FE29B86D24637632FBE34224C1472B43234974B346C19C222A817373825D4F55F9E999C53067FE3AA3B202B088FBFC092A32F7181E813D199A1102FEA0BCF11115738A72BE956533FFD16B8A1FC2BE01E4A3BB411DDDDAD57A3D5D0FB186D0D6A49B8217D10AF1D9DF4DE0D9B0E328AD81C766DAA859D7B156EAD1FC8C37D1762F5F7A7A954BB20662582C24',
        'usuarioid': 'e092ed10-dfdd-437c-9fe0-ab6bf9725410'
    }
    for i in obras_concluidas_sem_pasta_no_fechamento:
        resposta = requests.post(url, headers=header, json = {'id': i}).json()
        try:
            status_pasta = resposta['Content']['EnvioPastaStatus']
            if status_pasta == "PASTA PENDENTE DE ENVIO" or status_pasta == "OBRA EM EXECUÇÃO":
                print(i)
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
                
                unidade = obras_concluidas_completo.loc[obras_concluidas_completo['PROJETO'] == str(i)]['UTD'].values
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

    planilha_michelle = read_sheets('1WyjUGW3IP_or21BDUmi8QSf_Moa8RG53CgOyT5L_0OU','PENDENTE AS BUILT!A:F', 'FORMATTED_VALUE')
    df_pendencias = pd.DataFrame(planilha_michelle, columns = planilha_michelle.pop(0))
    projetos = df_pendencias['PROJETO']
    for i in projetos:
        print(i)
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