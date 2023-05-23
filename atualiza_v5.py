import requests
import pandas as pd
from datetime import datetime
import gspread

def consulta_geoex(projeto):
    url = 'https://geoex.com.br/api/EPS/ConsultarProjeto/Item'
    header = {
        'cookie': '_ga=GA1.1.1408546827.1679344756; TemaEscuro=true; Home.Buscar.Texto=; ConsultarNota.Numero=9101900448; ASP.NET_SessionId=z1sozdj0fdp3ar2migybc5eq; ConsultarProjeto.Numero=B-0957112; _ga_ZBQMHFHTL8=GS1.1.1683121115.101.0.1683121538.0.0.0; .ASPXAUTH=314ECA4861344A51E9F9BAAF4656B837F1F0377AB43E9F94451A5FF3ADB0641FBFBF6BC6BA1DAAD93A2D933CA0DEA25A284CB2AF75B9DD7C38DD2A2F8410033EF1D8C96568F7D0D594407193A9B23C2DAC34081C9DF37085EB46CF011749D5D912B93BE1798EAA447A89DE77C9D66DFC2C507DDDF739FB8BF9B4E234039DFE65B41E53BB7B9E26EDDAD51319C9D083B4C8F75D577666B2B384A9CCFCC6F6EE4F3F4A7AE57EAC33D207BEE705587083430485709B84D62DFA0745B730644FC2385B029635CE08C06FD0166E3B65905C52',
        'ususarioid': 'e092ed10-dfdd-437c-9fe0-ab6bf9725410'
    }
    body = {
        'id': projeto
    }
 
    try:
        resposta = requests.post(url = url, json = body, headers = header)
        resposta = resposta.json()
    except:
        print('Não foi possível acessar a página do Geoex.')
        return('erro')

    if resposta['Content'] != None:
        id_projeto = resposta['Content']['ProjetoId']
        if resposta['Content']['GseProjeto'] != None:
            status_estagio_hektor = resposta['Content']['GseProjeto']['Status']['Nome']
        else:
            status_estagio_hektor = ''

        if resposta['Content']['EnvioPastaStatus'] != None:
            status_pasta = resposta['Content']['EnvioPastaStatus']
        else:
            status_pasta = ''

        url = 'https://geoex.com.br/api/ConsultarProjeto/TermoGeo/Itens'
        body = {
            'ProjetoId': id_projeto,
            'Paginacao': {
                'Pagina': '1',
                'TotalPorPagina': '10'
            }
        }
        resposta = requests.post(url, headers=header, json=body).json()
        if resposta['Content']['Items'] != []:
            auxiliar_hektor = resposta['Content']['Items'][0]['HistoricoStatus']['Nome']
        else:
            auxiliar_hektor = ''
        
        return status_estagio_hektor, auxiliar_hektor, status_pasta
    else:
        print('Não foi possível acessar o projeto', projeto, 'no Geoex.')
        auxiliar_hektor = ''
        status_estagio_hektor = ''
        status_pasta = ''
    return status_estagio_hektor, auxiliar_hektor, status_pasta


def atualiza_cada_aba(aba):
    gs = gspread.service_account(filename='service_account.json')
    sh = gs.open_by_key('1OGcmrWmbZs0ouApHKaVfYEJEhzfEfBIa7eMezkLvk84')
    v5 = sh.worksheet(aba).get_all_values()
    v5 = pd.DataFrame(v5, columns=v5.pop(0))
    projetos = v5['PROJETO']
    status_estagio_hektor = []
    auxiliar_hektor = []
    status_pasta = []
    for i in projetos:
        if i != None and i != '':
            ############################### ATUALIZA STATUS HECKTOR
            status = consulta_geoex(i)
            if status != 'erro':
                status_estagio_hektor.append([status[0]])
                auxiliar_hektor.append([status[1]])
                status_pasta.append([status[2]])
            else:
                status_estagio_hektor.append([''])
                auxiliar_hektor.append([''])
                status_pasta.append([''])
            
    sh.worksheet(aba).update('AP2:AP', status_estagio_hektor)
    sh.worksheet(aba).update('AY2:AY', auxiliar_hektor)
    sh.worksheet(aba).update('K2:K', status_pasta)



def atualiza_v5():
    print('[' + datetime.now().strftime("%H:%M") + ']', 'Atualizando a V5...')

    print('Atualizando Conquista...')
    atualiza_cada_aba('OBRAS CONQUISTA')

    print('Atualizando Jequié...')
    atualiza_cada_aba('OBRAS JEQUIE')

    print('Atualizando Irecê...')
    atualiza_cada_aba('OBRAS IRECE')

    print('Atualizando Lapa...')
    atualiza_cada_aba('OBRAS LAPA')

    print('Atualizando Guanambi...')
    atualiza_cada_aba('OBRAS GUANAMBI')

    print('Atualizando Barreiras...')
    atualiza_cada_aba('OBRAS BARREIRAS')

    print('Atualizando Ibotirama...')
    atualiza_cada_aba('OBRAS IBOTIRAMA')

    agora = datetime.now().strftime('%H:%M')
    print('['+ datetime.now().strftime("%H:%M") + "] V5 atualizada!")


if __name__ == '__main__':
    atualiza_v5()
    