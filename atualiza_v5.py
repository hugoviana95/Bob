import requests
import pandas as pd
from datetime import datetime
import gspread

def consulta_geoex(projeto):
    url = 'https://geoex.com.br/EPS/ConsultarProjeto/Item'
    header = {
        'cookie': '_ga=GA1.1.1408546827.1679344756; TemaEscuro=true; ASP.NET_SessionId=okg3jb253fykgd13kxtodv0x; _ga_ZBQMHFHTL8=GS1.1.1679395496.2.0.1679395496.0.0.0; .ASPXAUTH=9CCD03C8D413CAFDA79DBE2A6C5AB2FC9052F77CE232866B7A82B6D6AAA808C074E756774B4E08B0826CA747019AD9C319711153931228848B8CF7E36DEEAF1A49192FD96B6836E7B7CCBD1DFA567C9AD0AD3BF42A8B17244F92C32CCF574723FD1116917CF5E38D1AEBC137C1039C0B72C825AE6456B13653B08CA682BB84CC03ECFD66CE8400131080898D89BFFD4840856E2683B712B83486D9CBE89B26E54E8298EEACFC17FE69B826E3014217E8881E1969791D30768928995B40BF8FCC17CEE6F78EE90CAA035572591E613DDD; Home.Buscar.Texto='
    }
    body = {
        'id': projeto
    }
 
    try:
        resposta = requests.post(url = url, json = body, headers = header).json()
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

        url = 'https://geoex.com.br/ConsultarProjeto/TermoGeo/Itens'
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
    