
from copy import error
import requests
import pandas as pd
from read_sheets import read_sheets
from write_sheets import write_sheets
from datetime import datetime

def consulta_v5(aba):
    #Consulta os números de projeto da V5 da aba indicada
    try:
        range = (aba + '!A:AX')
        id = '1OGcmrWmbZs0ouApHKaVfYEJEhzfEfBIa7eMezkLvk84'
        result = read_sheets(id, range, 'FORMATTED_VALUE')
        v5 = pd.DataFrame(result, columns = result.pop(0))
        return(v5)
    except error:
        print('Ocorreu algum erro ao ler a V5.')
        return

def consulta_geoex(projeto):
    url = 'https://geoex.com.br/EPS/ConsultarProjeto/Item'
    header = {
        'cookie': '_ga=GA1.1.628028127.1657558275; TemaEscuro=true; BaseConhecimento.Informativos.ModoLista=true; Home.Buscar.Texto=; ConsultarContrato.Contrato=4600053669; ConsultarNota.Numero=9101268118; ASP.NET_SessionId=nrn1liphf415ibelpmy4luwu; ConsultarProjeto.Numero=B-1056104; _ga_ZBQMHFHTL8=GS1.1.1672084692.467.0.1672084692.0.0.0; .ASPXAUTH=11F1929B69C86D95AF5F85B8696AA8158BF9A445845CE0A8AE31F4B83B2CB7859757CEDAD2AEED747A460D2C85BDE0628C371B5B88691853E251A40D015F703D2F7A7CF1FE29B86D24637632FBE34224C1472B43234974B346C19C222A817373825D4F55F9E999C53067FE3AA3B202B088FBFC092A32F7181E813D199A1102FEA0BCF11115738A72BE956533FFD16B8A1FC2BE01E4A3BB411DDDDAD57A3D5D0FB186D0D6A49B8217D10AF1D9DF4DE0D9B0E328AD81C766DAA859D7B156EAD1FC8C37D1762F5F7A7A954BB20662582C24',
        'usuarioid': 'e092ed10-dfdd-437c-9fe0-ab6bf9725410'
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
        #print(status_pasta)


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


def atualiza_cada_aba(aba, carteira):
    id = '1OGcmrWmbZs0ouApHKaVfYEJEhzfEfBIa7eMezkLvk84'
    v5 = consulta_v5(aba)
    projetos = v5['PROJETO']
    status_estagio_hektor = []
    auxiliar_hektor = []
    status_pasta = []
    linha = 2
    for i in projetos:
        # print(i)
        if i != None and i != '':
            ############################### ATUALIZA STATUS HECKTOR
            status = consulta_geoex(i)
            if status != 'erro':
                #print(status)
                status_estagio_hektor.append([status[0]])
                auxiliar_hektor.append([status[1]])
                status_pasta.append([status[2]])
            else:
                status_estagio_hektor.append([''])
                auxiliar_hektor.append([''])
                status_pasta.append([''])
            
        
            ############################### ATUALIZA AR
            # if v5.loc[v5['PROJETO'] == i]['AR COELBA'].values == ['']:
            #     info_obra = carteira.loc[carteira['OBRA'] == i]
            #     if len(info_obra) > 1:
            #         ar = info_obra.loc[info_obra['CARTEIRA'] == 'MAIO']['AR'].values
            #         if ar.size == 0:
            #             ar = info_obra.loc[info_obra['CARTEIRA'] == 'ABRIL']['AR'].values
            #             if ar.size == 0:
            #                 ar = info_obra.loc[info_obra['CARTEIRA'] == 'MARÇO']['AR'].values
            #                 if ar.size == 0:
            #                     ar = info_obra.loc[info_obra['CARTEIRA'] == 'FEVEREIRO']['AR'].values
            #                     if ar.size == 0:
            #                         ar = info_obra.loc[info_obra['CARTEIRA'] == 'JANEIRO']['AR'].values
            #         write_sheets(id, aba + '!AS' + str(linha), [ar.tolist()])
            #     elif len(info_obra) == 1:
            #         ar = info_obra['AR'].values.tolist()
            #         write_sheets(id, aba + '!AS' + str(linha), [ar])

        linha += 1

    write_sheets(id, aba + '!AP2', status_estagio_hektor)
    write_sheets(id, aba + '!AY2', auxiliar_hektor)
    write_sheets(id, aba + '!K2', status_pasta)



def atualiza_v5():

    carteira_vtc = read_sheets('1ffjjOM5iTu6JmOuqcM_rHNIvgL3N0wsLRzPbVJ5St0M', 'Carteira Geral!A:W', 'FORMATTED_VALUE')
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

    carteira_geral = pd.concat([carteira_ire, carteira_vtc, carteira_jeq, carteira_brr, carteira_bjl, carteira_gbi], ignore_index = True)



    agora = datetime.now().strftime('%H:%M')
    print('[' + agora + ']', 'Atualizando a V5...')
    print('Atualizando Conquista...')
    atualiza_cada_aba('OBRAS CONQUISTA', carteira_geral)
    print('Atualizando Jequié...')
    atualiza_cada_aba('OBRAS JEQUIE', carteira_geral)
    print('Atualizando Irecê...')
    atualiza_cada_aba('OBRAS IRECE', carteira_geral)
    print('Atualizando Lapa...')
    atualiza_cada_aba('OBRAS LAPA', carteira_geral)
    print('Atualizando Guanambi...')
    atualiza_cada_aba('OBRAS GUANAMBI', carteira_geral)
    print('Atualizando Barreiras...')
    atualiza_cada_aba('OBRAS BARREIRAS', carteira_geral)
    print('Atualizando Ibotirama...')
    atualiza_cada_aba('OBRAS IBOTIRAMA', carteira_geral)
    agora = datetime.now().strftime('%H:%M')
    print('['+ agora + "] V5 atualizada!")


if __name__ == '__main__':
    atualiza_v5()
    