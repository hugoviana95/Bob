import pandas as pd
import gspread
from sqlalchemy import create_engine
from datetime import datetime

def atualiza_avanco_gpm(id, producao_gpm):
    gs = gspread.service_account(filename='service_account.json')
    sh = gs.open_by_key(id)
    ws = sh.worksheet("CARTEIRA")
    projetos = ws.col_values(3)
    projetos.pop(0)

    info_avanco = []

    for i in projetos:
        if len(i) == 7:
            projeto = "B-"+i
        elif len(i) == 6:
            projeto = "B-0"+i
        elif len(i) == 5:
            projeto = "B-00"+i
        else:
            info_avanco.append(['', '', ''])
            continue

        todos_servicos = producao_gpm.loc[producao_gpm['obra'] == projeto]

        qnt_poste = todos_servicos[todos_servicos['atividade'].str.contains('INSTALAR POSTE')]['quantidade'].sum()
        qnt_cabo_bt = todos_servicos[todos_servicos['atividade'].str.contains('INSTALAR CABO MULTIPLEX')]['quantidade'].sum()
        qnt_cabo_at = todos_servicos[todos_servicos['atividade'].str.contains('INSTALAR CABO AL CAA')]['quantidade'].sum()

        info_avanco.append([qnt_poste, qnt_cabo_at, qnt_cabo_bt])

    ws.update('Z2:AB', info_avanco)


def avanco_gpm():
    print('[' + datetime.now().strftime("%H:%M") + '] Atualizando avanço das obras na carteira...')

    engine = create_engine("mysql+pymysql://u369946143_pcpBahia:#Energia26#90@31.220.16.3/u369946143_pcpBahia", echo=False)
    producao_gpm = pd.read_sql_table('producao_gpm', engine)

    atualiza_avanco_gpm('1hIaJaiPm2JNl7ogPcAu8-172flUK2uLaDPs3hyeMQ6g', producao_gpm) #CONQUISTA
    atualiza_avanco_gpm('1-xNWYwTWVl9w-eHYGgMzFiV2TPlDCx88y82HnL2wx4U', producao_gpm) #BARREIRAS
    atualiza_avanco_gpm('1OGQse2IeSjxfZ-MRtXFarssQegqlAsZ8ruZaKwar6rY', producao_gpm) #GUANAMBI
    atualiza_avanco_gpm('1Fo2obLTZObf33d2vA_1GbzPzxCU1egJdX9okIF06oDo', producao_gpm) #IBOTIRAMA
    atualiza_avanco_gpm('1HLsZcMjsKiqqsKZnU_osIOmKFytkNqBTxKPhdqBzcmg', producao_gpm) #LAPA
    atualiza_avanco_gpm('1EruOLu5kNzq3Vn7Xj4XmgXBOlXXAFhEdmjBWS-rgvWk', producao_gpm) #JEQUIÉ
    atualiza_avanco_gpm('1gFStrS82U7PRX5gBd9jVvYVdkUDAYNFamNxoFATL2kc', producao_gpm) #IRECÊ

    print('[' + datetime.now().strftime("%H:%M") + '] Atualizando avanço das obras na carteira...')

if __name__ == '__main__':
    avanco_gpm()