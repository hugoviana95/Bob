"""
    88888888ba     ,ad8888ba,    88888888ba
    88      "8b   d8"'    `"8b   88      "8b
    88      ,8P  d8'        `8b  88      ,8P
    88aaaaaa8P'  88          88  88aaaaaa8P'
    88""""""8b,  88          88  88""""""8b,
    88      `8b  Y8,        ,8P  88      `8b
    88      a8P   Y8a.    .a8P   88      a8P
    88888888P"     `"Y8888Y"'    88888888P"
"""



import requests
import pandas as pd
import gspread
import math
import re
import os
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, text

class Bob:
    
    def __init__(self):
        print('BOB v6.0\n')

        self.path = os.path.dirname(os.path.abspath(__file__))

        # Autenticadores
        self.cookie_gpm = 'PHPSESSID=ijjifbg2rrqa5tmlapmc7q9krd'
        self.cookie_geoex = '_ga=GA1.1.1408546827.1679344756; TemaEscuro=true; Home.Buscar.Texto=; FirmaId=1; ConsultarNota.Numero=9002911253; ASP.NET_SessionId=0gedudd0oohiibktzat1c33x; ConsultarProjeto.Numero=B-1025757; _ga_ZBQMHFHTL8=GS1.1.1695836535.521.1.1695837206.0.0.0; .ASPXAUTH=74083B980C922F3CCE820093478D5F4B488C925C9DA78BF17C3DFC076DF7D4D642AFB7773D68D17DD662DE3CE3784C2AAA97C90A96D62F6935D625776289C2E9F0CEDB412B1D6A7F128A78C063D33DFBECECA64E685881CFE7C9DF1DF7E4C45E0BEFAB345EA069445519CA5E370928A20D6E27B1EF5247327E0743E5014B1DF730B8BD093A0AC614FF8456161BC285D3F18F04A9E98FC57BD872AEEFA71E328EDAFFB7C3C7963A31AFFCB3C8EF50A231C8F8D662DD17605EC87F2E2C2ADD7B29B5EFC9094E764679A8551B65BC8B7259'
        self.engine_db = create_engine("mysql+pymysql://u369946143_pcpBahia:#Energia26#90@31.220.16.3/u369946143_pcpBahia", echo=False)
        self.gspread_service = gspread.service_account(filename=os.path.join(self.path, 'service_account.json'))
        
        # Planilhas
        self.id_planilha_fechamento = '1OGcmrWmbZs0ouApHKaVfYEJEhzfEfBIa7eMezkLvk84'
        self.id_planilha_planejamento_conquista = '1hIaJaiPm2JNl7ogPcAu8-172flUK2uLaDPs3hyeMQ6g'
        self.id_planilha_planejamento_jequie = '1EruOLu5kNzq3Vn7Xj4XmgXBOlXXAFhEdmjBWS-rgvWk'
        self.id_planilha_planejamento_irece = '1gFStrS82U7PRX5gBd9jVvYVdkUDAYNFamNxoFATL2kc'
        self.id_planilha_planejamento_guanambi = '1OGQse2IeSjxfZ-MRtXFarssQegqlAsZ8ruZaKwar6rY'
        self.id_planilha_planejamento_lapa = '1HLsZcMjsKiqqsKZnU_osIOmKFytkNqBTxKPhdqBzcmg'
        self.id_planilha_planejamento_barreiras = '1-xNWYwTWVl9w-eHYGgMzFiV2TPlDCx88y82HnL2wx4U'
        self.id_planilha_planejamento_ibotirama = '1Fo2obLTZObf33d2vA_1GbzPzxCU1egJdX9okIF06oDo'

        self.id_planilha_programacao = '1ztV6DYCUkhefULyxJTBiaAKLB_x5zAHgK9icFqBDvf4'
        self.id_planilha_carteira = '1ztV6DYCUkhefULyxJTBiaAKLB_x5zAHgK9icFqBDvf4'


        self.unidades = ['conquista', 'guanambi', 'jequié', 'irecê', 'lapa', 'barreiras', 'ibotirama']




    """

    METODOS DE ATUALIZAÇÃO

    """

    def atualizar_pendencia_asbuilt(self):

        """
        Atualiza as obras pendentes de entrega de asbuilt no fechamento
        """

        print(f'[{datetime.now().strftime("%H:%M")}] Atualizando planilha de as-built...')
    

        ####################### LENDO CARTEIRAS ONLINE
        sh = self.gspread_service.open_by_key(self.id_planilha_carteira)
        carteira_geral = sh.worksheet("CARTEIRA").get_all_values()
        carteira_geral = pd.DataFrame(carteira_geral, columns = carteira_geral.pop(0))
        carteira_geral = carteira_geral.query("PROJETO != ''")
        carteira_geral = carteira_geral[['CARTEIRA', 'PROJETO', 'STATUS GERAL', 'UNIDADE', 'SUPERVISOR', 'MUNICÍPIO']]
        # print(carteira_geral)


        ####################### PROCURA POR OBRAS CONCLUÍDAS NAS CARTEIRAS
        obras_concluidas_completo = carteira_geral.query("`STATUS GERAL` == 'CONCLUÍDA'")
        # print(obras_concluidas_completo)
        # print(obras_concluidas_completo.info())
        obras_concluidas_completo = obras_concluidas_completo.loc[obras_concluidas_completo['CARTEIRA'] != '01/01/2023']
        obras_concluidas_completo = obras_concluidas_completo.loc[obras_concluidas_completo['CARTEIRA'] != '01/02/2023']
        obras_concluidas_completo = obras_concluidas_completo.loc[obras_concluidas_completo['CARTEIRA'] != '01/03/2023']
        obras_concluidas_completo = obras_concluidas_completo.loc[obras_concluidas_completo['CARTEIRA'] != '01/04/2023']
        obras_concluidas_completo = obras_concluidas_completo.loc[obras_concluidas_completo['CARTEIRA'] != '01/05/2023']
        obras_concluidas_completo = obras_concluidas_completo.loc[obras_concluidas_completo['CARTEIRA'] != '01/06/2023']
        obras_concluidas = pd.unique(obras_concluidas_completo['PROJETO'])
        obras_concluidas = obras_concluidas.astype(int)


        ####################### LENDO PLANILHA DO FECHAMENTO
        sh = self.gspread_service.open_by_key(self.id_planilha_fechamento)

        obras_recepcionadas_resolucao = sh.worksheet('Obras em resolução de problema').get_all_values()
        obras_recepcionadas_resolucao = pd.DataFrame(obras_recepcionadas_resolucao, columns = obras_recepcionadas_resolucao.pop(0))
        obras_recepcionadas_resolucao = obras_recepcionadas_resolucao.query("PROJETO != ''")
        obras_recepcionadas_resolucao = obras_recepcionadas_resolucao['PROJETO']

        obras_recepcionadas_vtc = sh.worksheet('OBRAS CONQUISTA').get_all_values()
        obras_recepcionadas_vtc = pd.DataFrame(obras_recepcionadas_vtc, columns = obras_recepcionadas_vtc.pop(0))
        obras_recepcionadas_vtc = obras_recepcionadas_vtc.query("PROJETO != ''")
        obras_recepcionadas_vtc = obras_recepcionadas_vtc['PROJETO']

        obras_recepcionadas_jeq = sh.worksheet('OBRAS JEQUIE').get_all_values()
        obras_recepcionadas_jeq = pd.DataFrame(obras_recepcionadas_jeq, columns = obras_recepcionadas_jeq.pop(0))
        obras_recepcionadas_jeq = obras_recepcionadas_jeq.query("PROJETO != ''")
        obras_recepcionadas_jeq = obras_recepcionadas_jeq['PROJETO']

        obras_recepcionadas_bjl = sh.worksheet('OBRAS LAPA').get_all_values()
        obras_recepcionadas_bjl = pd.DataFrame(obras_recepcionadas_bjl, columns = obras_recepcionadas_bjl.pop(0))
        obras_recepcionadas_bjl = obras_recepcionadas_bjl.query("PROJETO != ''")
        obras_recepcionadas_bjl = obras_recepcionadas_bjl['PROJETO']

        obras_recepcionadas_ire = sh.worksheet('OBRAS IRECE').get_all_values()
        obras_recepcionadas_ire = pd.DataFrame(obras_recepcionadas_ire, columns = obras_recepcionadas_ire.pop(0))
        obras_recepcionadas_ire = obras_recepcionadas_ire.query("PROJETO != ''")
        obras_recepcionadas_ire = obras_recepcionadas_ire['PROJETO']

        obras_recepcionadas_gbi = sh.worksheet('OBRAS GUANAMBI').get_all_values()
        obras_recepcionadas_gbi = pd.DataFrame(obras_recepcionadas_gbi, columns = obras_recepcionadas_gbi.pop(0))
        obras_recepcionadas_gbi = obras_recepcionadas_gbi.query("PROJETO != ''")
        obras_recepcionadas_gbi = obras_recepcionadas_gbi['PROJETO']

        obras_recepcionadas_brr = sh.worksheet('OBRAS BARREIRAS').get_all_values()
        obras_recepcionadas_brr = pd.DataFrame(obras_recepcionadas_brr, columns = obras_recepcionadas_brr.pop(0))
        obras_recepcionadas_brr = obras_recepcionadas_brr.query("PROJETO != ''")
        obras_recepcionadas_brr = obras_recepcionadas_brr['PROJETO']

        obras_recepcionadas_ibt = sh.worksheet('OBRAS IBOTIRAMA').get_all_values()
        obras_recepcionadas_ibt = pd.DataFrame(obras_recepcionadas_ibt, columns = obras_recepcionadas_ibt.pop(0))
        obras_recepcionadas_ibt = obras_recepcionadas_ibt.query("PROJETO != ''")
        obras_recepcionadas_ibt = obras_recepcionadas_ibt['PROJETO']

        obras_recepcionadas_geral = pd.concat([obras_recepcionadas_resolucao, obras_recepcionadas_vtc, obras_recepcionadas_jeq , obras_recepcionadas_brr, obras_recepcionadas_gbi, obras_recepcionadas_bjl, obras_recepcionadas_ire, obras_recepcionadas_ibt], ignore_index = True)

        cont = 0
        for i in obras_recepcionadas_geral:
            if ((i != None) and (i != '')):
                # print(i)
                obras_recepcionadas_geral[cont] = int(i[2:9])
            cont += 1

        obras_concluidas_sem_pasta_no_fechamento = []
        obras_concluidas = obras_concluidas.tolist()
        obras_recepcionadas_geral = obras_recepcionadas_geral.tolist()


        ####################### CONFERE QUAIS OBRAS JÁ ESTÃO NA PLANILHA DO FECHAMENTO
        cont = 0
        for cont, i in enumerate(obras_concluidas): 
            if i in obras_recepcionadas_geral:
                pass
            else:
                obras_concluidas_sem_pasta_no_fechamento.append(obras_concluidas[cont])
            cont += 1

        ####################### CONFERE QUAIS OBRAS ESTÃO PENDENTES DE ENVIO DA PASTA NO GEOEX
        status_aceitos = ['PASTA ACEITA', 'PASTA ACEITA COM RESTRIÇÃO', 'PASTA REJEITADA', 'PASTA REPOSTADA', 'PASTA POSTADA PARA RECEPÇÃO']
        projetos_pendente_asbuilt = [['UNIDADE', 'PROJETO', 'TÍTULO', 'VALOR DO PROJETO', 'DATA DE ENERGIZAÇÃO', 'SUPERVISOR', 'MUNICÍPIO']]
        url = 'https://geoex.com.br/api/EPS/ConsultarProjeto/Item'
        header = {
            'cookie': self.cookie_geoex,
            'usuarioid': 'e092ed10-dfdd-437c-9fe0-ab6bf9725410'
        }
        for i in obras_concluidas_sem_pasta_no_fechamento:
            print(i)
            resposta = requests.post(url, headers=header, json = {'id': str(i)})
            print(resposta)
            resposta = resposta.json()
            try:
                status_pasta = resposta['Content']['EnvioPastaStatus']
                print(status_pasta)
                if not(status_pasta in status_aceitos):
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
                    print(i)
                    projetos_pendente_asbuilt.append([unidade, i, titulo, vl_projeto, data_energ, supervisor, municipio])
                    print(projetos_pendente_asbuilt)
            except:
                print('sem acesso ao projeto ', i)


        sh = self.gspread_service.open_by_key('1WyjUGW3IP_or21BDUmi8QSf_Moa8RG53CgOyT5L_0OU')
        planilha_michelle = sh.worksheet('PENDENTE AS BUILT').get_all_values()
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
            
        sh = self.gspread_service.open_by_key('1GQ5pLG2DddGrEuRJILe-3g_Rwzhg-82EkVFZnX1_we4')
        pastas_pendentes = sh.worksheet('pastas pendentes')
        
        pastas_pendentes.update([['']*7]*10000)
        pastas_pendentes.update(projetos_pendente_asbuilt)
        sh.worksheet('data atualização').update('A1', [[datetime.now().strftime("%d/%m/%Y %H:%M")]])

        print(f'[{ datetime.now().strftime("%H:%M") }] As-builts atualizados!')


    def atualizar_avanco_gpm(self, unidade):

        """
            Atualizar avanço das obras na planilha online
        """

        while True:
            if unidade == 'conquista_1':
                id = self.id_planilha_planejamento_conquista
                sh = self.gspread_service.open_by_key(id)
                ws = sh.worksheet("CARTEIRA_VTC_1")
                break
            if unidade == 'conquista_2':
                id = self.id_planilha_planejamento_conquista
                sh = self.gspread_service.open_by_key(id)
                ws = sh.worksheet("CARTEIRA_VTC_2")
                break
            if unidade == 'itapetinga':
                id = self.id_planilha_planejamento_conquista
                sh = self.gspread_service.open_by_key(id)
                ws = sh.worksheet("CARTEIRA_ITAPETINGA")
                break
            elif unidade == 'barreiras':
                id = self.id_planilha_planejamento_barreiras
                sh = self.gspread_service.open_by_key(id)
                ws = sh.worksheet("CARTEIRA")
                break
            elif unidade == 'lapa':
                id = self.id_planilha_planejamento_lapa
                sh = self.gspread_service.open_by_key(id)
                ws = sh.worksheet("CARTEIRA")
                break
            elif unidade == 'ibotirama':
                id = self.id_planilha_planejamento_ibotirama
                sh = self.gspread_service.open_by_key(id)
                ws = sh.worksheet("CARTEIRA")
                break
            elif unidade == 'jequié':
                id = self.id_planilha_planejamento_jequie
                sh = self.gspread_service.open_by_key(id)
                ws = sh.worksheet("CARTEIRA")
                break
            elif unidade == 'irecê':
                id = self.id_planilha_planejamento_irece
                sh = self.gspread_service.open_by_key(id)
                ws = sh.worksheet("CARTEIRA")
                break
            elif unidade == 'guanambi':
                id = self.id_planilha_planejamento_guanambi
                sh = self.gspread_service.open_by_key(id)
                ws = sh.worksheet("CARTEIRA")
                break
            else:
                unidade = input(f'Selecionar uma das unidades abaixo:\n{self.unidades}')


        print(f'[{datetime.now().strftime("%H:%M")}] Atualizando avanço das obras na carteira de {unidade}...')

        producao_gpm = pd.read_sql_table('producao_gpm', self.engine_db)
        producao_gpm['obra'] = producao_gpm['obra'].str.extract(r'(\d{1,7})')
        producao_gpm.dropna(inplace=True, subset=['obra'])
        producao_gpm['obra'] = producao_gpm['obra'].astype(int)

        projetos = ws.col_values(3)
        projetos.pop(0)

        info_avanco = []
        info_avanco_cava = []
        mao_de_obra_lm = []
        mao_de_obra_lv = []

        for i in projetos:
            if i:
                todos_servicos = producao_gpm.loc[producao_gpm['obra'] == int(i)]

                qnt_cava = int(todos_servicos[todos_servicos['atividade'].str.contains('CAVA NORMAL EM SOLO COMUM')]['quantidade'].sum())
                qnt_poste = int(todos_servicos[todos_servicos['atividade'].str.contains('INSTALAR POSTE')]['quantidade'].sum())
                qnt_cabo_bt = float(todos_servicos[todos_servicos['atividade'].str.contains('INSTALAR CABO MULTIPLEX')]['quantidade'].sum())
                qnt_cabo_at = float(todos_servicos[todos_servicos['atividade'].str.contains('INSTALAR CABO AL CAA')]['quantidade'].sum())

                info_avanco.append([qnt_poste, qnt_cabo_at, qnt_cabo_bt])
                info_avanco_cava.append([qnt_cava])

                lm = float(todos_servicos[todos_servicos['codigo_atividade'].str.contains('SDEM')]['valor_tot'].sum() + todos_servicos[todos_servicos['codigo_atividade'].str.contains('SDMM')]['valor_tot'].sum() + todos_servicos[todos_servicos['codigo_atividade'].str.contains('SIR0')]['valor_tot'].sum())
                lv = float(todos_servicos[todos_servicos['codigo_atividade'].str.contains('SDEV')]['valor_tot'].sum() + todos_servicos[todos_servicos['codigo_atividade'].str.contains('SDMV')]['valor_tot'].sum())
                mao_de_obra_lm.append([lm])
                mao_de_obra_lv.append([lv])
                # print(i, lm, lv)
            else:
                info_avanco.append([0, 0, 0])
                info_avanco_cava.append([0])
                mao_de_obra_lm.append([0])
                mao_de_obra_lv.append([0])


        ws.update('AM2:AO', info_avanco)
        ws.update('AL2', info_avanco_cava)
        ws.update('X2', mao_de_obra_lm)
        ws.update('Y2', mao_de_obra_lv)

        print(f'[{datetime.now().strftime("%H:%M")}] Avanço das obras na carteira de {unidade} atualizado...')


    def atualizar_producao_db(
            self,
            data_inicio = datetime.strftime(datetime.today() - timedelta(days=7), '%Y-%m-%d'),
            data_fim = datetime.strftime(datetime.today(), '%Y-%m-%d')
    ):

        """
            Atualiza a tabela producao_gpm no banco de dados
        """

        print(f'[{datetime.now().strftime("%H:%M")}] Atualizando produção no banco de dados...')

        # print('Data início: ', data_inicio)
        # print('Data fim: ', data_fim)

        # BUSCA TODOS OS CÓDIGOS DE SERVIÇOS REALIZADOS
        url = 'https://sirtecba.gpm.srv.br/gpm/geral/consulta_servico.php'
        header = {
            'cookie': self.cookie_gpm
        }
        params = {
            "data_inicial": data_inicio,
            "data_final": data_fim,
            'submit': 'Pesquisar',
            'contrato': '11',
            'avancaTodos': 'Todas',
            'h_tot': '99999'
        }

        resposta = requests.post(url, headers = header, data = params)
        conteudo = resposta.content
        site = BeautifulSoup(conteudo,'html.parser') #Captura o html da página
        lista_codigos = []
        try:
            tabela = site.find('table', attrs={'class': 'tbl_100_c_5p_0h_lin zebra'})
            cod = tabela.findAll('font', attrs={'color': 'blue'})
            for i in cod:
                lista_codigos.append(i.text)
        except:
            pass
        
        params['contrato'] = '12' #Contratdo final 69
        resposta = requests.post(url, headers = header, data = params)
        conteudo = resposta.content
        site = BeautifulSoup(conteudo,'html.parser') #Captura o html da página
        try:
            tabela = site.find('table', attrs={'class': 'tbl_100_c_5p_0h_lin zebra'})
            cod = tabela.findAll('font', attrs={'color': 'blue'})
            for i in cod:
                lista_codigos.append(i.text)
        except:
            pass
        
        # print('sinal')

        # GERA A PLANILHA FINAL DAS ATIVIDADES REALIZADAS
        atividades = []
        if lista_codigos:
            for n_servico in lista_codigos:
                # print(n_servico)
                infos_servico = self.consultar_servico_gpm(n_servico)
                for atividade in infos_servico['atividades']:
                    atividades.append([
                        infos_servico['turma'],
                        infos_servico['obra'],
                        infos_servico['data'],
                        infos_servico['n_servico'],
                        atividade['cod_atividade'],
                        atividade['desc_atividade'],
                        atividade['quantidade'],
                        atividade['valor_unit'],
                        atividade['valor_tot'],
                    ])

        atividades = pd.DataFrame(atividades, columns=['turma', 'obra', 'data', 'n_servico', 'codigo_atividade', 'atividade', 'quantidade', 'valor_unit', 'valor_tot'])
        atividades['data'] = pd.to_datetime(atividades['data'], format='%d/%m/%Y %H:%M:%S')
        # print(atividades)
        
        con = self.engine_db.connect()
        try:
            con.execute(text(f"DELETE FROM producao_gpm WHERE data between '{data_inicio}' and '{data_fim}'"))
            con.commit()
            atividades.to_sql('producao_gpm', if_exists='append', index=False, con=con)
            con.execute(text(f"UPDATE data_hora_atualizacoes SET data_hora_atualizacao = '{datetime.strftime(datetime.now(),format='%Y-%m-%d %H:%M:%S')}'"))
            con.commit()
            con.close()
        except:
            con.rollback()
            con.execute(text(f"DELETE FROM producao_gpm WHERE data between '{data_inicio}' and '{data_fim}'"))
            con.commit()
            atividades.to_sql('producao_gpm', if_exists='append', index=False, con=con)
            con.execute(text(f"UPDATE data_hora_atualizacoes SET data_hora_atualizacao = '{datetime.strftime(datetime.now(),format='%Y-%m-%d %H:%M:%S')}' WHERE base_atualizada = 'Lançamentos GPM'"))
            con.commit()
            con.close()


        print(f'[{datetime.now().strftime("%H:%M")}] Producão GPM atualizada no DB')


    def atualizar_gpm_exportacao_dados_obras(
            self,
            data_inicio = datetime.strftime(datetime.today() - timedelta(days=30), '%Y-%m-%d'),
            data_fim = datetime.strftime(datetime.today(), '%Y-%m-%d')
    ):
        print(f'[{datetime.now().strftime("%H:%M")}] Atualizando produção GPM no banco de dados...')

        df = self.consultar_gpm_exportacao_dados_obras(data_inicio, data_fim)

        df['qtd_atividade'] = df['qtd_atividade'].str.replace('.','')
        df['qtd_atividade'] = df['qtd_atividade'].str.replace(',','.').astype(float)

        df['valor_unitario'] = df['valor_unitario'].str.replace('.','')
        df['valor_unitario'] = df['valor_unitario'].str.replace(',','.').astype(float)

        df['valor_total'] = df['valor_total'].str.replace('.','')
        df['valor_total'] = df['valor_total'].str.replace(',','.').astype(float)

        df['data_servico'] = pd.to_datetime(df['data_servico'], format='%d/%m/%Y')
        df['data_deslocamento'] = pd.to_datetime(df['data_deslocamento'], format='%d/%m/%Y %H:%M')
        df['data_inicio'] = pd.to_datetime(df['data_inicio'], format='%d/%m/%Y %H:%M')
        df['data_fim'] = pd.to_datetime(df['data_fim'], format='%d/%m/%Y %H:%M')
        df['abertura_turno'] = pd.to_datetime(df['abertura_turno'], format='%d/%m/%Y %H:%M')
        df['fechamento_turno'] = pd.to_datetime(df['fechamento_turno'], format='%d/%m/%Y %H:%M')

        # Define novas colunas
        df['n_projeto'] = df['cod_pep_obra'].str.extract(r'(\d{1,7})')
        df['n_projeto'].fillna(0, inplace = True)
        df['n_projeto'] = df['n_projeto'].astype(int)

        df['equipe'] = df['des_equipe']
        df.loc[df['des_equipe'].str.contains('BAR -'), 'equipe'] = df.loc[df['des_equipe'].str.contains('BAR -'), 'des_equipe'].str.extract(r'(BAR - [A-Z0-9]+)')[0]

        con = self.engine_db.connect()
        try:
            con.execute(text(f"DELETE FROM gpm_exportacao_dados_obras WHERE data_servico between '{data_inicio}' and '{data_fim}'"))
            con.commit()
            df.to_sql('gpm_exportacao_dados_obras', if_exists='append', index=False, con=con)
            con.execute(text(f"UPDATE data_hora_atualizacoes SET data_hora_atualizacao = '{datetime.strftime(datetime.now(),format='%Y-%m-%d %H:%M:%S')}' WHERE base_atualizada = 'Lançamentos GPM 2.0'"))
            con.commit()
            con.close()
        except:
            con.rollback()
            con.execute(text(f"DELETE FROM gpm_exportacao_dados_obras WHERE data_servico between '{data_inicio}' and '{data_fim}'"))
            con.commit()
            df.to_sql('gpm_exportacao_dados_obras', if_exists='append', index=False, con=con)
            con.execute(text(f"UPDATE data_hora_atualizacoes SET data_hora_atualizacao = '{datetime.strftime(datetime.now(),format='%Y-%m-%d %H:%M:%S')}' WHERE base_atualizada = 'Lançamentos GPM 2.0'"))
            con.commit()
            con.close()

        print(f'[{datetime.now().strftime("%H:%M")}] Producão GPM atualizada')


    def atualizar_v5(self, aba):
        """
            
            Atualiza as planilhas da V5 com as informações do geoex

        """
        sh = self.gspread_service.open_by_key(self.id_planilha_fechamento)
        v5 = sh.worksheet(aba).get_all_values()
        v5 = pd.DataFrame(v5, columns=v5.pop(0))
        projetos = v5['PROJETO']
        status_estagio_hektor = []
        auxiliar_hektor = []
        status_pasta = []
        
        for i in projetos:
            if i != None and i != '':
                ############################### ATUALIZA STATUS HECKTOR
                print(i)
                info_geoex = self.consultar_projeto_geoex(i)

                # Implementar validação de erros!!!
                if info_geoex != 'erro':
                    id_projeto = info_geoex['Content']['ProjetoId']

                    if info_geoex['Content']['GseProjeto'] != None:
                        status_estagio_hektor = info_geoex['Content']['GseProjeto']['Status']['Nome']
                    else:
                        status_estagio_hektor = ''

                    if info_geoex['Content']['EnvioPastaStatus'] != None:
                        status_pasta = info_geoex['Content']['EnvioPastaStatus']
                    else:
                        status_pasta = ''

                    info_termo = self.consultar_termo_geoex(id_projeto)


                    if info_termo['Content']['Items'] != []:
                        auxiliar_hektor = info_termo['Content']['Items'][0]['HistoricoStatus']['Nome']
                    else:
                        auxiliar_hektor = ''


                else:
                    status_estagio_hektor.append([''])
                    auxiliar_hektor.append([''])
                    status_pasta.append([''])
                
        sh.worksheet(aba).update('AP2:AP', status_estagio_hektor)
        sh.worksheet(aba).update('AY2:AY', auxiliar_hektor)
        sh.worksheet(aba).update('K2:K', status_pasta)


    def atualizar_acompanhamento_equipes(self):
        print(f"[{datetime.strftime(datetime.now(), format='%H:%M')}] Atualizando acompanhamento diário")

        hoje = datetime.strftime(datetime.now(), format='%d/%m/%Y')

        programacao = self.gspread_service.open_by_key(self.id_planilha_programacao)
        programacao = programacao.worksheet('PROGRAMAÇÃO')
        programacao = programacao.get('A:R')
        programacao = pd.DataFrame(programacao, columns=programacao.pop(0))

        carteira = self.gspread_service.open_by_key(self.id_planilha_carteira)
        carteira = carteira.worksheet('CARTEIRA')
        carteira = carteira.get('A:BF')
        carteira = pd.DataFrame(carteira, columns=carteira.pop(0))
        carteira = carteira[['PROJETO', 'TITULO', 'MUNICÍPIO', 'AR', 'PRIORIDADE', 'UNIDADE', 'LATITUDE', 'LONGITUDE']]
        carteira = carteira.loc[(carteira['LATITUDE'] != '') | (carteira['LONGITUDE'] != '')]
        carteira = carteira.loc[(carteira['LATITUDE'] != None) | (carteira['LONGITUDE'] != None)] 
        carteira = carteira.drop_duplicates(subset=['PROJETO'])
        carteira = carteira.rename(columns={'PROJETO': 'Projeto'})

        # placas = self.gspread_service.open_by_key('1R43FPCV7yOtMg0A58OZpIfhzbO5oKUA4d_O-fKRavPE')
        # placas = placas.worksheet('Página1')
        # placas = placas.get_all_records()
        # placas = pd.DataFrame(placas)

        turnos_abertos = self.consultar_abertura_turnos()
        turnos_abertos['cod_turno_tur'] = turnos_abertos['cod_turno_tur'].astype(int)
        turnos_abertos = turnos_abertos.rename(columns={'des_equipe': 'Equipe'})

        checklist_turnos = self.consultar_checklist_turno()

        coord_bases = pd.DataFrame({
            'UNIDADE': ['VITÓRIA DA CONQUISTA', 'GUANAMBI', 'JEQUIÉ', 'IRECÊ', 'BARREIRAS', 'IBOTIRAMA', 'BOM JESUS DA LAPA', 'ITAPETINGA'],
            'coord_base_lat': [-14.912110, -14.207817, -13.875085, -11.289493, -12.102239, -12.177864, -13.245444, -15.2405684],
            'coord_base_lon': [ -40.870711, -42.805218, -40.100514, -41.850959, -44.973280, -43.215805, -43.368768, -40.2369279]
        })

        acompanhamento_diario_equipes = programacao.loc[programacao['Data Execução'] == hoje]
        acompanhamento_diario_equipes = acompanhamento_diario_equipes[['Data Execução', 'Equipe', 'Projeto', 'Etapa', 'Supervisor', 'Mão de Obra']]
        acompanhamento_diario_equipes = acompanhamento_diario_equipes.merge(carteira[['Projeto', 'MUNICÍPIO', 'UNIDADE', 'LATITUDE', 'LONGITUDE']], on='Projeto', how='left')
        acompanhamento_diario_equipes = acompanhamento_diario_equipes.merge(turnos_abertos[['cod_turno_tur', 'Equipe', 'dta_solicitacao', 'placa']], on='Equipe', how='left')
        acompanhamento_diario_equipes = acompanhamento_diario_equipes.rename(columns = {'cod_turno_tur': 'Turno', 'placa': 'Placa'})
        acompanhamento_diario_equipes = acompanhamento_diario_equipes.merge(checklist_turnos[['Turno', 'Equipe possui câmera funcional?']], on='Turno', how='left')
        acompanhamento_diario_equipes = acompanhamento_diario_equipes.merge(coord_bases, on='UNIDADE', how='left')
        # acompanhamento_diario_equipes = acompanhamento_diario_equipes.merge(placas[['Equipe', 'ID FROTALOG']], on='Equipe', how='left')

        acompanhamento_diario_equipes = acompanhamento_diario_equipes.loc[acompanhamento_diario_equipes['Projeto'] != '-']
        acompanhamento_diario_equipes = acompanhamento_diario_equipes.loc[acompanhamento_diario_equipes['Projeto'] != 'FOLGA']

        acompanhamento_diario_equipes['LATITUDE'] = acompanhamento_diario_equipes['LATITUDE'].str.replace(',', '.')
        acompanhamento_diario_equipes['LATITUDE'] = acompanhamento_diario_equipes['LATITUDE'].astype(float)
        acompanhamento_diario_equipes['LONGITUDE'] = acompanhamento_diario_equipes['LONGITUDE'].str.replace(',', '.')
        acompanhamento_diario_equipes['LONGITUDE'] = acompanhamento_diario_equipes['LONGITUDE'].astype(float)

        acompanhamento_diario_equipes['dta_solicitacao'] = pd.to_datetime(acompanhamento_diario_equipes['dta_solicitacao'], format=('%d-%m-%Y %H:%M:%S'))
        acompanhamento_diario_equipes['Data Execução'] = pd.to_datetime(acompanhamento_diario_equipes['Data Execução'], format=('%d/%m/%Y'))
        # acompanhamento_diario_equipes['Mão de Obra'] = acompanhamento_diario_equipes['Mão de Obra'].str.replace('.', '')
        # acompanhamento_diario_equipes['Mão de Obra'] = acompanhamento_diario_equipes['Mão de Obra'].str.replace(',', '.')
        # acompanhamento_diario_equipes['Mão de Obra'] = acompanhamento_diario_equipes['Mão de Obra'].str.replace('R$', '')
        # acompanhamento_diario_equipes['Mão de Obra'] = acompanhamento_diario_equipes['Mão de Obra'].str.replace('-', '0')
        # acompanhamento_diario_equipes['Mão de Obra'] = acompanhamento_diario_equipes['Mão de Obra'].str.replace('', '0')
        # acompanhamento_diario_equipes['Mão de Obra'] = acompanhamento_diario_equipes['Mão de Obra'].astype(float)


        acompanhamento_diario_equipes['data_hora_localizacao'] = ''
        acompanhamento_diario_equipes['ultima_localizacao_lat'] = ''
        acompanhamento_diario_equipes['ultima_localizacao_lon'] = ''
        acompanhamento_diario_equipes['distancia_obra_prog_loc_equipe'] = ''
        acompanhamento_diario_equipes['distancia_base_loc_equipe'] = ''
        acompanhamento_diario_equipes['situacao_abertura_turno'] = ''
        acompanhamento_diario_equipes['situacao_equipe'] = ''

        for index, linha in acompanhamento_diario_equipes.iterrows():
            if pd.isna(linha['Placa']):
                pass
            else:
                localizacao = self.consultar_localizacao_viatura_frotalog(linha['Placa'])
                if localizacao['status'] == 'Viatura não localizada':
                    acompanhamento_diario_equipes.loc[index, 'situacao_equipe'] = 'Viatura não localizada'
                    continue
                if localizacao['status'] == 'Viatura localizada':
                    df_pontos = pd.DataFrame(localizacao['info']['markers'])
                    df_pontos['timestamp'] = pd.to_datetime(df_pontos['timestamp'], format="%Y-%m-%dT%H:%M:%S%z")


                    df_pontos['distancia_equipe_obra'] = df_pontos.apply(lambda row: self.calcular_distancia_radial([row['location']['latitude'], row['location']['longitude']], [linha['LATITUDE'], linha['LONGITUDE']]), axis=1)
                    df_pontos['distancia_equipe_base'] = df_pontos.apply(lambda row: self.calcular_distancia_radial([row['location']['latitude'], row['location']['longitude']], [linha['coord_base_lat'], linha['coord_base_lon']]), axis=1)


                    localizacao_atual = df_pontos.iloc[-1]['location']

                    acompanhamento_diario_equipes.loc[index, 'data_hora_localizacao'] = df_pontos.iloc[-1]['timestamp']
                    acompanhamento_diario_equipes.loc[index, 'ultima_localizacao_lat'] = localizacao_atual['latitude']
                    acompanhamento_diario_equipes.loc[index, 'ultima_localizacao_lon'] = localizacao_atual['longitude']
                    acompanhamento_diario_equipes.loc[index, 'distancia_obra_prog_loc_equipe'] = df_pontos.iloc[-1]['distancia_equipe_obra']
                    acompanhamento_diario_equipes.loc[index, 'distancia_base_loc_equipe'] = df_pontos.iloc[-1]['distancia_equipe_base']
                    
                    distancia_obra_base = self.calcular_distancia_radial([linha['LATITUDE'], linha['LONGITUDE']], [linha['coord_base_lat'], linha['coord_base_lon']])

                    ### Define direcionamento da equipe
                    qnt_pontos = df_pontos.shape[0]
                    if df_pontos.iloc[-1]['distancia_equipe_base'] < 2:
                        acompanhamento_diario_equipes.loc[index, 'situacao_equipe'] = 'Equipe na base'
                        continue
                    if df_pontos.iloc[-1]['distancia_equipe_obra'] < 5:
                        acompanhamento_diario_equipes.loc[index, 'situacao_equipe'] = 'Equipe na obra'
                        continue
                    if df_pontos.iloc[-1]['distancia_equipe_obra'] > (10 + distancia_obra_base): # Se a equipe andou 10km a mais que a distancia da base para a obra
                        acompanhamento_diario_equipes.loc[index, 'situacao_equipe'] = 'Desvio de rota'
                        continue
                    else:
                        if qnt_pontos > 2:
                            referencia_base = df_pontos.iloc[-1]['distancia_equipe_base'] - df_pontos.iloc[-3]['distancia_equipe_base']
                            referencia_obra = df_pontos.iloc[-1]['distancia_equipe_obra'] - df_pontos.iloc[-3]['distancia_equipe_obra']

                            if referencia_obra > 0 and referencia_base < 0:
                                acompanhamento_diario_equipes.loc[index, 'situacao_equipe'] = 'Indo em direção a base'
                            elif referencia_obra < 0:
                                acompanhamento_diario_equipes.loc[index, 'situacao_equipe'] = 'Indo em direção a obra'
                            elif referencia_obra == 0 and referencia_base == 0:
                                acompanhamento_diario_equipes.loc[index, 'situacao_equipe'] = 'Equipe parada fora da base'
                            else:
                                acompanhamento_diario_equipes.loc[index, 'situacao_equipe'] = 'Desvio de rota'
                        elif qnt_pontos <= 2:
                            acompanhamento_diario_equipes.loc[index, 'situacao_equipe'] = 'Equipe parada fora da base'
                                

        # Verifica pontualidade da abertura do turno
        agora = datetime.now()
        oito_e_dez = agora.replace(hour=8, minute=10, second=0, microsecond=0)
        acompanhamento_diario_equipes['situacao_abertura_turno'] = 'Pendente'
        acompanhamento_diario_equipes.loc[acompanhamento_diario_equipes['dta_solicitacao'] <= oito_e_dez, 'situacao_abertura_turno'] = 'Aberto'
        acompanhamento_diario_equipes.loc[acompanhamento_diario_equipes['dta_solicitacao'] > oito_e_dez, 'situacao_abertura_turno'] = 'Aberto atrasado'

        acompanhamento_diario_equipes = acompanhamento_diario_equipes.rename(columns={
            'Data Execução': 'data',
            'Equipe': 'equipe',
            'UNIDADE': 'unidade',
            'Equipe possui câmera funcional?': 'smc',
            'Turno': 'cod_turno_gpm',
            'Placa': 'placa_viatura',
            'dta_solicitacao': 'data_hora_abertura_turno',
            'Mão de Obra': 'mao_de_obra',
            'LATITUDE': 'localizacao_obra_prog_lat',
            'LONGITUDE': 'localizacao_obra_prog_lon',
            'Etapa': 'etapa_obra',
            'Projeto': 'obra_programada',
            'MUNICÍPIO': 'municipio',
            'Supervisor': 'supervisor',
        })

        data_hora_atualizacao = datetime.strftime(datetime.now(), format='%Y-%m-%d %H:%M:%S')

        con = self.engine_db.connect()
        try:
            con.execute(text(f"DELETE FROM acompanhamento_diario_equipes WHERE data = '{datetime.strftime(datetime.now(), format='%Y-%m-%d')}'"))
            con.commit()
            acompanhamento_diario_equipes.to_sql('acompanhamento_diario_equipes', if_exists='append', index=False, con=con)
            con.execute(text(f"UPDATE data_hora_atualizacoes SET data_hora_atualizacao = '{data_hora_atualizacao}' WHERE base_atualizada = 'Acompanhamento diário equipes'"))
            con.commit()
            con.close()
        except:
            con.rollback()
            con.execute(text(f"DELETE FROM acompanhamento_diario_equipes WHERE data = '{datetime.strftime(datetime.now(), format='%Y-%m-%d')}'"))
            con.commit()
            acompanhamento_diario_equipes.to_sql('acompanhamento_diario_equipes', if_exists='append', index=False, con=con)
            con.execute(text(f"UPDATE data_hora_atualizacoes SET data_hora_atualizacao = '{data_hora_atualizacao}' WHERE base_atualizada = 'Acompanhamento diário equipes'"))
            con.commit()
            con.close()

        print(f"[{datetime.strftime(datetime.now(), format='%H:%M')}] Acompanhamento diário atualizado")





    """

    METODOS DE CONSULTA

    """


    def consultar_projeto_geoex(self, projeto):
        url = 'https://geoex.com.br/api/EPS/ConsultarProjeto/Item'
        
        params = {
            'id': projeto
        }

        header = {
            'cookie': self.cookie_geoex
        }

        r = requests.post(url, params=params, headers=header)
        return r.json()


    def consultar_servico_gpm(self, codigo_serv):
        
        """
            Consulta informa as servicos no GPM
        """

        url = 'https://sirtecba.gpm.srv.br/gpm/geral/relatorio_servico.php'
        header = {
            'cookie': self.cookie_gpm
        }

        data = {
            'cod_srv': codigo_serv
        }

        resposta = requests.post(url, headers=header, data=data)
        conteudo = resposta.content
        site = BeautifulSoup(conteudo,'html.parser') #Captura o html da página

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

        coluna = linhas[11].findAll('td') # REGISTRA A DATA
        data = coluna[3].text
        data = data[0:10]
        data = datetime.strptime(data, '%d/%m/%Y')

        coluna = linhas[14].findAll('td') # REGISTRA O NÚMERO DA OBRA
        obra = coluna[1].text
        # if obra[0] == 'B':
            # obra = obra[0:9]
        obra = re.search(r'\d{7}', obra)
        if obra:
            obra = int(obra.group(0))

        coluna = linhas[22].findAll('td') # REGISTRA OBSERVAÇÃO CENTRAL
        obs_central = coluna[1].text

        coluna = linhas[23].findAll('td') # REGISTRA OBSERVAÇÃO SERVIÇO
        obs_serivco  = coluna[1].text

        atividades = []
        tabelas = site.findAll('table', attrs={'class': 'tbl_itens'}) # REGISTRA AS ATIVIDADES
        tabela = tabelas[2]
        linhas = tabela.findAll('tr')
        if len(linhas) == 2: # REGISTRO EM CASO DE NENHUMA ATIVIDADE CADASTRADA PELA TURMA
            atividades.append({
                    'cod_atividade': '',
                    'desc_atividade': '',
                    'quantidade': '',
                    'valor_unit': '',
                    'valor_tot': '',
                })
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

                atividades.append({
                    'cod_atividade': codigo_ativ,
                    'desc_atividade': atividade,
                    'quantidade': quantidade,
                    'valor_unit': valor_unit,
                    'valor_tot': valor_tot,
                })

        dados_servico = {
            'n_servico': codigo_serv,
            'data': data,
            'obra': obra,
            'turma': turma,
            'obs_servico': obs_serivco,
            'obs_central': obs_central,
            'atividades': atividades
        }

        return(dados_servico)    


    def consultar_localizacao_viatura_frotalog(self, placa):
        self.atualizar_cookie_frotalog()

        # df_equipes = pd.read_sql_table('equipes', con=self.engine_db)
        df_equipes = pd.read_excel(os.path.join(self.path, 'placas_veiculos.xlsx'))
        df_equipes = df_equipes.dropna(subset=['Id_frotalog'])
        cod_viatura = df_equipes.loc[df_equipes['Placa'] == placa]['Id_frotalog'].values
        if cod_viatura:
            cod_viatura = int(df_equipes.loc[df_equipes['Placa'] == placa]['Id_frotalog'].values)
        else:
            return {'status': 'Placa sem ID vinculado'}

        srv = 'routeRebuild'
        vehiclieId = str(cod_viatura)
        startDate = datetime.strftime(datetime.now() - timedelta(hours=12), '%Y-%m-%dT%H:%M:%S')
        endDate = datetime.strftime(datetime.now(), '%Y-%m-%dT%H:%M:%S')
        precision = '30'

        url = f'https://www.frotalog.com.br/MBServerO/routeRebuildServlet?srv={srv}&vehicleId={vehiclieId}&startDate={startDate}&endDate={endDate}&precision={precision}'
        header={'cookie':self.cookie_frotalog}

        r = requests.get(url=url, headers=header)

        if r.text == '': # Se não houver retorno da API, atualiza o cookie e tenta novamente
            self.atualizar_cookie_frotalog()
            header={'cookie':self.cookie_frotalog}
            r = requests.get(url=url, headers=header)
            if r.text != '':
                return {
                    'status': 'Viatura localizada',
                    'info': r.json()
                }
            else:
                return {
                    'status': 'Viatura não localizada'
                }
        else:
            return {
                'status': 'Viatura localizada',
                'info': r.json()
            }

    
    def consultar_localizacao_viatura_gpm(self, contrato):
        """
        Consulta localização das equipes por contrato

        Retorno em formato de DataFrame Pandas

        12 = Contrato Oeste
        11 = Contrato Sudoeste
        """

        url = 'https://sirtecba.gpm.srv.br/gpm/geral/resumo_online_rpc.php'

        params =  {
            'opc': "rpc", 
            'ct': '12', 
            'ts': '', 
            'processo': '', 
            'co': '',
            'li': '', 
            'eq': '', 
            'tp': 'mp', 
            'localidade': '', 
            'centrodeservico': '',
        }

        header = {'cookie': self.cookie_gpm}

        r = requests.post(url, data=params, headers=header).json()

        df = pd.DataFrame(r['equipes'], columns=['cod_equipe1', 'equipe', 'placa_viatura', 'latitude', 'longitude', 'link', 'data_hora_registro', '1', '2', '3', '4', '5', 'encarregado', '6', 'supervisor', '7', '8'])

        return(df)


    def consultar_abertura_turnos(
            self,
            data_inicio = datetime.strftime(datetime.now(), format='%d/%m/%Y'),
            data_fim = datetime.strftime(datetime.now(), format='%d/%m/%Y')
        ):


        url = f'https://sirtecba.gpm.srv.br/gpm/geral/consulta_turno_csv.php?isMobile=0&cod_turno=&num_os=&tip_srv=&contrato=-1&situ=&data_inicial={data_inicio}&data_final={data_fim}&insp=&regra=&apar=&veic=&obra=&h_tot=999&min=0&max=30&maxAux=0&base=1&top=30&mdl_equipe=&mdl_cent=-1&mdl_lider=&mdl_coord='

        header = {'cookie': self.cookie_gpm}

        r = requests.get(url, headers=header)
        open(os.path.join(self.path, r'Downloads\abertura_turnos.zip'), 'wb').write(r.content)
        df = pd.read_csv(os.path.join(self.path, r'Downloads\abertura_turnos.zip'), sep=';', compression='zip', low_memory=False)

        return df


    def consultar_checklist_turno(self, data = datetime.strftime(datetime.now(), format='%d/%m/%Y')):

        url_checklist_ccm = f'https://sirtecba.gpm.srv.br/gpm/geral/checklist_perguntas_respostas.php?tipo=csv&data_in={data}+00:00&data_out={data}+23:59&final=3&quali=todos&tp_form=106'

        colunas = [
            "Turno",
            "Equipe",
            "Data Execução",
            "COVID - Possui algum colaborador da equipe com sintomas?",
            "Equipe possui câmera funcional?",
            # "Qual foi o tema do DDS de hoje?",
            "Todos os EPI´s estão em condições de uso (uniformes / isolantes / equipamentos para escalada segura, etc)?",
            "Todos os EPC´s e ferramentas estão em condições de uso (aterramentos temporários, coberturas isolantes, escadas, detector de tensão, etc)?",
            "O veículo está em condições de uso (documentação do veículo  / equipamentos obrigatórios  / sem vazamentos /  sinalização elétrica)?",
            "O(s) colaborador(es) se encontra(m) em condições física, mental e emocional para realizar suas atividades diárias?"
        ]

        header = {'cookie': self.cookie_gpm}

        r = requests.get(url_checklist_ccm, headers=header)
        try:
            open(os.path.join(self.path, r'Downloads\checklist_diario_ccm.zip'), 'wb').write(r.content)
            df_checklist_diario_ccm = pd.read_csv(os.path.join(self.path, r'Downloads\checklist_diario_ccm.zip'), sep=';', compression='zip')
            # print(df_checklist_diario_ccm)
            df_checklist_diario_ccm = df_checklist_diario_ccm.drop_duplicates(subset=['Turno'])
            df_checklist_diario_ccm = df_checklist_diario_ccm[colunas]
            return(df_checklist_diario_ccm)
        except Exception as e:
            return e


    def consultar_cubo_servico_gpm(self, ref):

        """
            Busca as informações da pagina Cubo Serviço do GPM (https://sirtecba.gpm.srv.br/gpm/geral/cubo_servico.php)
        """
        
        url = f'https://sirtecba.gpm.srv.br/gpm/geral/cubo_servico.php?exp=excel&ref={ref}&mod=3&cont=11, 12'
        header = {
            'cookie': self.cookie_gpm
        }

        r = requests.get(url, headers = header)
        open(os.path.join(self.path, r"Downloads\Cubo Serviços - GPM.zip"), 'wb').write(r.content)
        df = pd.read_csv(os.path.join(self.path, r"Downloads\Cubo Serviços - GPM.zip"), sep=';', compression='zip', low_memory=False)

        return df
    
    
    def consultar_projetos_em_curso(self):
        url = 'https://geoex.com.br/api/Job/Download/Preparar'
        header = {'cookie': self.cookie_geoex}
        params = {
            "Titulo": "Job - PROJETOS - EM CURSO",
            "IsOpen": 'false',
            "Itens": [
                {
                    "JobId": "274f6dda-a3db-4cd2-96d7-bc4a969aa1e7"
                }
            ]
        }

        r = requests.post(url, json=params, headers=header).json()
        link_download = r['Content']['Itens'][0]['Url']
        zip = requests.get(link_download)
        open(os.path.join(self.path, r'Downloads\projetos_em_curso.zip'), 'wb').write(zip.content)
        df = pd.read_csv(os.path.join(self.path, r'Downloads\projetos_em_curso.zip'), sep=';',encoding='ISO-8859-1', compression='zip', low_memory=False)
        
        return(df)


    def consultar_gpm_exportacao_dados_obras(self, data_inicio, data_fim):
        """
            Consulta no GPM a pagina:
                
                Obras Eletricas >> Relatórios >> Exportação dados obras
        """

        url = f'https://sirtecba.gpm.srv.br/gpm/geral/exporta_obras_detalhado.php?data_inicial={data_inicio}&data_final={data_fim}&data_inicial_b=&data_final_b=&data_inicial_v=&data_final_v=&data_inicial_d=&data_final_d=&data_inicial_c=&data_final_c=&data_inicial_co=&data_final_co=&cliente=&num_os=&turno=&num_se=&coord=&bairro=&lider=&insp='

        header = {
            'cookie': self.cookie_gpm
        }

        r = requests.get(url, headers=header)
        open(os.path.join(self.path, r'Downloads\relatorio_servicos_gpm.zip'), 'wb').write(r.content)

        df = pd.read_csv(os.path.join(self.path, r'Downloads\relatorio_servicos_gpm.zip'), compression='zip', sep=';', low_memory=False)

        return(df)


    def consultar_termo_geoex(self, id_projeto):
        url = 'https://geoex.com.br/api/ConsultarProjeto/TermoGeo/Itens'
        body = {
            'ProjetoId': id_projeto,
            'Paginacao': {
                'Pagina': '1',
                'TotalPorPagina': '10'
            }
        }
        header = {
            'cookie': self.cookie_geoex
        }

        resposta = requests.post(url, headers=header, json=body).json()

        return resposta


    """
    
    OUTROS MÉTODOS

    """

    def atualizar_cookie_frotalog(self):
        # Atualizar cookie

        url = 'https://www.frotalog.com.br/MBServerO/sessionControl.do'
        data = {
            'dispatch': 'handleSessions',
            'userName': 'hugo.viana',
            'password': '@Sirtec2023',
        }

        r = requests.post(url, data=data)
        r = r.headers
        self.cookie_frotalog = r['set-cookie']


    def calcular_distancia_radial(self, coord1, coord2):

        """
        Distância calculada com base na Fórmula de Haversine
        """
        
        # Converter coordenadas de graus para radianos
        lat1, long1 = math.radians(coord1[0]), math.radians(coord1[1])
        lat2, long2 = math.radians(coord2[0]), math.radians(coord2[1])

        # Raio médio da Terra em metros
        r = 6371000

        # Aplicar a fórmula de Haversine
        dlat = lat2 - lat1
        dlong = long2 - long1
        a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlong/2)**2
        c = 2 * math.asin(math.sqrt(a))
        distancia = r * c

        return distancia/1000 # Retorna valor em Km


    def baixar_base_2023(self):
        print('Lendo a planilha...')
        gs = gspread.service_account(filename=os.path.join(self.path, 'service_account.json'))
        sh = gs.open_by_key(self.id_planilha_carteira)
        ws = sh.worksheet("CARTEIRA")

        carteira = pd.DataFrame(ws.get_all_records())
        carteira = carteira.loc[carteira['PROJETO'] != '']

        ws = sh.worksheet("PROGRAMAÇÃO")
        programacao = pd.DataFrame(ws.get_all_records())
        programacao = programacao.loc[programacao['Projeto'] != '']

        print('Gerando excel...')
        # carteira.to_excel('BASE 2023.xlsx', index=False)
        with pd.ExcelWriter(os.path.join(self.path,r'Downloads\BASE 2023.xlsx'), engine='xlsxwriter') as writer:
            carteira.to_excel(writer, sheet_name='CARTEIRA', index=False)
            programacao.to_excel(writer, sheet_name='PROGRAMAÇÃO', index=False)