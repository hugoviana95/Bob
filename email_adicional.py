from numpy import rec
import pandas as pd
from read_sheets import read_sheets
from write_sheets import write_sheets
from datetime import datetime, timedelta
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from openpyxl import load_workbook


def gera_planilha(id, id2, id3, id4):

    hoje = datetime.today()
    ontem = hoje - timedelta(1)
    ontem = ontem.strftime('%d/%m/%Y')


    ####### PLANILHA ALMOX VTC
    planilha = read_sheets(id, 'Controle de movimentação!A:Z', 'FORMATTED_VALUE')

    df = pd.DataFrame(planilha, columns = planilha.pop(0))
    # df = df[['Status', 'Tipo de Movimento', 'Tipo de Projeto', 'OBSERVAÇÕES', 'Codigo', 'Tipo', 'Projeto/OC', 'Centro', 'Depósito', 'Movimento', 'Descrição', 'Quantidade']]
    df = df.loc[df['OBSERVAÇÕES'] == '']
    df = df.loc[df['Status'] == 'DIRECIONADA AO PCP']

    df = pd.merge(df.loc[df['Tipo'] == 'Projeto CCM'], df.loc[df['Tipo'] == 'Projeto KIT'], how = 'outer')
    df = df.sort_values(by=['Projeto/OC'])
    print(df)
    wb = load_workbook('modelo.xlsx')
    sh = wb['MODELO_SOLICITAÇÃO DE RESERVA']
    
    solicitante = 'HUGO VIANA'
    utep = 'SUDOESTE'
    recebedor = '3ST001'

    for i in df.values.tolist():
        centro = int(i[17])
        cod = int(i[19])
        movimento = int(i[20])
        descricao = i[21]
        qtd = i[23].replace('.', '').replace(',', '.')
        qtd = float(qtd)

        num = i[16]

        if num[-5:] == '-SUPR':
            num = num[:-5]
        try:
            num = int(num[-7:])
            if num > 999999:
                prj = 'B-' + str(num) + '-SUPR'
            elif num > 99999:
                prj = 'B-0' + str(num) + '-SUPR'
        except:
            try:
                num = int(i[-6:])
                prj = 'B-0' + str(num) + '-SUPR'
            except:
                prj = '-'
                print('### ERRO AO GERAR PLANILHA DE ADICIONAIS!!! ###')

        linha = ['', solicitante, utep, prj, '', ontem, 'SIRTEC', 'FALHA NO ORÇAMENTO', cod, descricao, qtd, 'SUPR', centro, recebedor, movimento]
        sh.append(linha)


   

    ####### PLANILHA DE ADICIONAIS VTC
    planilha2 = read_sheets(id2, 'Página1!A:Q', 'FORMATTED_VALUE') 
    df2 = pd.DataFrame(planilha2, columns=planilha2.pop(0))
    df2 = df2.loc[df2['STATUS'] == 'NÃO CRIADA']
    df2 = df2.reset_index(drop = True)
    df2['CÓDIGO DO MATERIAL'] = df2['CÓDIGO DO MATERIAL'].astype(int)
    df2['CENTRO'] = df2['CENTRO'].astype(int)
    df2['TIPO DE MOVIMENTO'] = df2['TIPO DE MOVIMENTO'].astype(int)
    qtd = []
    for i in df2['QTD.']:
        i = i.replace('.', '')
        i = i.replace(',', '.')
        qtd.append(i)
    df2['QTD.'] = pd.DataFrame(qtd)
    df2['QTD.'] = df2['QTD.'].astype(float)
    lista = df2.values.tolist()
    for i in lista:
        sh.append(i)


    
    ####### PLANILHA ALMOX GBI
    planilha = read_sheets(id3, 'Controle de movimentação!A:AD', 'FORMATTED_VALUE')

    df = pd.DataFrame(planilha, columns = planilha.pop(0))
    # df = df[['OBSERVAÇÕES', 'Codigo', 'Tipo', 'Projeto/OC', 'Centro', 'Depósito', 'Movimento', 'Descrição', 'Quantidade']]
    df = df.loc[df['OBSERVAÇÕES'] == '']
    df = df.loc[df['Status'] == 'DIRECIONADA AO PCP']

    df = pd.merge(df.loc[df['Tipo'] == 'Projeto CCM'], df.loc[df['Tipo'] == 'Projeto KIT'], how = 'outer')
    df = df.sort_values(by=['Projeto/OC'])
    
    solicitante = 'HUGO VIANA'
    utep = 'SUDOESTE'
    recebedor = '3ST700'

    for i in df.values.tolist():
        centro = int(i[21])
        cod = int(i[23])
        movimento = int(i[24])
        descricao = i[25]
        qtd = i[27].replace('.', '').replace(',', '.')
        qtd = float(qtd)

        num = i[20]

        if num[-5:] == '-SUPR':
            num = num[:-5]

        try:
            num = int(num[-7:])
            if num > 999999:
                prj = 'B-' + str(num) + '-SUPR'
            elif num > 99999:
                prj = 'B-0' + str(num) + '-SUPR'
        except:
            try:
                num = int(i[-6:])
                prj = 'B-0' + str(num) + '-SUPR'
            except:
                print('### ERRO AO GERAR PLANILHA DE ADICIONAIS!!! ###')

        linha = ['', solicitante, utep, prj, '', ontem, 'SIRTEC', 'FALHA NO ORÇAMENTO', cod, descricao, qtd, 'SUPR', centro, recebedor, movimento]
        sh.append(linha)


   

    ####### PLANILHA DE ADICIONAIS GBI
    planilha2 = read_sheets(id4, 'Página1!A:Q', 'FORMATTED_VALUE') 
    df2 = pd.DataFrame(planilha2, columns=planilha2.pop(0))
    df2 = df2.loc[df2['STATUS'] == 'NÃO CRIADA']
    df2 = df2.reset_index(drop = True)
    df2['CÓDIGO DO MATERIAL'] = df2['CÓDIGO DO MATERIAL'].astype(int)
    df2['CENTRO'] = df2['CENTRO'].astype(int)
    df2['TIPO DE MOVIMENTO'] = df2['TIPO DE MOVIMENTO'].astype(int)
    qtd = []
    for i in df2['QTD.']:
        i = i.replace('.', '')
        i = i.replace(',', '.')
        qtd.append(i)
    df2['QTD.'] = pd.DataFrame(qtd)
    df2['QTD.'] = df2['QTD.'].astype(float)
    lista = df2.values.tolist()
    for i in lista:
        sh.append(i)
  

    wb.save('adicional.xlsx')



def envia_email(remetente, destinatarios):
    # Configuração
    host = 'email-ssl.com.br'
    port = 587
    user = remetente
    password = '@Seguranca83@49'

    # Criando objeto
    print('Criando objeto servidor...')
    server = smtplib.SMTP(host, port)

    # Login com servidor
    print('Login...')
    server.ehlo()
    server.starttls()
    server.login(user, password)

    # Criando mensagem
    message = ("Bom dia!<br />Segue anexo com solicitação de reservas adicionais.<br /><br />Obs. Caso não tenham sido criada as reservas solicitadas no dia anterior, criar somente as reservas solicitadas neste e-mail.<br /><br />")
    print('Criando mensagem...')
    email_msg = MIMEMultipart()
    email_msg['From'] = user
    email_msg['To'] = ', '.join(destinatarios)
    email_msg['Subject'] = 'RESERVA ADICIONAL - SIRTEC'
    print('Adicionando texto...')
    email_msg.attach(MIMEText(message, 'html'))

    print('Obtendo arquivo...')
    filename = 'adicional.xlsx' 
    filepath = './adicional.xlsx'
    attachment = open(filepath, 'rb')

    print('Lendo arquivo...')
    att = MIMEBase('application', 'octet-stream')
    att.set_payload(attachment.read())
    encoders.encode_base64(att)
    att.add_header('Content-Disposition', f'attachment; filename= {filename}')

    attachment.close()
    print('Adicionando arquivo ao email...')
    email_msg.attach(att)

    # Enviando mensagem
    print('Enviando mensagem...')
    server.sendmail(email_msg['From'], destinatarios, email_msg.as_string())
    print('Mensagem enviada!')
    server.quit()


def email_adicional():

    ################# Planilha VTC
    gera_planilha('1WgK8d1LoqvoHh48s8cAiEPLCKNeTaW4YvIsK_4ksgQY', '1aShY46WRCGR3dFbzPL0Npht-lxR8ioygulHKvF6L6-w', '12PL4LhpQG-btFN7Eio0tCw5aglZ9PM1FkqXuJAj7fhY', '1ibkkR1lk2GXIl2zjfZwbvzAvv4LEZOh8bpXL1xnIwns') 
    envia_email(

        'reservas.sirtecba@sirtec.com.br',

        [
        'rebeca.barbosa@neoenergia.com',
        'david.sgoncalves@neoenergia.com',
        'maria.sousa@neoenergia.com',
        'hugo.viana@sirtec.com.br',
        'carlos.batista@sirtec.com.br',
        'henrique.carneiro@sirtec.com.br',
        'silbene.abreu@sirtec.com.br',
        'rdasilva@boslan.com',
        'brenda.moreira@sirtec.com.br',
        'witon.demetrio@sirtec.com.br',
        'carla.gomes@sirtec.com.br',
        'daiane.carvalho@sirtec.com.br',
        'iago.dias@sirtec.com.br',
        'max.filho@sirtec.com.br',
        ]
    )


if __name__ == '__main__':
    email_adicional()