import threading
from atualiza_turno import atualiza_turno
from atualiza_producao import atualiza_producao
from atualiza_v5 import atualiza_v5
from atualiza_asbuilt import atualiza_asbuilt
from faturamento import atualiza_faturamento
from email_adicional import email_adicional
from producao_diaria import atualiza_servicos
from time import sleep
from datetime import datetime


def turno():
    while True:
        sleep(600)
        try:
            atualiza_turno()
        except:
            print("Ocorreu algum erro ao atualizar a abertura de turnos")

def asbuilt():
    while True:
        sleep(1800)
        try:
            atualiza_asbuilt()
        except:
            print("Ocorreu algum erro ao atualizar a planilha de as built")
                   

def producao():
    while True:
        sleep(3800)
        try:
            atualiza_producao()
        except:
            print("Ocorreu algum erro ao atualizar produção")

def v5():
    while True:
        sleep(4800)
        try:
            atualiza_v5()
        except:
            print("Ocorreu algum erro ao atualizar a V5")

def faturamento():
    while True:
        agora = datetime.now().strftime('%H:%M')
        if agora == "05:00":
            atualiza_faturamento()
        sleep(1)

def producao_diaria():
    while True:
        agora = datetime.now().strftime('%H:%M')
        if agora == "05:30":
            atualiza_servicos()
        sleep(1)

def adicional():
    while True:
        hoje = datetime.today().isoweekday()
        if hoje in [1,2,3,4,5]:
            agora = datetime.now().strftime('%H:%M:%S')
            if agora == '07:00:00':
                email_adicional()
                sleep(0.5)

print('BOB v4.2\n')

t = []
t.append(threading.Thread(target = turno, name = 'atualiza_turno', daemon = True))
t.append(threading.Thread(target = producao, name = 'atualiza_producao', daemon = True))
t.append(threading.Thread(target = v5, name = 'atualiza_v5', daemon = True))
t.append(threading.Thread(target = asbuilt, name = 'atualiza_asbuilt', daemon = True))
t.append(threading.Thread(target = faturamento, name = 'faturamento', daemon = True))
t.append(threading.Thread(target = adicional, name = 'adicional', daemon = True))
t.append(threading.Thread(target = producao_diaria, name = 'adicional', daemon = True))
for i in t: 
    i.start()

while True:
    comando = input('')
    if comando == 'turnos':
        atualiza_turno()
    if comando == 'producao':
        atualiza_producao()
    if comando == 'v5':
        atualiza_v5()
    if comando == 'asbuilt':
        atualiza_asbuilt()
    if comando == 'stop':
        break