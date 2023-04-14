import threading
from atualiza_turno import atualiza_turno
from atualiza_avanco_gpm import avanco_gpm
from atualiza_v5 import atualiza_v5
from atualiza_asbuilt import atualiza_asbuilt
# from email_adicional import email_adicional
from atualiza_producao_db import atualiza_producao_db
from time import sleep
from datetime import datetime
import schedule

print('BOB v5.0\n')

# Atualizações da abertura de turno
schedule.every().day.at("06:00").do(atualiza_turno)
schedule.every().day.at("06:30").do(atualiza_turno)
schedule.every().day.at("07:00").do(atualiza_turno)
schedule.every().day.at("07:10").do(atualiza_turno)
schedule.every().day.at("07:20").do(atualiza_turno)
schedule.every().day.at("07:30").do(atualiza_turno)
schedule.every().day.at("07:40").do(atualiza_turno)
schedule.every().day.at("07:50").do(atualiza_turno)
schedule.every().day.at("08:00").do(atualiza_turno)
schedule.every().day.at("08:30").do(atualiza_turno)
schedule.every().day.at("09:00").do(atualiza_turno)
schedule.every().day.at("10:00").do(atualiza_turno)
schedule.every().day.at("11:00").do(atualiza_turno)
schedule.every().day.at("12:00").do(atualiza_turno)
schedule.every().day.at("13:00").do(atualiza_turno)
schedule.every().day.at("14:00").do(atualiza_turno)
schedule.every().day.at("15:00").do(atualiza_turno)
schedule.every().day.at("16:00").do(atualiza_turno)
schedule.every().day.at("18:00").do(atualiza_turno)
schedule.every().day.at("20:00").do(atualiza_turno)
schedule.every().day.at("22:00").do(atualiza_turno)
schedule.every().day.at("23:59").do(atualiza_turno)

# Atualizações da V5
schedule.every().day.at("07:30").do(atualiza_v5)
schedule.every().day.at("09:30").do(atualiza_v5)
schedule.every().day.at("11:30").do(atualiza_v5)
schedule.every().day.at("13:30").do(atualiza_v5)
schedule.every().day.at("15:30").do(atualiza_v5)
schedule.every().day.at("17:30").do(atualiza_v5)
schedule.every().day.at("19:30").do(atualiza_v5)
schedule.every().day.at("21:30").do(atualiza_v5)
schedule.every().day.at("23:30").do(atualiza_v5)

# Atualizações de asbuilt
schedule.every().day.at("06:30").do(atualiza_asbuilt)
schedule.every().day.at("08:30").do(atualiza_asbuilt)
schedule.every().day.at("10:30").do(atualiza_asbuilt)
schedule.every().day.at("12:30").do(atualiza_asbuilt)
schedule.every().day.at("14:30").do(atualiza_asbuilt)
schedule.every().day.at("16:30").do(atualiza_asbuilt)
schedule.every().day.at("18:30").do(atualiza_asbuilt)
schedule.every().day.at("20:30").do(atualiza_asbuilt)
schedule.every().day.at("22:30").do(atualiza_asbuilt)

# Atualiza avanço das obras na carteira
schedule.every().day.at("07:10").do(avanco_gpm)
schedule.every().day.at("19:00").do(avanco_gpm)

# Atualização lançamento das equipes no banco de dados
schedule.every().day.at("08:00").do(atualiza_producao_db)
schedule.every().day.at("13:00").do(atualiza_producao_db)
schedule.every().day.at("18:00").do(atualiza_producao_db)


def loop_schedule():
    while True:
        schedule.run_pending()
        sleep(1)

threading.Thread(target=loop_schedule, daemon=True).start()

while True:
    comando = input('')
    if comando == 'turnos':
        atualiza_turno()
    if comando == 'avancogpm':
        avanco_gpm()
    if comando == 'v5':
        atualiza_v5()
    if comando == 'asbuilt':
        atualiza_asbuilt()
    if comando == 'producaodb':
        atualiza_producao_db()
    if comando == 'stop':
        break