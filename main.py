import threading
import schedule
from datetime import datetime
from time import sleep

from bob import Bob
from atualiza_turno import atualiza_turno
from atualiza_v5 import atualiza_v5
# from email_adicional import email_adicional

bob = Bob()

# Atualizações da abertura de turno
# schedule.every().day.at("06:00").do(atualiza_turno)
# schedule.every().day.at("06:30").do(atualiza_turno)
# schedule.every().day.at("07:00").do(atualiza_turno)
# schedule.every().day.at("07:10").do(atualiza_turno)
# schedule.every().day.at("07:20").do(atualiza_turno)
# schedule.every().day.at("07:30").do(atualiza_turno)
# schedule.every().day.at("07:40").do(atualiza_turno)
# schedule.every().day.at("07:50").do(atualiza_turno)
# schedule.every().day.at("08:00").do(atualiza_turno)
# schedule.every().day.at("08:30").do(atualiza_turno)
# schedule.every().day.at("09:00").do(atualiza_turno)
# schedule.every().day.at("10:00").do(atualiza_turno)
# schedule.every().day.at("11:00").do(atualiza_turno)
# schedule.every().day.at("12:00").do(atualiza_turno)
# schedule.every().day.at("13:00").do(atualiza_turno)
# schedule.every().day.at("14:00").do(atualiza_turno)
# schedule.every().day.at("15:00").do(atualiza_turno)
# schedule.every().day.at("16:00").do(atualiza_turno)
# schedule.every().day.at("18:00").do(atualiza_turno)
# schedule.every().day.at("20:00").do(atualiza_turno)
# schedule.every().day.at("22:00").do(atualiza_turno)
# schedule.every().day.at("23:59").do(atualiza_turno)

# Atualizações do acompanhamento das equipes
schedule.every().day.at("06:30").do(bob.atualizar_pendencia_asbuilt)
schedule.every().day.at("07:10").do(bob.atualizar_acompanhamento_equipes)
schedule.every().day.at("07:30").do(bob.atualizar_acompanhamento_equipes)
schedule.every().day.at("07:33").do(atualiza_v5)
schedule.every().day.at("07:50").do(bob.atualizar_acompanhamento_equipes)
schedule.every().day.at("08:00").do(bob.atualizar_producao_db)

schedule.every().day.at("08:10").do(lambda: bob.atualizar_avanco_gpm('conquista_1'))
schedule.every().day.at("08:10").do(lambda: bob.atualizar_avanco_gpm('conquista_2'))
schedule.every().day.at("08:10").do(lambda: bob.atualizar_avanco_gpm('itapetinga'))
schedule.every().day.at("08:13").do(lambda: bob.atualizar_avanco_gpm('barreiras'))
schedule.every().day.at("08:15").do(lambda: bob.atualizar_avanco_gpm('jequié'))
schedule.every().day.at("08:17").do(lambda: bob.atualizar_avanco_gpm('irecê'))
schedule.every().day.at("08:19").do(lambda: bob.atualizar_avanco_gpm('guanambi'))
schedule.every().day.at("08:22").do(lambda: bob.atualizar_avanco_gpm('itapetinga'))
schedule.every().day.at("08:25").do(lambda: bob.atualizar_avanco_gpm('lapa'))

schedule.every().day.at("08:30").do(bob.atualizar_acompanhamento_equipes)
schedule.every().day.at("08:37").do(bob.atualizar_pendencia_asbuilt)
schedule.every().day.at("09:00").do(bob.atualizar_acompanhamento_equipes)
schedule.every().day.at("09:30").do(atualiza_v5)
schedule.every().day.at("09:30").do(bob.atualizar_acompanhamento_equipes)
schedule.every().day.at("10:00").do(bob.atualizar_acompanhamento_equipes)
schedule.every().day.at("10:05").do(bob.atualizar_producao_db)
schedule.every().day.at("10:30").do(bob.atualizar_pendencia_asbuilt)
schedule.every().day.at("10:30").do(bob.atualizar_acompanhamento_equipes)
schedule.every().day.at("11:00").do(bob.atualizar_acompanhamento_equipes)
schedule.every().day.at("11:30").do(atualiza_v5)
schedule.every().day.at("11:30").do(bob.atualizar_acompanhamento_equipes)
schedule.every().day.at("12:00").do(bob.atualizar_acompanhamento_equipes)
schedule.every().day.at("12:30").do(bob.atualizar_pendencia_asbuilt)
schedule.every().day.at("13:00").do(bob.atualizar_producao_db)
schedule.every().day.at("13:00").do(bob.atualizar_acompanhamento_equipes)
schedule.every().day.at("13:30").do(atualiza_v5)
schedule.every().day.at("14:00").do(bob.atualizar_acompanhamento_equipes)
schedule.every().day.at("14:30").do(bob.atualizar_pendencia_asbuilt)
schedule.every().day.at("15:00").do(bob.atualizar_acompanhamento_equipes)
schedule.every().day.at("15:30").do(atualiza_v5)
schedule.every().day.at("16:00").do(bob.atualizar_acompanhamento_equipes)
schedule.every().day.at("16:30").do(bob.atualizar_pendencia_asbuilt)
schedule.every().day.at("17:30").do(atualiza_v5)
schedule.every().day.at("18:00").do(bob.atualizar_producao_db)
schedule.every().day.at("18:30").do(bob.atualizar_pendencia_asbuilt)
schedule.every().day.at("19:30").do(atualiza_v5)
schedule.every().day.at("20:30").do(bob.atualizar_pendencia_asbuilt)
schedule.every().day.at("21:30").do(atualiza_v5)
schedule.every().day.at("22:00").do(bob.atualizar_producao_db)
schedule.every().day.at("22:30").do(bob.atualizar_pendencia_asbuilt)
schedule.every().day.at("23:30").do(atualiza_v5)


def loop_schedule():
    while True:
        try:
            schedule.run_pending()
        except Exception as e:
            print('erro')
            print(e)

        sleep(1)

threading.Thread(target=loop_schedule, daemon=True).start()

while True:
    comando = input('')
    if comando == 'turnos':
        atualiza_turno()
    if comando == 'avancogpm':
        unidade = input(f'A planilha de qual unidade que deseja atualizar? \n {bob.unidades} \n')
        bob.atualizar_avanco_gpm(unidade)
    if comando == 'v5':
        atualiza_v5()
    if comando == 'asbuilt':
        bob.atualizar_pendencia_asbuilt()
    if comando == 'producaodb':
        bob.atualizar_producao_db()
    if comando == 'stop':
        break
