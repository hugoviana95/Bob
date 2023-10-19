from bob import Bob
from atualiza_v5 import atualiza_v5

bob = Bob()

print('Para obter ajuda digite "ajuda"')

while True:
    lista_comandos = [
        'sair',
        'ajuda',
        'atualizar v5',
        'atualizar asbuilt',
        'atualizar base gpm',
        'atualizar acompanhamento das equipes',
        'baixar base'
    ]

    comando = input('\nInforme algum comando: ')

    if not(comando in lista_comandos):
        print('Comando não identificado...')
        continue

    try:
        if comando == lista_comandos[0]:
            break

        if comando == lista_comandos[1]:
            print(lista_comandos)

        if comando == lista_comandos[2]:
            atualiza_v5()


        if comando == lista_comandos[3]:
            bob.atualizar_pendencia_asbuilt()

        if comando == lista_comandos[4]:
            bob.atualizar_gpm_exportacao_dados_obras()
        
        if comando == lista_comandos[5]:
            bob.atualizar_acompanhamento_equipes()
        
        if comando == lista_comandos[6]:
            bob.baixar_base_2023()


    except Exception as e:
        print(e)

