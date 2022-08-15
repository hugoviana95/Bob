from datetime import datetime
from read_sheets import read_sheets
from write_sheets import write_sheets


def atualiza_faturamento():
    data = datetime.today().strftime('%d/%m/%Y')
    planilha = read_sheets('1ySrGYSlqy9IGxlwBa8_a_T_DGPCIQqDDL8ysgC7Ih6Y', 'A:A', 'FORMATTED_VALUE')
    ultima_linha = len(planilha) + 1


    if [data] in planilha:
        pass
    else:
        controle_diario = read_sheets('19xV_P6KIoZB9U03yMcdRb2oF_Q7gVdaukjAvE4xOvl8', 'Controle Diario!E:I', 'UNFORMATTED_VALUE')
        faturamento_juliana = read_sheets('19xV_P6KIoZB9U03yMcdRb2oF_Q7gVdaukjAvE4xOvl8', 'Faturamento-Juliana!A:AY', 'UNFORMATTED_VALUE')
        
        tabela = []

        ############################ CONQUISTA
        regional = 'Sudoeste'
        unidade = 'Vitória da Conquista'
        setor = 'STC'
        valor_entregue = controle_diario[56][1]
        pedido_recebido = faturamento_juliana[109][3]
        tabela.append([data, regional, unidade, setor, valor_entregue, pedido_recebido])

        setor = 'Manutenção'
        valor_entregue = controle_diario[47][1]
        pedido_recebido = faturamento_juliana[109][5]
        tabela.append([data, regional, unidade, setor, valor_entregue, pedido_recebido])

        setor = 'Leitura'
        valor_entregue = controle_diario[64][1]
        pedido_recebido = faturamento_juliana[109][4]
        tabela.append([data, regional, unidade, setor, valor_entregue, pedido_recebido])

        setor = 'CCM'
        valor_entregue = controle_diario[27][1]
        pedido_recebido = faturamento_juliana[109][0]
        tabela.append([data, regional, unidade, setor, valor_entregue, pedido_recebido])

        setor = 'Linha Viva'
        valor_entregue = controle_diario[39][1]
        pedido_recebido = faturamento_juliana[109][2]
        tabela.append([data, regional, unidade, setor, valor_entregue, pedido_recebido])


        ############################ JEQUIÉ
        unidade = 'Jequié'
        setor = 'STC'
        valor_entregue = controle_diario[114][1]
        pedido_recebido = faturamento_juliana[109][8]
        tabela.append([data, regional, unidade, setor, valor_entregue, pedido_recebido])

        setor = 'Manutenção'
        valor_entregue = controle_diario[106][1]
        pedido_recebido = faturamento_juliana[109][10]
        tabela.append([data, regional, unidade, setor, valor_entregue, pedido_recebido])

        setor = 'Leitura'
        valor_entregue = controle_diario[122][1]
        pedido_recebido = faturamento_juliana[109][9]
        tabela.append([data, regional, unidade, setor, valor_entregue, pedido_recebido])

        setor = 'CCM'
        valor_entregue = controle_diario[98][1]
        pedido_recebido = faturamento_juliana[109][7]
        tabela.append([data, regional, unidade, setor, valor_entregue, pedido_recebido])


        ############################ BRUMADO
        unidade = 'Brumado'
        setor = 'STC'
        valor_entregue = controle_diario[75][1]
        pedido_recebido = faturamento_juliana[109][22]
        tabela.append([data, regional, unidade, setor, valor_entregue, pedido_recebido])

        setor = 'Leitura'
        valor_entregue = controle_diario[83][1]
        pedido_recebido = faturamento_juliana[109][23]
        tabela.append([data, regional, unidade, setor, valor_entregue, pedido_recebido])


        ############################ IRECÊ
        regional = 'Centro'
        unidade = 'Irecê'
        setor = 'STC'
        valor_entregue = controle_diario[159][1]
        pedido_recebido = faturamento_juliana[109][18]
        tabela.append([data, regional, unidade, setor, valor_entregue, pedido_recebido])

        setor = 'CCM'
        valor_entregue = controle_diario[143][1]
        pedido_recebido = faturamento_juliana[109][17] + faturamento_juliana[109][20]
        tabela.append([data, regional, unidade, setor, valor_entregue, pedido_recebido])

        setor = 'Leitura'
        valor_entregue = controle_diario[167][1]
        pedido_recebido = faturamento_juliana[109][19]
        tabela.append([data, regional, unidade, setor, valor_entregue, pedido_recebido])



        ############################ ITABERABA
        unidade = 'Itaberaba'
        setor = 'STC'
        valor_entregue = controle_diario[197][1]
        pedido_recebido = faturamento_juliana[109][25]
        tabela.append([data, regional, unidade, setor, valor_entregue, pedido_recebido])

        setor = 'Leitura'
        valor_entregue = controle_diario[205][1]
        pedido_recebido = faturamento_juliana[109][26]
        tabela.append([data, regional, unidade, setor, valor_entregue, pedido_recebido])


        ############################ FEIRA DE SANTANA
        unidade = 'Feira de Santana'
        setor = 'STC'
        valor_entregue = controle_diario[178][1]
        pedido_recebido = faturamento_juliana[109][28]
        tabela.append([data, regional, unidade, setor, valor_entregue, pedido_recebido])

        setor = 'Leitura'
        valor_entregue = controle_diario[186][1]
        pedido_recebido = faturamento_juliana[109][29]
        tabela.append([data, regional, unidade, setor, valor_entregue, pedido_recebido])


        ############################ SERRINHA
        unidade = 'Serrinha'
        setor = 'STC'
        valor_entregue = controle_diario[216][1]
        pedido_recebido = faturamento_juliana[109][31]
        tabela.append([data, regional, unidade, setor, valor_entregue, pedido_recebido])

        setor = 'Leitura'
        valor_entregue = controle_diario[224][1]
        pedido_recebido = faturamento_juliana[109][35]
        tabela.append([data, regional, unidade, setor, valor_entregue, pedido_recebido])


        ############################ BARREIRAS
        regional = 'Oeste'
        unidade = 'Barreiras'
        setor = 'STC'
        valor_entregue = controle_diario[252][1]
        pedido_recebido = faturamento_juliana[109][38]
        tabela.append([data, regional, unidade, setor, valor_entregue, pedido_recebido])

        setor = 'CCM'
        valor_entregue = controle_diario[244][1]
        pedido_recebido = faturamento_juliana[109][37] + faturamento_juliana[109][40]
        tabela.append([data, regional, unidade, setor, valor_entregue, pedido_recebido])

        setor = 'Leitura'
        valor_entregue = controle_diario[260][1]
        pedido_recebido = faturamento_juliana[109][39]
        tabela.append([data, regional, unidade, setor, valor_entregue, pedido_recebido])


        ############################ BOM JESUS DA LAPA
        regional = 'Oeste'
        unidade = 'Bom Jesus da Lapa'
        setor = 'STC'
        valor_entregue = controle_diario[326][1]
        pedido_recebido = faturamento_juliana[109][43]
        tabela.append([data, regional, unidade, setor, valor_entregue, pedido_recebido])

        setor = 'CCM'
        valor_entregue = controle_diario[318][1]
        pedido_recebido = faturamento_juliana[109][42] + faturamento_juliana[109][45]
        tabela.append([data, regional, unidade, setor, valor_entregue, pedido_recebido])

        setor = 'Leitura'
        valor_entregue = controle_diario[334][1]
        pedido_recebido = faturamento_juliana[109][44]
        tabela.append([data, regional, unidade, setor, valor_entregue, pedido_recebido])


        ############################ GUANAMBI
        regional = 'Oeste'
        unidade = 'Guanambi'
        setor = 'STC'
        valor_entregue = controle_diario[367][1]
        pedido_recebido = faturamento_juliana[109][13]
        tabela.append([data, regional, unidade, setor, valor_entregue, pedido_recebido])

        setor = 'CCM'
        valor_entregue = controle_diario[350][1]
        pedido_recebido = faturamento_juliana[109][12]
        tabela.append([data, regional, unidade, setor, valor_entregue, pedido_recebido])

        setor = 'Manutenção'
        valor_entregue = controle_diario[358][1]
        pedido_recebido = faturamento_juliana[109][15]
        tabela.append([data, regional, unidade, setor, valor_entregue, pedido_recebido])

        setor = 'Leitura'
        valor_entregue = controle_diario[375][1]
        pedido_recebido = faturamento_juliana[109][14]
        tabela.append([data, regional, unidade, setor, valor_entregue, pedido_recebido])


        ############################ IBOTIRAMA
        regional = 'Oeste'
        unidade = 'Ibotirama'
        setor = 'STC'
        valor_entregue = controle_diario[284][1]
        pedido_recebido = faturamento_juliana[109][48]
        tabela.append([data, regional, unidade, setor, valor_entregue, pedido_recebido])

        setor = 'CCM'
        valor_entregue = controle_diario[276][1]
        pedido_recebido = faturamento_juliana[109][47] + faturamento_juliana[109][50]
        tabela.append([data, regional, unidade, setor, valor_entregue, pedido_recebido])

        setor = 'Leitura'
        valor_entregue = controle_diario[292][1]
        pedido_recebido = faturamento_juliana[109][49]
        tabela.append([data, regional, unidade, setor, valor_entregue, pedido_recebido])


        write_sheets('1ySrGYSlqy9IGxlwBa8_a_T_DGPCIQqDDL8ysgC7Ih6Y', 'Página1!A' + str(ultima_linha), tabela)

if __name__ == '__main__':
    atualiza_faturamento()