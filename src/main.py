import os
from pprint import pprint
from csv_functions import save_csv
from db_functions import *


def menu() -> None:
    while True:
        print_menu()

        opcao = input("Escolha uma opção: ")

        if opcao == '1':
            table_name = input("Que tabela deseja buscar? ")

            for row in read_all_from_table(table_name=table_name):
                pprint(row)

        elif opcao == '2':
            csv_file = input("Qual o caminho do CSV: ")

            if not check_csv_existence(csv_file):
                print("Erro: O arquivo CSV não existe.")
                continue

            table_name = input("Qual vai ser o nome da tabela? ")
            save_csv(csv_file=csv_file, table_name=table_name)

        elif opcao == '3':
            table_name = input("De qual tabela deseja ler os registros?")
            entity = input("De qual entidade deseja ler os registros? (Escreva em inglês): ")
            
            read_by_entity(table_name, entity)


        elif opcao == '4':
            print("Saindo.")
            break
        else:
            print("Opção inválida. Escolha novamente.")


def check_csv_existence(csv_file: str) -> bool:
    return os.path.isfile(csv_file)


def print_menu() -> None:
    print(
        "1. Ler todos os registros de uma tabela\n" + "2. Gravar arquivo no banco de dados\n" "3. Ler registro por entidade (Mundo, país, cidade, etc.):\n" + "4. Sair")


if __name__ == "__main__":
    menu()
