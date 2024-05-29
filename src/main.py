def menu() -> None:

    while True:
        print_menu()

        opcao = input("Escolha uma opção: ")

        match opcao:
            case 1:
                print("Opção 1")
            case 2:
                print("Opção 2")


def print_menu() -> None:
    print(
        "1. Ler Csv \n " 
        + "2. Gravar arquivo no banco de dados \n " 
        + "3.Sair")


if __name__ == "__main__":
    menu()
