command_list = ['help', 'exit', 'createtables', 'droptables', 'listp', 'insertdata', 'doall', 'pub']


def dk_help():
    print("Возможные команды: ")
    for command in command_list:
        print(f'[{command}]', end=' ')
    print()


if __name__ == '__main__':
    dk_help()
