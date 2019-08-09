import atexit


def start_mysql_instance():

    atexit.register(stop_mysql_instance)

def reset_mysql_instance():

    pass

def stop_mysql_instance():

    print('stopping mysql service...')
    pass
    print('done.')


