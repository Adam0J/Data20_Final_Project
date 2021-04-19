import configparser

_config = configparser.ConfigParser()
_config.read('Data20_Final_Project/config.ini')


def server():
    return _config['USERINFO']['server']


def database():
    return _config['USERINFO']['database']


def driver():
    return _config['USERINFO']['driver']

