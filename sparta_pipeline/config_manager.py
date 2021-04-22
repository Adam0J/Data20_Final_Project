import configparser

_config = configparser.ConfigParser()
_config.read('../config.ini')


def server():
    return _config['USERINFO']['server']


def database():
    return _config['USERINFO']['database']


def driver():
    return _config['USERINFO']['driver']


def log():
    return _config['DEFAULT']['logging_level']