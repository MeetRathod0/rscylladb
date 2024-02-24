import logging as lg
lg.getLogger("cassandra").setLevel('ERROR')

def create_logger(name:str):

    formatter = lg.Formatter(
        '%(asctime)s  %(levelname)s  %(message)s')
    console_handler = lg.StreamHandler()
    console_handler.setLevel(lg.DEBUG)
    console_handler.setFormatter(formatter)
    logger = lg.getLogger(name)
    logger.addHandler(console_handler)
    logger.setLevel(lg.DEBUG)
    logger.propagate = False
    return logger