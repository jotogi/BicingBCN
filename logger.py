import logging

'''
https://www.cienciadedatos.net/documentos/py05_logging_con_python.html

El sistema de registro está compuesto por cuatro tipos de objetos que interactúan.
Cada módulo o aplicación que desea registrar utiliza una instancia de Logger
para agregar información a los registros. Invocar al registrador crea un LogRecord,
que se utiliza para mantener la información en la memoria hasta que se procesa.
Un Logger puede tener varios objetos Handler configurados para recibir y procesar registros.
El Handler usa un Formatter para convertir los registros en mensajes de salida.
'''



# Definición del logger root
# -----------------------------------------------------------------------------
# logging.basicConfig(
#     format = '%(asctime)-5s %(name)-15s %(levelname)-8s %(message)s',
#     level  = logging.INFO,
#     filemode = "a"
#     )


def get_handler(loger_level = logging.DEBUG, filename_level = logging.DEBUG, LOGGER_FILENAME:str='')->logging.Logger:
    logger = logging.getLogger(__name__)
    logger.setLevel(loger_level)

    #Handlers
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(loger_level)
    stream_handler_format = logging.Formatter('%(asctime)-5s %(name)-15s %(levelname)-8s %(message)s')
    stream_handler.setFormatter(stream_handler_format)
    logger.addHandler(stream_handler)

    if LOGGER_FILENAME != '':
        file_handler = logging.FileHandler(filename=LOGGER_FILENAME)
        file_handler.setLevel(filename_level)
        file_handler_format = logging.Formatter('%(asctime)-5s %(name)-15s %(levelname)-8s %(message)s')
        file_handler.setFormatter(file_handler_format)
        logger.addHandler(file_handler)

    return logger


def main():
    logger = get_handler('missages.log')
    logger.debug('Log debug')
    logger.info('Log info')

if __name__ == '__main__':
    main()