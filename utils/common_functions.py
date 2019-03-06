import logging

import logging
import os
from datetime import datetime
#from logging.handlers import RotatingFileHandler

def create_directory (folder_name):
    '''
    Description:
        The function will check if the log folder already exists if not
        it will try to create
    Input:
        folder_name # 
    Return:
        True if log folder already created or if creation was node sucessfully
        raise an exception if folder cannot be created
    '''
    if not os.path.exists(folder_name):
        try:
            os.makedirs(folder_name)
        except OSError as e:
            raise e
    return True

def logging_errors(string_text, showing_traceback , print_on_screen ):
    '''
    Description:
        The function will log the error information to file.
    Input:
        logger # contains the logger object
        string_text # information text to include in the log

    Variables:
        subject # text to include in the subject email
    '''
    logger = logging.getLogger(__name__)
    logger.error('-----------------    ERROR   ------------------')
    logger.error(string_text )
    if showing_traceback :
        logger.error('Showing traceback: ',  exc_info=True)
    logger.error('-----------------    END ERROR   --------------')
    if print_on_screen :
        from datetime import datetime
        print('********* ERROR **********')
        print(string_text)
        print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        print('When processing run . Check log for detail information')
        print('******* END ERROR ********')
    return ''
    
def logging_warnings(string_text, print_on_screen ):
    '''
    Description:
        The function will log the error information to file.
        Optional can send an email to inform about the issue
    Input:
        logger # contains the logger object
        string_text # information text to include in the log
    '''
    logger = logging.getLogger(__name__)
    logger.warning('-----------------    WARNING   ------------------')
    logger.warning(string_text )
    logger.warning('-----------------    END WARNING   --------------')
    if print_on_screen :
        from datetime import datetime
        print('******* WARNING ********')
        print(string_text)
        print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        print('When processing run . Check log for detail information')
        print('**** END WARNING *******')
    return ''

def open_log(log_name, log_folder):
    log_name=os.path.join(log_folder, log_name)
    #def create_log ():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    #create the file handler
    handler = logging.handlers.RotatingFileHandler(log_name, maxBytes=200000, backupCount=5)
    handler.setLevel(logging.DEBUG)

    #create a Logging format
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    #add the handlers to the logger
    logger.addHandler(handler)

    return logger

