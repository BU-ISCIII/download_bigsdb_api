#!/usr/bin/env python3
import argparse
import sys
import os
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
import requests
from urllib.parse import urlparse
from urllib.request import urlopen, URLError

# -output_dir /srv/project_wgmlst/pasteur_schema schema -api_url pasteur_listeria -schema_name cgMLST1748 
# -out /srv/tmp/ schema -api_url enterobase -schema_name wgMLST -database ecoli -api_key  API_KEY_ENTEROBASE 

def open_log(log_name):
    working_dir = os.getcwd()
    log_name=os.path.join(working_dir, log_name)
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


def check_arg(args=None):
    text_description = str('This program will download the locus fasta files for a selected schema using the API REST request. So far only pubMLST, bigsdb and EnteroBase are supported.')
    
    parser = argparse.ArgumentParser(prog = 'get_files_from_rest_api.py', 
                                    formatter_class=argparse.RawDescriptionHelpFormatter,
                                    description = text_description)
    parser.add_argument('-v' ,'--version', action='version', version='%(prog)s 0.1.3')
    
    parser.add_argument('-out','-output_dir', help = 'Directory where the result files will be stored')
    subparser = parser.add_subparsers(help = 'interactive/schema are the 2 available options to download the locus', dest = 'chosen_method')
    
    interactive_parser = subparser.add_parser('interactive', help = 'interactive downloads the  schema for pubMLST and bigsdb ')
    interactive_parser.add_argument('-db','--db_url', choices = ['pubMLST',''] ,help = 'database url to download the locus files. "pubMLST" value can be used as nick name to connect to pubMLST database')
    
    schema_parser = subparser.add_parser('schema', help = 'Download the locus fasta files for a given schema')
    schema_parser.add_argument('--api_url', help = 'Nick name to connect to REST API : accepted values are : bigsdb , pasteur_listeria, pubMLST')
    schema_parser.add_argument('--schema_name', help = 'Name of the schema where the locus are defined')
    schema_parser.add_argument('--database', help = 'Database name reqired for enterobase', required= False)
    schema_parser.add_argument('--api_key', help = 'File name with the Token Key for enterobase', required= False)

    #parser.add_argument('-api_url', help = 'Nick name to connect to REST API : accepted values are : bigsdb , pasteur_listeria, pubMLST')
    #parser.add_argument('-schema_name', help = 'Name of the schema where the locus are defined')
    
    return parser.parse_args()

def enterobase_create_request(address):
    '''
    Description:
        Function used for enterobase REST_API to build the header request
        with the API_KEY of the user
        Return request
    Input:
        address    # string containg the address to connect to the server
    variables:
        api_token # contain the Token key of the user
        base64string    # Api Key in base64 format 
    Return:
        True /False
    '''
    with open ('API_KEY_ENTEROBASE' ,'r') as key_file :
        api_token = key_file.read()
    request = urllib.request.Request(request_str)
    #request = urllib2.Request(request_str)
    base64string = base64.encodebytes(('%s:%s' % (api_token,'')).encode()).decode().replace('\n', '')
    request.add_header("Authorization", "Basic %s" % base64string)
    return request



def get_locus_enterobase (api_url, database, schema_name):
    '''
    Description:
        Function will check if all projects given in the project list
        are defined on database
        Return True if all are in , False if not
    Input:
        project_list    #list of the project to check
    variables:
        logger # logging object to write in the log file
    Return:
        True /False
    '''
    #/loci?limit=5&offset=0&scheme=wgMLST
    address = SERVER_ADDRESS + '/api/v2.0/%s/schemes?scheme_name=%s&limit=%d&scheme=%s' %(DATABASE, scheme, 4000, scheme)
    response = urlopen(enterobase_create_request(address))
    data = json.load(response)
    import pdb; pdb.set_trace()
    return True


def get_locus_list( api_url, schema_name, logger):
    r = requests.get(api_url)
    logger.info('Connecting to %s to get the schemas' , api_url)
    if r.status_code != 200 :
        logger.error('Unable to connect to %s ', api_url)
        return False
    schema_db = r.json()
    schema_index = ''
    for index in range(len(schema_db['schemes'])):
        if schema_name == schema_db['schemes'][index]['description'] :
            schema_index = schema_db['schemes'][index]['scheme']
            break
    if schema_index == '' :
        logger.error('The given schema name %s, it is not included in the schema database', schema_name)
        return False
    r = requests.get(schema_index)
    if r.status_code != 200 :
        return False
    r_json = r.json()
    locus_list = []
    for loci in range(r_json['locus_count']) :
        locus_list.append(r_json['loci'][loci])
            
    logger.info('The locus list for the schema %s has been successfully fetched ', schema_name)
    return locus_list


def download_fasta_locus (locus_list, output_dir, logger):
    download_counter = 0
    for loci in locus_list :
        tmp_split = loci.split('/')
        loci_name = tmp_split[-1]
        r = requests.get(loci + '/alleles_fasta')
        if r.status_code != 200 :
            logger.error('Unable to download the fasta file  for allele %s ', loci_name)
            
        else :
            fasta_alleles = r.text
            fasta_file =  os.path.join(output_dir, str(loci_name + '.fasta'))
            with open (fasta_file , 'w') as fasta_fh :
                fasta_fh.write(fasta_alleles)
            download_counter += 1
    if download_counter == len(locus_list) :
        return True
    else :
        logger.info('All alleles have been successfully downloaded and saved on %s', output_dir)
        return False

api_url = {'bigsdb':'http://api.bigsdb.pasteur.fr' ,
           'pasteur_listeria': 'http://api.bigsdb.pasteur.fr/db/pubmlst_listeria_seqdef_public/schemes' ,
           'pubMLST_neisseria' : 'http://rest.pubmlst.org/db/pubmlst_neisseria_isolates/isolates',
           'pubMLST' : 'http://rest.pubmlst.org/',
           'enterobase': 'http://enterobase.warwick.ac.uk/api/v2.0/'}

def url_validation (url):
    result = urlparse(url)
    return result.scheme and result.netloc

def validate_db_conection (url) :
    try:
        urlopen(url)
        return True
    except URLError:
        return False

def print_menu (value_list, db_url) :
    invalid_selection = True
    while invalid_selection :
        os.system('clear')
        
        print ('You are connected to database : ', db_url)
        print ('\n')
        print(30 * '-', ' MENU ', 30 *'-','\n')
        for index, value in enumerate(value_list) :
            print(index, 2*'', value)
        print ('\n q  To Quit')
        choice_value = input(' Enter your selection  >>  ')
        if choice_value == 'q' or choice_value == 'Q' :
            invalid_selection = False
        else :
            try:
                value_integer = int(choice_value)
            except:
                continue
            if  0 <= value_integer <= len(value_list)-1 :
                invalid_selection = False
    return choice_value

def get_database_options (db_url, logger):
    r = requests.get(db_url)
    logger.info('Connecting to %s to get the options.' , db_url)
    if r.status_code != 200 :
        logger.error('Unable to connect to %s ', db_url)
        return False
    return r.json()

if __name__ == '__main__' :

    if len (sys.argv) == 1 :
        print('Usage: get_files_from_rest_api.py [OPTION] ')
        print('Try  get_files_from_rest_api.py --help for more information.')
        exit(0)
    arguments = check_arg(sys.argv[1:])
    start_time = datetime.now()
    # open log file
    logger = open_log ('rest_api.log')
    
    try:
        os.makedirs(arguments.out)
    except:
        print('Unable to create the directory to download the files\n')
        exit (0)
    
    if arguments.chosen_method =='interactive' :
        if arguments.db_url in api_url :
            db_url = api_url[arguments.db_url ]
        else :
            valid_url = url_validation (arguments.db_url)
            if not valid_url :
               print ('Invalid url format')
               exit(0)
            else:
                db_url = arguments.db_url
        if not validate_db_conection(db_url) :
            print ('Unable to connect to database ', db_url)
            exit(0)
        # get the available databases 
        if not 'db' in db_url :
            selection = 'databases'
            
            db_output = get_database_options (db_url, logger)
            option_list = []
            for index in range( len( db_output)) :
                option_list.append(db_output[index]['description'])
            choice = print_menu(option_list, db_url)
            if choice == 'q' or choice == 'Q' :
                print ('Exiting the program. Returning to shell prompt')
                exit(0)
            else :
                db_selection = db_output[int(choice)][selection]
                option_list = []
                for index in range (len(db_selection)) :
                    option_list.append(db_selection[index]['description'])
                choice = print_menu(option_list, db_url)
                if choice == 'q' or choice == 'Q' :
                    print ('Exiting the program. Returning to shell prompt')
                    exit(0)
                else :
                    # get the schemes href
                    db_url = db_selection[index]['href']
                    db_output = get_database_options (db_url, logger)
                    option_list = []
                    if 'schemes' in db_output :
                        db_url = db_output['schemes']
                        db_output = get_database_options (db_url, logger)
                    #    for index in range(len(db_output)) :
                            
                    for index in range( len( db_output['schemes'])) :
                        option_list.append(db_output['schemes'][index]['description'])
                    choice = print_menu(option_list, db_url)
                    if choice == 'q' or choice == 'Q' :
                        print ('Exiting the program. Returning to shell prompt')
                        exit(0)
                    # get the allele list for the schema
                    db_url = db_output['schemes'][int(choice)]['scheme']
                    db_output = get_database_options (db_url, logger)
                    locus_list =[]
                    for index in range(len(db_output['loci'])):
                        locus_list.append(db_output['loci'][index])
                    
                    # get gasta files for each locus
                    fasta_locus = download_fasta_locus (locus_list, arguments.output_dir, logger)
                    if not fasta_locus :
                        logger.error('Locus list for the schema %s cannot be fetched ', arguments.schema_name)
                        print('Some of the alleles files cannot be downloaded. Check log file')
                    else:
                        print ('All alleles have been downloaded from the schema')

        print ('Exiting the interactive dialog\n Returning to shell prompt \n')  
    else:
        if arguments.api_url not in api_url :
            print ('The requested rest api it is not allowed \n')
            exit (0)
        else :
            rest_api_url = api_url [arguments.api_url]

        if api_url == 'enterobase':
            locus_list = get_locus_enterobase (rest_api_url, arguments.schema_name)
        else:
            locus_list = get_locus_list (rest_api_url, arguments.schema_name, logger)
        if not locus_list :
            logger.error('Locus list for the schema %s cannot be fetched ', arguments.schema_name)
            print ('Unable to get the locus list for the schema ' , arguments.schema_name )
            exit(0)
        fasta_locus = download_fasta_locus ( locus_list, arguments.output_dir, logger)
        if not fasta_locus :
            logger.error('Locus list for the schema %s cannot be fetched ', arguments.schema_name)
            print('Some of the alleles files cannot be downloaded. Check log file')
        else:
            print ('All alleles have been downloaded from the schema')
