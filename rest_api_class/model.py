import urllib.request ,  urllib.error
import base64
import json
import os
import io 
import gzip

# "http://enterobase.warwick.ac.uk/api/v2.0/ecoli/wgMLST/loci?scheme=wgMLST&limit=50&offset=50"

# "http://enterobase.warwick.ac.uk/schemes/Escherichia.wgMLST/b3356.fasta.gz"
class EnterobaseApi :
    def __init__ (self, api_url, api_key, database, schema):
        with open (api_key ,'r') as key_file :
            self.api_token = key_file.read()
        self.auth_header = self._compose_auth_header (self.api_token)
        self.database = database
        self.schema = schema
        self.api_url = api_url


    def _compose_auth_header ( self, api_key):
        '''
        Description:
            Function used for enterobase REST_API to build the header request
            with the API_KEY of the user
        '''
        base64string = base64.encodebytes(('%s:%s' % (api_key,'')).encode()).decode().replace('\n', '')

        return  ["Authorization", "Basic %s" % base64string]

    def _get_number_of_records_to_fetch (self, data) :
        
        return int(data['links']["total____records"])
    
    def get_locus_in_schema (self):
        '''
        Description:
            Function used for getting the location of the loci for the 
            schema in enterobase 
        Variables:
            address     # contain the http request
            data        # contain the response information in json format
            ofsset      # offset value in the iteration
            remaining_records # has the number of records left to download
            response    # response of the request
        Return:
            Return a dictionnary with loci name as key and the http address 
            for each loci in the schema in the value. or an exception if 
            an error occurrs when connecting
        '''
        more_to_fetch = True
        locus_addr = {}
        offset = 0
        limit = 5000
        try: 
            while more_to_fetch :
                address =  '%s%s/%s/loci?scheme=%s&limit=%d&offset=%d' %(self.api_url ,self.database, self.schema,  self.schema, limit ,offset )
                request = urllib.request.Request(address)
                request.add_header(self.auth_header[0] , self.auth_header[1])
                
                # "http://enterobase.warwick.ac.uk/api/v2.0/ecoli/wgMLST/loci?scheme=wgMLST&limit=50&offset=50"
                response = urllib.request.urlopen(request)
                data = json.load(response)
                remaining_records = self._get_number_of_records_to_fetch (data)
                
                for loci_addr in data['loci'] :
                    locus_addr[loci_addr['locus']] = loci_addr['download_alleles_link']
                offset += limit
                if total_records < limit :
                    more_to_fetch = False
                print ('Fetched ' , str(len(locus_addr)) ,' locus address. Remaining to download ', str(remaining_records) , ' records')
        
        except urllib.error.URLError as e:
            raise e
        return locus_addr
        

    def download_fasta_locus (self, download_address, out_dir, loci_name):
        '''
        Description:
            Function used for download the fasta loci. 
            If file is compress it is decompressed in memory and then
            saved in fasta extension
        Input:
            download_address    # address to download the file
            out_dir     # ouput directory to save the file
            loci_name   # used to name the file
            response    # response of the request
        Variables:
            in_         # contain the compress file downloaded
            data        # contain the loci in fasta format
        Return:
            Return True if everthing is ok, or an exception if 
            an error occurrs when connecting
        '''
        request = urllib.request.Request(download_address)
        request.add_header(self.auth_header[0] , self.auth_header[1])
        try:
            response = urllib.request.urlopen(request)
        
        except urllib.error.URLError as e: 
            raise e
        
        file_name = os.path.join(out_dir, loci_name + '.fasta')

        try:
            if download_address.endswith('.gz') :
                in_ = io.BytesIO()
                in_.write(response.read())
                in_.seek(0)
                with gzip.GzipFile(fileobj=in_, mode='rb') as fo:
                    gunzipped_bytes_obj = fo.read()
            else :
                data = response.read()
            with open (file_name, 'w') as fh:
                fh.write(gunzipped_bytes_obj.decode())
        except urllib.error.URLError as e: 
            raise e
            
        return True
