#"http://enterobase.warwick.ac.uk/api/v2.0/login"
import urllib.request 
import base64
import json

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


    def __iter__ (self):
        return self
    '''  
    def __next__ (self):
        self.
    '''
    def _compose_auth_header ( self, api_key):
        '''
        Description:
            Function used for enterobase REST_API to build the header request
            with the API_KEY of the user
        '''
        base64string = base64.encodebytes(('%s:%s' % (api_key,'')).encode()).decode().replace('\n', '')

        return  ["Authorization", "Basic %s" % base64string]

    def get_locus_in_schema (self):
        '''
        Description:
            Function used for getting the location of the locis for the schema in enterobase 
            Return a list with the http address foe each loci in the schema
        '''
        not_completed = True
        locus_addr = []
        address =  '%s%s/%s/loci?limit=%d&scheme=%s' %(self.api_url ,self.database, self.schema, 5000, self.schema)
        while not_completed :
            request = urllib.request.Request(address)
            request.add_header(self.auth_header[0] , self.auth_header[1])
            
            # "http://enterobase.warwick.ac.uk/api/v2.0/ecoli/wgMLST/loci?scheme=wgMLST&limit=50&offset=50"
            response = urllib.request.urlopen(request)

            data = json.load(response)
            
            import pdb; pdb.set_trace()
            for loci_addr in ['loci']['download_alleles_link']:
                locus_addr.append(loci_addr)
            
            if not 'next' in data['links']:
                not_completed = False
            
        '''
        "links": {
            "paging": {
              "next": "http://enterobase.warwick.ac.uk/api/v2.0/ecoli/wgMLST/loci?scheme=wgMLST&limit=50&offset=50"
            },
            "records": 50,
            "total____records": 25002
            
            "loci": [
            {
              "database": "ESCwgMLST",
              "download_alleles_link": "http://enterobase.warwick.ac.uk/schemes/Escherichia.wgMLST/b3356.fasta.gz",
              "locus": "b3356",
              "locus_barcode": "ESW_AA0001AA_LO",
              "scheme": "wgMLSTv1"
            }
        '''
        return locus_addr
        
