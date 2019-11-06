import requests
import datetime
import hmac
import hashlib
import base64
import argparse
import ADLSconfig as config

class ADLS_connection:
    def __init__(self, file_path, storage_account_name, storage_account_key, api_version='2018-11-09'):
        self.file_path = file_path
        self.storage_account_name = storage_account_name
        self.storage_account_key = storage_account_key
        self.api_version = api_version

    def getSAS(self, request_verb, request_params, request_time, content_length):
        string_params = {
            'verb': request_verb,
            'Content-Encoding': '',
            'Content-Language': '',
            'Content-Length': str(content_length) if content_length>0 else '',
            'Content-MD5': '',
            'Content-Type': '',
            'Date': '',
            'If-Modified-Since': '',
            'If-Match': '',
            'If-None-Match': '',
            'If-Unmodified-Since': '',
            'Range': '',
            'CanonicalizedHeaders': 'x-ms-date:' + request_time + '\nx-ms-version:' + self.api_version + '\n',
            'CanonicalizedResource': '/' + self.storage_account_name + self.file_path +  ''.join(f'\n%s:%s'%(k, request_params[k]) for k in request_params)
        }
        string_to_sign = (string_params['verb'] + '\n' 
                  + string_params['Content-Encoding'] + '\n'
                  + string_params['Content-Language'] + '\n'
                  + string_params['Content-Length'] + '\n'
                  + string_params['Content-MD5'] + '\n' 
                  + string_params['Content-Type'] + '\n' 
                  + string_params['Date'] + '\n' 
                  + string_params['If-Modified-Since'] + '\n'
                  + string_params['If-Match'] + '\n'
                  + string_params['If-None-Match'] + '\n'
                  + string_params['If-Unmodified-Since'] + '\n'
                  + string_params['Range'] + '\n'
                  + string_params['CanonicalizedHeaders']
                  + string_params['CanonicalizedResource'])

        signed_string = base64.b64encode(hmac.new(base64.b64decode(self.storage_account_key), msg=string_to_sign.encode('utf-8'), digestmod=hashlib.sha256).digest()).decode()
        return signed_string

    def send_request(self, request_verb, request_params={}, content=''):
        request_time = datetime.datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')
        signed_string = self.getSAS(request_verb, request_params, request_time, len(content))
        headers = {
            'x-ms-date' : request_time,
            'x-ms-version' : self.api_version,
            'Authorization' : ('SharedKey ' + self.storage_account_name + ':' + signed_string)
        }

        url = ('https://' + self.storage_account_name + '.dfs.core.windows.net'+self.file_path + ('?' if request_params else  '') +'&'.join(f'%s=%s'%(k, request_params[k]) for k in request_params))
        print(f'making a %s request to %s'%(request_verb, url))
        if(request_verb == 'PUT'):
            response = requests.put(url, headers = headers, data=content)
        elif(request_verb == 'PATCH'):
            response = requests.patch(url, headers = headers, data=content)
        elif(request_verb == 'GET'):
            response = requests.get(url, headers = headers, data=content)
        elif(request_verb == 'HEAD'):
            response = requests.head(url, headers = headers, data=content)
        return {'code': response.status_code, 'body': response.json() if response.content.decode() != '' else {}}