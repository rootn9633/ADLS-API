import requests
import datetime
import hmac
import hashlib
import base64
import threading
import argparse
import ADLSconfig as config

parser = argparse.ArgumentParser(description='upload file to ADLS')
parser.add_argument('upload_file', type=str, help='file to be uploaded')
args = parser.parse_args()

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

        if(request_verb == 'PUT'):
            response = requests.put(url, headers = headers, data=content)
        elif(request_verb == 'PATCH'):
            response = requests.patch(url, headers = headers, data=content)
        elif(request_verb == 'GET'):
            response = requests.get(url, headers = headers, data=content)
        elif(request_verb == 'HEAD'):
            response = requests.head(url, headers = headers, data=content)
        return response.status_code

def ADLS_file_upload(content_file, file_path, storage_account_name, storage_account_key, batch_size=1E6, api_version='2018-11-09'):
    connection = ADLS_connection(file_path, storage_account_name, storage_account_key, api_version)
    # check file
    status = connection.send_request('HEAD')
    if status == 200:
        print('file exists')    
        return
    # create file
    connection.send_request('PUT', request_params={'resource': 'file'})
    # append file
    position = 0
    with open(content_file, mode='r') as f:
        # connection.send_request('PATCH', request_params={'action': 'append', 'position': str(position)}, content=batch)
        threads = list()
        for batch in iter(lambda: f.read(int(batch_size)), ""):
            x = threading.Thread(target=connection.send_request, args=('PATCH', ), kwargs={'request_params':{'action': 'append', 'position': str(position)}, 'content':batch})
            threads.append(x)
            x.start()
            position += len(batch)
        for index, thread in enumerate(threads):
            thread.join()
            print("thread %02d/%02d done"%(index+1, len(threads)))

    # flush file
    status =connection.send_request('PATCH', request_params={'action': 'flush', 'position': str(position)})
    if status == 200:
        print('uploaded successfully')
    else:
        print('failed with status ', status)

if __name__ == "__main__":
    filename = args.upload_file
    ADLS_file_upload(filename, config.ADLS_file_path+filename, config.storage_account_name, config.storage_account_key, batch_size=config.batch_size)