import os
import threading
import math
import ADLSconfig as config
from ADLSconnection import ADLS_connection

def ADLS_file_upload(content_file, file_path, storage_account_name, storage_account_key, replace_existing=False, upload_streams=config.upload_streams, api_version='2018-11-09'):
    connection = ADLS_connection(file_path, storage_account_name, storage_account_key, api_version)

    # check file
    if not replace_existing:
        status = connection.send_request('HEAD')['code']
        if status == 200:
            print('file exists')
            return 'exists'

    # create file
    response = connection.send_request('PUT', request_params={'resource': 'file'})
    code, body = [response[k] for k in ['code', 'body']]
    if code != 201:
        print('failed to create file with status ', code)
        print(body)
        return 'failed'

    # append file
    position = 0
    with open(content_file, mode='r') as f:
        threads = list()
        batch_size = math.ceil(os.path.getsize(content_file)/upload_streams)
        for batch in iter(lambda: f.read(int(batch_size)), ""):
            x = threading.Thread(target=connection.send_request, args=('PATCH', ), kwargs={'request_params':{'action': 'append', 'position': str(position)}, 'content':batch})
            threads.append(x)
            x.start()
            position += len(batch)
        for index, thread in enumerate(threads):
            thread.join()
            print("thread %02d/%02d done"%(index+1, len(threads)))

    # flush file
    response = connection.send_request('PATCH', request_params={'action': 'flush', 'position': str(position)})
    code, body = [response[k] for k in ['code', 'body']]
    if code == 200:
        print('uploaded successfully')
        return 'successful'
    else:
        print('failed to flush file with status ', code)
        print(body)
        return 'failed'

def ADLS_file_list(filesystem, directory, storage_account_name, storage_account_key, batch_size=1E6, api_version='2018-11-09'):
    connection = ADLS_connection(filesystem, storage_account_name, storage_account_key, api_version)
    # list files
    response = connection.send_request('GET', request_params={'directory':directory, 'recursive': 'false', 'resource': 'filesystem'})
    code, body = [response[k] for k in ['code', 'body']]
    
    if code != 200:
        print('error: request returned ', code)
        print(body)
        return None

    if 'paths' not in body:
        print('error: response body is does not have "path" attribute')
        print(body)
        return None
        
    files = []
    for f in body['paths']:
        if 'isDirectory' in f and f['isDirectory'] == 'true':
            files.append(f['name']+'/')
        else:
            files.append(f['name'])
    return files