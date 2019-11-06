import threading
import ADLSconfig as config
from ADLSconnection import ADLS_connection

def ADLS_file_upload(content_file, file_path, storage_account_name, storage_account_key, batch_size=1E6, api_version='2018-11-09'):
    connection = ADLS_connection(file_path, storage_account_name, storage_account_key, api_version)
    # check file
    status = connection.send_request('HEAD')['code']
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
    status =connection.send_request('PATCH', request_params={'action': 'flush', 'position': str(position)})['code']
    if status == 200:
        print('uploaded successfully')
    else:
        print('failed with status ', status)

def ADLS_file_list(filesystem, directory, storage_account_name, storage_account_key, batch_size=1E6, api_version='2018-11-09'):
    connection = ADLS_connection(filesystem, storage_account_name, storage_account_key, api_version)
    # check file
    body = connection.send_request('GET', request_params={'directory':directory, 'recursive': 'false', 'resource': 'filesystem'})['body']
    
    files = []
    if 'paths' not in body:
        print(body)
    else:
        for f in body['paths']:
            if 'isDirectory' in f and f['isDirectory'] == 'true':
                files.append(f['name']+'/')
            else:
                files.append(f['name'])
    return files