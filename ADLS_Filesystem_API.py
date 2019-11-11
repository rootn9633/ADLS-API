import ADLSconfig as config
from ADLSconnection import ADLS_connection

def ADLS_filesystem_list(storage_account_name, storage_account_key, api_version='2018-11-09'):
    connection = ADLS_connection('/', storage_account_name, storage_account_key, api_version)
    # get directory list
    response = connection.send_request('GET', request_params={'resource': 'account'})
    code, body = [response[k] for k in ['code', 'body']]

    if code != 200:
        print('error: request returned ', code)
        print(body)
        return None

    if 'filesystems' not in body:
        print('error: response body is does not have "filesystems" attribute')
        print(body)
        return None
        
    dirs = []
    filesystem = body['filesystems']
    for dir in filesystem:
        dirs.append(dir['name'] + '/')
    return dirs

if __name__ == "__main__":
    filename = ''
    print(ADLS_filesystem_list(config.storage_account_name, config.storage_account_key))