import ADLSconfig as config
from ADLSconnection import ADLS_connection

def ADLS_filesystem_list(storage_account_name, storage_account_key, api_version='2018-11-09'):
    connection = ADLS_connection('/', storage_account_name, storage_account_key, api_version)
    # get directory list
    body = connection.send_request('GET', request_params={'resource': 'account'})['body']
    dirs = []
    if 'filesystems' not in body:
        print(body)
    else:
        filesystem = body['filesystems']
        for dir in filesystem:
            dirs.append(dir['name'] + '/')
    return dirs

if __name__ == "__main__":
    filename = ''
    print(ADLS_filesystem_list(config.storage_account_name, config.storage_account_key))