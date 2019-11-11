from ADLS_Filesystem_API import ADLS_filesystem_list
from ADLS_Path_API import ADLS_file_upload, ADLS_file_list
from ADLSconnection import ADLS_connection
import ADLSconfig as config
import argparse

def choose_filesystem():
    filesystems = ADLS_filesystem_list(config.storage_account_name, config.storage_account_key)
    if filesystems == None:
        print('error when trying to list filesystems')
        return None
    print('choose a filesystem:')
    while True:
        for idx,f in enumerate(filesystems):
            print(f'[%d] %s'%(idx+1, f))
        choice = int(input('> '))-1
        if choice >= 0 and choice < len(filesystems):
            return filesystems[choice]
        else:
            print('choice out of range')

def choose_file(filesystem, directory):
    filesystem = filesystem[:-1]
    files = ADLS_file_list(filesystem, directory, config.storage_account_name, config.storage_account_key)
    if files == None:
        print('error when trying to list files')
        return None
    directories = ['.', '..'] + [d.split(directory)[1] if len(directory) > 0 else d for d in files if d[-1] == '/']
    files = [d.split(directory)[1] if len(directory) > 0 else d for d in files if d[-1] != '/']
    print('choose a directory:')
    while True:
        for idx, d in enumerate(directories):
            print(f'[%d] %s'%(idx+1, d))
        for f in files:
            print(f'    %s'%f)
        choice = int(input('> '))-1
        if choice >= 0 and choice < len(directories)+2:
            return directories[choice]
        else:
            print('choice out of range')

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='upload file to ADLS')
    parser.add_argument('upload_file', type=str, help='file to be uploaded')
    args = parser.parse_args()
    filename = args.upload_file
    current_path = []
    while True:
        if len(current_path) == 0:
            filesystem = choose_filesystem()

            if filesystem == None:
                break

            current_path.append('/'+filesystem)
        else:
            directory = choose_file(current_path[0], ''.join(current_path[1:]))

            if directory == None:
                break

            if directory == '.':
                result = ADLS_file_upload(filename, ''.join(current_path)+filename, config.storage_account_name, config.storage_account_key)
                if result == 'exists':
                    choice = input('do you want to override the existing file? (y/n) ')
                    if choice == 'y':
                        result = ADLS_file_upload(filename, ''.join(current_path)+filename, config.storage_account_name, config.storage_account_key, replace_existing=True)
                        exit()
                else:
                    exit()
            elif directory == '..':
                del current_path[-1]
            else:
                current_path.append(directory)