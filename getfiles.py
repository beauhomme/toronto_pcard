import wget
import zipfile

def getfile(url):
    print('downloading.....')
    filename = url.split('/')[-1]
    wget.download(url)
    print('\nDownload Complete!!!')

def unzipfiles(filename):
    try:
        with zipfile.ZipFile(filename) as file: # opening the zip file using 'zipfile.ZipFile' class
            print("Ok")
            with zipfile.ZipFile(filename, 'r') as file:
                # printing all the contents of the zipfile to a fle
                files = file.namelist()
                with open(r"./extractedfilelist.txt", "w") as f:
                    f.write("\n".join(files))
                    f.close()
     
            # extracting all the files
                print('Extracting all the files now...')
                file.extractall(path='./infiles')
        print('Done!')
    except zipfile.BadZipFile: # if the zip file has any errors then it prints the error message which you wrote under the 'except' block
        print('Error: Zip file is corrupted')
    




url = 'https://ckan0.cf.opendata.inter.prod-toronto.ca/download_resource/6b394ed0-1de6-403b-a907-a3608509e300'
file = url.split('/')[-1]
path = "C:\\Users\\test\\pyfortress\\pcard\\"
filename = path+file 
 
# getfile(url)
#nzipfiles(filename)
# print(filename)