import os
import sys
import hashlib
import time
import boto3
import pickle
import argparse

###############
###############
glacierInfo = {
	'vaultName': 'MediaBackup',
	'accountId': '363431390362'
}

config = {
	'root': 'Digital Photos',
	'manifestFile': 'manifest.p',
	'blockSize': 128*1024*1024
}

###############
# Manifest
###############
class Manifest:
    def __init__(self, manifestFile, rootDir, verbosity):
        self.manifest = {}
        self.numFiles = 0
        self.manifestFile = manifestFile
        self.rootDir = rootDir
        self.verbosity = verbosity if verbosity else 0

    def setPath(self, rootDir):
        self.rootDir = rootDir

    def setVerbosity(self, verboseLevel):
        self.verbosity = verboseLevel

    def write(self):
        print('Writing ' + manifestFile)
        pickle.dump(self.manifest, open(manifestFile, 'wb'))

    def load(self):
        self.manifest = pickle.load(open(manifestFile, 'rb'))
        print('Number of files: ' + str(len(self.manifest)))

    def sha256(self, filepath):
        sha256 = hashlib.sha256()
        with open(filepath, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b''):
                sha256.update(chunk)
        return sha256.hexdigest()

    def log(self, msg):
        if self.verbosity == 1:
            print('Manifest: ' + msg)

    def createManifest(self):
        self.log('Parsing ' + self.rootDir + ' into ' + self.manifestFile)
        #start = time.time()
        for dirpath, dirnames, filenames in os.walk(self.rootDir):
            for file in filenames:
                self.numFiles = self.numFiles + 1
                fullPath = os.path.join(dirpath, file).replace('\\', '/')
                self.log(fullPath)
                sha256Hash = self.sha256(fullPath)
                data = { 
                    'sha256': sha256Hash
                }
                self.manifest[fullPath] = data
        #end = time.time()
        #print(self.manifest)
        #print(numFiles)
        #timeInMs = (end - start) * 1000
        #msPerFile = timeInMs / timeInMs
        #print('Time: ' + format(timeInMs, '.2f') + ' ms')
        #print('Ave per file: ' + format(msPerFile, '.2f') + 'ms')

    def md5(self, filepath):
        hash_md5 = hashlib.md5()
        with open(filepath, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b''):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()    

    def tests3(self):
        s3 = boto3.resource('s3')
        for bucket in s3.buckets.all():
            print(bucket.name)  
        
    def backupToGlacier(self):
        print('backing up')
        client = boto3.client('glacier')
        desc = client.describe_vault(vaultName = glacierInfo['vaultName'])
        for filePath in anifest:
            print('Sending ' + filePath)
            with open(filePath, 'rb') as f:
                content = f.read()
                response = client.upload_archive(vaultName = 'MediaBackup', body = content)
                print('Response: ')
                print('  HTTPStatusCode: ' + str(response['ResponseMetadata']['HTTPStatusCode']))
                print('  Response ID: ' + response['archiveId'])
                print('  Location: ' + response['location'])
                print('  Checksum: ' + response['checksum'])
                print('  sha256:   ' + anifest[filePath]['sha256'])
                print('')
                self.manifest[filePath]['response'] = response

def getVaultInformation():
	print('Getting vault information')
	client = boto3.client('glacier')
	info = client.describe_vault(vaultName = glacierInfo['vaultName'])
	print('Vault information:')
	print('Name      : ' + info['VaultName'])
	print('ARN       : ' + info['VaultARN'])
	print('Created   : ' + info['CreationDate'])
	print('# Archives: ' + str(info['NumberOfArchives']))
	print('Size (b)  : ' + str(info['SizeInBytes']))


def retrieveArchive(filepath, entry):
	archiveid = entry['response']['archiveId']
	print(filepath + ': ')
	print('  Archive ID: ' + archiveid)
	client = boto3.client('glacier')
	response = client.initiate_job(
            vaultName = glacierInfo['vaultName'], 
            jobParameters = {
                    'Type': 'archive-retrieval',
                    'ArchiveId': archiveid,
                    'Tier': 'Expedited',
                    'Description': 'Get archive'
            }
	)

	print("Waiting for job")
	jobId = response['jobId']
	glacier = boto3.resource('glacier')
	job = glacier.Job(glacierInfo['accountId'], glacierInfo['vaultName'], response['jobId'] )
	done = False
	while not done:
            time.sleep(1)
            job.reload()
            print("Checking...")
            done = job.completed
	print("done")
	outputData = job.get_output()
	output = outputData['body']
	with open('output.jpg', 'wb') as fd:
            writeDone = False
            while not writeDone:
                block = output.read(config['blockSize'])
                print("Writing block: " + str(len(block)))
                if len(block) < config['blockSize']:
                        writeDone = True
                fd.write(block)

###############
# main
###############
parser = argparse.ArgumentParser(description='Glacier managment')
parser.add_argument('-c', '--create', action='store_true', help='Create manifest')
parser.add_argument('-u', '--upload', action='store_true', help='Upload files')
parser.add_argument('-i', '--info', action='store_true', help='Get vault information')
parser.add_argument('-r', '--retrieve', help='Retrieve file')
parser.add_argument('-d', '--dump', action='store_true', help='Dump manifest')
parser.add_argument('-p', '--path', help='Path to process')
parser.add_argument('-m', '--manifest', help='Manifest file')
parser.add_argument('-v', '--verbose', type=int, help='Verbose')
args = parser.parse_args()
print(args)

if args.manifest :
    if args.dump :
        manifest = Manifest(args.manifest)
        if args.verbose: 
            manifest.setVerbosity(1)
        manifest.dumpManifest()
        os._exit(0)
    elif args.create :
        verbosity = args.verbose if args.verbose else 0
        if args.path :
            manifest = Manifest(args.manifest,args.path, verbosity)
            if args.verbose: 
                manifest.setVerbosity(1)
            if os.path.isfile(args.manifest):
                print('Deleting existing file')
                os.remove(args.manifest)
            manifest.createManifest()
        else :
            print('path not specified')
            args.print_help()
    elif args.upload :
        if(not os.path.isfile(config['manifestFile'])):
            createManifest(config['root'])

        manifest = pickle.load(open(config['manifestFile'], 'rb'))
        updatedManifest = backupToGlacier(manifest)
        print('Writing updated manifest.p')
        pickle.dump(updatedManifest, open(config['manifestFile'], 'wb'))
    else:
        print('Bad options')
        parser.print_help()
else:
    if args.retrieve :
        print('Retrieving ' + args.retrieve)
        if(not os.path.isfile(config['manifestFile'])):
                print('No manifest.  Aborting.')
                os._exit(1)
        manifest = pickle.load(open(config['manifestFile'], 'rb'))
        if(manifest[args.retrieve]): 
                retrieveArchive(args.retrieve, manifest[args.retrieve])

    elif args.info :
        getVaultInformation()


