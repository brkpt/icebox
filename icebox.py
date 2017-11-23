import os
import sys
import hashlib
import time
import boto3
import pickle
import argparse

###############
glacierInfo = {
	'vaultName': 'MediaBackup',
	'accountId': '363431390362'
}

config = {
	'root': 'data/Bees',
	'manifestFile': 'manifest.p',
	'blockSize': 128*1024*1024
}

###############

def md5(filepath):
    hash_md5 = hashlib.md5()
    with open(filepath, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b''):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()    

def sha256(filepath):
	sha256 = hashlib.sha256()
	with open(filepath, 'rb') as f:
		for chunk in iter(lambda: f.read(4096), b''):
			sha256.update(chunk)
	return sha256.hexdigest()

def createManifest(rootdir):
	print('Generating manifest from: ' + rootdir)
	manifest = {}
	numFiles = 0
	#start = time.time()
	for dirpath, dirnames, filenames in os.walk(rootdir):
		for file in filenames:
			numFiles = numFiles + 1
			fullPath = os.path.join(dirpath, file).replace('\\', '/')
			sha256Hash = sha256(fullPath)
			data = { 
				'sha256': sha256Hash
			}
			manifest[fullPath] = data
	#end = time.time()
	#print(manifest)
	#print(numFiles)
	#timeInMs = (end - start) * 1000
	#msPerFile = timeInMs / timeInMs
	#print('Time: ' + format(timeInMs, '.2f') + ' ms')
	#print('Ave per file: ' + format(msPerFile, '.2f') + 'ms')
	print('Writing ' + config['manifestFile'])
	pickle.dump(manifest, open(config['manifestFile'], 'wb'))
    
def tests3():
    s3 = boto3.resource('s3')
    for bucket in s3.buckets.all():
        print(bucket.name)  
        
def backupToGlacier(manifest):
	print('backing up')
	client = boto3.client('glacier')
	desc = client.describe_vault(vaultName = glacierInfo['vaultName'])
	for filePath in manifest:
		print('Sending ' + filePath)
		with open(filePath, 'rb') as f:
			content = f.read()
			response = client.upload_archive(vaultName = 'MediaBackup', body = content)
			print('Response: ')
			print('  HTTPStatusCode: ' + str(response['ResponseMetadata']['HTTPStatusCode']))
			print('  Response ID: ' + response['archiveId'])
			print('  Location: ' + response['location'])
			print('  Checksum: ' + response['checksum'])
			print('  sha256:   ' + manifest[filePath]['sha256'])
			print('')
			manifest[filePath]['response'] = response
	return manifest

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

def dumpManifest():
	manifest = pickle.load(open(config['manifestFile'], 'rb'))
	for filepath in manifest:
		print(filepath + ':')
		print('  Archive ID: ' + manifest[filepath]['response']['archiveId'])
		print('  Location  : ' + manifest[filepath]['response']['location'])
		print('')

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

parser = argparse.ArgumentParser(description='Glacier managment')
parser.add_argument('-c', '--create', action='store_true', help='Create manifest')
parser.add_argument('-u', '--upload', action='store_true', help='Upload files')
parser.add_argument('-i', '--info', action='store_true', help='Get vault information')
parser.add_argument('-r', '--retrieve', help='Retrieve file')
parser.add_argument('-d', '--dump', action='store_true', help='Dump manifest')
args = parser.parse_args()

print(args)
if(args.retrieve):
	print('Retrieving ' + args.retrieve)
	if(not os.path.isfile(config['manifestFile'])):
		print('No manifest.  Aborting.')
		os._exit(1)
	manifest = pickle.load(open(config['manifestFile'], 'rb'))
	if(manifest[args.retrieve]): 
		retrieveArchive(args.retrieve, manifest[args.retrieve])

if(args.create): 
	if(os.path.isfile(config['manifestFile'])):
		print('Deleting existing file')
		os.remove(config['manifestFile'])

	createManifest(config['root'])

if(args.dump):
	dumpManifest()
	os._exit(0)

if(args.info):
	getVaultInformation()

if(args.upload):
	if(not os.path.isfile(config['manifestFile'])):
		createManifest(config['root'])

	manifest = pickle.load(open(config['manifestFile'], 'rb'))
	updatedManifest = backupToGlacier(manifest)
	print('Writing updated manifest.p')
	pickle.dump(updatedManifest, open(config['manifestFile'], 'wb'))



