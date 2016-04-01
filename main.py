
import json
import click
import os
import shutil
import requests
import zipfile
import io
import getpass
import sys
import re
import codecs


###
#
#	Global variables.
#
###

PATH_CONFIG_SETTINGS_FETCH = 'configs/fetch_settings.json'
PATH_CONFIG_SETTINGS_ES = 'configs/es_settings.json'
PATH_DATA_FOLDERS = 'data/'


###
#
#	CLI command functions.
#
###

@click.group(
	help='This is a command line interface for fetching and loading data from Riksdagens Öppna API into an Elasticsearch instance.'
)
def cli():
	pass

@click.command(help='Check that the ES instance is up and running.')
def check_connection():
	
	# Get configuration.
	es_settings = get_es_config()

	# Check authentication and prompt for credentials if needed.
	user, pw = get_credentials(es_settings['shield_authentication'])
	
	# Make sure ES is up and running.
	try:
		response = es_get_query(es_settings['host_url'] + ':' + es_settings['host_port'], None, (user, pw))
		print('Elasticsearch is up and running!')
		print(json.dumps(response.json(), indent=4))
		print('\n')
	except requests.exceptions.RequestException as e:
		print(e)
		print('Failed to establish connection.\n')
		sys.exit(1)

@click.command(help='Get information from the index.')
def get_index_info():

	# Get configuration.
	es_settings = get_es_config()

	# Check authentication and prompt for credentials if needed.
	user, pw = get_credentials(es_settings['shield_authentication'])

	print('ES info.')

	# Get document type count
	response = es_get_query(
		es_settings['host_url'] + ':' + es_settings['host_port'] + '/' + es_settings['index_name'] + "/_search",
		data=json.dumps({
			'aggregations': {
				"count_by_type": {
					"terms": {
						"field": "_type"
					}
				}
			},
			'size': 0
		}),
		credentials=(user, pw)
	)
	type_info = json.loads(response.content.decode('UTF-8'))

	for bucket in type_info['aggregations']['count_by_type']['buckets']:
		print('Type: ' + bucket['key'] + '\t' + 'Count: ' + str(bucket['doc_count']))
	
	# Get index storage size
	response = es_get_query(es_settings['host_url'] + ':' + es_settings['host_port'] + '/' + es_settings['index_name'] + "/_stats/store", data=None, credentials=(user, pw))
	index_info = json.loads(response.content.decode('UTF-8'))

	print('Index storage size: ' + '{:.2f}'.format(index_info['indices'][es_settings['index_name']]['total']['store']['size_in_bytes']/1000000) + ' MB')
	print('')
	print('Data folder info.')

	size = 0
	for d in os.listdir(PATH_DATA_FOLDERS):
		size += sum(os.path.getsize(PATH_DATA_FOLDERS + d + '/' + f) for f in os.listdir(PATH_DATA_FOLDERS + d) if os.path.isfile(PATH_DATA_FOLDERS + d + '/' + f))
		count = len(os.listdir(PATH_DATA_FOLDERS + d))
		print('Folder: ' + d + '\tCount: ' + str(count))

	print('Data folder storage size: ' + '{:.2f}'.format(size/1000000) + ' MB')
	print('\n')

		
@click.command(help='Create ES index.')
def create_index():
	
	# Get configuration.
	es_settings = get_es_config()

	# Check authentication and prompt for credentials if needed.
	user, pw = get_credentials(es_settings['shield_authentication'])

	# Create new index
	es_post_query(
		es_settings['host_url'] + ':' + es_settings['host_port'] + '/' + es_settings['index_name'], 
		data=json.dumps({
			'settings': es_settings['index_settings'],
			'mappings': {doc_type['name']: doc_type['mappings'] for doc_type in es_settings['document_types']}
		}), 
		credentials=(user, pw)
	)
	
@click.command(help='Remove ES index and all its data entries.')
@click.option('--index', prompt='Index name', help='The index to be removed.')
def remove_index(index):
	
	# Get configuration.
	es_settings = get_es_config()

	# Check authentication and prompt for credentials if needed.
	user, pw = get_credentials(es_settings['shield_authentication'])

	# Clear cache
	es_post_query(es_settings['host_url'] + ':' + es_settings['host_port'] + '/' + index + "/_cache/clear", None, (user, pw))

	# Delete the index.
	es_delete_query(es_settings['host_url'] + ':' + es_settings['host_port'] + '/' + index, (user, pw))

@click.command(help='Fetch all data from Riksdagens Öppna API.')
def fetch_data():

	# Get configurations.
	fetch_settings = get_fetch_config()

	# Traverse document types to fetch.
	for doc_type in fetch_settings['document_types']:

		# If folders already exists delete and recreate them.
		print('Removing ' + PATH_DATA_FOLDERS + doc_type + ' and all its content.\n')
		clean_data_directory(doc_type)

		# Iterate all links for a specific document type.
		for link in fetch_settings['api_info'][doc_type]['links']:
			print('Fetching: ' + link + fetch_settings['api_info'][doc_type]['file_ending'])

			# Get data from Riksdagens Öppna API and extract the content int data folder.
			response = data_request(fetch_settings['api_url'] + fetch_settings['api_info'][doc_type]['url_path'] + link + fetch_settings['api_info'][doc_type]['file_ending'])
			extract_files(response.content, PATH_DATA_FOLDERS + doc_type)

			print('')

	print('All data fetched!\n')

@click.command(help='Load data into ES.')
def load_data():
	
	# Get configuration.
	es_settings = get_es_config()

	# Check authentication and prompt for credentials if needed.
	user, pw = get_credentials(es_settings['shield_authentication'])

	# Bulk insert document into ES index.
	for doc_type in es_settings['document_types']:

		if doc_type['load_data']:

			print('Inserting type: ' + doc_type['name'] + '\n')

			# Set hard limit
			hard_limit = float('inf') if doc_type['hard_limit'] == None else doc_type['hard_limit']

			counter = 0
			old_count = 0
			insert_str = ''
			for f in os.listdir(PATH_DATA_FOLDERS + doc_type['data_folder']):
				counter += 1

				# Parse file names for error control.
				'''
				if not re.match("^[a-öA-Ö0-9_]*$", f[:-5]):
					f_new = ''.join(e for e in f[:-5] if e.isalnum())
				else:
					f_new = f[:-5]
				'''

				# Prepare bulk to insert.
				data_file = codecs.open(PATH_DATA_FOLDERS + doc_type['data_folder'] + '/' + f, 'r', 'utf-8-sig')
				json_obj = json.loads(remove_comments(data_file.read()))[doc_type['ignore_initial_key']]
				json_obj[es_settings['document_name_field_key']] = f
				insert_str += json.dumps({'index': {'_index': es_settings['index_name'], '_type': doc_type['name']}}) + '\n'
				insert_str += json.dumps(json_obj) + '\n'

				# Insert bulk.
				if (counter % es_settings['bulk_insert_rate']) == 0:
					print('Inserting entries: ' + str(old_count) + '-' + str(counter))

					try:
						es_post_query(es_settings['host_url'] + ':' + es_settings['host_port'] + '/_bulk', insert_str, (user, pw))
					except requests.exceptions.RequestException as e:
						print(e)
						print('Post query failed.\n')
						sys.exit(1)
					
					old_count = counter + 1
					insert_str = ''

				# Check hard limit
				if counter >= hard_limit:
					break

			# Insert last bulk.
			if counter >= old_count:
				print('Inserting entries: ' + str(old_count) + '-' + str(counter))
				es_post_query(es_settings['host_url'] + ':' + es_settings['host_port'] + '/_bulk', insert_str, (user, pw))
				print('\n')

@click.command(help='Remove and create index for search queries.')
def clean_query_index(index):
	
	# Get configuration.
	es_settings = get_es_config()

	# Check authentication and prompt for credentials if needed.
	user, pw = get_credentials(es_settings['shield_authentication'])

	# Variables
	queries_index = 'queries'

	# Delete the index.
	response = requests.get(es_settings['host_url'] + ':' + es_settings['host_port'] + '/' + queries_index, data=None, auth=(user, pw))
	print(response.status_code)

	# If index exist, delete it.


	# Create index

	queries_mapping_settings = json.dumps(
		"mappings": {
			"type_search_query": {
				"properties": {
					"term": {"type": "string", "index": "not_analyzed"},
					"type": {"type": "string", "index": "not_analyzed"},
					"date": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss"}
				}
			}
		}
	)


###
#
#	ES communication functions.
#
###

def es_get_query(url, data=None, credentials=(None, None)):

	response = requests.get(url, data=data, auth=credentials)

	if response.status_code != 200:
		print('Reponse Code: ' + str(response.status_code))
		print('Response Info: ' + response.content.decode('UTF-8'))
		print('URL: ' + url)
		sys.exit(1)

	return response

def es_post_query(url, data=None, credentials=(None, None)):

	response = requests.post(url, data=data, auth=credentials)

	if response.status_code != 200:
		print('Reponse Code: ' + str(response.status_code))
		print('Response Info: ' + response.content.decode('UTF-8'))
		print('URL: ' + url)
		sys.exit(1)
	else:
		print('Post query successfully completed!\n')

def es_delete_query(url, credentials=(None, None)):

	response = requests.delete(url, auth=credentials)

	if response.status_code != 200:
		print('Reponse Code: ' + str(response.status_code))
		print('Response Info: ' + response.content.decode('UTF-8'))
		print('URL: ' + url)
		sys.exit(1)
	else:
		print('Delete query successfully completed!\n')



###
#
#	Help functions.
#
###

def get_credentials(credentials_required):

	if credentials_required:
		user = input('Username: ')
		pw = getpass.getpass('Password: ')
		return (user, pw)
	else:
		return (None, None)


def get_es_config():

	with open(PATH_CONFIG_SETTINGS_ES) as f:
		es_settings = json.loads(f.read())

	return es_settings

def get_fetch_config():

	with open(PATH_CONFIG_SETTINGS_FETCH) as f:
		fetch_settings = json.loads(f.read())

	return fetch_settings


def clean_data_directory(dir_name):

	if os.path.exists(PATH_DATA_FOLDERS + dir_name):
		shutil.rmtree(PATH_DATA_FOLDERS + dir_name)

	os.makedirs(PATH_DATA_FOLDERS + dir_name)

def data_request(url):

	response = requests.get(url)

	if response.status_code != 200:
		print('Reponse Code: ' + str(response.status_code))
		print('Response Info: ' + response.content.decode('UTF-8'))
		print('URL: ' + url)
		sys.exit(1)

	return response

def extract_files(data, path):

	z = zipfile.ZipFile(io.BytesIO(data))
	z.extractall(path)

def remove_comments(string):

	return re.sub(re.compile("/\*.*?\*/", re.DOTALL ), '', string)




###
#
#	Main.
#
###

if __name__ == '__main__':

	cli.add_command(get_index_info)
	cli.add_command(check_connection)
	cli.add_command(create_index)
	cli.add_command(remove_index)
	cli.add_command(fetch_data)
	cli.add_command(load_data)

	print('\n')

	cli()