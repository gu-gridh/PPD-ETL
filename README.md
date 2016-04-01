# PPD ETL

Simple CLI tool for downloading data from [Rikdagens öppna data](http://data.riksdagen.se/) and upload it to an ElasticSearch instance.

### Requirements

* Python 3.4.3
* Pip 8.0.2
* Virtualenv 13.1.2

### Setup

Activate environment and install required python libraries.

```bash
$ virtualenv env
$ source env/bin/activate
$ pip install -r requirements.txt
```

### Usage

Activate environment and run main file.

```bash
$ source env/bin/activate
$ python main.py
```

### Force Pull

Force pull repo.

```bash
$ git fetch --all
$ git reset --hard origin/master
```

### Commands

* __check_connection:__ Check that the ES instance is up and running.
* __create_index:__ Create ES index.
* __fetch_data:__ Fetch data from Riksdagens API.
* __get_index_info:__ Get information from the index.
* __load_data:__ Load data into ES.
* __remove_index:__ Remove ES index and all data entries.
* __clean_query_index:__ Remove and create index for search queries.

### Configuration Files

* __fetch_settings.json:__ Settings for getting data from Riksdagens API.
* __es_settings.json:__ Settings and mappings for ES index.
