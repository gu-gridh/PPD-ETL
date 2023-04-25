# PPD ETL

Simple CLI tool for downloading data from [Rikdagens öppna data](http://data.riksdagen.se/) and upload it to an ElasticSearch instance.

### Requirements

* Python 3.6

### Setup

Activate environment and install required python libraries.

```bash
$ python3 -m venv venv
$ source venv/bin/activate
$ pip install -r requirements.txt
```

### Usage

Activate environment and run main file.

```bash
$ source venv/bin/activate
$ export LC_ALL=en_US.utf-8
$ export LANG=en_US.utf-8
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

#### Procedure for updating data

1. `fetch_data`
2. `remove_index` (enter index name `ppd`)
3. `clean_query_index`
4. `create_index`
5. `load_data`

### Configuration Files

* __fetch_settings.json:__ Settings for getting data from Riksdagens API.
* __es_settings.json:__ Settings and mappings for ES index.
