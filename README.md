
## Semantic Search for COVID-19 Using Twitter

This repo contains the source code for our semantic search application. Our demo is built by using Django and Elasticsearch. 

Check our demo at: http://kb1.cse.ohio-state.edu:8000/covid19/

### System overview

Our system works as follows: (1) A user types in a search query on the webpage. (2) Webpage makes a request to the Elasticsearch server and gets the results back from search engine. (3) Results are rendered in the webpage.

#### Start Elasticsearch service

Please download Elasticsearch package on your server (we use version 6.8.1). Then start Elasticsearch service by running the following command under elasticsearch directory.

```
./bin/elasticsearch
```

The default address for Elasticsearch is `127.0.0.1:9200`. You could check if you have successfully started the service by visiting the above address and having the `You Know, for Search` message.

#### Set up Elasticsearch

In this demo, we directly import data into Elasticsearch, rather than first store the data in a database and sync with Elasticsearch. We provide our code for loading data in `load_data.py`. 

In general, it contains the following steps.

1. Declare the client: Once the client is declared, we are able to send requests to it.

```
es_client = Elasticsearch(hosts="http://127.0.0.1:9200/")
``` 

2. Build index: Specify which attributes you want to index (thus searchable) by Elasticsearch in `mappings`.

```
mappings = YOUR_DEFINED_MAPPING
es_client.indices.create(index=YOUR_INDEX_NAME, body=mappings)
```

3. Import data: We use `helpers.bulk` to improve the speed of loading a large amount of data.

```
helpers.bulk(es_client, action)
```

#### Prepare date

Suppose you have a new collection of tweets, please follow the instructions at https://github.com/viczong/extract_COVID19_events_from_Twitter for processing them and extracting text spans for slot filling questions.

Please note that for reducing the file size, the BERT processed files `BERT_PROCESSED_FILE.jsonl` do not contain fields such as tweet timestamp. Thus we need to use the original file `ORGINAL_TWEETS_FILE.jsonl` for getting these information. We provide sample data files under `sample_data_file` folder for your reference.

Once you have already prepared these two files, run the following command for indexing tweets into Elasticsearch.

```
python load_data.py --index_name covid19_positive
                    --file_name BERT_PROCESSED_FILE.jsonl 
                    --user_info_file ORGINAL_TWEETS_FILE.jsonl
```

 
#### Run web demo

You could build our demo by running the following command:

```py
python manage.py runserver 0.0.0.0:YOUR_PORT
```

### Dependencies

```
elasticsearch == 6.8.1
django == 2.2
elasticsearch_dsl
pymemcache
```
