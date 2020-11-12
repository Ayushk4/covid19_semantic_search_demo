from elasticsearch import Elasticsearch
from elasticsearch import helpers

import json
import argparse
import numpy as np
import gzip

### declare the client
es_client = Elasticsearch(hosts="http://127.0.0.1:9200/")
RANDOM_SEED = 901

TEXT_TYPE = "text"
KEYWORD_TYPE = "keyword"


def readJSONLine(path):

    output = []
    with open(path, 'r') as f:
        for line in f:
            output.append(json.loads(line))

    return output


def writeGZIPJSONLine(data, location):
    with gzip.open(location, "wb") as f:
        for each_line in data:
            f.write((json.dumps(each_line)+'\n').encode())


def deleteIndex(index_name):
    es_client.indices.delete(index_name)
    return None


def buildIndex(index_name):

    mappings = None
    if 'positive' in index_name:
        mappings = {"mappings": {
            "tweet": {
                "properties": {
                    "id": {"type": KEYWORD_TYPE, "index": True},
                    "text": {"type": TEXT_TYPE, "index": False},
                    "category": {"type": KEYWORD_TYPE, "index": True},
                    "date": {"type": KEYWORD_TYPE, "index": True},
                    "timestamp": {"type": KEYWORD_TYPE, "index": True},
                    "relation": {"type": KEYWORD_TYPE, "index": True},
                    ## TEXT SEARCH
                    "name": {"type": TEXT_TYPE, "index": True},
                    "when": {"type": TEXT_TYPE, "index": True},
                    "where": {"type": TEXT_TYPE, "index": True},
                    "gender_male": {"type": TEXT_TYPE, "index": True},
                    "gender_female": {"type": TEXT_TYPE, "index": True},
                    "employer": {"type": TEXT_TYPE, "index": True},
                    "close_contact": {"type": TEXT_TYPE, "index": True},
                    "recent_travel": {"type": TEXT_TYPE, "index": True},
                    "age": {"type": TEXT_TYPE, "index": True},
                    ## KEY AGGR
                    "name_KEY": {"type": KEYWORD_TYPE, "index": True},
                    "when_KEY": {"type": KEYWORD_TYPE, "index": True},
                    "where_KEY": {"type": KEYWORD_TYPE, "index": True},
                    "gender_male_KEY": {"type": KEYWORD_TYPE, "index": True},
                    "gender_female_KEY": {"type": KEYWORD_TYPE, "index": True},
                    "employer_KEY": {"type": KEYWORD_TYPE, "index": True},
                    "close_contact_KEY": {"type": KEYWORD_TYPE, "index": True},
                    "recent_travel_KEY": {"type": KEYWORD_TYPE, "index": True},
                    "age_KEY": {"type": KEYWORD_TYPE, "index": True},
                }
            }
        }
        }
    elif 'negative' in index_name:
        mappings = {"mappings": {
            "tweet": {
                "properties": {
                    "id": {"type": KEYWORD_TYPE, "index": True},
                    "text": {"type": TEXT_TYPE, "index": False},
                    "category": {"type": KEYWORD_TYPE, "index": True},
                    "date": {"type": KEYWORD_TYPE, "index": True},
                    "timestamp": {"type": KEYWORD_TYPE, "index": True},
                    "relation": {"type": KEYWORD_TYPE, "index": True},
                    ## TEXT SEARCH
                    "name": {"type": TEXT_TYPE, "index": True},
                    "when": {"type": TEXT_TYPE, "index": True},
                    "where": {"type": TEXT_TYPE, "index": True},
                    "gender_male": {"type": TEXT_TYPE, "index": True},
                    "gender_female": {"type": TEXT_TYPE, "index": True},
                    "close_contact": {"type": TEXT_TYPE, "index": True},
                    "age": {"type": TEXT_TYPE, "index": True},
                    "how_long": {"type": TEXT_TYPE, "index": True},
                    ## KEY AGGR
                    "name_KEY": {"type": KEYWORD_TYPE, "index": True},
                    "when_KEY": {"type": KEYWORD_TYPE, "index": True},
                    "where_KEY": {"type": KEYWORD_TYPE, "index": True},
                    "gender_male_KEY": {"type": KEYWORD_TYPE, "index": True},
                    "gender_female_KEY": {"type": KEYWORD_TYPE, "index": True},
                    "close_contact_KEY": {"type": KEYWORD_TYPE, "index": True},
                    "age_KEY": {"type": KEYWORD_TYPE, "index": True},
                    "how_long_KEY": {"type": KEYWORD_TYPE, "index": True},
                }
            }
        }
        }
    elif 'can_not_test' in index_name:
        mappings = {"mappings": {
            "tweet": {
                "properties": {
                    "id": {"type": KEYWORD_TYPE, "index": True},
                    "text": {"type": TEXT_TYPE, "index": False},
                    "category": {"type": KEYWORD_TYPE, "index": True},
                    "date": {"type": KEYWORD_TYPE, "index": True},
                    "timestamp": {"type": KEYWORD_TYPE, "index": True},
                    "relation": {"type": KEYWORD_TYPE, "index": True},
                    "symptoms": {"type": KEYWORD_TYPE, "index": True},
                    ## TEXT SEARCH
                    "name": {"type": TEXT_TYPE, "index": True},
                    "when": {"type": TEXT_TYPE, "index": True},
                    "where": {"type": TEXT_TYPE, "index": True},
                    ## KEY AGGR
                    "name_KEY": {"type": KEYWORD_TYPE, "index": True},
                    "when_KEY": {"type": KEYWORD_TYPE, "index": True},
                    "where_KEY": {"type": KEYWORD_TYPE, "index": True},
                }
            }
        }
        }
    elif 'death' in index_name:
        mappings = {"mappings": {
            "tweet": {
                "properties": {
                    "id": {"type": KEYWORD_TYPE, "index": True},
                    "text": {"type": TEXT_TYPE, "index": False},
                    "category": {"type": KEYWORD_TYPE, "index": True},
                    "date": {"type": KEYWORD_TYPE, "index": True},
                    "timestamp": {"type": KEYWORD_TYPE, "index": True},
                    "relation": {"type": KEYWORD_TYPE, "index": True},
                    ## TEXT SEARCH
                    "name": {"type": TEXT_TYPE, "index": True},
                    "when": {"type": TEXT_TYPE, "index": True},
                    "where": {"type": TEXT_TYPE, "index": True},
                    "age": {"type": TEXT_TYPE, "index": True},
                    ## KEYWORD AGGS
                    "name_KEY": {"type": KEYWORD_TYPE, "index": True},
                    "when_KEY": {"type": KEYWORD_TYPE, "index": True},
                    "where_KEY": {"type": KEYWORD_TYPE, "index": True},
                    "age_KEY": {"type": KEYWORD_TYPE, "index": True},
                }
            }
        }
        }
    elif 'cure' in index_name:
        mappings = {"mappings": {
            "tweet": {
                "properties": {
                    "id": {"type": KEYWORD_TYPE, "index": True},
                    "text": {"type": TEXT_TYPE, "index": False},
                    "category": {"type": KEYWORD_TYPE, "index": True},
                    "date": {"type": KEYWORD_TYPE, "index": True},
                    "timestamp": {"type": KEYWORD_TYPE, "index": True},
                    "opinion": {"type": KEYWORD_TYPE, "index": True},
                    ### TEXT SEARCH
                    "what_cure": {"type": TEXT_TYPE, "index": True},
                    "who_cure": {"type": TEXT_TYPE, "index": True},
                    ## KEYWORD AGGS
                    "what_cure_KEY": {"type": KEYWORD_TYPE, "index": True},
                    "who_cure_KEY": {"type": KEYWORD_TYPE, "index": True},
                }
            }
        }
        }

    ## generate index
    es_client.indices.create(index=index_name, body=mappings)

    return None


def insertToES(index_name, input_data):

    action = None
    if 'positive' in index_name:
        action = [{
            "_index": index_name,
            "_type": "tweet",
            "_id": each_line['id'],
            "_source": {
                "id": each_line['id'],
                "text": each_line['text'],
                "category": 'positive',
                "date": each_line['user_info']['timestamp'].split(' ')[0],
                "timestamp": each_line['user_info']['timestamp'],
                "relation": each_line['prediction_reorganize']['relation'],
                ## TEXT SEARCH
                "age": each_line['prediction_reorganize']['age'],
                "close_contact": each_line['prediction_reorganize']['close_contact'],
                "employer": each_line['prediction_reorganize']['employer'],
                "gender_male": each_line['prediction_reorganize']['gender_male'],
                "gender_female": each_line['prediction_reorganize']['gender_female'],
                "name": each_line['prediction_reorganize']['name'],
                "recent_travel": each_line['prediction_reorganize']['recent_travel'],
                "when": each_line['prediction_reorganize']['when'],
                "where": each_line['prediction_reorganize']['where'],
                ## KEY AGGR
                "age_KEY": each_line['prediction_reorganize']['age'],
                "close_contact_KEY": each_line['prediction_reorganize']['close_contact'],
                "employer_KEY": each_line['prediction_reorganize']['employer'],
                "gender_male_KEY": each_line['prediction_reorganize']['gender_male'],
                "gender_female_KEY": each_line['prediction_reorganize']['gender_female'],
                "name_KEY": each_line['prediction_reorganize']['name'],
                "recent_travel_KEY": each_line['prediction_reorganize']['recent_travel'],
                "when_KEY": each_line['prediction_reorganize']['when'],
                "where_KEY": each_line['prediction_reorganize']['where'],
            }
        } for each_line in input_data]
    elif 'negative' in index_name:
        action = [{
            "_index": index_name,
            "_type": "tweet",
            "_id": each_line['id'],
            "_source": {
                "id": each_line['id'],
                "text": each_line['text'],
                "category": 'negative',
                "date": each_line['user_info']['timestamp'].split(' ')[0],
                "timestamp": each_line['user_info']['timestamp'],
                "relation": each_line['prediction_reorganize']['relation'],
                ## TEXT
                "age": each_line['prediction_reorganize']['age'],
                "how_long": each_line['prediction_reorganize']['how_long'],
                "close_contact": each_line['prediction_reorganize']['close_contact'],
                "gender_male": each_line['prediction_reorganize']['gender_male'],
                "gender_female": each_line['prediction_reorganize']['gender_female'],
                "name": each_line['prediction_reorganize']['name'],
                "when": each_line['prediction_reorganize']['when'],
                "where": each_line['prediction_reorganize']['where'],
                ## KEYWORD
                "age_KEY": each_line['prediction_reorganize']['age'],
                "how_long_KEY": each_line['prediction_reorganize']['how_long'],
                "close_contact_KEY": each_line['prediction_reorganize']['close_contact'],
                "gender_male_KEY": each_line['prediction_reorganize']['gender_male'],
                "gender_female_KEY": each_line['prediction_reorganize']['gender_female'],
                "name_KEY": each_line['prediction_reorganize']['name'],
                "when_KEY": each_line['prediction_reorganize']['when'],
                "where_KEY": each_line['prediction_reorganize']['where'],
            }
        } for each_line in input_data]
    elif 'can_not_test' in index_name:
        action = [{
            "_index": index_name,
            "_type": "tweet",
            "_id": each_line['id'],
            "_source": {
                "id": each_line['id'],
                "text": each_line['text'],
                "category": 'can_no_test',
                "date": each_line['user_info']['timestamp'].split(' ')[0],
                "timestamp": each_line['user_info']['timestamp'],
                "relation": each_line['prediction_reorganize']['relation'],
                "symptoms": each_line['prediction_reorganize']['symptoms'],
                ## TEXT
                "name": each_line['prediction_reorganize']['name'],
                "when": each_line['prediction_reorganize']['when'],
                "where": each_line['prediction_reorganize']['where'],
                ## KEYWORD
                "name_KEY": each_line['prediction_reorganize']['name'],
                "when_KEY": each_line['prediction_reorganize']['when'],
                "where_KEY": each_line['prediction_reorganize']['where'],
            }
        } for each_line in input_data]
    elif 'death' in index_name:
        action = [{
            "_index": index_name,
            "_type": "tweet",
            "_id": each_line['id'],
            "_source": {
                "id": each_line['id'],
                "text": each_line['text'],
                "category": 'death',
                "date": each_line['user_info']['timestamp'].split(' ')[0],
                "timestamp": each_line['user_info']['timestamp'],
                "relation": each_line['prediction_reorganize']['relation'],
                ## TEXT
                "name": each_line['prediction_reorganize']['name'],
                "when": each_line['prediction_reorganize']['when'],
                "where": each_line['prediction_reorganize']['where'],
                "age": each_line['prediction_reorganize']['age'],
                ## KEYWORD
                "name_KEY": each_line['prediction_reorganize']['name'],
                "when_KEY": each_line['prediction_reorganize']['when'],
                "where_KEY": each_line['prediction_reorganize']['where'],
                "age_KEY": each_line['prediction_reorganize']['age'],
            }
        } for each_line in input_data]
    elif 'cure' in index_name:
        action = [{
            "_index": index_name,
            "_type": "tweet",
            "_id": each_line['id'],
            "_source": {
                "id": each_line['id'],
                "text": each_line['text'],
                "category": 'cure',
                "date": each_line['user_info']['timestamp'].split(' ')[0],
                "timestamp": each_line['user_info']['timestamp'],
                "opinion": each_line['prediction_reorganize']['opinion'],
                ## TEXT
                "what_cure": each_line['prediction_reorganize']['what_cure'],
                "who_cure": each_line['prediction_reorganize']['who_cure'],
                ## KEYWORD
                "what_cure_KEY": each_line['prediction_reorganize']['what_cure'],
                "who_cure_KEY": each_line['prediction_reorganize']['who_cure'],
            }
        } for each_line in input_data]

    helpers.bulk(es_client, action)

    return None


def dataPreprocessing(input_data, user_info):

    ## build dictionary
    user_info_dict = {}
    for each_line in user_info:
        user_info_dict[each_line['id']] = each_line

    ## merge
    for each_line in input_data:
        ## append user info
        curr_user = user_info_dict[each_line['id']]
        each_line['user_info'] = curr_user

        ## split by :: to recover the original pred
        for each_key in each_line['prediction_reorganize'].keys():
            each_line['prediction_reorganize'][each_key] = [i.lower().replace('#', '') for i in each_line['prediction_reorganize'][each_key].split('::')]

        ## replace AUTHOR OF THE TWEET with actual user name
        if 'name' in each_line['prediction_reorganize'].keys():
            curr_new = []
            for each_one in each_line['prediction_reorganize']['name']:
                if each_one != 'near author of the tweet':
                    # if each_one == 'author of the tweet':
                    #     curr_new.append(each_line['user_info']['fullname'])
                    # else:
                    curr_new.append(each_one)
            each_line['prediction_reorganize']['name'] = curr_new

        if 'close_contact' in each_line['prediction_reorganize'].keys():
            curr_new = []
            for each_one in each_line['prediction_reorganize']['close_contact']:
                if each_one != 'near author of the tweet':
                    # if each_one == 'author of the tweet':
                    #     curr_new.append(each_line['user_info']['fullname'])
                    # else:
                    curr_new.append(each_one)
            each_line['prediction_reorganize']['close_contact'] = curr_new

        ## deal with binary choices
        if 'relation' in each_line['prediction_reorganize'].keys():
            if each_line['prediction_reorganize']['relation'] != ['not specified']:
                each_line['prediction_reorganize']['relation'] = ['yes']

        ##### cure category - opinion has been properly treated
        if 'opinion' in each_line['prediction_reorganize'].keys():
            if each_line['prediction_reorganize']['opinion'] != ['not specified']:
                each_line['prediction_reorganize']['opinion'] = ['yes']

    return input_data


def dataDeduplication(input_data):

    input_data_sorted = sorted(input_data, key=lambda x: x['id'])

    input_data_dict = {}
    for each_line in input_data_sorted:
        text_cleaned = ' '.join([i for i in each_line['text'].split(' ') if 'http' not in i and '@' not in i and 'pic.twitter.com' not in i and 't.co' not in i])
        if not text_cleaned in input_data_dict:
            ### remove urls
            input_data_dict[text_cleaned] = each_line['id']

    es_data_index = [{'text': i, 'id': input_data_dict[i], 'from': '1st'} for i in input_data_dict.keys()]
    es_data_index_dict = {}
    for each_line in es_data_index:
        es_data_index_dict[each_line['id']] = []

    es_data = []
    for each_line in input_data:
        if each_line['id'] in es_data_index_dict:
            es_data.append(each_line)

    return es_data_index, es_data


def loadData(index_name, file_name, user_info_file):

    ## read in data
    input_data = readJSONLine(file_name)
    print('length of input data', len(input_data))

    user_info = readJSONLine(user_info_file)

    ## random shuffle
    np.random.seed(RANDOM_SEED)
    np.random.shuffle(input_data)

    input_data_processed = dataPreprocessing(input_data, user_info)
    es_data_index, es_data = dataDeduplication(input_data_processed)

    print(len(es_data))

    writeGZIPJSONLine(es_data_index,
                      '/data/zong/demo-streaming_temp/unique_ones/'+index_name.replace('covid19_', '')+'_unique_ori.jsonl.gz')

    ## first delete previous index
    try:
        deleteIndex(index_name)
    except:
        pass

    ## build new index
    buildIndex(index_name)

    ## load data
    insertToES(index_name, es_data)

    return None


if __name__ == '__main__':

    parser = argparse.ArgumentParser()

    parser.add_argument("--index_name", type=str, required=True)
    parser.add_argument("--file_name", type=str, required=True)
    parser.add_argument("--user_info_file", type=str, required=True)
    args = parser.parse_args()

    loadData(args.index_name, args.file_name, args.user_info_file)

    ### commands
    # python load_data.py --index_name covid19_positive --file_name /data/zong/covid-large_scale_exp/final_data/positive.jsonl --user_info_file /data/zong/covid-large_scale_exp/positive_user_info.jsonl
    # python load_data.py --index_name covid19_negative --file_name /data/zong/covid-large_scale_exp/final_data/negative.jsonl --user_info_file /data/zong/covid-large_scale_exp/negative_user_info.jsonl
    # python load_data.py --index_name covid19_cure --file_name /data/zong/covid-large_scale_exp/final_data/cure.jsonl --user_info_file /data/zong/covid-large_scale_exp/cure_user_info.jsonl
    # python load_data.py --index_name covid19_can_not_test --file_name /data/zong/covid-large_scale_exp/final_data/can_not_test.jsonl --user_info_file /data/zong/covid-large_scale_exp/can_not_test_user_info.jsonl
    # python load_data.py --index_name covid19_death --file_name /data/zong/covid-large_scale_exp/final_data/death.jsonl --user_info_file /data/zong/covid-large_scale_exp/death_user_info.jsonl
