from elasticsearch import Elasticsearch
from elasticsearch import helpers

import json
import argparse
import numpy as np

### declare the client
es_client = Elasticsearch(hosts="http://127.0.0.1:9200/")
RANDOM_SEED = 901


def readJSONLine(path):

    output = []
    with open(path, 'r') as f:
        for line in f:
            output.append(json.loads(line))

    return output


def deleteIndex(index_name):
    es_client.indices.delete(index_name)
    return None


def buildIndex(index_name):

    mappings = None
    if 'positive' in index_name:
        mappings = {"mappings": {
            "tweet": {
                "properties": {
                    "id": {"type": "keyword", "index": True},
                    "text": {"type": "keyword", "index": False},
                    "category": {"type": "keyword", "index": True},
                    "date": {"type": "keyword", "index": True},
                    "timestamp": {"type": "keyword", "index": True},
                    "name": {"type": "keyword", "index": True},
                    "when": {"type": "keyword", "index": True},
                    "where": {"type": "keyword", "index": True},
                    "relation": {"type": "keyword", "index": True},
                    "gender_male": {"type": "keyword", "index": True},
                    "gender_female": {"type": "keyword", "index": True},
                    "employer": {"type": "keyword", "index": True},
                    "close_contact": {"type": "keyword", "index": True},
                    "recent_travel": {"type": "keyword", "index": True},
                    "age": {"type": "keyword", "index": True},
                    "tweet_link": {"type": "keyword", "index": False},
                }
            }
        }
        }
    elif 'negative' in index_name:
        mappings = {"mappings": {
            "tweet": {
                "properties": {
                    "id": {"type": "keyword", "index": True},
                    "text": {"type": "keyword", "index": False},
                    "category": {"type": "keyword", "index": True},
                    "date": {"type": "keyword", "index": True},
                    "timestamp": {"type": "keyword", "index": True},
                    "name": {"type": "keyword", "index": True},
                    "when": {"type": "keyword", "index": True},
                    "where": {"type": "keyword", "index": True},
                    "relation": {"type": "keyword", "index": True},
                    "gender_male": {"type": "keyword", "index": True},
                    "gender_female": {"type": "keyword", "index": True},
                    "close_contact": {"type": "keyword", "index": True},
                    "age": {"type": "keyword", "index": True},
                    "how_long": {"type": "keyword", "index": True},
                    "tweet_link": {"type": "keyword", "index": False},
                }
            }
        }
        }
    elif 'can_not_test' in index_name:
        mappings = {"mappings": {
            "tweet": {
                "properties": {
                    "id": {"type": "keyword", "index": True},
                    "text": {"type": "keyword", "index": False},
                    "category": {"type": "keyword", "index": True},
                    "date": {"type": "keyword", "index": True},
                    "timestamp": {"type": "keyword", "index": True},
                    "name": {"type": "keyword", "index": True},
                    "when": {"type": "keyword", "index": True},
                    "where": {"type": "keyword", "index": True},
                    "relation": {"type": "keyword", "index": True},
                    "symptoms": {"type": "keyword", "index": True},
                    "tweet_link": {"type": "keyword", "index": False},
                }
            }
        }
        }
    elif 'death' in index_name:
        mappings = {"mappings": {
            "tweet": {
                "properties": {
                    "id": {"type": "keyword", "index": True},
                    "text": {"type": "keyword", "index": False},
                    "category": {"type": "keyword", "index": True},
                    "date": {"type": "keyword", "index": True},
                    "timestamp": {"type": "keyword", "index": True},
                    "name": {"type": "keyword", "index": True},
                    "when": {"type": "keyword", "index": True},
                    "where": {"type": "keyword", "index": True},
                    "relation": {"type": "keyword", "index": True},
                    "age": {"type": "keyword", "index": True},
                    "tweet_link": {"type": "keyword", "index": False},
                }
            }
        }
        }
    elif 'cure' in index_name:
        mappings = {"mappings": {
            "tweet": {
                "properties": {
                    "id": {"type": "keyword", "index": True},
                    "text": {"type": "keyword", "index": False},
                    "category": {"type": "keyword", "index": True},
                    "date": {"type": "keyword", "index": True},
                    "timestamp": {"type": "keyword", "index": True},
                    "what_cure": {"type": "keyword", "index": True},
                    "who_cure": {"type": "keyword", "index": True},
                    "opinion": {"type": "keyword", "index": True},
                    "tweet_link": {"type": "keyword", "index": False},
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
            "_source": {
                "id": each_line['id'],
                "text": each_line['text'],
                "category": 'positive',
                "date": each_line['user_info']['timestamp'].split(' ')[0],
                "timestamp": each_line['user_info']['timestamp'],
                "age": each_line['prediction_reorganize']['age'],
                "close_contact": each_line['prediction_reorganize']['close_contact'],
                "employer": each_line['prediction_reorganize']['employer'],
                "gender_male": each_line['prediction_reorganize']['gender_male'],
                "gender_female": each_line['prediction_reorganize']['gender_female'],
                "name": each_line['prediction_reorganize']['name'],
                "recent_travel": each_line['prediction_reorganize']['recent_travel'],
                "relation": each_line['prediction_reorganize']['relation'],
                "when": each_line['prediction_reorganize']['when'],
                "where": each_line['prediction_reorganize']['where'],
                "tweet_link": 'twitter.com' + each_line['user_info']['url']
            }
        } for each_line in input_data]
    elif 'negative' in index_name:
        action = [{
            "_index": index_name,
            "_type": "tweet",
            "_source": {
                "id": each_line['id'],
                "text": each_line['text'],
                "category": 'negative',
                "date": each_line['user_info']['timestamp'].split(' ')[0],
                "timestamp": each_line['user_info']['timestamp'],
                "age": each_line['prediction_reorganize']['age'],
                "how_long": each_line['prediction_reorganize']['how_long'],
                "close_contact": each_line['prediction_reorganize']['close_contact'],
                "gender_male": each_line['prediction_reorganize']['gender_male'],
                "gender_female": each_line['prediction_reorganize']['gender_female'],
                "name": each_line['prediction_reorganize']['name'],
                "relation": each_line['prediction_reorganize']['relation'],
                "when": each_line['prediction_reorganize']['when'],
                "where": each_line['prediction_reorganize']['where'],
                "tweet_link": 'twitter.com' + each_line['user_info']['url']
            }
        } for each_line in input_data]
    elif 'can_not_test' in index_name:
        action = [{
            "_index": index_name,
            "_type": "tweet",
            "_source": {
                "id": each_line['id'],
                "text": each_line['text'],
                "category": 'can_no_test',
                "date": each_line['user_info']['timestamp'].split(' ')[0],
                "timestamp": each_line['user_info']['timestamp'],
                "name": each_line['prediction_reorganize']['name'],
                "relation": each_line['prediction_reorganize']['relation'],
                "when": each_line['prediction_reorganize']['when'],
                "where": each_line['prediction_reorganize']['where'],
                "symptoms": each_line['prediction_reorganize']['symptoms'],
                "tweet_link": 'twitter.com' + each_line['user_info']['url']
            }
        } for each_line in input_data]
    elif 'death' in index_name:
        action = [{
            "_index": index_name,
            "_type": "tweet",
            "_source": {
                "id": each_line['id'],
                "text": each_line['text'],
                "category": 'death',
                "date": each_line['user_info']['timestamp'].split(' ')[0],
                "timestamp": each_line['user_info']['timestamp'],
                "name": each_line['prediction_reorganize']['name'],
                "relation": each_line['prediction_reorganize']['relation'],
                "when": each_line['prediction_reorganize']['when'],
                "where": each_line['prediction_reorganize']['where'],
                "age": each_line['prediction_reorganize']['age'],
                "tweet_link": 'twitter.com' + each_line['user_info']['url']
            }
        } for each_line in input_data]
    elif 'cure' in index_name:
        action = [{
            "_index": index_name,
            "_type": "tweet",
            "_source": {
                "id": each_line['id'],
                "text": each_line['text'],
                "category": 'cure',
                "date": each_line['user_info']['timestamp'].split(' ')[0],
                "timestamp": each_line['user_info']['timestamp'],
                "what_cure": each_line['prediction_reorganize']['what_cure'],
                "who_cure": each_line['prediction_reorganize']['who_cure'],
                "opinion": each_line['prediction_reorganize']['opinion'],
                "tweet_link": 'twitter.com' + each_line['user_info']['url']
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


def loadData(index_name, file_name, user_info_file):

    ## read in data
    input_data = readJSONLine(file_name)
    print('length of input data', len(input_data))

    user_info = readJSONLine(user_info_file)

    ## random shuffle
    np.random.seed(RANDOM_SEED)
    np.random.shuffle(input_data)

    input_data_processed = dataPreprocessing(input_data, user_info)

    ## first delete previous index
    try:
        deleteIndex(index_name)
    except:
        pass

    ## build new index
    buildIndex(index_name)

    ## load data
    insertToES(index_name, input_data_processed)

    return None


if __name__ == '__main__':

    parser = argparse.ArgumentParser()

    parser.add_argument("--index_name", type=str, required=True)
    parser.add_argument("--file_name", type=str, required=True)
    parser.add_argument("--user_info_file", type=str, required=True)
    args = parser.parse_args()

    loadData(args.index_name, args.file_name, args.user_info_file)

    ### commands
    # python load_data.py --index_name covid19_negative --file_name /data/zong/covid-large_scale_exp/final_data/negative.jsonl --user_info_file /data/zong/covid-large_scale_exp/negative_user_info.jsonl
    # python load_data.py --index_name covid19_positive --file_name /data/zong/covid-large_scale_exp/final_data/positive.jsonl --user_info_file /data/zong/covid-large_scale_exp/positive_user_info.jsonl
    # python load_data.py --index_name covid19_cure --file_name /data/zong/covid-large_scale_exp/final_data/cure.jsonl --user_info_file /data/zong/covid-large_scale_exp/cure_user_info.jsonl
    # python load_data.py --index_name covid19_can_not_test --file_name /data/zong/covid-large_scale_exp/final_data/can_not_test.jsonl --user_info_file /data/zong/covid-large_scale_exp/can_not_test_user_info.jsonl
    # python load_data.py --index_name covid19_death --file_name /data/zong/covid-large_scale_exp/final_data/death.jsonl --user_info_file /data/zong/covid-large_scale_exp/death_user_info.jsonl
