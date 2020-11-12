
from elasticsearch_dsl.query import Q, MultiMatch, SF
from elasticsearch_dsl import Search, A
from django.http import JsonResponse
import json

import requests
from pymemcache.client.base import Client

from collections import Counter

POSITIVE_SLOT_ORDER = ['name', 'where', 'close_contact', 'recent_travel', 'employer', 'age', 'start_date', 'end_date', 'top_K']
NEGATIVE_SLOT_ORDER = ['name', 'where', 'close_contact', 'age', 'how_long', 'start_date', 'end_date', 'top_K']
CAN_NOT_TEST_SLOT_ORDER = ['name', 'where', 'symptoms', 'start_date', 'end_date', 'top_K']
DEATH_SLOT_ORDER = ['name', 'where', 'age', 'start_date', 'end_date', 'top_K']
CURE_ORDER = ['what_cure', 'who_cure', 'opinion', 'start_date', 'end_date', 'top_K']

DEFAULT_SIZE = 500
top_K = 500
each_N = 10

class JsonSerde(object):
    def serialize(self, key, value):
        if isinstance(value, str):
            return value.encode('utf-8'), 1
        return json.dumps(value).encode('utf-8'), 2

    def deserialize(self, key, value, flags):
        if flags == 1:
            return value.decode('utf-8')
        if flags == 2:
            return json.loads(value.decode('utf-8'))
        raise Exception("Unknown serialization format")

mc_client = Client('127.0.0.1:11211', serde=JsonSerde())

##### slot including date and number of results returned
reserved_slot = 3

def map_slot(query_phrase_split, category_flag):

    ## first mapping input into corresponding slot
    query_slot_mapping = {}
    if category_flag == 'positive':
        slot_order = POSITIVE_SLOT_ORDER
    if category_flag == 'negative':
        slot_order = NEGATIVE_SLOT_ORDER
    if category_flag == 'can_not_test':
        slot_order = CAN_NOT_TEST_SLOT_ORDER
    if category_flag == 'death':
        slot_order = DEATH_SLOT_ORDER
    if category_flag == 'cure':
        slot_order = CURE_ORDER
    for idx, each_split in enumerate(query_phrase_split):
        query_slot_mapping[slot_order[idx]] = each_split
    return query_slot_mapping


def gen_query(query_slot_mapping, category_flag):

    ## build search dsl
    input_query = Search()

    # print(query_slot_mapping)

    ## then process must and must_not conditions
    input_must_condition = []
    for each_item in query_slot_mapping.items():
        if each_item[0] not in ['start_date', 'end_date', 'top_K']:
            if each_item[1] != '' and each_item[1] != '?':
                input_must_condition.append("Q('match', "+each_item[0]+'="'+each_item[1]+'")')

    ## process must not conditions
    input_must_not_condition = []
    input_agg_slot = []
    for each_item in query_slot_mapping.items():
        if each_item[1] == '?':
            input_must_not_condition.append("Q('match', "+each_item[0]+"_KEY='not specified')")
            input_agg_slot.append(each_item[0])

    ##### SOME DIRECT BACK_END PROCESS: not show "AUTHOR OF THE TWEET"
    if category_flag in ['positive', 'negative']:
        input_must_not_condition.append("Q('match', name_KEY='author of the tweet')")

    ##### deal with close contact - people can't be in close contact with themselves
    # if 'close_contact' in query_slot_mapping:
    #     if query_slot_mapping['close_contact'] != '' and query_slot_mapping['close_contact'] != '?' and query_slot_mapping['name'] != '':
    #         input_must_not_condition.append("Q('match', name='"+query_slot_mapping['close_contact']+"')")

    print(input_must_not_condition)

    ## add time range constraint
    ##### only apply it when you have some searching conditions
    if input_must_condition + input_must_not_condition:
        input_query = input_query.filter('range', date={'gte': query_slot_mapping['start_date'],
                                                        'lte': query_slot_mapping['end_date']})

    q = Q('bool', must=[eval(i) for i in input_must_condition],
                  must_not=[eval(i) for i in input_must_not_condition])
    input_query = input_query.query(q)

    ## perform aggregation
    ##### only aggregate towards one star, if more than one star, then merge on name slot
    if len(input_agg_slot) == 1:
        a = A('terms', field=input_agg_slot[0]+'_KEY', size=query_slot_mapping['top_K'])
    else:
        a = A('terms', field='name_KEY')
    # first add top K result
    input_query.aggs.bucket('top_K', a)
    # then identify how many tweets for each bucket (NEED TO CONSIDER SORTING HERE - sorting it when loading data)
    a = A('top_hits', _source={"includes": ['id', 'text', 'timestamp', 'tweet_link']},
                      size=each_N,
                      sort=[{'timestamp': {"order": "desc"}}])
    input_query.aggs['top_K'].bucket('each_N', a)

    ## control the returned value
    input_query.update_from_dict({"size": DEFAULT_SIZE, "sort": [{"timestamp": "desc"}]})

    print(input_query.to_dict())

    # {"query": {"bool": {"must": [{"wildcard": {"where": "Columbus"}}], "must_not": [], "should": []}}, "from": 0,
    #  "size": 10, "sort": [], "aggs": {}}
    #
    # ## finally build up query
    # positive_query = {"bool": {"must": [{"wildcard": i} for i in positive_must_condition.items()]}}

    return input_query, input_agg_slot


def get_search_query(phrase):

    print(phrase)

    ## replace * with ?
    phrase = phrase.replace('*', '?')
    ## read in phrase
    phrase_split = phrase.split(';')
    # category flag
    category_flag = phrase_split[0]
    # actual phrase
    actual_phrase_split = [i.lower() for i in phrase_split[1:]]

    phrase_for_mc = phrase.replace(' ', '')

    ## filter based on category
    results = []
    input_agg_slot = []
    if ['' for i in range(len(actual_phrase_split)-reserved_slot)] != actual_phrase_split[:-reserved_slot]:
        input_slot_mapping = map_slot(actual_phrase_split, category_flag)
        curr_query, input_agg_slot = gen_query(input_slot_mapping, category_flag)
        curr_query = curr_query.to_dict()

        ## add cached mechanism
        print(phrase_for_mc)

        if not mc_client.get(phrase_for_mc):
        # if True:
            print('[I] query')
            if category_flag == 'positive':
                results = requests.get(url="http://127.0.0.1:9200/covid19_positive/_search", json=curr_query).json()
            if category_flag == 'negative':
                results = requests.get(url="http://127.0.0.1:9200/covid19_negative/_search",
                                       json=curr_query).json()
            if category_flag == 'can_not_test':
                results = requests.get(url="http://127.0.0.1:9200/covid19_can_not_test/_search",
                                       json=curr_query).json()
            if category_flag == 'death':
                results = requests.get(url="http://127.0.0.1:9200/covid19_death/_search",
                                       json=curr_query).json()
            if category_flag == 'cure':
                results = requests.get(url="http://127.0.0.1:9200/covid19_cure/_search",
                                       json=curr_query).json()
            if '?' in phrase_for_mc:
                results = results['aggregations']
                mc_client.set(phrase_for_mc, results, expire=3600)
            else:
                results = results['hits']
                mc_client.set(phrase_for_mc, results, expire=3600)
        else:
            print('[I] results from cache')
            results = mc_client.get(phrase_for_mc)

        ### cache results

        ## add current category
        print(curr_query)
    # print(results['hits'])

    return results, input_agg_slot, phrase


def search(phrase):

    print(phrase)

    phrase_ori = phrase

    if '#@#' in phrase:
        phrase = phrase.replace('#@#', '')

    ### get results
    result, input_agg_slot, ori_phrase = get_search_query(phrase)

    ### add post-processing code here
    print(input_agg_slot)

    res = []
    if result:
        if input_agg_slot:
            for idx, i in enumerate(result['top_K']['buckets']):
                curr_bucket = []
                for idx_2, j in enumerate(i['each_N']['hits']['hits']):
                    if idx_2 == 0:
                        curr_count_info = {}
                        curr_count_info['name'] = i['key']
                        if len(input_agg_slot) == 1:
                            curr_count_info['aggre_name'] = input_agg_slot[0]
                        else:
                            curr_count_info['aggre_name'] = 'name'
                        curr_count_info['count'] = i['doc_count']
                    curr_info = j['_source'].copy()
                    curr_info['name'] = i['key']
                    curr_info['is_link'] = 'no'
                    curr_bucket.append(curr_info)
                ### only deal with one star situation
                if len(input_agg_slot) == 1:
                    ori_phrase_replace = ori_phrase.replace('?', i['key'])
                    curr_info = {}
                    curr_info['search_query'] = ori_phrase_replace
                    curr_info['is_link'] = 'LINK'
                    res.append({'data': curr_bucket, 'order_idx': idx, 'aggre_name': input_agg_slot[0],
                                'count': i['doc_count'], 'name': i['key'], 'status': 'MERGE',
                                'search_query': [curr_info]})
                else:
                    res.append({'data': curr_bucket, 'order_idx': idx, 'aggre_name': input_agg_slot[0],
                                'count': i['doc_count'], 'name': i['key'], 'status': 'MERGE'})
        else:
            # print(result['hits']['hits'][:2])
            if '#@#' in phrase_ori:
                curr_return = [i['_source'] for i in result['hits']]
                if curr_return:
                    res.append({'data': curr_return, 'status': 'SINGLE'})

    if ('?' in phrase_ori or '*' in phrase_ori or '#@#' in phrase_ori) and res:
        res.append({"status": "STATUS_MSG", "message": "Your search results are:"})
    else:
        if '?' not in phrase_ori and '*' not in phrase_ori:
            res.append({"status": "STATUS_MSG", "message": 'Our semantic search system requires one "?" mark. Please check our instructions.'})
        else:
            res.append({"status": "STATUS_MSG", "message": "No returned search results."})


    # print(res)

    return res


def search_json(phrase):
    return JsonResponse(search(phrase), safe=False)


def search_direct(phrase):
    return search(phrase+'#@#')[:-1]



# query = {"size": 5, "query": {"bool": {"must_not": {"term": {"name": "not specified"}}}},
#          "aggs": {"hosts": {"terms": {"field": "name", "size": 100},
#                             "aggs": {"most_freq": {
#                                 "top_hits": {"_source": {"includes": ["id", "text", "name"]}, "size": 10}}}}}}