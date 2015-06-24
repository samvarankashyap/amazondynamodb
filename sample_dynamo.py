import boto.dynamodb
from boto.dynamodb.schema import Schema
from  boto.dynamodb.condition import *
import pdb
pdb.set_trace()
conn = boto.dynamodb.connect_to_region('us-west-2')

def get_data(query):
    table = conn.get_table('consumer_table')
    #items = table.scan(scan_filter={'c_id': EQ('1427578')})
    items = table.scan(scan_filter=query)
    #print items.response["Items"]
    return items.response["Items"]

def prepare_query(posted_data):
    data = {}
    for x in posted_data:
        data[x]= CONTAINS(posted_data[x])
    return data

def filter_data(posted_data):
    data = {}
    for x in posted_data:
        if posted_data[x]!='':
            data[x] = posted_data[x]
    return data

posted_data = {'c_id': '14250', 'zip':''}
posted_data = filter_data(posted_data)
print posted_data
posted_data = prepare_query(posted_data)
print posted_data 
posted_data = get_data(posted_data)
print posted_data
