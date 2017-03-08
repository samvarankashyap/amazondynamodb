import boto.dynamodb
from boto.dynamodb.schema import Schema
from  boto.dynamodb.condition import *
conn = boto.dynamodb.connect_to_region('us-west-2')


def get_data(table, query):
    table = conn.get_table(table)
    # example query
    # items = table.scan(scan_filter={'c_id': EQ('1427578')})
    items = table.scan(scan_filter=query)
    return items.response["Items"]


def prepare_query(posted_data):
    data = {}
    for record in posted_data:
        data[record]= CONTAINS(posted_data[record])
    return data


def filter_data(posted_data):
    data = {}
    for record in posted_data:
        if posted_data[record]!='':
            data[record] = posted_data[record]
    return data


posted_data = {'c_id': '14250', 'zip':''}
posted_data = filter_data(posted_data)
print posted_data
posted_data = prepare_query(posted_data)
print posted_data 
posted_data = get_data('consumer_data',posted_data)
print posted_data
