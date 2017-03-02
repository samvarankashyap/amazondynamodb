# name : samvaran kashyap 
import time
import os
import bottle
# importing csv package to read the csv files using csv file reader
import csv
# importing python debugger for debugging the python program
import pdb
# importing the boto package to interact with aws resources
import boto
# importing key resource for createing Key while uploading the data to s3 bucket (filename)
# importing urllib2 to download the files from the external resoources 
import urllib2
# importing memcache for the interacting with memcache end point of amazon
import memcache
# import random to generate random words 
import random
import json
# importing MySQLdb client to interact with the RDS endpoint of amazon 
import MySQLdb as mdb
import hashlib
import boto.dynamodb
from bottle import route, run, template
from bottle import request, response, HTTPResponse
from boto.dynamodb2.table import Table
from boto.dynamodb2.fields import HashKey, RangeKey, GlobalAllIndex
from boto.dynamodb2.table import Table
from boto.dynamodb2.types import NUMBER
from  boto.dynamodb.condition import *
from boto.dynamodb.batch import BatchWriteList
from boto.dynamodb.batch import *
from boto.dynamodb.item import Item
from boto.s3.key import Key
#import the set dataset
from sets import Set
# importing hashlib for the key generation for the key digest for memcache
from boto.dynamodb.schema import Schema

# declaring end point from where the data file that needs to be downloaded
#URL="https://data.cityofnewyork.us/api/views/9fg9-grkj/rows.csv?accessType=DOWNLOAD"
URL="https://data.consumerfinance.gov/api/views/x94z-ydhh/rows.csv?accessType=DOWNLOAD"
# declaring db endpoint for db connection
FILE_NAME = "data.csv"
#declaring database name 
DB_NAME ="test"
#declaring bucket name
BUCKET_NAME ="cloudbucketskr"
# declaring list of memcached servers 
# declaring client to interact with memcached `
# declating db connection object that persists throughout the program
conn = boto.dynamodb.connect_to_region('us-west-2')
dynamodb_conn = boto.connect_dynamodb()
table_name = 'consumer_table'
dynamodb_table = dynamodb_conn.get_table(table_name)


@route('/')
def main():
    # initialising database 
    # starting time to initalise database
    #s_time = time.time()
    # initialising the database
    #table = initialise_db()
    # getting endtime to initialise the database 
    #e_time = time.time()
    # calculating the total initailising time
    #init_time = e_time - s_time
    #inserting data into database
    #calculating the starttime
    #start_time = time.time()
    # inserting into database
    #insert_into_db(table)
    # calculating end time
    #end_time = time.time()
    #i_time = end_time - start_time
    #print init_time
    #print i_time
    #return the final string
    return template('webinterface.tpl')


@bottle.route('/querybuilder',  method='POST')
def query_builder():
    #pdb.set_trace()
    # function creates output of the queries based on the posted parameters
    if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
        posted_dict =  request.forms.dict
        filtered_data = filter_data(posted_dict)
        query = prepare_query(filtered_data)
        data = get_data(query)
        data = json.dumps(data)
        resp = HTTPResponse(body=data,status=200)
        return resp
    else:
        return 'This is a normal request'


def get_data(query):
    table = conn.get_table(table_name)
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
        if posted_data[x][0]!='':
            data[x] = posted_data[x][0]
    return data


def initialise_db():
    # executing the queries with cache
    """This function is to responsible for the initialisation the the database and tables """
    try:
        consumer_table_schema = conn.create_schema(
        hash_key_name='c_id',
        hash_key_proto_value=str,
        )
        table = conn.create_table(
        name='consumer_table',
        schema=consumer_table_schema,
        read_units=1,
        write_units=1
        )
        return table
    except Exception as e:
        print str(e)


def insert_into_db(table):
    # connecting to the database with independent connection
    table = conn.get_table('consumer_table')
    f = open(FILE_NAME, 'rt')
    fields = ('c_id','product','subproduct','issue','subissue','state','zip','via','date_r','data_s','company','company_r','timely_r','consumer_d')
    # initialising the counter variable
    c=0
    try:
        # creating the csv reader to read the file
        #reader = csv.reader(f)
        reader = csv.DictReader(f, fields)
        list_25 = []
        # looping through each row of the file
        for row in reader:
            # printing the row
            #print row
            if row['c_id']=='Complaint ID':
                continue
            c+=1
            clean_row = clean_data(row)
            item = Item(table, row['c_id'],'',clean_row) 
            list_25.append(item)
            if len(list_25) == 25:
                 the_batch = conn.new_batch_write_list()
                 the_batch.add_batch(table, puts= list_25)
                 conn.batch_write_item(the_batch)
                 list_25 = []
            if c == 50001:
                break
            print "Record no::"+str(c)+"\n"
    finally:
        # closing the file
        f.close()


def clean_data(data):
    for x in data:
        if data[x]=='':
            data[x]="None"
    return data


def generate_random_query(gt200):
    # function responsible for generating a random query where it takes gt 200 as input and  where if gt200 is True it gives output with greater than 200
    # sample query in sql syntax select * from table where <parameter>
    # initialising the sample query
    init_q ="select"
    # sample create table statement for the consumer table
    #CREATE TABLE IF NOT EXISTS CONSUMER ( id int NOT NULL AUTO_INCREMENT, c_id INTEGER ,product VARCHAR(100),subproduct VARCHAR(100),issue VARCHAR(1000),subissue VARCHAR(1000) , state VARCHAR(100) ,zipcode INTEGER ,submittedvia VARCHAR(50), data_r VARCHAR(50) , data_s VARCHAR(50) ,  company VARCHAR(100) , company_resp VARCHAR(100) , timely_r VARCHAR(50) , disputed VARCHAR(10) , PRIMARY KEY (id))"
    # initialising the fields for the database query to be generated
    fields = ["id", "c_id","product","subproduct","issue","subissue","state","zipcode","submittedvia","data_r","data_s","company","company_resp","timely_r","disputed"]
    # selecting random projections from 0 to 15 , ie., selecting number of fields to be selected in the existing query
    projections = random.randint(0,15)
    #initailising a set for the unique fields to be selected  
    fields_to_taken = Set([])
    # looping through the projections to select unique fiels
    for i in range(0,projections+1):
        #print i,":",projections,"\n"
        # adding the projections to the set
        fields_to_taken.add(" "+random.choice(fields))
    # creating the query from the fields that are randomly chosen 
    for x in fields_to_taken:
        #appending to the field with comma
        init_q += x +","
    #pdb.set_trace()
    # striping the trailing comma
    init_q = init_q.strip(",")
    # generating random number two numbers to limit the records
    min_num = random.randint(1,250)
    #max_num = random.randint(25000,50000)
    
    # checking whther the gt200 flag is true or not 
    if gt200 == True:
        # if true generate random number between 200 and 800 tuples 
        num = random.randint(200,800)
        init_q += " from CONSUMER where id < "+str(num)
    else:
        init_q =  init_q+" from CONSUMER where id < "+str(min_num)
    #retuning the genrated random query by limiting it based on id of th table
    return init_q


# function is responsible for generating small queries whose output is limited
def generate_queries(query_count):
    # initialising set with inital query which is selecting all records from the consumer table
    e = Set(['select * from CONSUMER'])
    # looping through the large number where and adding the genrated query to the existing set
    for i in range(1, 999999):
        #calling the function generate random query with True flag and adding it to the set
        e.add(generate_random_query(False))
        # priniting the len of the set initialised
        print len(e)
        # breaking out of the loop if the query_count that needs to be generated matches the length of the query set
        if query_count == len(e):
            break
    #returning the query set
    return e


# function to generate random queries which have output greater than 200 less than 800
def generate_queries_gt200(query_count):
    # initialising set with inital query which is selecting all records from the consumer table
    e = Set(['select * from CONSUMER'])
    # looping through the large number where and adding the genrated query to the existing set
    for i in range(1, 999999):
        #calling the function generate random query with True flag and adding it to the set
        e.add(generate_random_query(True))
        # priniting the len of the set initialised
        print len(e)
        # breaking out of the loop if the query_count that needs to be generated matches the length of the query set
        if query_count == len(e):
            break
    # returning the query set
    return e


if __name__ == '__main__':
    run(host='0.0.0.0', port=8080, debug=True)
