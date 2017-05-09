# -*- coding: utf-8 -*-

from pymongo import MongoClient
from datetime import datetime, timedelta
import json
import os
import pytz
import requests

client = MongoClient(os.environ['URL_MONGO'])
db = client['bukaanalytics']
collection_users = db['users']
collection_products = db['products']
collection_stats = db['stats']

user_id = 9925909

def get_yesterday_entry(product_id, date_string):
  return collection_stats.find_one({'product_id' : product_id, 'date' : date_string})

# TODO insert new user after each new login from app
# r = requests.get('https://api.bukalapak.com/v2/users/%d/profile.json' % user_id)
# raw_data = r.json()
# data = {}
# data['seller_id'] = '%d' % user_id
# data['name'] = raw_data['user']['name']

# collection.find_one_and_replace(
#   {'seller_id' : "%d" % user_id},
#   data,
#   upsert=True
# )

user_cursor = collection_users.find({}, {'_id':0, 'seller_id':1, 'name':1})
for user in user_cursor:
  page_number = 1
  finished = False
  while (not finished):
    product_request = requests.get('https://api.bukalapak.com/v2/users/%d/products.json?page=%d' % (user_id, page_number))
    raw_data = product_request.json()
    if(len(raw_data['products']) > 0):
      page_number += 1
      for product in raw_data['products']:
        #add products to db
        data_product = {}
        data_product['product_id'] = product['id']
        data_product['name'] = product['name']
        data_product['price'] = product['price']
        data_product['seller_id'] = product['seller_id']
        collection_products.find_one_and_replace(
          {'product_id' : product['id']},
          data_product,
          upsert=True
        )
        #add daily stat for each product
        datetime_jakarta = datetime.now(pytz.timezone('Asia/Jakarta'))
        datetime_jakarta_yesterday = datetime_jakarta - timedelta(days=1)
        entry_yesterday = get_yesterday_entry(product['id'], datetime_jakarta_yesterday.strftime("%Y-%m-%d"))
        data_stat = {}
        data_stat['date'] = datetime_jakarta.strftime("%Y-%m-%d")
        data_stat['day_name'] = datetime_jakarta.strftime("%A")
        data_stat['product_id'] = product['id']
        data_stat['view_total'] = product['view_count']
        data_stat['view_count'] = (data_stat['view_total'] - entry_yesterday['view_total']) if (entry_yesterday != None) else 0
        data_stat['sold_total'] = product['sold_count']
        data_stat['sold_count'] = (data_stat['sold_total'] - entry_yesterday['sold_total']) if (entry_yesterday != None) else 0
        data_stat['interest_total'] = product['interest_count']
        data_stat['interest_count'] = (data_stat['interest_total'] - entry_yesterday['interest_total']) if (entry_yesterday != None) else 0 
        collection_stats.insert_one(data_stat)
    else:
      finished = True