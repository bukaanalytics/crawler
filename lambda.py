# -*- coding: utf-8 -*-

from pymongo import MongoClient
import requests
import json
import os

client = MongoClient(os.environ['URL_MONGO'])
db = client['bukaanalytics']
collection_users = db['users']
collection_products = db['products']
collection_stats = db['stats']

user_id = 9925909

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
        #TODO add daily stat for each product
    else:
      finished = True
