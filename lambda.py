# -*- coding: utf-8 -*-

from pymongo import MongoClient
from datetime import datetime, timedelta
import json
import os
import pytz
import requests
import time
import urllib

client = MongoClient(os.environ['URL_MONGO'])
db = client['bukaanalytics']
collection_users = db['users']
collection_products = db['products']
collection_stats = db['stats']

def get_stat_entry(product_id, date_string):
  return collection_stats.find_one({'product_id' : product_id, 'date' : date_string})

def update_product_list(product):
  #upsert products to db
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

def update_product_stat(product):
  #add daily stat for each product
  datetime_jakarta = datetime.now(pytz.timezone('Asia/Jakarta'))
  datetime_jakarta_yesterday = datetime_jakarta - timedelta(days=1)
  entry_yesterday = get_stat_entry(product['id'], datetime_jakarta_yesterday.strftime("%Y-%m-%d"))

  data_stat = {}
  data_stat['date'] = datetime_jakarta.strftime("%Y-%m-%d")
  data_stat['date_epoch'] = int(time.time())
  data_stat['day_name'] = datetime_jakarta.strftime("%A")
  data_stat['product_id'] = product['id']
  data_stat['view_total'] = product['view_count']
  data_stat['view_count'] = (data_stat['view_total'] - entry_yesterday['view_total']) if (entry_yesterday != None) else 0
  data_stat['sold_total'] = product['sold_count']
  data_stat['sold_count'] = (data_stat['sold_total'] - entry_yesterday['sold_total']) if (entry_yesterday != None) else 0
  data_stat['interest_total'] = product['interest_count']
  data_stat['interest_count'] = (data_stat['interest_total'] - entry_yesterday['interest_total']) if (entry_yesterday != None) else 0 

  avg_market_stat = get_average_market_stat(product['name'])
  data_stat['market_view_total'] = avg_market_stat['avg_view']
  data_stat['market_view_count'] = (data_stat['market_view_total'] - entry_yesterday['market_view_total']) if (entry_yesterday != None) else 0
  data_stat['market_sold_total'] = avg_market_stat['avg_sold']
  data_stat['market_sold_count'] = (data_stat['market_sold_total'] - entry_yesterday['market_sold_total']) if (entry_yesterday != None) else 0
  data_stat['market_interest_total'] = avg_market_stat['avg_interest']
  data_stat['market_interest_count'] = (data_stat['market_interest_total'] - entry_yesterday['market_interest_total']) if (entry_yesterday != None) else 0

  collection_stats.insert_one(data_stat)

def get_average_market_stat(product_name):
  #get 50 first results
  search_count = 50;
  search_request = requests.get('https://api.bukalapak.com/v2/products.json?keywords=%s&per_page=%d' % (urllib.quote_plus(product_name), search_count))
  raw_search = search_request.json()
  raw_search_count = len(raw_search['products'])

  avg_market_stat = {}
  avg_market_stat['avg_view'] = 0
  avg_market_stat['avg_sold'] = 0
  avg_market_stat['avg_interest'] = 0

  if(raw_search_count > 0):
    avg_view = 0;
    avg_sold = 0;
    avg_interest = 0;
    for product in raw_search['products']:
      avg_view += product['view_count']
      avg_sold += product['sold_count']
      avg_interest += product['interest_count']
    #currently using integer division, will change variables to floating numbers if necessary
    avg_view = avg_view / raw_search_count
    avg_sold = avg_sold / raw_search_count
    avg_interest = avg_interest / raw_search_count
    avg_market_stat['avg_view'] = avg_view
    avg_market_stat['avg_sold'] = avg_sold
    avg_market_stat['avg_interest'] = avg_interest
  return avg_market_stat

def handler(event, context):
  #get all from 'users' collection
  user_cursor = collection_users.find({}, {'_id':0, 'seller_id':1, 'name':1})
  for user in user_cursor:
    page_number = 1
    finished = False
    while (not finished):
      #iterate over all product pages from a user
      product_request = requests.get('https://api.bukalapak.com/v2/users/%d/products.json?page=%d' % (user['seller_id'], page_number))
      raw_data = product_request.json()
      if(len(raw_data['products']) > 0):
        page_number += 1
        for product in raw_data['products']:
          update_product_list(product)
          update_product_stat(product)
      else:
        finished = True