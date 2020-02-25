# -*- coding: utf-8 -*-
# @Time    : 2020/2/16 16:39
# @Author  : Yunjie Cao
# @FileName: lambdaEs.py
# @Software: PyCharm
# @Email   ：YunjieCao@hotmail.com

import json
import urllib3
import boto3
import re
import requests
from botocore.exceptions import ClientError
from requests_aws4auth import AWS4Auth
import random

def lambda_handler(event, context):
    
    # information need: cuisine, phone number
    region = 'us-east-1' # e.g. us-west-1
    service = 'es'
    credentials = boto3.Session().get_credentials()
    awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)
    
    response = requests.get('https://6xe9a45bt1.execute-api.us-east-1.amazonaws.com/v1/chatbot')
    dics = json.loads(response.text)
    
    l = len(dics)
    
    more_info_list = []
    query_list = []
    phone_list = []
    for dic_ in dics:
        dic = json.loads(dic_)
        
        query = dic['cuisine']
        phone = '+1' + dic['phone']
        es_url = 'https://search-assign1-nnv3cq5yx5gkhmk5pubv6mdtqq.us-east-1.es.amazonaws.com/restaurants/Restaurant'
        url = es_url + '/_search?q=' + query
        res = requests.get(url, auth=awsauth).json()
        
        candidates = set()
        for restaurant in res['hits']['hits']:
            candidates.add(restaurant['_source']['business_id'])
        candidates = list(candidates)
        random.shuffle(candidates)
        
        """
        candidates are business_ids, next, 
        interact with dynamodb to get full information of restaurants
        """
        dynamodb = boto3.client('dynamodb',
                                  aws_access_key_id='****',
                                  aws_secret_access_key='****',
                                  region_name="us-east-1")
        more_info = dynamodb.get_item(TableName='yelp-restaurants', Key={'Business_ID': {'S': candidates[0]}})
        more_info = more_info.get('Item', None)
        
        if more_info is not None:
            more_info_list.append(more_info)
            query_list.append(query)
            phone_list.append(phone)
            #send_to_user(more_info, credentials, query, phone)
    
    send_to_user(more_info_list, credentials, query_list, phone_list)        
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!'),
        'sqs num' : l,
    }


def send_to_user(restaurant_infos, credentials, cuisines, phones):
    
    client = boto3.client(
        "sns",
        aws_access_key_id="*****",
        aws_secret_access_key="******",
        region_name="us-east-1"
    )
    
    for i, phone in enumerate(phones):
        try:
            response = client.publish(
                PhoneNumber=phone,
                Message=format_message(cuisines[i], restaurant_infos[i])
            )
        except ClientError as e:
            print(e.response['Error']['Message'])
        
    
    
def format_message(cuisine, restaurant_info):
    message = 'Hello. Do you want to have {}? This is your recommendation! The restaurant name is {}'.format(cuisine, restaurant_info['Name']['S'])
    message += '. The address is {}'.format(restaurant_info['Address']['S'])
    message += ". This restaurant has {} reviews and the average rating is {}".format(restaurant_info['Number_of_review']['N'], restaurant_info['Rating']['N'])
    message += ". Accurate location latitude is {} longitude is {}. Zip code is {}. Enjoy your meal!".format(restaurant_info["Coordinates"]["M"]["latitude"]['N'], restaurant_info["Coordinates"]["M"]["longitude"]['N'], restaurant_info["Zip_Code"]['S'])
    return message
