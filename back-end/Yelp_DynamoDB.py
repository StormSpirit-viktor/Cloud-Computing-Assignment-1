import json
import time
from datetime import datetime
import decimal
import requests
import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Attr, Key

# with reference to sample code:
# https://github.com/Yelp/yelp-fusion/blob/master/fusion/python/sample.py
# DynamoDB
class DynamoDB:
    def __init__(self):
        # Create a dynamodb_resource resource
        self.dynamodb_resource = boto3.resource(
                    'dynamodb',
                    region_name = 'us-east-1',
                    aws_access_key_id = '**********************',
                    aws_secret_access_key = '**********************************')

        self.table_name = 'yelp-restaurants'
        self.table = self.__get_table()
        self.YelpAPI = {
            'API_HOST': 'https://api.yelp.com',
             'SEARCH_PATH':  '/v3/businesses/search',
            'CLIENT_ID' : "****************************************",
            'MY_API_KEY': "****************************************"
        }
        self.headers = {
                'Authorization': 'Bearer %s' % self.YelpAPI['MY_API_KEY'],
            }
        self.url = self.YelpAPI['API_HOST'] + self.YelpAPI['SEARCH_PATH']
        self.cuisine_list = ["brunch", "salad", "Japanese", "Thai", "pizza"]

    def __get_table(self):
        '''
        Get the DynamoDB table called "yelp-restaurants"
        If table does not exist, then create a table
        :return: table
        '''
        tables_list = [table.name for table in self.dynamodb_resource.tables.all()]
        if self.table_name not in tables_list:
            table = self.dynamodb_resource.create_table(
                TableName=self.table_name,
                KeySchema=[
                    {
                        'AttributeName': 'Business_ID',
                        'KeyType': 'HASH'
                    }
                ],
                AttributeDefinitions=[
                    {
                        'AttributeName': 'Business_ID',
                        'AttributeType': 'S'
                    }
                ],
                ProvisionedThroughput={
                    'ReadCapacityUnits': 10,
                    'WriteCapacityUnits': 10
                }
            )
            time.sleep(20)  # wait until table being ready
        else:
            table = self.dynamodb_resource.Table(self.table_name)
        return table

    def insert_to_es(self, id, cuisine):
        """
        insert results from Yelp api to elastic search
        :param id: business_id
        :param cuisine: cuisine
        :return: null
        """
        es_url = 'https://search-assign1-nnv3cq5yx5gkhmk5pubv6mdtqq.us-east-1.es.amazonaws.com/restaurants/Restaurant'
        data = dict()
        data['business_id'] = id
        data['cuisine'] = cuisine
        data = json.dumps(data)
        headers = dict()
        headers['content-type'] = 'application/json'
        res = requests.post(url=es_url, data=data, headers=headers)
        # print(res.json())

    def search_by_cuisine(self, cuisine, location='Manhattan', SEARCH_LIMIT = 1, MIN_REQUIRED=1000):
        '''
        Given a cuisine and location
        request YelpAPI to fetch the results and store in the DynamoDB table
        until the number of restaurants of the given cuisine type meeting the minimum number requirement

        :param cuisine: cuisine to be searched by YelpAPI
        :param location: location to be searched by YelpAPI, default to be Manhattan as required
        :param SEARCH_LIMIT: number of results return by YelpAPI, default to be 1
        :param MIN_REQUIRED: minimum number requirement for the given cuisine type
        :return: boolean True if meeting the minimum number requirement, else False
        '''
        offset = 0
        found = self.get_cuisine_number(cuisine)
        while found < MIN_REQUIRED:
            url_params = {
                'term': cuisine.replace(' ', '+'),
                'location': location.replace(' ', '+'),
                'offset': offset,
                'limit': SEARCH_LIMIT
            }

            response = requests.request('GET', self.url, headers=self.headers, params=url_params).json()
            if 'businesses' not in response:
                print(response)
                print('not statisfy min requriement')
                return False

            for restaurant in response['businesses']:
                offset += 1
                is_add = self.insert_item_to_table(restaurant, cuisine)
                if is_add:  found = self.get_cuisine_number(cuisine)  # update found restaurant
        return True

    def get_cuisine_number(self, cuisine):
        '''
        Given a cuisine type, find the number of matching restaurants already stored in the table
        :param cuisine: cuisine type
        :return: the number of restaurants
        '''
        if self.table.item_count == 0:
            return 0
        response = self.table.scan(
            FilterExpression=Attr("Cuisine").contains(cuisine),
            Select='COUNT'
        )
        return response['Count']

    def insert_item_to_table(self, restaurant, cuisine):
        '''
        Given a restaurant dict item, insert it into the DynamoDB table
        :param restaurant: restaurant dict with required information
        :param cuisine: cuisine type
        :return: boolean True if the restaurant item has been successfully added into the table as a new item,
        else False
        '''
        try_get_item = self.table.get_item(Key={'Business_ID': restaurant['id']})
        # If the item is already in the table
        if 'Item' in try_get_item:
            cuisine_list = try_get_item['Item']['Cuisine']
            if cuisine not in cuisine_list:
                cuisine_list.append(cuisine)
                self.table.update_item(
                    Key={'Business_ID': restaurant['id']},
                    AttributeUpdates={
                        'Cuisine': {
                            'Value': cuisine_list,
                            'Action': 'PUT'
                        }
                    })
            return False

       # Create a new item if the restaurant is still open
        if not restaurant['is_closed']:
            restaurant_item = {
                'Business_ID': restaurant['id'],
                'Cuisine': [cuisine],
                # 'Location': location,
                'Name': restaurant['name'],
                'Address': "".join(restaurant['location']['display_address']),
                'Coordinates': {
                    'latitude': decimal.Decimal(str(restaurant['coordinates']['latitude'])),
                    'longitude': decimal.Decimal(str(restaurant['coordinates']['longitude']))
                },
                'Rating': decimal.Decimal(str(restaurant['rating'])),
                'Number_of_review': restaurant['review_count'],
                'Zip_Code': restaurant['location']['zip_code'],
                'insertedAtTimestamp': datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')
            }

            try:
                self.table.put_item(
                    TableName=self.table_name,
                    Item=restaurant_item
                )
                self.insert_to_es(restaurant['id'], cuisine)
                return True
            except:
                print("Put item failed")
                return False

    def search_by_cuisine_list(self, cuisine_list = None):
        '''
        Given a list with different cuisine types
        search for every type to get the matching restaurants
        :param cuisine_list: list of cuisine types
        '''
        if not cuisine_list:
            cuisine_list = self.cuisine_list
        for cuisine in cuisine_list:
            print("Now searching for " + cuisine + " ...")
            self.search_by_cuisine(cuisine)
            print("Searching for " + cuisine + " Finished!")

Yelp_DB = DynamoDB()
Yelp_DB.search_by_cuisine_list(['brunch'])



