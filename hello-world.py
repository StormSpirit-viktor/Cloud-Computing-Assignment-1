import requests
import json

if __name__ == '__main__':
    url = 'https://6xe9a45bt1.execute-api.us-east-1.amazonaws.com/v1/chatbot'

    response = requests.get(url=url)
    dic = json.loads(response.text)[0]
    try:
        dic0 = json.loads(dic)
    except:
        dic0 = 0
        print(type(dic))
    print(type(dic0))
    print(dic0)

