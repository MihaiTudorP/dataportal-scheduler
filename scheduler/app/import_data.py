#!/usr/bin/python
import requests
import sys
import json
import datetime
import re

usage = '''
            Usage: import_data.py <FEED_URL> <TOKEN> <DATASET_URI> <CKAN_URL>
        '''

def exit(code, message=usage):
    print(message)
    sys.exit(code)

def retrieve_feed(feed_url):
    response = requests.get(feed_url)
    records = json.loads(response.content)
    if response.status_code==200:
        print("Feed retrieved successfully")
    else:
        print("Error retrieving the feed")
    return records

def setup(token, ckan_url, feed_url):
    ckan_params = config_ckan(token, ckan_url, dataset_uri);
    package_id = create_dataset(feed_url, ckan_params)
    create_initial_upload(ckan_params, feed_url, package_id)

def create_initial_upload(ckan_params, feed_url, package_id):
    records = retrieve_feed(feed_url)
    fields = generate_field_definition(records)
    data = {
        'resource': {
            'package_id': package_id,
            'name': retrieve_file_name(feed_url),
            'format': 'csv',
        },
        'records': records,
        'fields': fields
    }

    response = requests.post('{0}/api/action/datastore_create'.format(ckan_params["ckan_url"]),
                             data=json.dumps(data),
                             headers={'Content-type': 'application/json',
                                      'Authorization': ckan_params["token"]}, )

    if response.status_code != 200:
        raise Exception('Error: {0}'.format(response.content))

    resource_id = response.json()['result']['resource_id']

    print(
    '''
    The Dataset and DataStore resources were successfully created with {0} records.
    resource_id={1}
              '''.format(len(records), resource_id))
    return resource_id

def create_dataset(feed_url, ckan_params):
    print(create_dataset_name(feed_url))
    data = {
        "name": create_dataset_name(feed_url),
        "Title": retrieve_file_name(feed_url),
        "owner_org": "code4romania",
        "notes": ''' Generated automatically '''
    }
    return call_api_create_dataset(ckan_params, data)

def generate_field_definition(records):
    keys = records[0].keys()
    fields = []
    for key in keys:
        data_type = "text"
        if is_integer(records[0][key]):
            data_type = "integer"
        elif is_float(records[0][key]):
            data_type = "float"
        fields.append({'id':key, 'type':data_type})
    return fields

def is_float(x):
    try:
        float(x)
    except:
        return False
    return True

def is_integer(x):
    try:
        int(x)
    except:
        return False
    return True

def call_api_create_dataset(ckan_params, data):
    response = requests.post('{0}/api/action/package_create'.format(ckan_params["ckan_url"]),
                             data=json.dumps(data),
                             headers={'Content-type': 'application/json',
                                      'Authorization': ckan_params["token"]},)

    if response.status_code != 200:
        raise Exception('Error creating dataset: {0}'.format(response.content))

    return response.json()['result']['id']

def create_dataset_name(feed_url):
    domain_name = retrieve_domain_name(feed_url)
    file_name = retrieve_file_name(feed_url)
    return change_non_alpha_or_minus_to_minus(domain_name + "-" + file_name).lower()

def change_non_alpha_or_minus_to_minus(str):
    return re.sub('[^0-9a-zA-Z]+', '-', str)

def retrieve_file_name(feed_url):
    return feed_url.rstrip("/").rstrip(" ").split("/")[-1].split(".")[0]


def retrieve_domain_name(feed_url):
    return feed_url.lstrip("http://").split("/")[0]


def update(token, ckan_url, dataset_uri, feed_url):
    ckan_params = config_ckan(token, ckan_url, feed_url)
    pass

def config_ckan(token, ckan_url):
    ckan_url = ckan_url.rstrip("/")
    ckan_params = {"token": token, "ckan_url": ckan_url}
    print(ckan_params)
    return ckan_params

def config_ckan(token, ckan_url, dataset_uri):
    ckan_url = ckan_url.rstrip("/")
    ckan_params = {"token": token, "ckan_url": ckan_url, "dataset_uri": dataset_uri}
    print(ckan_params)
    return ckan_params

if __name__ == '__main__':
    if len(sys.argv) < 5:
        exit(1)

    feed_url = sys.argv[1]
    token = sys.argv[2]
    dataset_uri = sys.argv[3]
    ckan_url = sys.argv[4]

    print("Importing data..." +
          "\nFeed URL: " + feed_url +
          "\nToken: " + token +
          "\nDataset URI: " + dataset_uri +
          "\nCKAN url: " + ckan_url)
    try:
        setup(token, ckan_url, feed_url)
    except Exception as error:
        if (repr(error).__contains__("That URL is already in use.")):
            print("update")

