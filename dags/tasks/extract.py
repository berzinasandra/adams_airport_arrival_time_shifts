#!/usr/bin/python
# -*- coding: utf-8 -*-
import requests
import sys
import optparse

def extract_data_from_api():
    url = 'https://api.schiphol.nl/public-flights/flights'

    headers = {
      'accept': 'application/json',
	  'resourceversion': 'v4',
      'app_id': '6628c1a3',#'options.app_id',
	  'app_key': '8bb8007f390d5da534f8592c2a48cf1b' #'options.app_key'
	}
    import pdb;pdb.set_trace()
    
    try:
        response = requests.request('GET', url, headers=headers)
    except requests.exceptions.ConnectionError as error:
        print(error)

    if response.status_code == 200:
        flightList = response.json()
        print('found {} flights.'.format(len(flightList['flights'])))
        for flight in flightList['flights']:
            print('Found flight with name: {} scheduled on: {} at {}'.format(
                flight['flightName'],
                flight['scheduleDate'],
                flight['scheduleTime']))
    else:
        print(f"Oops something went wrong, Http response code: {response.status_code} - {response.text}")
