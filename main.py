import time
import json
import names
import random
from datetime import datetime
from google.cloud import pubsub_v1
from concurrent.futures import TimeoutError
import ast
import apache_beam as beam
import apache_beam.io.gcp.bigquery
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.io.gcp.internal.clients import bigquery

# pub/sub values
PROJECT_ID = "york-cdf-start"
TOPIC = "sh_test_topic"
SUBSCRIPTION_ID = "projects/york-cdf-start/subscriptions/sh_test_topic-sub" # my subscription to my topic
options = beam.options.pipeline_options.PipelineOptions()
standard_options = options.view_as(beam.options.pipeline_options.StandardOptions)
standard_options.streaming = True

#called in main to create a dictionary from messages in topic subscription
class Dictionary(beam.DoFn):
    def process(self, element):
#        print(element)
        json_string = element.decode() # initialize variable/ apply 'decode' method to reformat bytestring element to json string
#        print(json_string)

        dictionary = json.loads(json_string) #initialize variable/ apply 'loads' method to reformat json to dictionary
#        print(dictionary)
        yield dictionary #yields the objects to the pcollection, (names don't matter)

class SplitDictionary(beam.DoFn): #called in main
    def process(self, element):

        output = {} #initialize dictionary to hold data after processed

        # address transformation, split string into list to access and continue transformations
        address_list = element["order_address"].split(", ")
        # check for list length after split, 3 = regular address, <3 = skip address
        if len(address_list) == 3:
            #split list items to access required parts and store in a variable
            order_building_number = int(address_list[0].split(" ", 1)[0])
            order_street_name = address_list[0].split(" ", 1)[1]
            order_city = address_list[1]
            order_state_code = address_list[2].split(" ")[0]
            order_zip_code = int(address_list[2].split(" ")[1])

            addr = {}  #will hold address items in a dictionary
            #populate the addr dictionary with the required data in variables
            addr["order_building_number"] = order_building_number
            addr["order_street_name"] = order_street_name
            addr["order_city"] = order_city
            addr["order_state_code"] = order_state_code
            addr["order_zip_code"] = order_zip_code
            output["order_address"] = addr #add addr dictonary to output dictionary
        else:
            pass #skips over split string if resulting list != 3 items

        output["order_id"] = element["order_id"] # populates the dictionary with key:values
        output["customer_first_name"] = element["customer_name"].split(' ')[0] # split customer_name returns a list, choose the index
        output["customer_last_name"] = element["customer_name"].split(' ')[1]  # split customer_name returns a list, choose the index
        output["customer_ip"] = element["customer_ip"] # populates the dictionary with key:values
        output["order_currency"] = element["order_currency"] #use key to accces value, store it in th output dictionary

        item_price = [] # initiate list to hold each price
        for order_item in element['order_items']: #use index key to get to index value in the dictionary
            item_price.append(order_item['price']) #append price value to list
        shipping = element['cost_shipping'] #
        tax = element['cost_tax']

        # calcuate a cost_total field from the cost_shipping, cost_tax and each price field in the order_items list
        cost_total = (round(shipping + tax + sum(item_price), 2)) #variable holds value of shipping + tax plus sum() applied to list
        output["cost_total"] = cost_total #add field and value into dictionary
        #print(cost_total)
        print(output)
        yield output #stores the processed values/objects back into the "output" variable objects, Pcollection

#class CurrencySplit(beam.DoFn): #called in main
#    def process(self, element):

if __name__ == '__main__':
    PROJECT_ID = "york-cdf-start"

    TABLE_SCHEMA = {
        'fields': [
            {'name': 'order_id', 'type': 'INTEGER', 'mode': 'NULLABlE'},
            {'name': 'order_address', 'type': 'RECORD', 'mode': 'REPEATED', 'fields': [
                {'name': 'order_building_number', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                {'name': 'order_street_name', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'order_city', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'order_state_code', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'order_zip_code', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            ]},
            {'name': 'customer_first_name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'customer_last_name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'customer_ip', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'cost_total', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        ]
    }

    #to apply a write transformation you need: destination table name/dest.table create disposition/dest.table write disposition
    #BigQueryDisposition.CREATE_IF_NEEDED

    table_spec = bigquery.TableReference(
        PROJECT_ID="york-cdf-start",
        datasetId="SH_dataflow_project",
        tableID="test_table"
    )

    #Beam driver program must create a Pipeline instance and configure some options to pass to the Pipeline object
    with beam.Pipeline(options=options) as p:
        #use PubSub in beam pipeline to access topic subscription messages. sub_ID already set to subscription path
        pcoll = p | 'ReadFromPub' >> beam.io.ReadFromPubSub(subscription=SUBSCRIPTION_ID)
        #variable initialized to call class and then hold new value of class output "yield"
        dictionary = pcoll | 'pcoll into dictionary' >> beam.ParDo(Dictionary())
        split_data = dictionary | 'dict into split fields' >> beam.ParDo(SplitDictionary())
 #       currency_split = split_data | 'split_data gets sorted by currency' >> beam.ParDo(CurrencySplit())