from urllib import parse
import re
from kafka import KafkaConsumer, KafkaProducer
import json
import pandas as pd
from datetime import datetime, timedelta
from csv import DictWriter
import csv
import datetime

CHRLOG_TOPIC = "chrlogtest"
CODEC_TOPIC = "first-topic"

BROKERS = ["localhost:9092"]#, "localhost:8901", "localhost:8902"]

consumer = KafkaConsumer(CHRLOG_TOPIC, bootstrap_servers=BROKERS)
producer = KafkaProducer(bootstrap_servers=BROKERS)

def send_slack(msg):
    import requests
    WEBHOOK_URL = "https://hooks.slack.com/services/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"

    payload = {
        "channel": "#잡담",
        "username": "cmp",
        "text": msg
    }
    # print("payload", payload)
    requests.post(WEBHOOK_URL, json.dumps(payload))

def is_suspicious(message):
    string_original = message["uri"]

    string_decoded = parse.unquote(string_original)
    # message["uri_dec"] = string_decoded
    naverkwd = re.compile('naver')
    googlekwd = re.compile('google')
    youtubekwd = re.compile('youtube')
    # googlekwd2 = re.compile('/complete/')
    
    uri_rp=string_decoded.replace("?", "&")
    uri_spl=uri_rp.split("&")
    
    naverkw = re.compile('query=')
    naverkwo = re.compile('oquery=')
    
    googlekw = re.compile('q=')
    googlekwo = re.compile('oq=')
    googlekwp = re.compile('pq=')
    
    youtubekw = re.compile('search_query=')
    
    if naverkwd.search(string_decoded):
        Nq=[i for i in uri_spl if naverkw.search(i) and not naverkwo.search(i)]
        if Nq:
            message["search_kw"] = Nq[0].strip()[6:].replace("+", " ")
            # print('Naver', Nq)
            # print(string_decoded)
        else:
            message = False
            # pass
            
    elif googlekwd.search(string_decoded):
        Gq=[i for i in uri_spl if googlekw.search(i) and not googlekwo.search(i) and not googlekwp.search(i)]
        if Gq and not Gq[0].strip()[2:].replace("+", " ") == 'IsReq=3':
            message["search_kw"] = Gq[0].strip()[2:].replace("+", " ")
            # print('Google', Gq)
            # print(string_decoded)
        else:
            message = False
            # pass
    elif youtubekwd.search(string_decoded):
        Yq=[i for i in uri_spl if youtubekw.search(i)]
        if Yq:
            message["search_kw"] = Yq[0].strip()[13:].replace("+", " ")
            # print('Youtube', Yq)
            # print(string_decoded)
        else:
            message = False
            # pass
    else:
        NN=[i for i in uri_spl if googlekw.search(i) and not googlekwo.search(i) and not googlekwp.search(i)]
        if NN:
            print('NN', NN)
            message = False
        else:
            message = False
            # pass
    return message
##########################################################
search_df = []
pris = []
addr1s = []
rewatts = []
z = 1
for message in consumer:
    msg = json.loads(message.value.decode())
    searchkw = re.compile('search')
    if searchkw.search(msg["uri"]):
        message =is_suspicious(msg)
    else:
        continue

    if message:
        del message['uri']
        # producer.send(CODEC_TOPIC, json.dumps(message).encode("utf-8"))
        message['time'] = message['time'][0:2]+":"+message['time'][2:4]+":"+message['time'][4:6]
        search_df += [message]
        df = pd.DataFrame(search_df)
        df['time'] = pd.to_datetime(df['time'], format='%H:%M:%S', errors='raise')

        df1 = df[df['search_kw']== message['search_kw']]
        dt_datetime = datetime.datetime.strptime(message['time'],'%X')
        df2 = df1[df1['time'] >= dt_datetime - timedelta(seconds=300)]
        if len(df2)%3 == 0:
            intrkw = message['search_kw']
            
        # import sys

            csv_file = csv.reader(open('/Users/kuno/code/pipeline/Ncamp.csv', "r"), delimiter=",")
            for row in csv_file:
                if intrkw == row[1]:
                    if row[2]:
                        pris += [float(row[2])]
                    if row[5]:
                        addr1s += [row[5]]
                    if row[9]:
                        rewatts += [row[9]]
                    
                    pri = sum(pris)/len(pris)
                    addr1 = max(addr1s, key=addr1s.count)
                    rewatt = max(rewatts, key=rewatts.count)
                    z += 1
                    print(pri, addr1, rewatt)
                    break
            # if z%1==0:
            recom = []    
            for row in csv_file:
                if row[2] and int(row[2])>(pri*0.7) and int(row[2])<(pri*1.3) and row[5]==addr1 and row[9]==rewatt:
                    recom += [row[1]]
            recom = set(recom)
            print(recom)
            if recom:
                send_slack('추천캠핑장----'+str(recom)) 
    else: 
        pass
    
    

