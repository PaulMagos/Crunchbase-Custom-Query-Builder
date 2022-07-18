import http.client
import itertools
import json
import random

import requests

import functions
import time

conn = http.client.HTTPSConnection("www.crunchbase.com")

def new_header():
    return {
    'authority': "www.crunchbase.com",
    'accept': "application/json, text/plain, */*",
    'accept-language': "it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7",
    'content-type': "application/json",
    'cookie': "cid=CigoYGKE8k+sLgAxC2XQAg==; _pxvid=1be8412d-d6ad-11ec-948d-48736e755566; _hp2_props.973801186=%7B%22Logged%20In%22%3Afalse%2C%22Pro%22%3Afalse%2C%22cbPro%22%3Afalse%7D; _fbp=fb.1.1652879956920.1194875475; _ga=GA1.2.2082270193.1652879957; drift_aid=7f5a3163-e086-4bbc-9195-a6834e3436cf; driftt_aid=7f5a3163-e086-4bbc-9195-a6834e3436cf; __cflb=02DiuJLCopmWEhtqNz4kXQy9t2cDTGoJVdanEzjCRogqS; pxcts=f1571516-d823-11ec-ac56-616350626374; xsrf_token=vQp8mk+pV3FkX+/k/uzlrrY+NdaUMl9q4k0DSB9Uqok; _gid=GA1.2.1776945115.1653040980; _hp2_ses_props.973801186=%7B%22r%22%3A%22https%3A%2F%2Fwww.crunchbase.com%2F%22%2C%22ts%22%3A1653045446424%2C%22d%22%3A%22www.crunchbase.com%22%2C%22h%22%3A%22%2F%22%7D; _pxff_ne=1; _pxff_bsco=1; _gat_UA-60854465-1=1; OptanonConsent=isIABGlobal=false&datestamp=Fri+May+20+2022+15%3A24%3A48+GMT%2B0200+(Ora+legale+dell%E2%80%99Europa+centrale)&version=6.23.0&hosts=&consentId=a7171933-7a6f-4103-af1a-fb0f0f73ef44&interactionCount=1&landingPath=NotLandingPage&groups=C0001%3A1%2CBG8%3A1%2CC0002%3A1%2CC0004%3A1&AwaitingReconsent=false&geolocation=IT%3B52; OptanonAlertBoxClosed=2022-05-20T13:24:48.564Z; _hp2_id.973801186=%7B%22userId%22%3A%224610315488021485%22%2C%22pageviewId%22%3A%22509505772076813%22%2C%22sessionId%22%3A%225482063956028567%22%2C%22identity%22%3Anull%2C%22trackerVersion%22%3A%224.0%22%7D; _pxhd=YK2r2s7MYAuJUog4BZ4w9z3rcjzdTSDHjx7zaFe4EHVKIyTjmfUemIfZv1nrq-YXc8bge8Wscrf40DhgTjxMoQ",
    'origin': "https://www.crunchbase.com",
    'sec-ch-ua': "\" Not A;Brand\";v=\"99\", \"Chromium\";v=\"101\", \"Google Chrome\";v=\"101\"",
    'sec-ch-ua-platform': "\"Windows\"",
    'user-agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.67 Safari/537.36",
    'x-requested-with': "XMLHttpRequest",
    'x-xsrf-token': "vQp8mk+pV3FkX+/k/uzlrrY+NdaUMl9q4k0DSB9Uqok"
    }


def main():
    functions.queries_lite_create2()
    dataset = functions.read_json("SavedCountries_lite2")
    ed_country = ""
    work_country = ""
    year = ""
    gender = ""
    query = ""
    queries = ""
    while ed_country != "NULL" and work_country != "NULL" and year != "NULL" and gender != "NULL" and query != "NULL" and queries != "NULL":
        ed_country, work_country, year, gender, query, queries = functions.get_query_lite(queries, dataset)
        print("Connecting for query : " + ed_country + " " + work_country + " " + year + " " + gender)
        retry = True
        data = ''
        while retry:
            try:
                conn.request("POST", "/v4/data/searches/people?source=custom_query_builder", query, new_header())
                res = conn.getresponse()
                data = json.loads(res.read())["count"]
                retry = False
                print("Ok.. Next")
            except:
                print("Error connecting, make captcha")
                time.sleep(180)
                continue

        if ed_country not in dataset:
            dataset[ed_country] = {}
        if work_country not in dataset[ed_country]:
            dataset[ed_country][work_country] = {}
        if year not in dataset[ed_country][work_country]:
            dataset[ed_country][work_country][year] = {}
        dataset[ed_country][work_country][year].update({gender: data})
        functions.write_json(dataset, "SavedCountries_lite2.json")
        functions.write_json(queries, "queries_lite2.json")
        time.sleep(1)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
