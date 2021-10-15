import math
import time
import requests
import threading
import json
import pandas as pd
import datetime as dt
import marvmiloTools as mmt
from progress.bar import ShadyBar

#prepare script
requests.packages.urllib3.disable_warnings()


class Class:
    def __init__(self):
        #set values
        self.url = "https://f127dab4b2f84605b9e086d6bbf349a6.rb-elasticsearch.de.bosch.com:9243/{}pretty"
        self.auth = ("Read-mes", "Welcome@123")
        self.headers = {"Content-Type": "application/json"}
        self.ts_format = "%Y-%m-%dT%H:%M"
        self.max_threads = 10
        self.block_output = True
        self.print = mmt.ScriptPrint("Readout", block = self.block_output).print
        self.gte, self.lte, self.tz = None, None, None
        self.index_pattern = None
        self.readout_thread = None
        self.progress, self.progress_max = 0, 1
        self.readouts = None
        self.df = pd.DataFrame()

        #scroll query
        self.scroll = {
            "scroll": "1m",
            "scroll_id": None
        }

        #sarch query
        self.query = mmt.dictionary.toObj({
            "size": 10000,
            "sort": [
                {
                    "@timestamp": "asc"
                }
            ],
            "query": {
                "bool": {
                    "filter": [
                        {
                            "range": {
                                "@timestamp": {
                                    "gte": "",
                                    "lte": "",
                                    "time_zone": "+00:00"
                                }
                            }
                        }
                    ],
                    "must": [] 
                }
            },
            "_source": []
        })

    #curl GET function
    def get(self, url, payload = {}):
        payload = json.dumps(payload).encode("utf-8")
        resp = requests.get(url, headers = self.headers, auth = self.auth, data = payload, verify = False)
        return json.loads(resp.content.decode("utf-8"))

    #set timerange for query
    def set_timerange(self, gte, lte, tz = "+00:00"):
        self.gte, self.lte, self.tz = gte, lte, tz
        self.query.query.bool.filter[0].range["@timestamp"].gte = gte
        self.query.query.bool.filter[0].range["@timestamp"].lte = lte
        self.query.query.bool.filter[0].range["@timestamp"].time_zone = tz
        self.print(f"set timerange to: {self.query.query.bool.filter[0].range['@timestamp']}")

    #set keys of query
    def set_keys(self, keys):
        self.query._source = keys
        self.print(f"set keys to: {self.query._source}")

    #getting filters
    def get_filters(self):
        filters = dict()
        for filter in self.query.query.bool.must:
            key = list(filter.regexp.keys())[0]
            regex = filter.regexp[key].value
            filters[key] = regex
        return filters

    #adding filter for query
    def set_filter(self, key, regex):
        self.print(f"add filter {key}: \"{regex}\"")
        self.query.query.bool.must.append(mmt.dictionary.toObj({"regexp": {key: {"value": regex}}}))

    #delete filter in query
    def del_filter(self, key):
        self.print(f"deleting filter with key: {key}")
        index = list(self.get_filters()).index(key)
        del self.query.query.bool.must[index]

    #reseting all filters
    def reset_filters(self):
        self.print("reseting filters")
        self.query.query.bool.must = list()

    #set index pattern
    def set_index_pattern(self, index_pattern):
        self.index_pattern = index_pattern

    def progress_perc(self):
        perc = self.progress/self.progress_max*100
        if perc >= 100:
            if self.readout_thread.is_alive():
                return 99
            else:
                return 100
        return perc

    #get current results of readout
    def get_results(self):
        return self.df

    #for running readout
    def join(self):
        if self.block_output:
            while self.progress_perc() < 100:
                time.sleep(1)
        else:
            bar = ShadyBar(self.index_pattern, max = 100, suffix = "%(percent)d%%")
            while self.progress_perc() < 100:
                    bar.goto(self.progress_perc())
                    time.sleep(1)
            bar.goto(100)
            bar.finish()
        self.print(f"finished! took: {self.runtime}")

    #for getting data of timerange
    def __get_timerange__(self, begin, end):
        #prepare query
        timerange_query = self.query.copy()
        timerange_query.query.bool.filter[0].range["@timestamp"].gte = dt.datetime.strftime(begin, self.ts_format) + ":00.000"
        timerange_query.query.bool.filter[0].range["@timestamp"].lte = dt.datetime.strftime(end, self.ts_format) + ":00.000"

        #trigger readout
        resp = self.get(self.url.format(f"{self.index_pattern}/_search?scroll=1m&"), timerange_query.toDict())

        #scrolling
        while True:
            #break if response emty
            try:
                if not len(resp["hits"]["hits"]):
                    break
            except KeyError:
                break
            
            #hits frame is added to dataframes
            hits = [h["_source"] for h in resp["hits"]["hits"]]
            try:
                df_readout = pd.concat([df_readout, pd.DataFrame(hits)])
            except NameError:
                df_readout = pd.DataFrame(hits)
            
            #Scroll ID wird geupdated und neuer Readout erzeugt
            self.scroll["scroll_id"] = resp["_scroll_id"]
            resp = self.get(self.url.format("_search/scroll?"), self.scroll)
        
        #save readout
        try:
            df_readout["service"] = self.index_pattern
            self.readouts.append(df_readout)
        except UnboundLocalError:
            pass
        
    #readout function
    def readout(self):
        self.print(f"started readout from {self.index_pattern} with current query")
        self.progress = 0
        self.readouts = list()
        mmt.timer.reset()
        mmt.timer.start()

        #creating timeranges for readout threads
        timeranges = list()
        dt_gte = dt.datetime.strptime(self.gte, self.ts_format)
        dt_lte = dt.datetime.strptime(self.lte, self.ts_format)
        dif_hours = math.ceil((dt_lte - dt_gte).total_seconds()/3600)
        for hours in range(dif_hours):
            timeranges.append(dt_gte + dt.timedelta(hours = hours))
        timeranges.append(dt_lte)
        
        #creating readout threads
        readout_threads = list()
        for i, begin in enumerate(timeranges):
            try:
                end = timeranges[i+1]
                thread = threading.Thread(target = self.__get_timerange__, args = (begin, end,))
                readout_threads.append(thread)
            except IndexError:
                pass
        self.progress_max = len(readout_threads)*2
        
        #handling threads
        running_threads = list()
        while True:
            #start threads
            if len(running_threads) < self.max_threads and len(readout_threads):
                thread = readout_threads.pop(0)
                thread.start()
                running_threads.append(thread)
                self.progress += 1
            #remove finished threads
            for i, thread in enumerate(running_threads):
                if not thread.is_alive():
                    del running_threads[i]
                    self.progress += 1
            #finish handling if all threads are finished
            if not len(running_threads) and not len(readout_threads):
                break
        
        #finish readout
        try:
            self.df = pd.concat(self.readouts)
        except ValueError:
            self.progress = self.progress_max
            self.df = pd.DataFrame()
        try:
            self.runtime = mmt.timer.pause()
        except Exception:
            self.runtime = 0

    #start readout as thread
    def start(self):
        self.readout_thread = threading.Thread(target = self.readout)
        self.readout_thread.start()