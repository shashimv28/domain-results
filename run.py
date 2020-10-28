#%%
import pandas as pd
import os, glob
import time
import json
import requests
from datetime import datetime as dt
import urllib.request
import urllib
import random
from bs4 import BeautifulSoup as bs
from pathlib import Path
import uuid

#%%
# extract
cfg = json.loads(open("config.json").read())
root_url = cfg.get("config").get("root_url")
mode = cfg.get("config").get("mode")
agent = cfg.get("config").get("agent")
delay = cfg.get("config").get("delay")
remove = cfg.get("misc").get("remove")
removekeys = cfg.get("misc").get("remove_keys")
addquotes = cfg.get("misc").get("add_quotes")

#%%
# transform
suburb = cfg.get("addresses").get("suburb")
suburb_str = suburb.replace(" ","-")
state = cfg.get("addresses").get("state")
postcode = cfg.get("addresses").get("postcode")
search_str = "{}-{}-{}".format(suburb_str,state,postcode)

#%%
# get htmldoc using virtual session
def gethtmldoc(url,user_agent_list):
    #Pick a random user agent
    user_agent = random.choice(user_agent_list)
    #Set the headers 
    headers = {'User-Agent': user_agent}
    #Make the request
    request = urllib.request.Request(url,headers=headers)
    response = urllib.request.urlopen(request)
    html = response.read()
    #resp = requests.get(requrl)
    htmldoc = bs(html,'lxml')
    return htmldoc

# get htmldoc using system proxy
def gethtmldocauto(url,user_agent_list):
    user_agent = random.choice(user_agent_list)
    r = requests.get(url, proxies=urllib.request.getproxies(),headers={'User-Agent': user_agent})
    rdata = str(r.content)
    htmldoc = bs(rdata,'lxml')
    return htmldoc

def get_total_listsings(req_url):
    time.sleep(random.choice(delay))
    htmldoc = gethtmldoc(req_url,agent)
    listings = []
    for div in htmldoc.find_all("div"):
        hdr = div.find("h1")
        if str(hdr).lower() != "none":
            hdr_text = hdr.strong.get_text().strip().split()[0]
            listings.append(hdr_text)

    total_listings = int(list(set(listings))[0])

    listings = []
    for li in htmldoc.find_all("li",recursive=True):
        for attr in li.attrs:
            li_txt = str(li[attr])
            if li_txt.find("listing")>=0 and str(li["class"][0]).find("css")>=0:
                listings.append(li_txt)

    results_per_page = len(listings)

    return [total_listings,results_per_page]

def remove_key(json_str, removekeys):
    for key in removekeys:
        key_str = '"{}":'.format(key)
        key_str_indx_start=json_str.index(key_str,0)
        key_str_indx_end=json_str.index('",',key_str_indx_start)
        json_str_new = "{}{}".format(json_str[0:key_str_indx_start],json_str[key_str_indx_start+1,key_str_indx_end])
    return json_str_new

def remove_strings(json_str,remove):
    for rm in remove:
        json_str = json_str.replace(rm,"")
    
    return json_str

def add_quotes(json_str,values):
    for val in values:
        json_str = json_str.replace(":{}".format(val),':"{}"'.format(val))

    json_str = json_str.replace("'",'"')
    return json_str



def get_page_data(htmldoc,remove,removekeys,addquotes,ltype):
    jdata = []
    for scrp in htmldoc.find_all("script"):
        scrp_dat = str(scrp).strip()
        if ltype == "page":
            for attr in scrp.attrs:
                if attr == "type":
                    if str(scrp[attr]).find("json")>=0:
                        try:
                            scrp_dat = str(scrp)
                            scrp_dat = remove_strings(scrp_dat,remove)
                            scrp_dat = scrp_dat.replace('""[','[')
                            jdata.append(json.loads(scrp_dat))
                        except:
                            fpn="{}.json".format(str(uuid.uuid4()))
                            fp=open("{}".format(fpn),'w+')
                            fp.write(scrp_dat)
                        pass
                else:
                    pass
        elif ltype == "listing":
            if len(scrp.attrs) == 0:
                if scrp_dat.find("APP_PROPS")>=0:
                    scrp_dat = remove_strings(scrp_dat,remove)
                    try:
                        jdata.append(json.loads(scrp_dat))
                    except:
                        fpn="{}.json".format(str(uuid.uuid4()))
                        fp=open("{}".format(fpn),'w+')
                        fp.write(scrp_dat)
    
    return jdata[0]

# %%
req_url = "{}{}/{}/?page={}".format(root_url,mode,search_str,"1")
total_listings = get_total_listsings(req_url)[0]
results_per_page = get_total_listsings(req_url)[1]
results_per_page_usr = 20

# %%
cntr=1
rows=[]
extraction_time = dt.strftime(dt.now(),'%Y-%m-%d %H:%M:%S')
pages = round(total_listings/results_per_page_usr)
while(cntr <= pages):
    suburb = cfg.get("addresses").get("suburb")
    suburb_str = suburb.replace(" ","-")
    state = cfg.get("addresses").get("state")
    postcode = cfg.get("addresses").get("postcode")
    search_str = "{}-{}-{}".format(suburb_str,state,postcode)
    req_url = "{}{}/{}/?page={}".format(root_url,mode,search_str,str(cntr))
    time.sleep(random.choice(delay))
    htmldoc = gethtmldoc(req_url,agent)
    jdata = get_page_data(htmldoc,remove,removekeys,addquotes,"page")
    row=[extraction_time,cntr,req_url,jdata]
    rows.append(row)
    cntr=cntr+1

# %%
ddf = pd.DataFrame(rows,columns=["extraction_time","page_num","page_url","page_data"])
# %%
ddf.to_json("domain.json",orient='records')

# %%
rows=[]
try:
    for i, r in ddf.iterrows():
        jdata = r['page_data']
        extraction_time = r['extraction_time']
        page_num = r["page_num"]
        page_url = r["page_url"]
        for rec in jdata:
            url = rec.get("url")
            time.sleep(random.choice(delay))
            htmldoc = gethtmldoc(url,agent)
            jdatal = get_page_data(htmldoc,remove,removekeys,addquotes,"listing")
            rows.append({"page_data" : jdata,
            "listing_data" : jdatal})
except:
    print(rec)

#%%
rdf = pd.DataFrame(rows)
rdf.to_json("domain_results.json",orient='records')

# %%
