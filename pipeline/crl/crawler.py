from turtle import clear
import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
from tqdm import tqdm

import kw

def campsite(camp):
    res = requests.get('https://search.naver.com/search.naver?where=nexearch&sm=top_hty&fbm=1&ie=utf8&query=' + camp)
    html = res.content.decode('utf-8','replace')
    return html

def cmp_dt(name_list): 
  result_list1 = [['div','name', 'price', 'virev', 'blgrev', 'addr1', 'addr2', 'attr', 'rating', 'review_name', 'review_cnt']]
  result_list2 = [['div','name', 'price', 'virev', 'blgrev', 'addr1', 'addr2', 'attr', 'rating', 'review_name', 'review_cnt']]
  nan=[]
  for name in tqdm(name_list):
    soup = BeautifulSoup(campsite(name), 'html.parser')
    try:
      dv = soup.select("span._3ocDE")[0].text
    except:
      dv = ""
    try:      
      cmpsite = soup.select("span._3XamX")[0].text
    except:
      cmpsite = ""#name
    try:
      review_text = soup.select("div._2B33i > span._10uv0")
    except:
      review_text = ""
    try:
      virev = soup.select("div._20Ivz > span._1Y6hi > a > em")[0].text
    except:
      virev= ""
    try:
      blgrev = soup.select("div._20Ivz > span._1Y6hi > a > em")[1].text
    except:
      blgrev = ""    
    try:
      if "지도" in soup.select("ul._6aUG7 > li._1M_Iz > div")[1].text:
        addr1, addr2 = soup.select("ul._6aUG7 > li._1M_Iz > div")[1].text.split(" ")[0:2]
      else:
        addr1, addr2 = soup.select("ul._6aUG7 > li._1M_Iz > div")[0].text.split(" ")[0:2]
    except:
      try:
        addr1, addr2 = soup.select("ul._6aUG7 > li._1M_Iz > div")[0].text.split(" ")[0:2]
      except:  
        addr1, addr2 = "", ""
    try:
      attr = soup.select("ul._6aUG7 > li._1M_Iz > div")[-1].text
      if attr == "제로페이":
        attr = soup.select("ul._6aUG7 > li._1M_Iz > div")[-2].text
      http = re.compile('http')
      map = re.compile('.*지도')
      if http.search(attr):
        attr = ""
      if map.match(attr):
        addr1, addr2  = attr.split(" ")[0:2]
        attr = ""
    except:
      attr = ""
    review_text_list = []
    try:
      for i in range(0,len(review_text)):
          review_text = soup.select("div._2B33i > span._10uv0")[i].text
          review_text_list.append(review_text.replace("\"", ""))
    except:
      review_text_list = []
    try:  
      review_num = soup.select("div._2B33i > span._3IFxe")
    except:
      review_num = ""
    review_num_list = []
    try:
      for i in range(0,len(review_num)):
          review_num = soup.select("div._2B33i > span._3IFxe")[i].text[13:]
          review_num_list.append(review_num)
    except:
      review_num_list = ""
    try:
      rate_num = soup.select("div._20Ivz > span._1Y6hi._1A8_M > em")[0].text
    except:
      rate_num = ""
    try:
      price_num = soup.select("div._2QlZz > span.FSeNw")
    except:
      price_num = ""
    pri_li = []
    try:
      for i in range(0,len(price_num)):
        price_num = soup.select("div._2QlZz > span.FSeNw")[i].text
        if "~" in price_num:
          [x,y] = price_num.split("~")
          prib1, prib2 = re.sub(r'[^0-9]', '', x), re.sub(r'[^0-9]', '', y)
          pri_li += [int(prib1), int(prib2)]
        else:
          pria = re.sub(r'[^0-9]', '', price_num)
          pri_li += [int(pria)]
    except:
      pass
    if pri_li:
      price = int(sum(pri_li)/len(pri_li))
    else:
      price = ""
    if not review_text_list:
      review_text_list=""
    if not review_num_list:
      review_num_list=""
    try:
      for i in range(0, len(review_text_list)):
        result_list2.append([dv, cmpsite, price, virev, blgrev, addr1, addr2, attr, rate_num, review_text_list[i], review_num_list[i]])
      if len(review_text_list) == 0:
        if [dv, cmpsite, price, virev, blgrev, addr1, addr2, attr, rate_num, review_text_list, review_num_list]!=['', '', '', '', '', '', '', '', '', '', '']:
          result_list2.append([dv, cmpsite, price, virev, blgrev, addr1, addr2, attr, rate_num, review_text_list, review_num_list])
    except:
      pass
    if [dv, cmpsite, price, virev, blgrev, addr1, addr2, attr, rate_num, review_text_list, review_num_list]!=['', '', '', '', '', '', '', '', '', '', '']:
      result_list1.append([dv, cmpsite, price, virev, blgrev, addr1, addr2, attr, rate_num, review_text_list, review_num_list])
    else:  
      nan += [name]
#   cmp_df1 = pd.DataFrame(result_list1)
#   cmp_df2 = pd.DataFrame(result_list2)
#   return  cmp_df1, cmp_df2, nan
  return result_list1, result_list2, nan
  
result_list1, result_list2, nan = cmp_dt(kw.cmp)

import csv

f1 = open( 'N_cmp_info1.csv','w', encoding = 'utf-8-sig', newline='')
wr = csv.writer(f1)
for result in result_list1:
    wr.writerow(result)
f1.close()

f2 = open( 'N_cmp_info2.csv','w', encoding = 'utf-8-sig', newline='')
wr = csv.writer(f2)
for result in result_list2:
    wr.writerow(result)
f2.close()