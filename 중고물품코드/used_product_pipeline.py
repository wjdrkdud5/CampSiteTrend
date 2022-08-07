# -*- coding: utf-8 -*-
from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.utils.dates import days_ago

# mysql connection in python function
import pymysql

# Python Operator for Airflow
from airflow.operators.python import PythonOperator

# import for selenium crawling
import requests
import json
from fake_useragent import UserAgent
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from pyvirtualdisplay import Display
import pyperclip
import time
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait as wait
import pandas as pd
import threading
import re
from datetime import date, timedelta
from datetime import datetime

def _crawling(ti):
    HOST = 'localhost'
    PORT = 3306

    jungo_db = pymysql.connect(
        user = 'root',
        passwd='root',
        host = HOST,
        port = PORT,
        db = 'usedproduct',
        charset='utf8mb4'
    )
    

    user_agent = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.50 Safari/537.36'
    display = Display(visible=0,size=(1024,768))
    display.start()
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('user-agent={0}'.format(user_agent))
    driver = webdriver.Chrome('/home/ubuntu/chromedriver',options=options)

    driver.get("https://www.naver.com")

    USER_ID = 'sukozo'
    PASSWORD = 'sotjddmsfb1696'

    driver.get("https://www.naver.com")

    driver.find_element(
        By.CSS_SELECTOR,
        "#account > a"
    ).click()

    ID=driver.find_element(
        By.CSS_SELECTOR,
        "#id"
    )
    ID.click()

    ID.send_keys(USER_ID)
    time.sleep(1)

    PWD=driver.find_element(
        By.CSS_SELECTOR,
        "#pw"
    )
    PWD.click()

    PWD.send_keys(PASSWORD)
    time.sleep(1)

    driver.find_element(
        By.CSS_SELECTOR,
        "#log\.login"
    ).click()

    driver.get("https://cafe.naver.com/chocammall")

    crawling_result=[]
    def crawling_per_page():
        title_list = driver.find_elements(
            By.CSS_SELECTOR,
            '#main-area > div:nth-child(4) > table > tbody > tr > td.td_article > div.board-list > div > a'
        )

        sell_comp_list = driver.find_elements(
            By.CSS_SELECTOR,
            '#main-area > div:nth-child(4) > table > tbody > tr > td.td_article > div.board-list > div > span'
        )
        writer_list = driver.find_elements(
            By.CSS_SELECTOR,
            '#main-area > div:nth-child(4) > table > tbody > tr > td.td_name > div > table > tbody > tr > td > a'
        )
        w_date_list = driver.find_elements(
            By.CSS_SELECTOR,
            '#main-area > div:nth-child(4) > table > tbody > tr > td.td_date'
        )
        views_list = driver.find_elements(
            By.CSS_SELECTOR,
            '#main-area > div:nth-child(4) > table > tbody > tr > td.td_view'
        )

        # temporary list for deleting non proper data
        idx_del = []

        # title list
        for i,j in enumerate(title_list):
            if p.match(j.text):
                idx_del.append(i)
            
        idx_del.sort(reverse=True)
        
        for i in idx_del:
            del title_list[i]

        sell_comp_list_necessary = []
        # sell, complete list
        blank_flag=0
        for i in sell_comp_list:
            if (i.text[-2:]=='사진') and blank_flag:
                blank_flag=0
            elif (i.text[-2:]=='사진') and ~blank_flag:
                sell_comp_list_necessary.append('없음')
            
            if i.text[:2] == '판매':
                sell_comp_list_necessary.append('판매')
                blank_flag=1
            elif i.text[:3] == '예약중':
                sell_comp_list_necessary.append('예약중')
                blank_flag=1
            elif i.text[:2] == '완료':
                sell_comp_list_necessary.append('완료')
                blank_flag=1
            else:
                pass
        for i in range(len(title_list)):
            
            crawling_result.append([category ,title_list[i].text, sell_comp_list_necessary[i], writer_list[i].text, w_date_list[i].text, views_list[i].text])
    def crawling_per_category():
        driver.switch_to.frame('cafe_main')
        
        
        driver.find_element(
            By.CSS_SELECTOR,
            '#listSizeSelectDiv > a'
        ).click()
        driver.find_element(
            By.CSS_SELECTOR,
            '#listSizeSelectDiv > ul > li:nth-child(7) > a'
        ).click()
        
        pages = driver.find_elements(
            By.CSS_SELECTOR,
            '#main-area > div.prev-next > a'
        )
        if pages[len(pages)-1].text == '다음':

            crawling_per_page()
            if len(crawling_result[-1][4].split('.'))==4:
                if (date.today() - timedelta(days=0))>date.fromisoformat(crawling_result[-1][4][:-1].replace('.','-')):
                    
                    return
            for i in range(3,12):
                driver.find_element(
                    By.CSS_SELECTOR,
                    f'#main-area > div.prev-next > a:nth-child({i})'
                ).click()
                

                crawling_per_page()
                if len(crawling_result[-1][4].split('.'))==4:
                    if (date.today() - timedelta(days=0))>date.fromisoformat(crawling_result[-1][4][:-1].replace('.','-')):
                        
                        return

            driver.find_element(
                By.CSS_SELECTOR,
                '#main-area > div.prev-next > a:nth-child(12)'
            ).click()
        
        while True:
            pages = driver.find_elements(
                By.CSS_SELECTOR,
                '#main-area > div.prev-next > a'
            )
            if pages[len(pages)-1].text == '다음':
                
                crawling_per_page()
                
                if len(crawling_result[-1][4].split('.'))==4:
                    if (date.today() - timedelta(days=0))>date.fromisoformat(crawling_result[-1][4][:-1].replace('.','-')):
                        
                        return
                    
                
                for i in range(4,13):
                    driver.find_element(
                        By.CSS_SELECTOR,
                        f'#main-area > div.prev-next > a:nth-child({i})'
                    ).click()
                    
                    crawling_per_page()
                    
                    if len(crawling_result[-1][4].split('.'))==4:
                        if (date.today() - timedelta(days=0))>date.fromisoformat(crawling_result[-1][4][:-1].replace('.','-')):
                            
                            return
                
                
                if len(crawling_result[-1][4].split('.'))==4:
                    if (date.today() - timedelta(days=0))>date.fromisoformat(crawling_result[-1][4][:-1].replace('.','-')):
                        
                        return
                driver.find_element(
                    By.CSS_SELECTOR,
                    '#main-area > div.prev-next > a:nth-child(13)'
                ).click()
                
                
            else:
                
                crawling_per_page()
                if len(pages)>1:
                    for i in range(4,len(pages)+2):
                        driver.find_element(
                            By.CSS_SELECTOR,
                            f'#main-area > div.prev-next > a:nth-child({i})'
                        ).click()
                        crawling_per_page()
                        
                    break
                else:
                    break

    p = re.compile('^\[\d*\]')

    for i in range(214,215):
        category=driver.find_element(
            By.CSS_SELECTOR,
            f"#menuLink{i}"
        ).text
        print(category)
        driver.find_element(
            By.CSS_SELECTOR,
            f"#menuLink{i}"
        ).click()
        
        crawling_per_category()
        driver.switch_to.default_content()

    # add data to mysql
    cursor = jungo_db.cursor(pymysql.cursors.DictCursor)

    for i in crawling_result:
        sql = f"INSERT INTO Used_Product (category, title, sell_comp, writer, w_date, views) VALUES ('{i[0]}', '{i[1]}', '{i[2]}', '{i[3]}', '{i[4]}', '{i[5]}');"
        cursor.execute(sql)
    jungo_db.commit()
    jungo_db.close()
    



default_args = {
    'owner': 'airflow',
    "start_date": datetime(2022, 1, 1)
}


with DAG(
    dag_id='jungo_crawling',
    schedule_interval="@daily",
    default_args=default_args,
    tags=['example'],
    catchup=False) as dag:


# [START howto_operator_mysql]

    create_mysql_table = MySqlOperator(
        task_id='create_table_mysql', mysql_conn_id='mysql_conn_id',
        sql=r"""
            USE usedproduct;
            CREATE TABLE if not exists Used_Product(
                _id INT AUTO_INCREMENT,
                category VARCHAR(32) NOT NULL,
                title VARCHAR(200),
                sell_comp VARCHAR(32),
                writer VARCHAR(32),
                w_date VARCHAR(32),
                views VARCHAR(32),
                c_date VARCHAR(32),
                PRIMARY KEY(_id)
            );
        """,
        
    )

    data_crawling = PythonOperator(
        task_id="data_crawling",
        python_callable=_crawling,
        
    )


create_mysql_table >> data_crawling