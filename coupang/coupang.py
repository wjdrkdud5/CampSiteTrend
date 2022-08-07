from webdriver_manager.chrome import ChromeDriverManager
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import NoSuchElementException
import pandas as pd
import time

i = 1

product_list = []

while True:

    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    UserAgent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36'
    options.add_argument('user-agent=' + UserAgent)

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    driver.get(url='https://www.coupang.com/np/categories/328924?listSize=60&brand=&offerCondition=&filterType=&isPriceRange=false&minPrice=&maxPrice=&page=' + str(i) + '&channel=user&fromComponent=Y&selectedPlpKeepFilter=&sorter=saleCountDesc&filter=&component=328707&rating=0')
    time.sleep(5)
    try:
        product = driver.find_element(By.ID, 'productList')
        lis = product.find_elements(By.CLASS_NAME, 'baby-product')
        print('*' * 50 + ' ' + str(i) + 'Page Start!' + ' ' + '*' * 50)

        for li in lis:
            try:

                # 상품명
                name = li.find_element(By.CLASS_NAME, 'name').text
                # 가격
                price = li.find_element(By.CLASS_NAME, 'price-value').text
                # 별점
                rating = li.find_element(By.CLASS_NAME, 'rating').text
                # 리뷰수
                review = li.find_element(By.CLASS_NAME, 'rating-total-count').text

                print('Name:' + name)
                print('Price:' + price)
                print('Rating:' + rating)
                print('Review:' + review)

                product_info = {
                    'name': name,
                    'price': price,
                    'rating': rating,
                    'review': review
                }
                product_list.append(product_info)

            except Exception:
                pass


        print(len(product_list))
        print('*' * 50 + ' ' + str(i) + 'Page End!' + ' ' + '*' * 50)
        time.sleep(5)
        i += 1
        driver.quit()

    except NoSuchElementException:
        df = pd.DataFrame(product_list)
        print(df)
        df.to_csv('coupang_product_list_test.csv', encoding='utf-8-sig')
        exit(0)

