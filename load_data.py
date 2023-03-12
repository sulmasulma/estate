# -*- coding: utf-8 -*-
'''
아파트 매매 실거래가 api ETL 스크립트
create: 2023.02.23
'''

import sys, os
from sqlalchemy import create_engine
import pymysql
import logging, pickle, requests
from urllib import parse
import pandas as pd
import numpy as np
import xml.etree.ElementTree as ET
# from bs4 import BeautifulSoup
# from datetime import datetime
import time

## 시작 시간
start = time.time()

## 이 py 파일의 위치
# pc용
# curr_dir = os.getcwd()
# cron용
curr_path = os.path.realpath(__file__)
curr_dir = os.path.dirname(curr_path)

## db connection info
with open(curr_dir + '/dbinfo_estate.pickle', 'rb') as f:
    dbinfo = pickle.load(f)

## 정부 api key
with open(curr_dir + '/api_keys.pickle', 'rb') as f:
    api_keys = pickle.load(f)

## connect MySQL
try:
    conn = pymysql.connect(
        host=dbinfo['host'],
        user=dbinfo['username'],
        passwd=dbinfo['password'],
        db=dbinfo['database'],
        port=dbinfo['port'],
        use_unicode=True, charset='utf8'
    )
    cursor = conn.cursor()
except:
    logging.error("could not connect to rds")
    sys.exit(1)


endpoint = "http://openapi.molit.go.kr/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptTradeDev"
service_key = api_keys['apart']

### xml 데이터 파싱
def get_items(response, bas_ym, zip_code):
    root = ET.fromstring(response.content)
    item_list = []
    cnt = 0

    # api 요청 횟수 초과로 데이터 리턴하지 않을 때, 스크립트 종료
    if not root.find('body'):
        print('api 요청 횟수 초과. 종료')
        quit()
    
    for child in root.find('body').find('items'):
        cnt += 1
        elements = child.findall('*')
        data = {}
        data['no'] = '{}_{}'.format(bas_ym, str(cnt).zfill(4)) # 일련번호. 202208_0003 형식(pk로 사용). 근데 순번별 데이터가 변할 일이 있을까?
        # data['bas_ym'] = bas_ym
        data['zip_code'] = zip_code

        for ele in elements:
            tag = ele.tag.strip() # key. 처음에만 저장하면 되지 않나?
            text = ele.text.strip() # value
            data[tag] = text
        item_list.append(data)
    return item_list

def get_data(params):
    r = requests.get(endpoint, params=params)
    item_list = get_items(r, bas_ym=params['DEAL_YMD'], zip_code=params['LAWD_CD'])
    return item_list


## 가져온 데이터 전처리
def proc_df(data_frame):
    data = data_frame.copy()
    # 공백은 null로 바꾸기
    for col in data.columns:
        blank_cnt = data[data[col] == ''].shape[0]
        if blank_cnt > 0:
            # print(col, blank_cnt)
            data[col].replace({'': np.nan}, inplace=True)

    data['거래금액'] = data['거래금액'].str.replace(',', '').astype(int)
    data['층'] = data['층'].astype(int)
    data['전용면적'] = data['전용면적'].astype(float)
    data['해제여부'].replace({'O':'1', 'X':'0'}, inplace=True)
    data['bas_ym'] = data['no'].str[:6]
    data['bas_dt'] = data.apply(lambda x:'%s%s%s' % (x['년'],x['월'].zfill(2),x['일'].zfill(2)),axis=1)
    data.drop(columns=['년', '월', '일', 'bas_ym', '지역코드'], inplace=True)

    # 컬럼명 mysql에 맞게 바꾸기
    data.rename(columns={
        '거래금액': 'deal_amount',
        '거래유형': 'req_gbn',
        '건축년도': 'build_year',
        '도로명': 'road_name',
        '도로명건물본번호코드': 'road_name_bonbun',
        '도로명건물부번호코드': 'road_name_bubun',
        '도로명시군구코드': 'road_name_sigungu_code',
        '도로명일련번호코드': 'road_name_seq',
        '도로명지상지하코드': 'road_name_basement_code',
        '도로명코드': 'road_name_code',
        '법정동': 'dong',
        '법정동본번코드': 'bonbun',
        '법정동부번코드': 'bubun',
        '법정동시군구코드': 'sigungu_cd',
        '법정동읍면동코드': 'emd_code',
        '법정동지번코드': 'land_code',
        '아파트': 'apartment_name',
        '일련번호': 'reg_no',
        '전용면적': 'size',
        '중개사소재지': 'dealer_sigungu',
        '지번': 'jibun',
        '층': 'floor',
        '해제사유발생일': 'cancel_deal_type',
        '해제여부': 'cancel_deal_yn',
    }, inplace=True)

    return data


## 우편번호 데이터 전처리
def proc_zipdf(data_frame):
    zips = data_frame.copy()
    # 타입 변경
    zips['법정동코드'] = zips['법정동코드'].astype(str) 

    # 존재하는 지역만 남기기
    zips = zips[(zips['폐지여부'] == '존재')]

    # LAWD_CD 파라미터는 5자리 -> 시/군/구 단위로만 남기기 (277개)
    zips = zips[zips['법정동코드'].str[5:] == '00000']
    zips['법정동코드'] = zips['법정동코드'].str[:5]
    zips = zips[['법정동코드', '법정동명']]
    zips.columns = ['code', 'name']
    zips.reset_index(inplace=True, drop=True)

    # 기초단체만 api 조회됨 (시/군/구)
    zips_small = zips[zips['code'].str[2:] != '000']
    return zips_small


def main():
    ### 우편번호 목록 가져오기
    zips = pd.read_csv(curr_dir + '/zip_code.txt', sep='\t', encoding='cp949')
    zips_small = proc_zipdf(zips)

    # 한 동네(종로구)에 대해 6초
    # 6 * 261 -> 다 하면 26분 정도 걸릴 각
    # 모든 동네에 대해 202101 ~ 202212 해보기 -> 총 요청 횟수 261*24 = 6264
    # 근데 하루 트래픽 제한 1000. 24개월 기준, 하루에 40개 정도 지역만 적재 가능
    year = ['2021', '2022']

    ### api 데이터 가져오기 (zip_code 단위. 2년치씩 한번에 저장)
    # 3중 for문 말고, zip으로 해야 하나? zip(code, yy, mm). mm은 list(range)
    # cnt = 0
    for code, name in zips_small.values:
        part_start = time.time()
        # 현재 db에 해당 zip_code 데이터 있을 경우, 다음으로 넘어가기
        # 근데 요청할 때마다 이렇게 하면 오래 걸리지 않을까?
        sql = 'select distinct zip_code from apart'
        cursor.execute(sql)
        zips_db = [ele[0] for ele in cursor.fetchall()]
        if code in zips_db:
            continue

        print('{} 적재 시작'.format(name))
        estate_data = []
        # cnt += 1
        for yy in year:
            for mm in range(1, 13):
                bas_ym = yy + "%02d" % mm
                # print(bas_ym)
                params = {
                    'serviceKey': service_key,
                    'DEAL_YMD': bas_ym, # 계약월
                    'LAWD_CD': code,
                    'pageNo': '1',
                    'numOfRows': '1000', # 없으면 4행. 몇까지 되는지 보기
                }

                data_temp = get_data(params)
                estate_data.extend(data_temp)
                # print(bas_ym, data_temp.shape)

        ### 전처리
        estate_df = pd.DataFrame(estate_data)
        len_df = estate_df.shape[0]
        
        # 해당 조건 데이터 없을 경우 종료
        if len_df == 0:
            pass
        else:
            estate_df = proc_df(estate_df)

            ### mysql 데이터 insert
            # 단순 삽입만 가능한가? 필요시 pymysql로 쿼리 짜기
            db_connection_str = 'mysql+pymysql://{}:{}@{}/{}'.format(dbinfo['username'], dbinfo['password'], dbinfo['host'], dbinfo['database'])
            db_connection = create_engine(db_connection_str)
            # conn = db_connection.connect()
            estate_df.to_sql(name='apart', con=db_connection, if_exists='append',index=False) # 이 라이브러리는 이미 pk 있을 경우 데이터 replace 기능 있나? 근데 그럴 일이 있을지 모르겠음. pk도 내가 만든 거니까

        part_end = time.time()
        print('{} {}행 적재 완료. 소요 시간: {:.2f}s'.format(name, estate_df.shape[0], part_end - part_start))
        # if cnt == 5:
        #     break

    end = time.time()
    print('모든 데이터 적재 완료. 소요 시간: {:.2f}s'.format(end - start))



if __name__ == '__main__':
    main()
