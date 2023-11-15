import pandas as pd
import numpy as np
import requests
import time
import json
import sys
import os
from tqdm import tqdm

sys.path.append(os.getcwd())

from kbutil.osutil import make_file_path

# URL 요청
def request_url(data_list):    
    # URL 예시
    # url = 'https://api.conceptnet.io/c/en/great_white_shark?offset=0&limit=20'

    basic_url = 'https://api.conceptnet.io/c/en/'

    ko_keyword = data_list['keyword_ko']
    replace_keyword = data_list['replace_keyword_ko']
    en_keyword = data_list['keyword_en'].lower()
    en_keyword = en_keyword.replace(' ', '_')

    url_parameter = 'offset=0&limit=20'
    url = basic_url + en_keyword + '?' + url_parameter
    
    check_next = True
    collect_yn = 'Y'

    full_edges_data = []

    if en_keyword == '':
        collect_yn = 'N'
        check_next = False

    while check_next:
        time.sleep(1.93)

        r_data = requests.get(url)
        r_text = r_data.text

        json_data = json.loads(r_text)
    
        edges_data = json_data['edges']

        for one_edge in edges_data:
            full_edges_data.append(one_edge)
        
        view_data = dict()
        next_page = ''

        try:
            # 데이터가 있는 경우
            view_data = json_data['view']
        except:
            # 데이터가 없는 경우
            collect_yn = 'N'
            break;
    
        try:
            # 다음 페이지 접속 후 데이터 수집
            next_page = view_data['nextPage']            
            url_parameter = next_page.split('?', 1)[1]
            url = basic_url + en_keyword + '?' + url_parameter
        except:
            # 다음 페이지 없음 (마지막 페이지)
            check_next = False
            break; 
     
    file_name = ko_keyword + '!!!' + replace_keyword + '!!!' + en_keyword
    save_path = data_list['file_path'] + '/' + file_name + '.json'

    with open(save_path, "w", encoding="utf8") as file:
        file.write(json.dumps(full_edges_data, ensure_ascii=False, default=None, indent='\t'))        

    return collect_yn

if __name__ == '__main__':

    print("=== 대체 키워드가 있는 경우 Concepnet DATA 수집 코드 (START) >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    
    keyword_file = './data_info/replace_search_keyword_1123.xlsx'

    df_info = pd.read_excel(keyword_file, engine='openpyxl')
    df_info.fillna('', inplace=True)

    df_info = df_info[['big_category', 'small_category', 'keyword_ko', 'replace_keyword_ko', 'keyword_en']]
    np_big_category = df_info['big_category'].unique()

    big_category_list = []
    
    for idx, val_big in enumerate(np_big_category):
        folder_name = val_big
        big_category_list.append(folder_name)
    
    # 저장 경로 생성
    df_info['file_path'] = df_info.apply(lambda x: make_file_path(x, big_category_list, 'conceptnet_re'), axis = 1)
    
    # 객체 기준 Concepnet URL 요청 및 파일 저장
    tqdm.pandas()
    df_info['is_collect_yn'] = df_info.progress_apply(lambda x: request_url(x), axis=1)

    df_info.to_excel('./data_info/concepnet_re_collect_result_1123.xlsx')

    print('--- 대체 키워드가 있는 경우 Concepnet DATA 수집 코드 (SUCCES) ------------------------')
    print('------------------------------------------------------------------------------------')

