#!/usr/bin/env python

import sys
import time
import json
import csv
from pprint import pprint

from boto3 import resource
from boto3.dynamodb.conditions import Key

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from pyvirtualdisplay import Display # required for firefox on Ubuntu

class Site:
    URL = 'https://bwt.cbp.gov/index.html?com=1&pas=1&ped=1&plist=0708,3004,0901,0115,0712,0209,3800,0212,0106,3604,0104,3023,0109,0704,0211,0701,3401,3802,3803,3009,3310,2502,5355,2503,2406,2302,2601,2303,2402,2404,l245,2305,2304,2602,2603,2604,2506,2403,2309,2307,2310,2608,2504,2408,2505'
    ELEM_IDS = ['resultsMexican', 'resultsCanadian']
    PORT = 0
    COMMERCIAL = 3
    PASSENGER = 6
    PEDESTRIAN = 10

def scrape(url, elem_ids):
    try:
        if 'darwin' not in sys.platform: # Ubuntu, requires `apt-get install xvfb`
            display = Display(visible=0, size=(800, 600))
            display.start()
            # context manager example:
            # with Display(visible=0, size=(800, 600)) as display:
        # driver = webdriver.PhantomJS()
        # driver = webdriver.Chrome()
        driver = webdriver.Firefox()
        driver.get(Site.URL)
        for elem in elem_ids:
            element = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, elem))
            )
        src = driver.page_source
        if 'darwin' not in sys.platform:
            display.stop()
    except Exception as e:
        if 'darwin' not in sys.platform:
            display.popen.terminate()
        return None
    return src


def scrape_border_wait_times():
    data = []
    timestamp = int(time.time() * 1000000)
    src = scrape(Site.URL, Site.ELEM_IDS)
    if src is None:
        return None, None
    soup = BeautifulSoup(src, 'html.parser')
    resultsCanadian = soup.find(id='resultsCanadian')
    resultsMexican = soup.find(id='resultsMexican')
    for table in (resultsCanadian, resultsMexican):
        for row in table.tbody.find_all('tr'):
            port_entry = {}
            cells = row.find_all('td')
            if len(cells) == 1:
                continue
            for i, cell in enumerate(cells):
                if i == Site.PORT:
                    crossing = cell.find_all('b')[-1].i
                    if crossing:
                        port_entry['crossing'] = crossing.text
                        crossing.extract()
                    else:
                        port_entry['crossing'] = None
                    port_entry['port'] = cell.find('b').text
                elif i == Site.COMMERCIAL:
                    port_entry['commercial'] = get_delays(cell)
                elif i == Site.PASSENGER:
                    port_entry['passenger'] = get_delays(cell)
                elif i == Site.PEDESTRIAN:
                    port_entry['pedestrian'] = get_delays(cell)
                else:
                    continue
            data.append(port_entry)
    return data, timestamp

def get_delays(cell):
    crossing_status = {'current_time': None, 'delay': None, 'lane_info': None}
    if cell.text == 'N/A':
        return crossing_status
    elif cell.text in ('Lanes Closed', 'Update Pending'):
        crossing_status['lane_info'] = cell.text.lower()
        return crossing_status
    elif cell.span:
        crossing_status['delay'] = cell.span.text
    else:
        return crossing_status
    crossing_status['current_time'], _, _, _, crossing_status['lane_info'] = list(cell.children)
    crossing_status['current_time'] = crossing_status['current_time'].lstrip('At ')
    if '1 lanes' in crossing_status['lane_info']:
        crossing_status['lane_info'] = crossing_status['lane_info'].replace('1 lanes', '1 lane')
    crossing_status['lane_info'] = crossing_status['lane_info'].lower()
    crossing_status['delay'] = crossing_status['delay'].replace('min', 'minute')
    return crossing_status

def log_wait_times(wait_times, timestamp):
    db = resource('dynamodb')
    table = db.Table('border_wait_times')
    data = {'scraped_at': timestamp, 'wait_times': json.dumps(wait_times)}
    table.put_item(Item=data)

def update_latest_wait_times(wait_times, timestamp):
    db = resource('dynamodb')
    table = db.Table('latest_border_wait_times')
    table.update_item(Key={'id': 'latest'}, UpdateExpression="SET wait_times = :new_wait_times, scraped_at = :scraped_at", ExpressionAttributeValues={':new_wait_times': wait_times, ':scraped_at': timestamp})

def json2csv(data, csv_filepath):
    headers = ('port', 'crossing', 'comm_delay', 'comm_time', 'comm_lane', 'pass_delay', 'pass_time', 'pass_lane', 'ped_delay', 'ped_time', 'ped_lane')

    with open(csv_filepath, 'w') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for d in data:
            writer.writerow((d['port'],
                             d['crossing'],
                             d['commercial']['delay'],
                             d['commercial']['current_time'],
                             d['commercial']['lane_info'],
                             d['passenger']['delay'],
                             d['passenger']['current_time'],
                             d['passenger']['lane_info'],
                             d['pedestrian']['delay'],
                             d['pedestrian']['current_time'],
                             d['pedestrian']['lane_info']))

if __name__ == '__main__':
    wait_times, timestamp = scrape_border_wait_times()
    if (wait_times is None) and (timestamp is None):
        sys.exit(1)
    # print(wait_times)
    # print(timestamp)
    update_latest_wait_times(json.dumps(wait_times), timestamp)
    log_wait_times(wait_times, timestamp)
    # now = round(time.time())
    # CSV_FILE = f'./wait_times{now}.csv'
    # JSON_FILE = f'./wait_times{now}.json'
    # with open(JSON_FILE, 'w') as f:
    #     # f.write(json.dumps(wait_times))
    #     pprint(wait_times, stream=f)
    # json2csv(wait_times, CSV_FILE)
