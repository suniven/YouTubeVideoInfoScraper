# -*- coding: utf-8 -*-

import os
import googleapiclient.discovery
import httplib2
from configparser import ConfigParser
import pandas as pd
import json
import time
from datetime import datetime
import common.logger as logger

_logger = logger.Logger('info')


def read_config(filename):
    conf = ConfigParser()
    conf.read(filename)
    api_service_name = conf.get("youtube", "api_service_name")
    api_version = conf.get("youtube", "api_version")
    API_KEY = conf.get("youtube", "API_KEY")
    proxy_host = conf.get("proxy", "host")
    proxy_port = conf.getint("proxy", "port")

    return api_service_name, api_version, API_KEY, proxy_host, proxy_port


def build_client(api_service_name, api_version, API_KEY, proxy_host, proxy_port):
    proxy_info = httplib2.ProxyInfo(proxy_type=httplib2.socks.PROXY_TYPE_HTTP, proxy_host=proxy_host, proxy_port=proxy_port)
    http = httplib2.Http(timeout=10, proxy_info=proxy_info, disable_ssl_certificate_validation=False)
    youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=API_KEY, http=http)
    return youtube


def get_video_info(youtube, videoIdList, pageToken):
    # print("video {0} || PageToken {1}".format(videoId, pageToken))
    id_string = ','.join(videoIdList)
    request = youtube.videos().list(part="snippet, status, statistics", id=id_string, pageToken=pageToken)
    try:
        response = request.execute()
        return response
    except Exception as error:
        _logger.error(error)
        if "quotaExceeded" in str(error):
            print("quotaExceeded")
            return 'quotaExceeded'
        if "timed out" in str(error):
            print("timed out")
            return 'timed out'
        return None


def process_response(response):
    try:
        nextPageToken = response["nextPageToken"]
    except Exception as error:
        nextPageToken = None

    result = []
    for index, item in enumerate(response["items"]):
        video_id = item["snippet"]["videoId"]
        result.append({'video_id': video_id})
        print("{0}: video {1}".format(index, video_id))
    return nextPageToken, result


def main():
    api_service_name, api_version, API_KEY, proxy_host, proxy_port = read_config('config0.ini')
    youtube = build_client(api_service_name, api_version, API_KEY, proxy_host, proxy_port)

    df_id = pd.read_csv('0.csv', engine='python')
    videoIdList = df_id.video_id.to_list()
    step = 50  # 50个一组
    videoIdLists = [videoIdList[i:i + step] for i in range(0, len(videoIdList), step)]

    try:
        video_info = []
        for index, videoIdList in enumerate(videoIdLists):
            nextPageToken = None
            while True:
                # time.sleep(0.5)
                response = get_video_info(youtube, videoIdList, nextPageToken)
                if response == 'quotaExceeded':
                    if video_info:
                        filename = "./json_files/all_video_info_" + str(datetime.timestamp(datetime.now())) + ".json"
                        with open(filename, 'w', encoding='utf-8') as f:
                            json.dump(video_info, f, indent=4)
                        video_info = []
                    exit(0)
                if response == 'timed out':
                    print("retry...")
                    continue
                if not response:
                    break
                nextPageToken, result = process_response(response)
                video_info += result
                if not nextPageToken:  # 这一组的信息抓完了
                    if index % 199 == 0:
                        if video_info:
                            filename = "./json_files/all_video_info_" + str(datetime.timestamp(datetime.now())) + ".json"
                            with open(filename, 'w', encoding='utf-8') as f:
                                json.dump(video_info, f, indent=4)
                            video_info = []
                    break
    except Exception as error:
        _logger.error(error)
    finally:
        if video_info:
            filename = "./json_files/all_video_info_" + str(datetime.timestamp(datetime.now())) + ".json"
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(video_info, f, indent=4)


if __name__ == "__main__":
    main()
