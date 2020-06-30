"""
Created on Mon Jun 24 20:01:56 2020

@author: ji758507
"""
import urllib.request
import gzip
import xml.etree.ElementTree as ET
import pandas as pd
import datetime
from dateutil.relativedelta import relativedelta
import time
from lxml import etree as etree_lxml
from io import StringIO, BytesIO
import requests


def dataDownloader(downloadStartTime, downloadEndTime):
    # get the end download time
    # downloadEndTime = currentTime.replace(minute=0)

    # get the datetime range
    timeRange = pd.date_range(start=downloadStartTime, end=downloadEndTime, freq='T').tolist()  # Generate time series

    global_year = downloadStartTime.year
    global_month = downloadStartTime.month
    global_day = downloadStartTime.day
    data = []
    for iterate_time in timeRange:
        start = time.time()
        # do some stuff
        print(iterate_time)
        year = str(iterate_time)[0:4]
        month = str(iterate_time)[5:7]
        day = str(iterate_time)[8:10]
        hours = str(iterate_time)[11:13]
        minutes = str(iterate_time)[14:16]
        print(year, month, day, hours, minutes)

        if int(year) > global_year or int(month) > global_month or int(day) > global_day:
            data = pd.DataFrame(data)
            # convert to two digit string
            str_month = "{0:0=2d}".format(global_month)
            str_day = "{0:0=2d}".format(global_day)

            data.to_csv('../../../data/yuanjinghui/VDOT_TSS_Data_{}_{}_{}.csv'.format(global_year, str_month, str_day))
            data = []

        else:
            print('Keep writing VDOT_TSS_Data_{}{}{}.csv'.format(year, month, day))

        try:
            # read xml.gz files
            if iterate_time < datetime.datetime.strptime('2017-12-13 08:23:00', '%Y-%m-%d %H:%M:%S'):
                # start = time.time()

                # testfile = urllib.request.URLopener()
                # testfile.retrieve(
                #     "https://smarterroads.org/dataset/download/1?token=18cPAKB4l5QucRdOHyOMi0EwI9ZRwWHuyaxhEXPpMvNzMtWIBooGp1GuzzqNsWI7&file=hub_data/TSSData/{}/{}/{}/{}/TSSData_{}{}{}_{}{}.xml.gz".
                #         format(year, month, day, hours, year, month, day, hours, minutes),
                #     "file.gz")
                # input = gzip.open('file.gz', 'r')
                # # tree = ET.parse(input)
                # # root = tree.getroot()
                #
                # tree = etree_lxml.parse(input)
                # root = tree.getroot()
                #
                file = urllib.request.urlopen("https://smarterroads.org/dataset/download/1?token=18cPAKB4l5QucRdOHyOMi0EwI9ZRwWHuyaxhEXPpMvNzMtWIBooGp1GuzzqNsWI7&file=hub_data/TSSData/{}/{}/{}/{}/TSSData_{}{}{}_{}{}.xml.gz".
                        format(year, month, day, hours, year, month, day, hours, minutes))
                input = gzip.open(file, 'r')
                # tree = ET.parse(input)
                # root = tree.getroot()
                tree = etree_lxml.parse(input)
                root = tree.getroot()

                # stop = time.time()
                # print(stop - start)

            # read xml files
            else:
                # start = time.time()
                # testfile = urllib.request.URLopener()
                # testfile.retrieve(
                #     "https://smarterroads.org/dataset/download/1?token=18cPAKB4l5QucRdOHyOMi0EwI9ZRwWHuyaxhEXPpMvNzMtWIBooGp1GuzzqNsWI7&file=hub_data/TSSData/{}/{}/{}/{}/TSSData_{}{}{}_{}{}.xml".
                #         format(year, month, day, hours, year, month, day, hours, minutes),
                #     "file")
                # tree = ET.parse("file")
                # root = tree.getroot()

                file = urllib.request.urlopen("https://smarterroads.org/dataset/download/1?token=18cPAKB4l5QucRdOHyOMi0EwI9ZRwWHuyaxhEXPpMvNzMtWIBooGp1GuzzqNsWI7&file=hub_data/TSSData/{}/{}/{}/{}/TSSData_{}{}{}_{}{}.xml".
                        format(year, month, day, hours, year, month, day, hours, minutes))
                tree = etree_lxml.parse(file)
                root = tree.getroot()
                # file = requests.get("https://smarterroads.org/dataset/download/1?token=18cPAKB4l5QucRdOHyOMi0EwI9ZRwWHuyaxhEXPpMvNzMtWIBooGp1GuzzqNsWI7&file=hub_data/TSSData/{}/{}/{}/{}/TSSData_{}{}{}_{}{}.xml".
                #                     format(year, month, day, hours, year, month, day, hours, minutes))
                # root = ET.fromstring(file.content)

                # stop = time.time()
                # print(stop - start)

                # tree = ET.parse("file")
                # root = tree.getroot()
                # tree = etree_lxml.parse("file")
                # root = tree.getroot()

        except:
            print('file read error')
            # data = pd.DataFrame(data)
            # return data
            continue

        # parse the xml file and write to the dataframe
        for detectorList in root:
            # print(detectorList.attrib)
            for detector in detectorList:
                feature = {}
                # print(detector)
                # print(detector.attrib)
                # print(detector.attrib['{http://www.opengis.net/gml}id'])
                # feature['id'] = detector.attrib['{http://www.opengis.net/gml}id']
                for features in detector:
                    varName = features.tag
                    varName = varName.split("}")[-1]
                    # print(varName)
                    # print(features.text)
                    # add coordinates information
                    # if varName == 'the_geom':
                    #     for i in features:
                    #         for j in i:
                    #             # var1 = j.tag
                    #             # var1 = var1.split("}")[-1]
                    #             # print(var1)
                    #             # print(j.text)
                    #             feature['long'] = j.text.split(" ")[0]
                    #             feature['lat'] = j.text.split(" ")[1]

                    # print(features[''])
                    # for temp in features:
                    # print(features.attrib['{http://www.openroadsconsulting.com}detectorid'])
                    feature[varName] = features.text
                    # print(features)
                data.append(feature)
        global_year = int(year)
        global_month = int(month)
        global_day = int(day)

        stop = time.time()
        print(stop - start)

    data = pd.DataFrame(data)
    return data


# iterate_time = datetime.datetime.strptime('2017-12-13 07:22:00', '%Y-%m-%d %H:%M:%S')


if __name__ == '__main__':

    currentTime = datetime.datetime.now().replace(microsecond=0)

    # get the start download time
    # downloadStartTime = currentTime - datetime.timedelta(hours=1)
    # downloadStartTime = currentTime - relativedelta(years=2)
    # downloadStartTime = downloadStartTime.replace(day=1)
    # downloadStartTime = downloadStartTime.replace(hour=0)
    # downloadStartTime = downloadStartTime.replace(minute=0)
    # # start from July
    # downloadStartTime = downloadStartTime.replace(month=1)
    downloadStartTime = datetime.datetime.strptime('2019-10-01 00:00:00', '%Y-%m-%d %H:%M:%S')
    downloadEndTime = datetime.datetime.strptime('2020-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')

    data = dataDownloader(downloadStartTime, downloadEndTime)
    data.to_csv('../../../data/yuanjinghui/VDOT_TSS_Data.csv')

