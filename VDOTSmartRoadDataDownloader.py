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


def dataDownloader(currentTime):

    # get the start download time
    # downloadStartTime = currentTime - datetime.timedelta(hours=1)
    downloadStartTime = currentTime - relativedelta(years=3)
    downloadStartTime = downloadStartTime.replace(day=1)
    downloadStartTime = downloadStartTime.replace(hour=0)
    downloadStartTime = downloadStartTime.replace(minute=0)

    # get the end download time
    downloadEndTime = currentTime.replace(minute=0)

    # get the datetime range
    timeRange = pd.date_range(start=downloadStartTime, end=downloadEndTime, freq = 'T').tolist() # Generate time series

    global_month = downloadStartTime.month
    data = []
    for time in timeRange:
        print(time)
        year = str(time)[0:4]
        month = str(time)[5:7]
        day = str(time)[8:10]
        hours = str(time)[11:13]
        minutes = str(time)[14:16]
        print(year, month, day, hours, minutes)

        if int(month) > global_month:
            data = pd.DataFrame(data)
            data.to_csv('../../../data/yuanjinghui/VDOT_TSS_Data_{}{}.csv'.format(year, month))
            data = []

        else:
            print('Keep writing VDOT_TSS_Data_{}{}.csv'.format(year, month))

        try:
            # read xml.gz files
            if time < datetime.datetime.strptime('2017-12-13 08:23:00', '%Y-%m-%d %H:%M:%S'):

                testfile = urllib.request.URLopener()
                testfile.retrieve(
                    "https://smarterroads.org/dataset/download/1?token=18cPAKB4l5QucRdOHyOMi0EwI9ZRwWHuyaxhEXPpMvNzMtWIBooGp1GuzzqNsWI7&file=hub_data/TSSData/{}/{}/{}/{}/TSSData_{}{}{}_{}{}.xml.gz".
                        format(year, month, day, hours, year, month, day, hours, minutes),
                    "file.gz")
                input = gzip.open('file.gz', 'r')
                tree = ET.parse(input)
                root = tree.getroot()

            # read xml files
            else:
                testfile = urllib.request.URLopener()
                testfile.retrieve(
                    "https://smarterroads.org/dataset/download/1?token=18cPAKB4l5QucRdOHyOMi0EwI9ZRwWHuyaxhEXPpMvNzMtWIBooGp1GuzzqNsWI7&file=hub_data/TSSData/{}/{}/{}/{}/TSSData_{}{}{}_{}{}.xml".
                        format(year, month, day, hours, year, month, day, hours, minutes),
                    "file")
                tree = ET.parse("file")
                root = tree.getroot()
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
                    if varName == 'the_geom':
                        for i in features:
                            for j in i:
                                # var1 = j.tag
                                # var1 = var1.split("}")[-1]
                                # print(var1)
                                # print(j.text)
                                feature['long'] = j.text.split(" ")[0]
                                feature['lat'] = j.text.split(" ")[1]

                    # print(features[''])
                    # for temp in features:
                    # print(features.attrib['{http://www.openroadsconsulting.com}detectorid'])
                    feature[varName] = features.text
                    # print(features)
                data.append(feature)
        global_month = int(month)
    data = pd.DataFrame(data)
    return data


time = datetime.datetime.strptime('2017-06-01 23:32:00', '%Y-%m-%d %H:%M:%S')

if __name__ == '__main__':

    currentTime = datetime.datetime.now().replace(microsecond=0)
    data = dataDownloader(currentTime)
    data.to_csv('../../../data/yuanjinghui/VDOT_TSS_Data.csv')

