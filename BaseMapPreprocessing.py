"""
Created on Mon Jun 24 20:01:56 2020

@author: ji758507
"""
import pandas as pd
import numpy as np
import geopy.distance


def get_segment_sequence(I81N_Segment_SpatialJoin):

    test = []

    # initial the first start point of the first segment
    # ini_end_lat = I81N_Segment_SpatialJoin.loc[0, 'EndLat']
    # ini_end_long = I81N_Segment_SpatialJoin.loc[0, 'EndLong']
    ini_end_lat = I81N_Segment_SpatialJoin.loc[~I81N_Segment_SpatialJoin['StartLat'].isin(I81N_Segment_SpatialJoin['EndLat']), 'EndLat'].tolist()[0]
    ini_end_long = I81N_Segment_SpatialJoin.loc[~I81N_Segment_SpatialJoin['StartLat'].isin(I81N_Segment_SpatialJoin['EndLat']), 'EndLong'].tolist()[0]

    test.append(I81N_Segment_SpatialJoin.loc[~I81N_Segment_SpatialJoin['StartLat'].isin(I81N_Segment_SpatialJoin['EndLat']), :])
    j = 0
    while j < len(I81N_Segment_SpatialJoin):
        test_start_lat = I81N_Segment_SpatialJoin.loc[j, 'StartLat']
        test_start_long = I81N_Segment_SpatialJoin.loc[j, 'StartLong']

        if ini_end_lat == test_start_lat and ini_end_long == test_start_long:
            test.append(I81N_Segment_SpatialJoin.iloc[[j]])

            ini_end_lat = I81N_Segment_SpatialJoin.loc[j, 'EndLat']
            ini_end_long = I81N_Segment_SpatialJoin.loc[j, 'EndLong']

            j = 0
        else:
            j += 1

    # test = pd.DataFrame(test)
    test = pd.concat(test)
    test = test.reset_index(drop=True)
    test['sequence'] = test.index + 1

    return test


def get_up_down_ids(test, Selected_detector_configuration):
    test['up_station_id'] = np.nan
    test['down_station_id'] = np.nan
    test['up_detector_id'] = np.nan
    test['down_detector_id'] = np.nan
    for i in range(len(test)):
        mask_id = test.loc[i, 'mask_id']
        print(i)
        if mask_id == ' ':
            continue
        else:
            mask_id = mask_id.split("_")

            start_lat = test.loc[i, 'StartLat']
            start_long = test.loc[i, 'StartLong']

            end_lat = test.loc[i, 'EndLat']
            end_long = test.loc[i, 'EndLong']

            up_detector_id = []
            down_detector_id = []
            for k in range(len(mask_id)):
                tem_mask_id = int(mask_id[k])
                detector_lat = Selected_detector_configuration.loc[Selected_detector_configuration['mask_id'] == tem_mask_id, 'latitude'].tolist()[0]
                detector_long = Selected_detector_configuration.loc[Selected_detector_configuration['mask_id'] == tem_mask_id, 'longitude'].tolist()[0]

                dist2start = geopy.distance.geodesic((detector_long, detector_lat), (start_long, start_lat)).km
                dist2end = geopy.distance.geodesic((detector_long, detector_lat), (end_long, end_lat)).km

                station_id = Selected_detector_configuration.loc[Selected_detector_configuration['mask_id'] == tem_mask_id, 'ns2_statio'].tolist()[0]
                if dist2start < dist2end:
                    test.loc[i, 'up_station_id'] = station_id
                    up_detector_id.append(tem_mask_id)
                else:
                    test.loc[i, 'down_station_id'] = station_id
                    down_detector_id.append(tem_mask_id)

            if len(up_detector_id) > 0:
                test.loc[i, 'up_detector_id'] = '_'.join(str(x) for x in up_detector_id)
            if len(down_detector_id) > 0:
                test.loc[i, 'down_detector_id'] = '_'.join(str(x) for x in down_detector_id)

    return test


def distance(test, a, b, c, d):
    dist = []
    for i in range(len(test)):
        a_long = test.loc[i, a]
        b_lat = test.loc[i, b]

        c_long = test.loc[i, c]
        d_lat = test.loc[i, d]

        dist.append(geopy.distance.geodesic((a_long, b_lat), (c_long, d_lat)).miles)

    return dist


# fill nan by using upstream and downstream ids
def fill_empty_adjacent_ids(test, Selected_station_configuration):
    # up_id = down_id.shift(1), down_id = up_id.shift(-1)
    test.loc[test['up_station_id'].isnull(), 'up_station_id'] = test['down_station_id'].shift(1)
    test.loc[test['down_station_id'].isnull(), 'down_station_id'] = test['up_station_id'].shift(-1)

    test.loc[test['up_detector_id'].isnull(), 'up_detector_id'] = test['down_detector_id'].shift(1)
    test.loc[test['down_detector_id'].isnull(), 'down_detector_id'] = test['up_detector_id'].shift(-1)

    # ffill and backfill station ids
    test['up_station_id_ffill'] = test['up_station_id'].fillna(method='ffill', axis=0)
    test['up_station_id_backfill'] = test['up_station_id'].fillna(method='backfill', axis=0)

    test.loc[test['up_station_id_ffill'].isnull(), 'up_station_id_ffill'] = test['up_station_id_backfill']
    test.loc[test['up_station_id_backfill'].isnull(), 'up_station_id_backfill'] = test['up_station_id_ffill']

    # ffill and backfill detector ids
    test['up_detector_id_ffill'] = test['up_detector_id'].fillna(method='ffill', axis=0)
    test['up_detector_id_backfill'] = test['up_detector_id'].fillna(method='backfill', axis=0)

    test.loc[test['up_detector_id_ffill'].isnull(), 'up_detector_id_ffill'] = test['up_detector_id_backfill']
    test.loc[test['up_detector_id_backfill'].isnull(), 'up_detector_id_backfill'] = test['up_detector_id_ffill']

    # check the distance between segment start with ffill id, backfill id.
    test = test.join(Selected_station_configuration.set_index('ns2_statio'), on='up_station_id_ffill', rsuffix='_ffill')
    test = test.join(Selected_station_configuration.set_index('ns2_statio'), on='up_station_id_backfill', rsuffix='_backfill')

    test['up_distance_ffill'] = distance(test, 'StartLong', 'StartLat', 'longitude', 'latitude')
    test['up_distance_backfill'] = distance(test, 'StartLong', 'StartLat', 'longitude_backfill', 'latitude_backfill')

    # fill the up_station_id and down_station_id based on the condition of up_distance_ffill and up_distance_backfill
    test['up_station_id'] = np.where(test['up_distance_ffill'] <= test['up_distance_backfill'], test['up_station_id_ffill'], test['up_station_id_backfill'])
    test.loc[test['down_station_id'].isnull(), 'down_station_id'] = test['up_station_id'].shift(-1)
    test.loc[test['down_station_id'].isnull(), 'down_station_id'] = test['up_station_id']

    # fill the up_detector_id and down_detector_id based on the condition of up_distance_ffill and up_distance_backfill
    test['up_detector_id'] = np.where(test['up_distance_ffill'] <= test['up_distance_backfill'], test['up_detector_id_ffill'], test['up_detector_id_backfill'])
    test.loc[test['down_detector_id'].isnull(), 'down_detector_id'] = test['up_detector_id'].shift(-1)
    test.loc[test['down_detector_id'].isnull(), 'down_detector_id'] = test['up_detector_id']

    # exclude additional variables
    test = test.drop(['up_station_id_ffill', 'up_station_id_backfill', 'up_detector_id_ffill',
                      'up_detector_id_backfill', 'longitude', 'latitude', 'mask_id_ffill',
                      'longitude_backfill', 'latitude_backfill', 'mask_id_backfill', 'up_distance_ffill', 'up_distance_backfill'], axis=1)

    # identify the segment type, internal (0) or external (1)
    test['internal_external'] = np.where(test['Tmc'].str.contains('|'.join(['N', 'P'])), 0, 1)

    return test


def main(segment_name, Selected_detector_configuration, Selected_station_configuration):
    I81N_Segment_SpatialJoin = pd.read_csv('Geometric Data/{}_Segment_SpatialJoin.csv'.format(segment_name), index_col=0)
    I81N_Segment_SpatialJoin = I81N_Segment_SpatialJoin.sort_values(by=['StartLat', 'StartLong']).reset_index(drop=True)
    I81N_Segment_SpatialJoin['mask_id'] = I81N_Segment_SpatialJoin['mask_id'].astype(str)

    test = get_segment_sequence(I81N_Segment_SpatialJoin)
    test = get_up_down_ids(test, Selected_detector_configuration)
    test = fill_empty_adjacent_ids(test, Selected_station_configuration)
    test.to_csv('Geometric Data/{}_Segment_SpatialJoin_adjacent_info.csv'.format(segment_name))


Selected_detector_configuration = pd.read_csv('Geometric Data/Selected_detector_configuration.csv', index_col=0)
Selected_station_configuration = Selected_detector_configuration.groupby(by=['ns2_statio'], as_index=False).agg({"latitude": 'first', "longitude": 'first', "mask_id": lambda x: "_".join(map(str, x.tolist()))})

if __name__ == '__main__':
    # segment_name = 'I81N'
    segment_name = 'I81S'
    main(segment_name, Selected_detector_configuration, Selected_station_configuration)
