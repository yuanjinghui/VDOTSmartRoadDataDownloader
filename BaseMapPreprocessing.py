"""
Created on Mon Jun 24 20:01:56 2020

@author: ji758507
"""
import pandas as pd
import numpy as np
import geopy.distance


def get_segment_sequence(I81N_Segment_SpatialJoin):

    test = []
    ini_end_lat = I81N_Segment_SpatialJoin.loc[~((I81N_Segment_SpatialJoin['StartLat'].isin(I81N_Segment_SpatialJoin['EndLat'])) &
                                                 (I81N_Segment_SpatialJoin['StartLong'].isin(I81N_Segment_SpatialJoin['EndLong']))), 'EndLat'].tolist()[0]
    ini_end_long = I81N_Segment_SpatialJoin.loc[~((I81N_Segment_SpatialJoin['StartLat'].isin(I81N_Segment_SpatialJoin['EndLat'])) &
                                                  (I81N_Segment_SpatialJoin['StartLong'].isin(I81N_Segment_SpatialJoin['EndLong']))), 'EndLong'].tolist()[0]

    test.append(I81N_Segment_SpatialJoin.loc[~((I81N_Segment_SpatialJoin['StartLat'].isin(I81N_Segment_SpatialJoin['EndLat'])) &
                                               (I81N_Segment_SpatialJoin['StartLong'].isin(I81N_Segment_SpatialJoin['EndLong']))), :])
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
    for i in range(len(test)):
        mask_id = test.loc[i, 'id7']
        print(i)
        if mask_id == ' ':
            continue
        else:
            mask_id = mask_id.split("_")

            start_lat = test.loc[i, 'StartLat']
            start_long = test.loc[i, 'StartLong']

            end_lat = test.loc[i, 'EndLat']
            end_long = test.loc[i, 'EndLong']

            up_station_id = []
            down_station_id = []
            dist2start_list = []
            dist2end_list = []
            for k in range(len(mask_id)):
                tem_mask_id = int(mask_id[k])
                detector_lat = Selected_detector_configuration.loc[Selected_detector_configuration['id7'] == tem_mask_id, 'latitude'].tolist()[0]
                detector_long = Selected_detector_configuration.loc[Selected_detector_configuration['id7'] == tem_mask_id, 'longitude'].tolist()[0]

                dist2start = geopy.distance.geodesic((detector_lat, detector_long), (start_lat, start_long)).km
                dist2end = geopy.distance.geodesic((detector_lat, detector_long), (end_lat, end_long)).km

                station_id = Selected_detector_configuration.loc[Selected_detector_configuration['id7'] == tem_mask_id, 'station_id'].tolist()[0]
                if dist2start < dist2end:
                    up_station_id.append(station_id)
                    dist2start_list.append(dist2start)
                else:
                    down_station_id.append(station_id)
                    dist2end_list.append(dist2end)

            if len(up_station_id) > 0:
                temp = min(dist2start_list)
                res = [i for i, j in enumerate(dist2start_list) if j == temp][0]
                test.loc[i, 'up_station_id'] = up_station_id[res]
            if len(down_station_id) > 0:
                temp = min(dist2end_list)
                res = [i for i, j in enumerate(dist2end_list) if j == temp][0]
                test.loc[i, 'down_station_id'] = down_station_id[res]

    return test


def distance(test, a, b, c, d):
    dist = []
    for i in range(len(test)):
        a_long = test.loc[i, a]
        b_lat = test.loc[i, b]

        c_long = test.loc[i, c]
        d_lat = test.loc[i, d]

        dist.append(geopy.distance.geodesic((b_lat, a_long), (d_lat, c_long)).miles)

    return dist


# fill nan by using upstream and downstream ids
def fill_empty_adjacent_ids(test, Selected_station_configuration):
    test.loc[test['up_station_id'].isnull(), 'up_station_id'] = test['down_station_id'].shift(1)
    test.loc[test['down_station_id'].isnull(), 'down_station_id'] = test['up_station_id'].shift(-1)

    # ffill and backfill station ids
    test['up_station_id_ffill'] = test['up_station_id'].fillna(method='ffill', axis=0)
    test['up_station_id_backfill'] = test['up_station_id'].fillna(method='backfill', axis=0)

    test.loc[test['up_station_id_ffill'].isnull(), 'up_station_id_ffill'] = test['up_station_id_backfill']
    test.loc[test['up_station_id_backfill'].isnull(), 'up_station_id_backfill'] = test['up_station_id_ffill']

    # check the distance between segment start with ffill id, backfill id.
    test = test.join(Selected_station_configuration[['station_id', 'longitude', 'latitude']].set_index('station_id'), on='up_station_id_ffill', rsuffix='_ffill')
    test = test.join(Selected_station_configuration[['station_id', 'longitude', 'latitude']].set_index('station_id'), on='up_station_id_backfill', rsuffix='_backfill')

    test['up_distance_ffill'] = distance(test, 'StartLong', 'StartLat', 'longitude', 'latitude')
    test['up_distance_backfill'] = distance(test, 'StartLong', 'StartLat', 'longitude_backfill', 'latitude_backfill')

    # fill the up_station_id and down_station_id based on the condition of up_distance_ffill and up_distance_backfill
    test['up_station_id'] = np.where(test['up_distance_ffill'] <= test['up_distance_backfill'], test['up_station_id_ffill'], test['up_station_id_backfill'])
    test.loc[test['down_station_id'].isnull(), 'down_station_id'] = test['up_station_id'].shift(-1)
    test.loc[test['down_station_id'].isnull(), 'down_station_id'] = test['up_station_id']

    # exclude additional variables
    test = test.drop(['up_station_id_ffill', 'up_station_id_backfill', 'longitude', 'latitude', 'longitude_backfill', 'latitude_backfill', 'up_distance_ffill', 'up_distance_backfill'], axis=1)
    test = test.join(Selected_station_configuration[['station_id', 'id7']].set_index('station_id'), on='up_station_id', rsuffix='_up')
    test = test.join(Selected_station_configuration[['station_id', 'id7']].set_index('station_id'), on='down_station_id', rsuffix='_down')

    # identify the segment type, internal (0) or external (1)
    test['internal_external'] = np.where(test['Tmc'].str.contains('|'.join(['N', 'P'])), 0, 1)

    return test


def main(segment_name, Selected_detector_configuration, Selected_station_configuration):
    I81N_Segment_SpatialJoin = pd.read_csv('Jingwan/CA_Data_JF/Data_process_JF/{}/{}_segment_SpatialJoin.csv'.format(segment_name[0:4], segment_name), index_col=0)
    I81N_Segment_SpatialJoin = I81N_Segment_SpatialJoin.sort_values(by=['StartLat', 'StartLong']).reset_index(drop=True)
    I81N_Segment_SpatialJoin['id7'] = I81N_Segment_SpatialJoin['id7'].astype(str)
    I81N_Segment_SpatialJoin[['StartLat', 'StartLong', 'EndLat', 'EndLong']] = I81N_Segment_SpatialJoin[['StartLat', 'StartLong', 'EndLat', 'EndLong']].round(5)

    test = get_segment_sequence(I81N_Segment_SpatialJoin)
    test = get_up_down_ids(test, Selected_detector_configuration)
    test = fill_empty_adjacent_ids(test, Selected_station_configuration)
    test.to_csv('Jingwan/CA_Data_JF/Data_process_JF/{}_Segment_SpatialJoin_adjacent_info.csv'.format(segment_name))

    return test


Selected_detector_configuration = pd.read_csv('Traffic Data/PEMS/PEMS_Detectors_Selected_D4D7.csv', index_col=0)
Selected_station_configuration = Selected_detector_configuration[Selected_detector_configuration['type'].isin(['ML', 'HV'])].groupby(by=['latitude', 'longitude'], as_index=False).agg({"id7": lambda x: "_".join(map(str, x.tolist()))}).reset_index(drop=True)
Selected_station_configuration['station_id'] = 1000 + Selected_station_configuration.index

Selected_detector_configuration = pd.merge(Selected_detector_configuration, Selected_station_configuration[['latitude', 'longitude', 'station_id']], how='left', left_on=['latitude', 'longitude'], right_on=['latitude', 'longitude'])


if __name__ == '__main__':
    Final_segment_map = []
    for segment_name in ['101N_D7', '101NN_D4', '101NS_D4', '101S_D7', '101SN_D4', '101SS_D4', '105E', '105W', '405N', '405S']:
        try:
            I81N_Segment_SpatialJoin = pd.read_csv('Jingwan/CA_Data_JF/Data_process_JF/{}/{}_segment_SpatialJoin.csv'.format(segment_name[0:4], segment_name), index_col=0)
            I81N_Segment_SpatialJoin = I81N_Segment_SpatialJoin.sort_values(by=['StartLat', 'StartLong']).reset_index(drop=True)
            I81N_Segment_SpatialJoin['id7'] = I81N_Segment_SpatialJoin['id7'].astype(str)
            I81N_Segment_SpatialJoin[['StartLat', 'StartLong', 'EndLat', 'EndLong']] = I81N_Segment_SpatialJoin[['StartLat', 'StartLong', 'EndLat', 'EndLong']].round(5)

            test = get_segment_sequence(I81N_Segment_SpatialJoin)
            test = get_up_down_ids(test, Selected_detector_configuration)
            test = fill_empty_adjacent_ids(test, Selected_station_configuration)
            test.to_csv('Jingwan/CA_Data_JF/Data_process_JF/{}_Segment_SpatialJoin_adjacent_info.csv'.format(segment_name))

            test['Route_ID'] = segment_name
        except:
            print('error')
            continue
        print(segment_name)
        Final_segment_map.append(test)

    Final_segment_map = pd.concat(Final_segment_map, ignore_index=True).reset_index(drop=True)
    Final_segment_map.to_csv('Jingwan/CA_Data_JF/Data_process_JF/Final_segment_map.csv')
