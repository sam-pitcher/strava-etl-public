from firebase_admin import credentials
import pandas as pd
import numpy as np
import time
import datetime
import os
import requests
from google.cloud import bigquery
from strava_sdk import get_activities, get_activity, get_activity_streams
from firebase_functions import get_access_token, get_all_users
os.environ['TZ']='UTC'

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'strava-bigquery.json'

####################
# strava functions #
####################

def sync_activities(username, activity_id=None, per_page=1):
    print(f'User ID: {username}')
    access_token = get_access_token(username)
    if access_token is None:
        pass

    if activity_id == None:
        print('Getting activities using epoch as max time')
        # Find the latest activity epoch in database
        max_epoch = get_latest_activity_epoch(username)
        # Get an array of 1 activities (when writing it's only one activity)
        activities = get_activities(access_token=access_token,
        max_time=max_epoch,
        per_page=per_page)
    else:
        print(f'Getting activity using Activity ID: {activity_id}')
        activities = get_activity(access_token, activity_id)

    for activity in activities:
        ####################
        # ACTIVITY STREAMS #
        ####################

        try:
            # Run the act_streams function in strava_sdk to get the streams data
            act_streams = get_activity_streams(access_token, activity['activity_id'])

            # Add a new dictionary which has the same time keys as the act_streams for a join on time to fill the time gaps
            act_streams_times = {'time_key' :range(max(act_streams['time_key'])+1), 'time_new' :range(max(act_streams['time_key'])+1)}

            # Turn them both into dataframes
            df = pd.DataFrame(act_streams)
            df_times = pd.DataFrame(act_streams_times)
            print(df)
            print(df_times)
            print(activity)

            if int(activity['elapsed_time']) < 100000:
                print('less than')
                # Join them and fill in the gaps
                df_final = df_times.set_index('time_key').join(df.set_index('time_key')).interpolate()
            else:
                print('more than')
                df_final = df
                df_final['time_new'] = df_final['time']
            # Turn the NaN's to nulls for db
            df_final = df_final.replace({np.nan:None})
            df_final['latlng'] = df_final['latlng'].astype(str)
            df_final = df_final.replace({'None':None})
            df_final = df_final.replace({np.nan:None})

            # Turn the dataframe to a list
            include_list = ['watts','cadence','heartrate','altitude','temp','velocity_smooth','grade_smooth','distance','latlng','time_new']
            # df_final.replace({np.nan:None})
            act_streams_interpolated = df_final[include_list].to_dict(orient='records')
            print(df_final)
            print(act_streams_interpolated[:10])

        except:
            pass

        ####################
        # ROLLING AVERAGES #
        ####################
        rollings = [1,5,10,20,30,45,60,120,300,600,1200]
        rolling_dict = {}
        try:
            # List of the rolling averages for hr, power and speed
            if int(activity['elapsed_time']) < 100000:
                for i in rollings:
                    rolling_avg = df_final.rolling(i, win_type='triang').mean()
                    maxs = rolling_avg.max()
                    try:
                        max_hr = maxs.heartrate
                    except:
                        max_hr = None
                    try:
                        max_power = maxs.watts
                    except:
                        max_power = None
                    try:
                        max_speed = maxs.velocity_smooth
                    except:
                        max_speed = None

                    rolling_dict[f'max_hr_{i}'] = max_hr
                    rolling_dict[f'max_power_{i}'] = max_power
                    rolling_dict[f'max_speed_{i}'] = max_speed
                    
                    # print(rolling_dict)
                for i in rolling_dict:
                    # print(f'{i}: {rolling_dict[i]}')
                    try:
                        if rolling_dict[i] >= 0:
                            pass
                        else:
                            rolling_dict[i] = None
                    except:
                        rolling_dict[i] = None
            else:
                rolling_dict = {}
        except:
            act_streams_interpolated = {}
            for i in rollings:
                rolling_dict[f'max_hr_{i}'] = None
                rolling_dict[f'max_power_{i}'] = None
                rolling_dict[f'max_speed_{i}'] = None
        
        print(activity)
        print(rolling_dict)

        client = bigquery.Client()

        dataset_ref = client.dataset('activities')
        table_ref = dataset_ref.table('activities')
        table = client.get_table(table_ref)  # API call

        activity_timestamp = activity["timestamp"]
        activity_epoch = activity["epoch"]
        if time.time() - activity_epoch > 157680000:
            epoch = time.time() - 157680000 + 86400
            activity_timestamp = str(datetime.datetime.utcfromtimestamp(epoch).strftime('%Y-%m-%dT%H:%M:%SZ'))
# change back when running locally!!
        rows_to_insert = [
        {
            "activity_timestamp": activity_timestamp,
            "gear_id": activity["gear_id"],
            "icon_url": "www.google.com",
            "streams": act_streams_interpolated,
            # "streams": [act_streams_interpolated],
            "start_lat": activity["start_lat"],
            "altitude_url": "blank",
            "is_commute": activity["is_commute"],
            "maxs": [
            rolling_dict
            ],
            "name": activity["name"],
            "end_lng": activity["end_lng"],
            "name_id": f"{activity['name']}_{activity['activity_id']}",
            "polyline": activity["polyline"],
            "end_lat": activity["end_lng"],
            "max_heartrate": activity["start_lng"],
            "start_lng": activity["start_lng"],
            "max_power": activity["max_power"],
            "avg_power": activity["avg_power"],
            "avg_speed": activity["avg_speed"],
            "max_speed": activity["max_speed"],
            "timenow": datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'),
            "duration": activity["duration"],
            "avg_heartrate": activity["avg_heartrate"],
            "distance": activity["distance"],
            "epoch": activity["epoch"],
            "user_id": None,
            "username": username,
            "id": activity["activity_id"],
            "timestamp": activity["elevation"],
            "activity_type": activity["activity_type"],
            "elevation": activity["elevation"]
        }
        ]

        inserts = client.insert_rows_json(table, rows_to_insert)
        print(f'Inserts: {inserts}')

######################
# bigquery functions #
######################

def get_latest_activity_epoch(username):
    # client = bigquery.Client()
    print(f'Retrieving latest activity epoch for user ID: {username}...')
    QUERY = (f"SELECT max(epoch) as latest_activity_epoch FROM `activities.activities` WHERE username = '{username}'")
    query_df = pd.read_gbq(QUERY, dialect='standard', location='EU', use_bqstorage_api=True)
    try:
        latest_activity_epoch = int(query_df.latest_activity_epoch.item())
    except:
        latest_activity_epoch = 0
    print(f'Latest activity epoch: {latest_activity_epoch}')
    return latest_activity_epoch

def add_history_data():
    users = get_all_users()
    print(users)
    for user in users:
        sync_activities(user, activity_id=None, per_page=1)

add_history_data()