import os
# import mysql.connector
import pandas as pd
# import matplotlib.pyplot as plt
# import seaborn as sns
from flask import Flask, request
from flask_cors import CORS
import json

# %matplotlib inline
app = Flask(__name__)
CORS(app)

# user_name = os.envirn.get('backend')
# password = os.envirn.get('backend1234')


@app.route('/')
def hello_world():
    df = pd.read_csv('https://s3.ap-southeast-1.amazonaws.com/omnicuris.assets/marketing/data/query_result.csv')
    # df.loc[df['s_id'] == 2, ['total_watched']] = 3315
    # df.loc[df['s_id'] == 5, ['total_watched']] = 1493
    # df.loc[df['s_id'] == 6, ['total_watched']] = 4217
    # df.loc[df['s_id'] == 7, ['total_watched']] = 1993
    # df.loc[df['s_id'] == 11, ['total_watched']] = 2475
    # df.loc[df['s_id'] == 12, ['total_watched']] = 436
    # df.loc[df['s_id'] == 13, ['total_watched']] = 99
    # df.loc[df['s_id'] == 14, ['total_watched']] = 60
    # df.loc[df['s_id'] == 16, ['total_watched']] = 480
    # df.loc[df['s_id'] == 17, ['total_watched']] = 1
    # df.loc[df['s_id'] == 18, ['total_watched']] = 588
    # df.loc[df['s_id'] == 19, ['total_watched']] = 1209
    # df.loc[df['s_id'] == 20, ['total_watched']] = 62
    # df.loc[df['s_id'] == 23, ['total_watched']] = 1053
    # df.loc[df['s_id'] == 24, ['total_watched']] = 934
    # df.loc[df['s_id'] == 25, ['total_watched']] = 1455
    # df.loc[df['s_id'] == 26, ['total_watched']] = 1374
    # df.loc[df['s_id'] == 28, ['total_watched']] = 398
    # df.loc[df['s_id'] == 30, ['total_watched']] = 168
    # df.loc[df['s_id'] == 31, ['total_watched']] = 388
    # df.loc[df['s_id'] == 32, ['total_watched']] = 630
    # df.loc[df['s_id'] == 34, ['total_watched']] = 174
    # df.loc[df['s_id'] == 35, ['total_watched']] = 286
    # df.loc[df['s_id'] == 46, ['total_watched']] = 239
    # df['percentage'] = ((df['user_watched'])*100)/df['total_watched']
    # df['mean'] = (df.groupby(['s_id'])['percentage']
    #               .transform('mean'))
    # df.loc[(df['percentage'] > (df['mean'])*0.01),['engagement_level']] = 'VERY_HIGH'
    # df.loc[(((df['percentage']) * 10) > (df['mean'])) & ((df['percentage']) <= ((df['mean']) * 0.01)), ['engagement_level']] = 'HIGH'
    # df.loc[(((df['percentage']) * 40) > (df['mean'])) & ((df['mean']) >= ((df['percentage']) * 10)), ['engagement_level']] = 'MEDIUM'
    # df.loc[(((df['percentage']) * 100) > 0) & ((df['mean']) >= ((df['percentage']) * 40)), ['engagement_level']] = 'LOW'
    # df.loc[(df['percentage']) <= 0, ['engagement_level']] = 'NEVER'

    # df.to_csv('/Users/santhosh-omni/Desktop/query_result.csv')
    result = df[(df['engagement_level'] == ['LOW', 'MEDIUM'])]
    # print(a.head())
    # b = df[(df['engagement_level'] == 'LOW')]
    # print(b.head())
    # result = pd.concat([a, b])
    print(result.head())
    # return result.to_csv()
    # print(df.head())
    return 'Hello World.!'

@app.route('/', methods=['POST'])
def hello_world_post():
    # df = pd.read_csv('https://s3.ap-southeast-1.amazonaws.com/omnicuris.assets/marketing/data/query_result.csv')
    df = pd.read_csv('/Users/santhosh-omni/Desktop/query_result.csv')
    # Updating the progress to 100 id status is completed
    df.loc[df['status'] == 'COMPLETED', ['progress']] = 100
    #Group all the user id with speciality id and find the average
    # grouped_df = df.groupby("status")
    # mean_df = grouped_df.mean()
    # print("^^^^^^^^^^^^^^^^^")
    # print(mean_df.head())
    # print("^^^^^^^^^^^^^^^^^")
    grouped_multiple = df.groupby(['User ID', 'speciality_id'])
    # mean_df = grouped_multiple.mean('progress')
    # df['Data4'] = grouped_multiple.mean('progress')
    # df['Data4'] = df['progress'].groupby(df['User ID', 'speciality_id']).transform('sum')
    # df.map(df.groupby('User ID','speciality_id')['progress'].sum())

    df['progress_mean'] = df.groupby(['User ID','speciality_id']).progress.transform('mean')
    df = df.drop_duplicates(['User ID','speciality_id'])

    df['mean'] = (df.groupby(['speciality_id'])['progress_mean']
                  .transform('mean'))

    df.loc[(df['progress_mean'] > (df['mean'])*0.01),['engagement_level']] = 'VERY_HIGH'
    df.loc[(((df['progress_mean']) * 10) > (df['mean'])) & ((df['progress_mean']) <= ((df['mean']) * 0.01)), ['engagement_level']] = 'HIGH'
    df.loc[(((df['progress_mean']) * 40) > (df['mean'])) & ((df['mean']) >= ((df['progress_mean']) * 10)), ['engagement_level']] = 'MEDIUM'
    df.loc[(((df['progress_mean']) * 100) > 0) & ((df['mean']) >= ((df['progress_mean']) * 40)), ['engagement_level']] = 'LOW'
    df.loc[(df['progress_mean']) <= 0, ['engagement_level']] = 'NEVER'
    print("^^^^^^^^^^^^^^^^^")
    print(df.head())
    print("^^^^^^^^^^^^^^^^^")
    df.to_csv('/Users/santhosh-omni/Desktop/query_result_d.csv')
    # result = df[(df['engagement_level'] == ['LOW', 'MEDIUM'])]
    # print(a.head())
    # b = df[(df['engagement_level'] == 'LOW')]
    # print(b.head())
    # result = pd.concat([a, b])
    # print(df)
    # return result.to_csv()
    # print(df.head())
    return "Done"


@app.route('/user-engagement', methods=['POST'])
# @cross_origin()
def user_engagement():
    request_data = request.json
    # data = json.loads(request_data)
    print("**************************************")
    print(request)
    print(format(request_data))
    print("**************************************")
    result = None
    df = pd.read_csv('https://s3.ap-southeast-1.amazonaws.com/omnicuris.assets/marketing/data/final.csv')
    if 'specialityId' in request_data:
        dfTempSpeciality = None
        if result is None:
            result = df
        for s in request_data['specialityId']:
            res = result[(result['speciality_id'] == int(s))]
            dfTempSpeciality = pd.concat([dfTempSpeciality, res])
        result = dfTempSpeciality

    if 'engagementLevel' in request_data:
        dfTempEngagement = None
        if result is None:
            result = df
        for e in request_data['engagementLevel']:
            res = result[(result['engagement_level'] == e)]
            dfTempEngagement = pd.concat([dfTempEngagement, res])
        result = dfTempEngagement

    if 'activityLevel' in request_data:
        dfTempActivity = None
        if result is None:
            result = df
        for e in request_data['activityLevel']:
            res = result[(result['activity_level'] == e)]
            dfTempActivity = pd.concat([dfTempActivity, res])
        result = dfTempActivity

    if result is None:
        return None
    # response.headers.add("Access-Control-Allow-Origin", "*")
    # response = Flask.jsonify({'data': result.to_csv()})
    # response.headers.add("Access-Control-Allow-Origin", "*")
    result = result.drop(['status', 'speciality_id', 'tracker_id', 'progress_mean', 'progress', 'mean', 'access_status'], axis = 1)
    return result.to_csv(index=False)

