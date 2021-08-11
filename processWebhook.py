import os
# import mysql.connector
import pandas as pd
# import matplotlib.pyplot as plt
# import seaborn as sns
from flask import Flask, request
from flask_cors import CORS
from sqlalchemy import create_engine
import pymysql
import io
import numpy
import json

# %matplotlib inline
app = Flask(__name__)
CORS(app)
# user_name = os.envirn.get('backend')
# password = os.envirn.get('backend1234')

@app.route('/generate/report', methods=['POST'])
def generate_report():
    # Sql Connection
    #Staging
    # sqlEngine = create_engine('mysql+pymysql://root:1pctlnt99pchw@prod-migration.carufofskwa1.ap-southeast-1.rds.amazonaws.com/omnicuris',pool_recycle=36000)
    #Pre-prod
    # sqlEngine = create_engine('mysql+pymysql://backend:90He$$kIDoF33@db-preprod.carufofskwa1.ap-southeast-1.rds.amazonaws.com/omnicuris',pool_recycle=36000)
    #Production
    sqlEngine = create_engine('mysql+pymysql://prod_view:prod_view_22@core-prod.carufofskwa1.ap-southeast-1.rds.amazonaws.com/omnicuris',pool_recycle=36000)
    dbConnection = sqlEngine.connect()
    queryForCount = 'Select u.id as count From t_user u Order By u.id DESC LIMIT 1'
    dfCount = pd.read_sql(queryForCount, dbConnection);
    totalCount = dfCount["count"].loc[0]
    print (totalCount)
    startRange = 0
    endRange = 10000
    df = None
    print('***************Started User Tracking For User******************')
    while startRange<totalCount:
        query = 'Select u.id as "User ID", ' \
                's.name as "Speciality Of Interest", ' \
                's.id as speciality_id, ' \
                'uct.id as tracker_id, ' \
                'uct.progress, ' \
                'uct.status ' \
                'from t_user u ' \
                'Join t_user_speciality_of_interest soi On soi.user_id = u.id ' \
                'Join t_course_speciality cs On cs.speciality_id = soi.speciality_id ' \
                'Join t_course c On c.id = cs.course_id ' \
                'Join t_user_course uc On uc.user_id = soi.user_id And uc.course_id = c.id  ' \
                'Join t_user_course_tracker uct On uct.user_course_id = uc.id ' \
                'Join m_speciality s On s.id = cs.speciality_id ' \
                'Join t_chapter ch On ch.id = uct.widget_id and uct.widget_type = "CHAPTER" ' \
                'Where uct.is_archived = 0 ' \
                'And ch.is_webinar = 0 ' \
                'And s.id In (Select s.id From m_speciality s) ' \
                'And u.id Between '
        query += str(startRange)
        query += ' And '
        query += str(endRange)
        query += ' And '
        query += 'u.id Not In ' \
                 '(Select u.id From t_user u ' \
                 'Join t_user_role ur On ur.user_id = u.id ' \
                 'Where ' \
                 'u.id Between '
        query += str(startRange)
        query += ' And '
        query += str(endRange)
        query += ' And ur.role_id In (Select id From t_role Where id != 3) Group By u.id) '
        query += 'Group By soi.speciality_id, soi.user_id ' \
                 'Order By u.id asc, ' \
                 'uct.id asc'
        print('---------------------------------------')
        print(query)
        print('---------------------------------------')
        startRange = endRange
        endRange = endRange + 10000
        dfQuery = pd.read_sql(query, dbConnection)
        df = pd.concat([df, dfQuery], ignore_index=True)
    # Calculate progress mean for user with each speciality
    print("****************PROGRESS CALCULATION*********************")
    df.loc[(df['status'] == "COMPLETED"), ['progress']] = 100
    # Convert the column to int
    df["progress"] = df["progress"].astype(int)
    df['progress_mean'] = df.groupby(['User ID', 'speciality_id'])['progress'].transform('mean')
    #Remove the duplicate user id & speciality id column
    print("****************REMOVE DUPLICATE*********************")
    df = df.drop_duplicates(subset=['User ID', 'speciality_id'], keep='last')
    #Find the mean value for each speciality
    df['mean'] = (df.groupby(['speciality_id'])['progress_mean'].transform('mean'))
    print("****************Adding Un tracked users*********************")
    tempDf = None
    startRange = 0
    endRange = 10000
    print('***************Started User Un Tracking For User******************')
    while startRange<totalCount:
        query = 'Select u.id as "User ID", ' \
                's.name as "Speciality Of Interest", ' \
                's.id as speciality_id, ' \
                '0 as tracker_id, ' \
                '0 as progress, ' \
                '"NOT_STARTED" as status, ' \
                '0 as progress_mean, ' \
                '100 as mean ' \
                'FROM t_user_speciality_of_interest soi ' \
                'Join t_user u On soi.user_id = u.id ' \
                'Join m_speciality s On s.id = soi.speciality_id ' \
                'Where soi.user_id Between '
        query += str(startRange)
        query += ' And '
        query += str(endRange)
        query += ' And '
        query += ' soi.speciality_id In (Select s.id From m_speciality s) ' \
                 'And ' \
                 'soi.user_id Not In ' \
                 '(Select u.id From t_user u ' \
                 'Join t_user_role ur On ur.user_id = u.id ' \
                 'Where ' \
                 'u.id Between '
        query += str(startRange)
        query += ' And '
        query += str(endRange)
        query += ' And ur.role_id In (Select id From t_role Where id != 3) Group By u.id) '
        query += 'Group By soi.speciality_id, soi.user_id ' \
                 'Order By u.id asc '
        print('---------------------------------------')
        print(query)
        print('---------------------------------------')
        startRange = endRange
        endRange = endRange + 10000
        dfQuery = pd.read_sql(query, dbConnection)
        tempDf = pd.concat([tempDf, dfQuery], ignore_index=True)
    # Concat Records Un tracked records
    # format
    tempDf["User ID"] = tempDf["User ID"].astype(int)
    df["User ID"] = df["User ID"].astype(int)
    tempDf["speciality_id"] = tempDf["speciality_id"].astype(int)
    tempDf["User ID"] = tempDf["User ID"].astype(int)
    df = pd.concat([df, tempDf], ignore_index=True)
    # Remove duplicates
    df = df.drop_duplicates(subset=['User ID', 'speciality_id'], keep='first')
    # sort the records
    df.sort_values("User ID", axis=0, ascending=True,
                     inplace=True, na_position='first')

    print("****************ENGAGEMENT CALCULATION*********************")
    #Set Engagement level
    df.loc[(df['progress_mean'] > ((df['mean']) + ((df['mean'])*0.1))),['Engagement Level']] = 'VERY_HIGH'
    df.loc[(df['progress_mean'] <= ((df['mean']) + ((df['mean'])*0.1)))  & (df['progress_mean'] > ((df['mean']) - ((df['mean'])*0.1))), ['Engagement Level']] = 'HIGH'
    df.loc[(df['progress_mean'] <= ((df['mean']) - ((df['mean'])*0.1)))  & (df['progress_mean'] > ((df['mean']) - ((df['mean'])*0.4))), ['Engagement Level']] = 'MEDIUM'
    df.loc[(df['progress_mean'] <= ((df['mean']) - ((df['mean'])*0.4)))  & (df['progress_mean'] > ((df['mean']) - ((df['mean'])*1))), ['Engagement Level']] = 'LOW'
    df.loc[(df['progress_mean']) <= 0, ['Engagement Level']] = 'NEVER'

    #Retrive All the user Id
    arr = df["User ID"].to_numpy()
    arr = list(set(arr))
    string_ints = [str(int) for int in arr]

    #Adding activity level Deprecated
    print("****************Activity Level*********************")
    # > last 3 months
    # print("------------------Start Last 3 Months--------------")
    # queryLast3Months = 'Select u.id as "User ID", '\
    #                    'Count(Nullif(uwptt.user_id,0)) as last_3_visited_count, '\
    #                    'from t_user u '\
    #                    'Left Join t_user_product_wise_time_tracker uwptt On u.id = uwptt.user_id '\
    #                    'where u.id in (' + (",".join(string_ints) )+ ') '\
    #                    'And uwptt.updated_at >= DATE_ADD(NOW(), INTERVAL -3 Month) '\
    #                    'group by u.id order by u.id asc'
    # dfLast3 = pd.read_sql(queryLast3Months, dbConnection);
    # df = pd.merge(df, dfLast3, on="User ID")
    # print("xxxxxxxxxxxxxxxxxxEnd Last 3 Monthsxxxxxxxxxxxxxxxxxxxx")
    # #  < last 3 months > last 6 months
    # print("------------------Start Last 3 To 9 Months--------------")
    # queryLast3To9Months = 'Select u.id as "User ID", '\
    #                    'Count(Nullif(uwptt.user_id,0)) as last_3_to_9_visited_count, '\
    #                    'uwptt.updated_at as last_visited_at  from t_user u '\
    #                    'Left Join t_user_product_wise_time_tracker uwptt On u.id = uwptt.user_id '\
    #                    'where u.id in (' + (",".join(string_ints)) + ') '\
    #                    'And uwptt.updated_at >= DATE_ADD(NOW(), INTERVAL -9 Month) '\
    #                    'And uwptt.updated_at < DATE_ADD(NOW(), INTERVAL -3 Month) '\
    #                    'group by u.id order by u.id asc'
    # dfLast3To6 = pd.read_sql(queryLast3Months, dbConnection);
    # df = pd.merge(df, dfLast3To6, on="User ID")
    # print("xxxxxxxxxxxxxxxxxxEnd Last 3 To 9 Monthsxxxxxxxxxxxxxxxxxxxx")
    # #  < last 3 months > last 6 months
    # print("------------------Start Last 9 Months--------------")
    # queryLast9Months = 'Select u.id as "User ID", '\
    #                       'Count(Nullif(uwptt.user_id,0)) as last_3-9_visited_count, '\
    #                       'uwptt.updated_at as last_visited_at  from t_user u '\
    #                       'Left Join t_user_product_wise_time_tracker uwptt On u.id = uwptt.user_id '\
    #                       'where u.id in (' + (",".join(string_ints)) + ') '\
    #                       'And uwptt.updated_at >= DATE_ADD(NOW(), INTERVAL -9 Month) '\
    #                       'And uwptt.updated_at < DATE_ADD(NOW(), INTERVAL -3 Month) '\
    #                       'group by u.id order by u.id asc'
    # dfLast9 = pd.read_sql(queryLast9Months, dbConnection);
    # df = pd.merge(df, dfLast9, on="User ID")
    # print("xxxxxxxxxxxxxxxxxxEnd Last 9 Monthsxxxxxxxxxxxxxxxxxxxx")
    # print("****************SAVE FILE*********************")
    # values = {"last_3_visited_count": 0, "last_3_to_9_visited_count": 0, "last_9_visited_count": 0}
    # df.fillna(value=values)
    # print("------------------Adding Last Visited Date--------------")
    # queryLastVisited= 'Select u.id as "User ID", ' \
    #                    'uwptt.updated_at as last_visited_at  from t_user u ' \
    #                    'Left Join t_user_product_wise_time_tracker uwptt On u.id = uwptt.user_id ' \
    #                    'where u.id in (' + (",".join(string_ints)) + ') ' \
    #                    'group by u.id order by u.id asc'
    # dfLastVisited = pd.read_sql(queryLastVisited, dbConnection);
    # df = pd.merge(df, dfLastVisited, on="User ID")

    # Update the user details
    print("****************Adding User Details*********************")
    queryForUsers = 'Select u.id as "User ID", ' \
                    'Replace(u.first_name,\',\',\' \') as "First Name", ' \
                    'Replace(u.last_name,\',\',\' \') as "Last Name", ' \
                    'u.email as "Email", ' \
                    'Replace(u.mobile_number,\',\',\' \') as "Contact No.", ' \
                    'r.name as "Location/Region", ' \
                    'u.created_at as "Registration Date", ' \
                    'if(u.is_email_verified = 1, "YES", "NO") as "isEmailVerified", ' \
                    'if(u.is_dnd = 1, "YES", "NO") as "isDND", ' \
                    'if(u.registration_status = 2, "YES", "NO") as "isMCIVerified", ' \
                    'if(IsNull(ur.rep_code), 0, ur.rep_code) as "rep_code" ' \
                    'from t_user u ' \
                    'Join m_region r On u.registration_region_id = r.id ' \
                    'Join t_user_role ur On ur.user_id = u.id ' \
                    'Where u.id in (' + (",".join(string_ints)) + ') group by u.id order by u.id asc'
    # Concat the data by user id
    dfUserDetails = pd.read_sql(queryForUsers, dbConnection);
    df["User ID"] = df["User ID"].astype(int)
    df = pd.merge(df, dfUserDetails, on="User ID")

    #Adding Activity Level
    print("****************Activity Level Query*********************")
    queryLastVisited = 'Select u.id as "User ID", '\
                       'Count(Nullif(uwptt.user_id,0)) as "Number Of Visits", ' \
                       'uwptt.updated_at as "Last Activity Date" '\
                       'from t_user u '\
                       'Left Join t_user_product_wise_time_tracker uwptt On u.id = uwptt.user_id '\
                       'where u.id in (' + (",".join(string_ints) )+ ') '\
                       'group by u.id order by u.id asc'
    dfLastVisited = pd.read_sql(queryLastVisited, dbConnection);
    df = pd.merge(df, dfLastVisited.rename(columns={'User ID': 'User ID'}), on='User ID', how='left')
    values = {"Number Of Visits": 0}
    df.fillna(value=values)
    print("####################End Activity Level Query######################")

    print("****************Activity Level*********************")
    # print(len(df.axes[0]))
    #Format Registration Date
    df['Registration Date'] = pd.to_datetime(df['Registration Date'], format='%Y-%m-%d %H:%M:%S')
    #Filling Null Value For Last Active date
    df['Last Activity Date'].fillna("2000-01-01 00:00:00", inplace=True)
    # Format Registration Date
    df['Last Activity Date'] = pd.to_datetime(df['Last Activity Date'], format='%Y-%m-%d %H:%M:%S')
    # Format Number Of Visits
    df["Number Of Visits"] = df["Number Of Visits"].astype(int)
    #Find Last 3 & 9 month date
    now = pd.to_datetime('now', format='%Y-%m-%d %H:%M:%S')
    last3 = now - pd.DateOffset(months=3)
    last9 = now - pd.DateOffset(months=9)

    #Registration less than 3 months
    print("****************Activity Level Last 3 months*********************")
    # High
    df.loc[(df['Registration Date'] >= last3) & (df['Number Of Visits'] > 3), ['Activity Level']] = 'HIGH'
    # Medium
    df.loc[(df['Registration Date'] >= last3) & (df['Number Of Visits'] < 4 & (df['Number Of Visits'] > 1)), [
        'Activity Level']] = 'MEDIUM'
    #Less
    df.loc[(df['Registration Date'] >= last3) & (df['Number Of Visits'] < 2),['Activity Level']] = 'LESS'

    # Registration less than 3 to 9 months
    print("****************Activity Level Last 3 To 9 months*********************")
    # High
    df.loc[((df['Registration Date'] >= last9) & (df['Registration Date'] < last3)) & (df['Number Of Visits'] > 3), [
        'Activity Level']] = 'HIGH'
    df.loc[((df['Registration Date'] >= last9) & (df['Registration Date'] < last3)) & (((df['Number Of Visits'] < 4) & (df['Number Of Visits'] > 1)) & (df['Last Activity Date'] >= last3)), ['Activity Level']] = 'HIGH'
    # Medium
    # df.loc[((df['Registration Date'] >= last9) & (df['Registration Date'] < last3)) & (df['Number Of Visits'] < 4 & (df['Number Of Visits'] > 1)), ['Activity Level']] = 'MEDIUM'
    df.loc[((df['Registration Date'] >= last9) & (df['Registration Date'] < last3)) & (
            (df['Number Of Visits'] > 1) & (
            (df['Last Activity Date'] >= last9) & (df['Last Activity Date'] < last3))), [
               'Activity Level']] = 'MEDIUM'
    #Less
    df.loc[((df['Registration Date'] >= last9) & (df['Registration Date'] < last3)) & (df['Number Of Visits'] < 2), ['Activity Level']] = 'LESS'
    df.loc[((df['Registration Date'] >= last9) & (df['Registration Date'] < last3)) & ((df['Number Of Visits'] > 1) & (df['Last Activity Date'] < last9)), ['Activity Level']] = 'LESS'

    # Registration less than 9 months
    print("****************Activity Level Last 9 months*********************")
    #High
    df.loc[(df['Registration Date'] < last9) & ((df['Number Of Visits'] > 3) & (df['Last Activity Date'] >= last3)), ['Activity Level']] = 'HIGH'
    #Medium
    df.loc[(df['Registration Date'] < last9) & ((df['Number Of Visits'] < 4) & (df['Number Of Visits'] > 1)), ['Activity Level']] = 'MEDIUM'
    df.loc[(df['Registration Date'] < last9) & ((df['Number Of Visits'] > 3) & ((df['Last Activity Date'] >= last9) & (df['Last Activity Date'] < last3))), [
        'Activity Level']] = 'MEDIUM'
    #Less
    df.loc[(df['Registration Date'] < last9) & (df['Number Of Visits'] < 2), ['Activity Level']] = 'LESS'
    df.loc[(df['Registration Date'] < last9) & (df['Last Activity Date'] < last9), ['Activity Level']] = 'LESS'
    df.loc[(df['Registration Date'] < last9) & (((df['Number Of Visits'] < 4) & (df['Number Of Visits'] > 1)) & (df['Last Activity Date'] < last9)), [
        'Activity Level']] = 'LESS'
    print("####################End Activity Level ######################")

    df.to_csv('/home/santhosh-omni/data/data-prod.csv', index=False)
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
    df = pd.read_csv('https://s3.ap-southeast-1.amazonaws.com/omnicuris.assets/marketing/data/prod/data-prod.csv')
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
            res = result[(result['Engagement Level'] == e)]
            dfTempEngagement = pd.concat([dfTempEngagement, res])
        result = dfTempEngagement

    if 'activityLevel' in request_data:
        dfTempActivity = None
        if result is None:
            result = df
        for e in request_data['activityLevel']:
            res = result[(result['Activity Level'] == e)]
            dfTempActivity = pd.concat([dfTempActivity, res])
        result = dfTempActivity

    if result is None:
        return None
    # response.headers.add("Access-Control-Allow-Origin", "*")
    # response = Flask.jsonify({'data': result.to_csv()})
    # response.headers.add("Access-Control-Allow-Origin", "*")
    result = result.drop(['status', 'speciality_id', 'tracker_id', 'progress_mean', 'progress', 'mean', 'rep_code', 'Number Of Visits', 'Last Activity Date', 'Activity Level'], axis = 1)
    result = result[['User ID', 'First Name', 'Last Name', 'Email', 'Contact No.', 'Location/Region', 'Speciality Of Interest', 'Registration Date', 'isEmailVerified', 'isDND','isMCIVerified', 'Engagement Level']]
    # return result.to_csv(index=False)
    return result.to_csv(index=False)

