import os
# import mysql.connector
import pandas as pd
# import matplotlib.pyplot as plt
# import seaborn as sns
from flask import Flask, request
from flask_cors import CORS, cross_origin
from sqlalchemy import create_engine, text
import pymysql
import io
import numpy
import json

# %matplotlib inline
app = Flask(__name__)
CORS(app)
# user_name = os.envirn.get('backend')
# password = os.envirn.get('backend1234')

db_url = 'mysql+pymysql://root:1pctlnt99pchw@stg-database.omnicuris.com/omnicuris'

# db_url = 'mysql+pymysql://backend:90He$$kIDoF33@preprod-database.omnicuris.com/omnicuris'
#
# db_url = 'mysql+pymysql://prod_view:prod_view_22@core-prod.carufofskwa1.ap-southeast-1.rds.amazonaws.com/omnicuris'


@app.route('/generate/report', methods=['POST'])
def generate_report():
    # Sql Connection
    #Staging
    sqlEngine = create_engine(db_url,pool_recycle=36000)
    #Pre-prod
    # sqlEngine = create_engine('mysql+pymysql://backend:90He$$kIDoF33@db-preprod.carufofskwa1.ap-southeast-1.rds.amazonaws.com/omnicuris',pool_recycle=36000)
    #Production
    # sqlEngine = create_engine('mysql+pymysql://prod_view:prod_view_22@core-prod.carufofskwa1.ap-southeast-1.rds.amazonaws.com/omnicuris',pool_recycle=36000)
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
                'And uct.progress > 0 ' \
                'And s.id In (Select s.id From m_speciality s where id = 20) ' \
                'And u.is_dnd=0 ' \
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
                'Where ' \
                'u.is_dnd=0 ' \
                'And soi.user_id Between '
        query += str(startRange)
        query += ' And '
        query += str(endRange)
        query += ' And '
        query += ' soi.speciality_id In (Select s.id From m_speciality s where id = 20) ' \
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

    #Specify Enrollment Type
    df["rep_code"] = df["rep_code"].astype(int)
    df.loc[(df['rep_code'] >0), ['Enrollment Type']] = 'REP'
    df['Enrollment Type'].fillna("ORGANIC", inplace=True)

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
    #TODO: remove
    df["Activity Level"].fillna("MEDIUM", inplace=True)
    df.to_csv('/home/santhosh-omni/data/data-check-20-III.csv', index=False)
    return "Done"


@app.route('/v2/generate/report', methods=['POST'])
def generate_report_v2():
    # Sql Connection
    #Staging
    # sqlEngine = create_engine('mysql+pymysql://root:1pctlnt99pchw@prod-migration.carufofskwa1.ap-southeast-1.rds.amazonaws.com/omnicuris',pool_recycle=36000)
    #Pre-prod
    # sqlEngine = create_engine('mysql+pymysql://backend:90He$$kIDoF33@db-preprod.carufofskwa1.ap-southeast-1.rds.amazonaws.com/omnicuris',pool_recycle=36000)
    #Production
    sqlEngine = create_engine(db_url,pool_recycle=36000)
    dbConnection = sqlEngine.connect()
    #Fetch All The Speciality
    queryForSpecialityIds = 'Select s.id From m_speciality s Order By s.id ASC'
    dfSpecialityId = pd.read_sql(queryForSpecialityIds, dbConnection);
    arrSpeciality = dfSpecialityId["id"].to_numpy()
    df = None
    print('***************Started User Speciality Of Interest Time Watched*******************')
    for i in arrSpeciality:
        query = ''
        query += 'select * from ' \
                '(select a.id as "User ID", ' \
                'c.name as "Speciality Of Interest", ' \
                'c.id as speciality_id, ' \
                'if(b.time_watched is NULL, 0, b.time_watched) as time_watched ' \
                'from ' \
                't_user a ' \
                'join t_user_speciality_of_interest b ' \
                'join m_speciality c on a.id = b.user_id ' \
                'and b.speciality_id = c.id ' \
                'and CAST(AES_DECRYPT(FROM_BASE64(a.email),"0z74P67WdkoeKXCc") AS CHAR(50)) not like "%omni%" ' \
                'and CAST(AES_DECRYPT(FROM_BASE64(a.email),"0z74P67WdkoeKXCc") AS CHAR(50)) not like "%test%" ' \
                'and CAST(AES_DECRYPT(FROM_BASE64(a.email),"0z74P67WdkoeKXCc") AS CHAR(50)) not like "%dummy%" ' \
                'and CAST(AES_DECRYPT(FROM_BASE64(a.email),"0z74P67WdkoeKXCc") AS CHAR(50)) like "%@%" ' \
                'and b.speciality_id = '+str(i)+' ' \
                'group by b.user_id, b.speciality_id ' \
                ') s ' \
                'join ' \
                '(select a.speciality_id as sid, avg(time_watched) as mean from ' \
                '(select speciality_id, time_watched from t_user_speciality_of_interest ' \
                'where time_watched is not null and speciality_id = '+str(i) +' and time_watched!=0 ' \
                'group by user_id, speciality_id) a group by a.speciality_id) r on r.sid = s.speciality_id'
        print('---------------------------------------')
        print(query)
        print('---------------------------------------')
        dfQuery = pd.read_sql(text(query), dbConnection)
        df = pd.concat([df, dfQuery], ignore_index=True)
    df.sort_values("User ID", axis=0, ascending=True,
                     inplace=True, na_position='first')

    print("****************ENGAGEMENT CALCULATION*********************")
    #Set Engagement level
    df.loc[(df['time_watched'] >= (1.1 * df['mean'])), ['Engagement Level']] = 'VERY_HIGH'
    df.loc[(df['time_watched'] < (1.1 * df['mean'])) & (df['time_watched'] >= (0.9 * df['mean'])), [
        'Engagement Level']] = 'HIGH'
    df.loc[(df['time_watched'] < (0.9 * df['mean'])) & (df['time_watched'] >= (0.6 * df['mean'])), [
        'Engagement Level']] = 'MEDIUM'
    df.loc[(df['time_watched'] < (0.6 * df['mean'])), ['Engagement Level']] = 'LOW'
    df.loc[(df['time_watched']) <= 0, ['Engagement Level']] = 'NEVER'

    # print("****************ENGAGEMENT CALCULATION*********************")
    # #Set Engagement level
    # df.loc[(df['time_watched'] >= (1.1 * df['mean'])), ['Engagement Level']] = 'VERY_HIGH'
    # df.loc[(df['time_watched'] < (1.1 * df['mean'])) & (df['time_watched'] >= (0.7 * df['mean'])), [
    #     'Engagement Level']] = 'HIGH'
    # df.loc[(df['time_watched'] < (0.7 * df['mean'])) & (df['time_watched'] >= (0.3 * df['mean'])), [
    #     'Engagement Level']] = 'MEDIUM'
    # df.loc[(df['time_watched'] < (0.3 * df['mean'])), ['Engagement Level']] = 'LOW'
    # df.loc[(df['time_watched']) <= 0, ['Engagement Level']] = 'NEVER'

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
                    'CAST(AES_DECRYPT(FROM_BASE64(u.email),"0z74P67WdkoeKXCc") AS CHAR(50)) as "Email", ' \
                    'Replace(CAST(AES_DECRYPT(FROM_BASE64(u.mobile_number),"0z74P67WdkoeKXCc") AS CHAR(50)),\',\',\' \') as "Contact No.", ' \
                    'r.name as "Location/Region", ' \
                    'u.created_at as "Registration Date", ' \
                    'if(u.is_email_verified = 1, "YES", "NO") as "isEmailVerified", ' \
                    'if(u.is_dnd = 1, "YES", "NO") as "isDND", ' \
                    'if(u.registration_status = 2, "YES", "NO") as "isMCIVerified",' \
                    'if(u.uninstalled = 1, "YES",if(u.device_token is not null, "NO", "N/A")) as isUninstalled, ' \
                    'if(u.unsubscribed = 1, "YES",if(u.unsubscribed is not null, "NO", "N/A")) as isUnsubscribed ' \
                    'from t_user u ' \
                    'Join m_region r On u.registration_region_id = r.id ' \
                    'Join t_user_role ur On ur.user_id = u.id ' \
                    'Where u.id in (' + (",".join(string_ints)) + ') group by u.id order by u.id asc'
    # Concat the data by user id
    dfUserDetails = pd.read_sql(queryForUsers, dbConnection);
    df["User ID"] = df["User ID"].astype(int)
    df = pd.merge(df, dfUserDetails, on="User ID")

    #Set Rep Code
    dfRep = None
    for i in arrSpeciality:
        queryRep = ''
        queryRep += 'Select ' \
                    'u.id as "User ID", ' \
                    'sc.speciality_id, ' \
                    'If(uc.rep_code is not null, 1, 0) as rep_code ' \
                    'From t_user u ' \
                    'Join t_user_course uc On uc.user_id = u.id ' \
                    'Join t_course_speciality sc On sc.course_id = uc.course_id ' \
                    'Where sc.speciality_id = '+str(i)+' and uc.rep_code is not null ' \
                    'group by uc.user_id'
        print('---------------------------------------')
        print(queryRep)
        print('---------------------------------------')
        dfQueryRep = pd.read_sql(text(queryRep), dbConnection)
        print(dfQueryRep)
        dfRep = pd.concat([dfRep, dfQueryRep], ignore_index=True)
    df = pd.merge(df, dfRep, on=['User ID', 'speciality_id'], how='left')
    #Specify Enrollment Type
    df['rep_code'].fillna("0", inplace=True)
    df["rep_code"] = df["rep_code"].astype(int)
    df.loc[(df['rep_code'] != 0), ['Enrollment Type']] = 'REP'
    df['Enrollment Type'].fillna("ORGANIC", inplace=True)

    #Adding Activity Level
    print("****************Activity Level Query*********************")
    queryLastVisited = 'select user_id as "User ID", ' \
                       'count(*) as "Number Of Visits", ' \
                       'min(DATEDIFF(NOW(), m2)) as days_since_last, ' \
                       'm2 as "Last Activity Date", ' \
                       'registration ' \
                       'from ' \
                       '(select ' \
                       'a.user_id, ' \
                       'm2, ' \
                       'if(DATEDIFF(NOW(),b.created_at)<90,"NEW",if(DATEDIFF(NOW(),b.created_at)<270,"Medium","Old")) as registration ' \
                       'from ' \
                       '(select ' \
                       'a.user_id as user_id, ' \
                       'm2 ' \
                       'from ' \
                       '(select ' \
                       'b.user_id, ' \
                       'a.updated_at as m2 ' \
                       'from ' \
                       't_user_course_tracker a ' \
                       'join t_user_course b on a.user_course_id = b.id ' \
                       'and a.status!="NOT_STARTED" ' \
                       'and a.updated_at!="2020-07-01 18:26:12" ' \
                       'and a.updated_at!="2020-07-06 15:07:40" ' \
                       'and a.updated_at!="2020-07-25 13:50:41" ' \
                       'and a.updated_at!="2020-05-12 14:22:04" ' \
                       'and a.updated_at!="2020-05-15 10:45:31" ' \
                       'and a.updated_at!="2020-05-15 10:45:05" ' \
                       'and a.updated_at!="2020-05-15 11:16:07" ' \
                       'and a.updated_at!="2020-05-15 11:24:38" ' \
                       'and a.updated_at!="2020-06-29 12:14:08" ' \
                       'and a.updated_at!="2020-06-29 06:44:08" ' \
                       'and a.updated_at!="2019-12-19 09:59:40" ' \
                       'and a.updated_at!="2020-05-12 19:52:04" ' \
                       'and a.updated_at!="2020-05-15 16:54:38" ' \
                       'and a.updated_at!="2019-12-19 13:13:54" ' \
                       'and a.updated_at!="2019-12-19 10:10:54" ' \
                       'and a.updated_at!="2019-12-19 13:13:54" ' \
                       'and a.updated_at!="2020-05-15 16:15:31" ' \
                       'and a.updated_at!="2020-06-23 13:15:17" ' \
                       'and a.updated_at!="2019-12-19 10:22:39" ' \
                       'and a.updated_at!="2020-06-28 13:36:54" ' \
                       'and a.updated_at!="2020-05-15 16:46:07" ' \
                       'group by a.user_course_id, ' \
                       'date(a.updated_at)) a ' \
                       'union ' \
                       '(select ' \
                       'user_id, ' \
                       'a.updated_at as m2 ' \
                       'from ' \
                       't_user_activity_tracker a ' \
                       'where ' \
                       '(status="SEEN" or forward_swipe_count!=0 or backward_swipe_count!=0) order by a.updated_at desc) ' \
                       'union ' \
                       '(select ' \
                       'user_id, ' \
                       'a.updated_at as m2 ' \
                       'from t_user_webinar a ' \
                       'where attended_live = 1) ' \
                       'union ' \
                       '(select ' \
                       'user_id, ' \
                       'start_time as m2 ' \
                       'from ' \
                       't_user_product_wise_time_tracker order by start_time desc ) ) a ' \
                       'join t_user b ' \
                       'join t_user_role c on a.user_id = b.id ' \
                       'and b.id = c.user_id ' \
                       'where c.role_id = 3 ' \
                       'and CAST(AES_DECRYPT(FROM_BASE64(b.email),"0z74P67WdkoeKXCc") AS CHAR(50)) like "%@%" ' \
                       'and CAST(AES_DECRYPT(FROM_BASE64(b.email),"0z74P67WdkoeKXCc") AS CHAR(50)) not like "%dummy%" ' \
                       'and CAST(AES_DECRYPT(FROM_BASE64(b.email),"0z74P67WdkoeKXCc") AS CHAR(50)) not like "%test%"  ' \
                       'group by user_id, m2 ' \
                       'order by m2 desc' \
                       ') a ' \
                       'group by user_id'
    dfLastVisited = pd.read_sql(text(queryLastVisited), dbConnection);
    df = pd.merge(df, dfLastVisited.rename(columns={'User ID': 'User ID'}), on='User ID', how='left')
    values = {"Number Of Visits": 0}
    df.fillna(value=values)
    print("####################End Activity Level Query######################")

    print("****************Activity Level*********************")
    # print(len(df.axes[0]))
    #Format Registration Date
    #df['Registration Date'] = pd.to_datetime(df['Registration Date'], format='%Y-%m-%d %H:%M:%S')
    #Filling Null Value For Last Active date
    df['Last Activity Date'].fillna("2000-01-01 00:00:00", inplace=True)
    df['registration'].fillna("Old", inplace=True)
    df['Number Of Visits'].fillna(0, inplace=True)
    # Format Registration Date
    df['Last Activity Date'] = pd.to_datetime(df['Last Activity Date'], format='%Y-%m-%d %H:%M:%S')
    # Format Number Of Visits
    df["Number Of Visits"] = df["Number Of Visits"].astype(int)
    #Find Last 3 & 9 month date
    now = pd.to_datetime('now', format='%Y-%m-%d %H:%M:%S')
    last3 = now - pd.DateOffset(months=3)
    last9 = now - pd.DateOffset(months=9)

    # Old Registered Users
    print("****************Activity Level Old Users*********************")
    # Less
    df.loc[(df['registration'] == "Old") & (df['Number Of Visits'] < 2), ['Activity Level']] = 'LESS'
    df.loc[(df['registration'] == "Old") & (df['days_since_last'] > 270), ['Activity Level']] = 'LESS'
    # Medium
    df.loc[(df['registration'] == "Old") & (df['days_since_last'] <= 270) & (df['Number Of Visits'] < 4) & (
            df['Number Of Visits'] > 1), ['Activity Level']] = 'MEDIUM'
    df.loc[(df['registration'] == "Old") & (df['days_since_last'] >= 270) & (df['days_since_last'] <= 90) & (
            df['Number Of Visits'] > 3), ['Activity Level']] = 'MEDIUM'
    # High
    df.loc[(df['registration'] == "Old") & (df['days_since_last'] <= 90) & (df['Number Of Visits'] > 3), [
        'Activity Level']] = 'HIGH'

    # Medium Registered Users
    # Less
    df.loc[(df['registration'] == "Medium") & (df['Number Of Visits'] < 2), ['Activity Level']] = 'LESS'
    df.loc[(df['registration'] == "Medium") & (df['days_since_last'] > 270), ['Activity Level']] = 'LESS'
    # Medium
    df.loc[(df['registration'] == "Medium") & (df['days_since_last'] >= 270) & (df['days_since_last'] <= 90) & (
            df['Number Of Visits'] > 1), ['Activity Level']] = 'MEDIUM'
    # High
    df.loc[(df['registration'] == "Medium") & (df['days_since_last'] <= 90) & (df['Number Of Visits'] > 1), [
        'Activity Level']] = 'HIGH'

    # New Registered Users
    # Less
    df.loc[(df['registration'] == "NEW") & (df['Number Of Visits'] < 2), ['Activity Level']] = 'LESS'
    # Medium
    df.loc[(df['registration'] == "NEW") & (df['Number Of Visits'] > 1), ['Activity Level']] = 'MEDIUM'
    # High
    df.loc[(df['registration'] == "NEW") & (df['Number Of Visits'] > 3), ['Activity Level']] = 'HIGH'


    print("####################End Activity Level ######################")
    #TODO: remove
    df["Activity Level"].fillna("MEDIUM", inplace=True)
    df.to_csv('/home/santhosh-omni/data/data-v221-stg.csv', index=False)
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
    # df = pd.read_csv('/home/santhosh-omni/data/data-v81.csv')
    df = pd.read_csv('https://s3.ap-southeast-1.amazonaws.com/omnicuris.assets/marketing/data/stg/data.csv')
    # df = pd.read_csv('/home/santhosh-omni/data/data-v7.csv')
    # Specify Enrollment Type
    # df["rep_code"] = df["rep_code"].astype(int)
    # df.loc[(df['rep_code'] > 0), ['Enrollment Type']] = 'REP'
    # df.loc[(df['rep_code'] == 0), ['Enrollment Type']] = 'ORGANIC'
    # df.to_csv('/home/santhosh-omni/data/data-v5.csv', index=False)
    arr = df["User ID"].to_numpy()
    arr = list(set(arr))
    for col in df.columns:
        print(col)
    print("This size is", len(arr))
    if 'enrollmentType' in request_data:
        dfTempEnrollment = None
        if result is None:
            result = df
        for e in request_data['enrollmentType']:
            if e == "ORGANIC":
                res = result[(result['rep_code'] == 0)]
                dfTempEnrollment = pd.concat([dfTempEnrollment, res])
            elif e == "REP":
                res = result[(result['rep_code'] > 0)]
                dfTempEnrollment = pd.concat([dfTempEnrollment, res])
        result = dfTempEnrollment

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

    if 'cmeIds' in request_data:
        sqlEngine = create_engine(
            db_url,
            pool_recycle=36000)
        dbConnection = sqlEngine.connect()
        dfCmeIds = None
        if result is None:
            result = df
        string_ints = [str(int) for int in request_data['cmeIds']]
        print(','.join(string_ints))
        # Find The User Ids For That CME
        queryUserCme = "Select user_id as id From t_user_course where course_id in ("+','.join(string_ints)+")"
        dfCmeUserIds = pd.read_sql(text(queryUserCme), dbConnection);
        res = result[result['User ID'].isin(dfCmeUserIds['id'])]
        dfCmeIds = pd.concat([dfCmeIds, res])
        result = dfCmeIds

    if 'surveyIds' in request_data:
        sqlEngine = create_engine(
            db_url,
            pool_recycle=36000)
        dbConnection = sqlEngine.connect()
        dfSurveyIds = None
        if result is None:
            result = df
        string_ints = [str(int) for int in request_data['surveyIds']]
        # Find The User Ids For That Survey
        queryUserSurvey = "Select user_id as id From t_user_survey where survey_id in ("+','.join(string_ints)+")"
        dfSurveyUserIds = pd.read_sql(text(queryUserSurvey), dbConnection);
        res = result[result['User ID'].isin(dfSurveyUserIds['id'])]
        dfSurveyIds = pd.concat([dfSurveyIds, res])
        result = dfSurveyIds

    if 'medshotIds' in request_data:
        sqlEngine = create_engine(
            db_url,
            pool_recycle=36000)
        dbConnection = sqlEngine.connect()
        dfMedshotIds = None
        if result is None:
            result = df
        string_ints = [str(int) for int in request_data['medshotIds']]
        # Find The User Ids For That Medhsot
        queryUserMedshot = "Select user_id as id From t_user_medshot_project where medshot_project_id in ("+','.join(string_ints)+")"
        dfMedshotUserIds = pd.read_sql(text(queryUserMedshot), dbConnection);
        res = result[result['User ID'].isin(dfMedshotUserIds['id'])]
        print(res)
        dfMedshotIds = pd.concat([dfMedshotIds, res])
        result = dfMedshotIds

    if 'patientProjectIds' in request_data:
        sqlEngine = create_engine(
            db_url,
            pool_recycle=36000)
        dbConnection = sqlEngine.connect()
        dfPPIds = None
        if result is None:
            result = df
        string_ints = [str(int) for int in request_data['patientProjectIds']]
        # Find The User Ids For That Patient
        queryUserPP = "Select doctor_id as id From t_qr_map where project_id in ("+','.join(string_ints)+")"
        print(queryUserPP)
        dfPPUserIds = pd.read_sql(text(queryUserPP), dbConnection);
        res = result[result['User ID'].isin(dfPPUserIds['id'])]
        dfPPIds = pd.concat([dfPPIds, res])
        result = dfPPIds

    if 'marketData' in request_data:
        dfTempMarket = None
        if result is None:
            result = df
        # Mail Unsubscribed
        # if "UNSUBSCRIBED" in request_data['marketData']:
        #     res = result[(result['isUnsubscribed'] == "YES")]
        #     dfTempMarket = pd.concat([dfTempMarket, res])
        # # Not installed
        # if "NOT_INSTALLED" in request_data['marketData']:
        #     res = result[(result['isUninstalled'] == "N/A")]
        #     dfTempMarket = pd.concat([dfTempMarket, res])
        # # Uninstalled
        # if "UNINSTALLED" in request_data['marketData']:
        #     res = result[(result['isUninstalled'] == "YES")]
        #     dfTempMarket = pd.concat([dfTempMarket, res])
        for e in request_data['marketData']:
            # Mail Unsubscribed
            if e =="UNSUBSCRIBED":
                res = result[(result['isUnsubscribed'] == "YES")]
                dfTempMarket = pd.concat([dfTempMarket, res])
            # Not installed
            if e == "NOT_INSTALLED":
                res = result[(result['isUninstalled'] == "N/A")]
                dfTempMarket = pd.concat([dfTempMarket, res])
                res = result[(result['isUninstalled'].isna())]
                dfTempMarket = pd.concat([dfTempMarket, res])
            # Uninstalled
            if e == "UNINSTALLED":
                res = result[(result['isUninstalled'] == "YES")]
                dfTempMarket = pd.concat([dfTempMarket, res])
        result = dfTempMarket

    if result is None:
        return None
    arrCheck = result["User ID"].to_numpy()
    print("Total: ", len(arrCheck))
    # response.headers.add("Access-Control-Allow-Origin", "*")
    # response = Flask.jsonify({'data': result.to_csv()})
    # response.headers.add("Access-Control-Allow-Origin", "*")
    # result = result.drop(['status', 'speciality_id', 'tracker_id', 'progress_mean', 'progress', 'mean', 'rep_code'], axis = 1)
    result = result[['User ID', 'First Name', 'Last Name', 'Email', 'Contact No.', 'Location/Region', 'Speciality Of Interest', 'Engagement Level','Activity Level', 'Registration Date','Number Of Visits', 'Last Activity Date','Enrollment Type', 'isEmailVerified', 'isDND','isMCIVerified', 'isUninstalled', 'isUnsubscribed' ]]
    # return result.to_csv(index=False)
    return result.to_csv(index=False)

@app.route('/cogent/v1/dashboard', methods=['POST'])
# @cross_origin()
def analytics():
    request_data = request.json
    # data = json.loads(request_data)
    print("**************************************")
    print(request)
    print(format(request_data))
    print("**************************************")
    result = None
    sqlEngine = create_engine(
        db_url,
        pool_recycle=36000)
    dbConnection = sqlEngine.connect()

    query = ''
    query += 'SELECT ump.user_id,u.current_device,u.device_token,u.uninstalled,ump.created_at, ' \
             'r.name as region ' \
             'FROM t_user_medshot_project ump ' \
             'Join t_user u On u.id = ump.user_id ' \
             'Left Join m_region r On u.registration_region_id = r.id  ' \
             'Where ump.is_archived = 0 ' \
             'And u.is_archived = 0 '

    if 'range_start' in request_data:
        query+='And ump.created_at between \"'+request_data['range_start']+'\" And \"'+request_data['range_end'] +'\" '
    elif 'interval_months' in request_data:
        query += 'And ump.created_at >= DATE_ADD(NOW(), INTERVAL -' + str(request_data['interval_months']) + ' Month) '
    elif 'interval_days' in request_data:
        query += 'And ump.created_at >= DATE_ADD(NOW(), INTERVAL -' + str(request_data['interval_days']) + ' Day) '

    query+='group by ump.user_id'

    # print (query)


    dfMedshotUser = pd.read_sql(query, dbConnection)

    # print(dfMedshotUser)

    ## Total Users
    arr = dfMedshotUser["user_id"].to_numpy()
    arr = list(set(arr))
    totUser = len(arr)

    ## Total Web User
    totWeb = dfMedshotUser["current_device"].isnull().sum()
    # print(totWeb)

    ##Total App Users
    ## Total Android
    totMobAnd = len(dfMedshotUser[dfMedshotUser['current_device'].str.contains("ANDROID", regex=False, na=False)].index)
    ## Total IOS
    totMobIos = len(dfMedshotUser[dfMedshotUser['current_device'].str.contains("IOS", regex=False, na=False)].index)
    ## Total Mobile
    totApp = totMobAnd + totMobIos
    print(totApp)
    ## Total App Downloaded
    appDownload = dfMedshotUser["device_token"].notnull().sum()


    string_ints = [str(int) for int in arr]

    # queryActiveUser = ''
    # # 'where user_id in ('+(",".join(string_ints)) +') ' \
    # queryActiveUser += 'SELECT * FROM t_user_article_tracker ' \
    #                    'where is_archived = 0 '
    # if 'range_start' in request_data:
    #     query+='And created_at between \"'+request_data['range_start']+'\" And \"'+request_data['range_end'] +'\" '
    # elif 'interval_months' in request_data:
    #     query += 'And created_at >= DATE_ADD(NOW(), INTERVAL -' + str(request_data['interval_months']) + ' Month) '
    #
    # queryActiveUser += 'group by user_id'
    # # print(queryActiveUser)
    #
    # dfActiveUser = pd.read_sql(queryActiveUser, dbConnection)
    # # print(dfActiveUser)

    querySpeciality = ''
    querySpeciality += 'SELECT uat.*,s.id as speciality_id,s.name as speciality_name, r.name as region FROM omnicuris.t_user_article_tracker uat ' \
                       'Join t_article_speciality asp On asp.article_id = uat.article_id ' \
                       'Join m_speciality s On s.id = asp.speciality_id ' \
                       'Join t_user u On uat.user_id = u.id ' \
                       'Left Join m_region r On u.registration_region_id = r.id ' \
                       'where uat.is_archived = 0 ' \
                       'And s.is_archived = 0 '
    if 'range_start' in request_data:
        querySpeciality += 'And uat.created_at between \"' + request_data['range_start'] + '\" And \"' + request_data['range_end'] + '\" '
    elif 'interval_months' in request_data:
        querySpeciality += 'And uat.created_at >= DATE_ADD(NOW(), INTERVAL -' + str(request_data['interval_months']) + ' Month)  '
    elif 'interval_days' in request_data:
        querySpeciality += 'And uat.created_at >= DATE_ADD(NOW(), INTERVAL -' + str(request_data['interval_days']) + ' Day)  '

    # print(querySpeciality)

    dfSpeciality = pd.read_sql(text(querySpeciality), dbConnection)
    # print(dfSpeciality)

    ## Top Speciality
    # dfTop = dfSpeciality.groupby(['speciality_name'])['name'].describe()[['count']]
    dfTop = dfSpeciality.groupby('speciality_name').size().sort_values(ascending=False)
    # print (dfTop.to_json(orient ='columns'))
    sepcInfo = []
    # dfTop['percent'] = (dfTop[1] /
    #                   dfTop[1].sum()) * 100
    # dfTop = dfTop / dfTop.groupby(level=[0]).transform("sum")
    # for x in dfTop.to_json(orient ='columns'):
    dct = json.loads(dfTop.to_json(orient ='columns'))
    print(dct)
    for key in dct.keys():
        sepcInfo.append({"name": key, "count": dct[key] })
    # print(sepcInfo)

    ## State wise
    dfState = dfSpeciality.groupby('region').size().sort_values(ascending=False)
    # print(dfState)
    # print(dfState.to_json(orient='columns'))
    stateInfo = []
    # for x in dfTop.to_json(orient ='columns'):
    dct = json.loads(dfState.to_json(orient='columns'))
    for key in dct.keys():
        stateInfo.append({"x": key, "y": dct[key]})
    # print(sepcInfo)

    ## Active Users
    activeUser = len(dfSpeciality['user_id'].unique())

    x = {
        "user_info":{
            "total":{
                "user": int(totUser),
                "web": int(totWeb),
                "app": {
                    "tot": int(totApp),
                    "android": int(totMobAnd),
                    "ios": int(totMobIos)
                },
            },
            "app_downloaded": int(appDownload),
            "active_user": int(activeUser),
        },
        "speciality": {
            "info": sepcInfo,
            "state_wise": stateInfo
        }
    }
    return x

@app.route('/cogent/v2/dashboard/active', methods=['POST'])
# @cross_origin()
def analyticsActive():
    request_data = request.json
    # data = json.loads(request_data)
    print("**************************************")
    print(request)
    print(format(request_data))
    print("**************************************")
    result = None
    sqlEngine = create_engine(
        db_url,
        pool_recycle=36000)
    dbConnection = sqlEngine.connect()

    queryActiveUser = 'Select count(distinct(user_id)) as count From t_user_article_tracker ' \
                      'Where is_archived = false ' \
                      'And user_id Not In (Select distinct(user_id) From t_user_medshot_project) '
    if 'range_start' in request_data:
        queryActiveUser += 'And created_at Between cast("' + str(
            request_data['range_start']) + '" as Date) And cast(DATE_ADD("' + str(
            request_data['range_end']) + '", INTERVAL 1 Day) as Date) '
    elif 'interval_days' in request_data:
        queryActiveUser += 'And cast(created_at as Date) >= cast(DATE_ADD(NOW(), INTERVAL -' + str(
            request_data['interval_days']) + ' Day) as Date) '
    dfActiveAllUser = pd.read_sql(queryActiveUser, dbConnection)

    queryActiveUser = 'Select count(distinct(user_id)) as count From t_user_article_tracker ' \
                      'Where is_archived = false ' \
                      'And user_id In (Select distinct(user_id) From t_user_medshot_project) '
    if 'range_start' in request_data:
        queryActiveUser += 'And created_at Between cast("' + str(
            request_data['range_start']) + '" as Date) And cast(DATE_ADD("' + str(
            request_data['range_end']) + '", INTERVAL 1 Day) as Date) '
    elif 'interval_days' in request_data:
        queryActiveUser += 'And cast(created_at as Date) >= cast(DATE_ADD(NOW(), INTERVAL -' + str(
            request_data['interval_days']) + ' Day) as Date) '
        print(queryActiveUser)
    dfActiveInUser = pd.read_sql(queryActiveUser, dbConnection)
    totalActiveData = {
        "organic": int(dfActiveAllUser["count"]),
        "in_organic": int(dfActiveInUser["count"])
    }

    return totalActiveData

@app.route('/cogent/v2/dashboard/reg', methods=['POST'])
# @cross_origin()
def analyticsActiveReg():
    request_data = request.json
    # data = json.loads(request_data)
    print("**************************************")
    print(request)
    print(format(request_data))
    print("**************************************")
    result = None
    sqlEngine = create_engine(
        db_url,
        pool_recycle=36000)
    dbConnection = sqlEngine.connect()

    # queryInOrganicUser = 'Select count(*) as count From (Select user_id, min(created_at) dat From t_user_medshot_project ' \
    #                      'Where is_archived = false '
    # if 'range_start' in request_data:
    #     queryInOrganicUser += 'Group By user_id Having dat Between cast("' + str(
    #         request_data['range_start']) + '" as Date) And cast(DATE_ADD("' + str(
    #         request_data['range_end']) + '", INTERVAL 1 Day) as Date) '
    # elif 'interval_days' in request_data:
    #     queryInOrganicUser += 'Group By user_id Having dat > cast(DATE_ADD(NOW(), INTERVAL -' + str(
    #         request_data['interval_days']) + ' Day) as Date) '
    # queryInOrganicUser += ' ) as k'
    # dfInOrganicUser = pd.read_sql(queryInOrganicUser, dbConnection)
    #
    # queryOrganicUser = 'SELECT Count(*) as count From (SELECT user_id, MIN(created_at) dat ' \
    #                    'FROM t_user_article_tracker ' \
    #                    'Where is_archived = false ' \
    #                    'And user_id not in (Select distinct(user_id) From t_user_medshot_project where is_archived = false) ' \
    #                                                                       'GROUP BY user_id HAVING '
    # if 'range_start' in request_data:
    #     queryOrganicUser += 'dat Between cast("'+str(request_data['range_start'])+'" as Date) And cast(DATE_ADD("'+str(request_data['range_end'])+'", INTERVAL 1 Day) as Date) '
    # elif 'interval_days' in request_data:
    #     queryOrganicUser += 'cast(dat as Date) > cast(DATE_ADD(NOW(), INTERVAL -'+str(request_data['interval_days'])+' Day) as Date) '
    # queryOrganicUser += ') as k'
    # dfOrganicUser = pd.read_sql(queryOrganicUser, dbConnection)

    query = ''
    query += 'SELECT '
    if 'range_start' in request_data:
        query += 'COUNT(IF(e.access Between cast("'+str(request_data['range_start'])+'" as Date) And cast(DATE_ADD("'+str(request_data['range_end'])+'", INTERVAL 1 Day) as Date),1,NULL)) in_organic, '
        query += 'COUNT(IF(f.access Between cast("'+str(request_data['range_start'])+'" as Date) And cast(DATE_ADD("'+str(request_data['range_end'])+'", INTERVAL 1 Day) as Date),1,NULL)) organic '
    elif 'interval_days' in request_data:
        query += 'COUNT(IF(cast(e.access as Date) >= cast(DATE_ADD(NOW(), INTERVAL -'+str(request_data['interval_days'])+' Day) as Date),1,NULL)) in_organic, '
        query += 'COUNT(IF(cast(f.access as Date) >= cast(DATE_ADD(NOW(), INTERVAL -'+str(request_data['interval_days'])+' Day) as Date),1,NULL)) organic '
            #
            # 'COUNT(IF(e.access >= DATE_SUB(CURRENT_DATE(),INTERVAL 30 DAY),1,NULL)) in_organic,  ' \
            # 'COUNT(IF(f.access >= DATE_SUB(CURRENT_DATE(),INTERVAL 30 DAY),1,NULL)) organic ' \
    query +='FROM ' \
            't_user a ' \
            'JOIN t_user_role b ' \
            'JOIN t_user_speciality_of_interest c ' \
            'ON a.id = b.user_id ' \
            'AND c.user_id = a.id ' \
            'AND b.role_id = 3 ' \
            'LEFT JOIN m_speciality d ON c.speciality_id = d.id ' \
            'LEFT JOIN (SELECT DISTINCT a.user_id, MIN(a.created_at) access FROM	t_user_article_tracker a LEFT JOIN t_user_medshot_project b on a.user_id = b.user_id WHERE b.id IS NOT NULL GROUP BY a.user_id) e ON a.id = e.user_id ' \
            'LEFT JOIN (SELECT DISTINCT a.user_id, MIN(a.created_at) access FROM	t_user_article_tracker a LEFT JOIN t_user_medshot_project b on a.user_id = b.user_id WHERE b.id IS NULL GROUP BY a.user_id) f ON a.id = f.user_id ' \
            'WHERE ' \
            'c.type = "MEDSHOTS"'

    print(query)
    dfUser = pd.read_sql(text(query), dbConnection)

    totalActiveData = {
        "organic": int(dfUser["organic"]),
        "in_organic": int(dfUser["in_organic"]),
    }

    return totalActiveData

@app.route('/cogent/v2/dashboard/total', methods=['POST'])
# @cross_origin()
def analyticsTotal():
    request_data = request.json
    sqlEngine = create_engine(
        db_url,
        pool_recycle=36000)
    dbConnection = sqlEngine.connect()

    queryAllUser = ''
    queryAllUser += 'Select distinct(u.id), u.current_device From t_user u ' \
                    'Join t_user_speciality_of_interest soi On soi.user_id = u.id ' \
                    'Join t_user_role ur On ur.user_id = u.id ' \
                    'Where soi.type = "MEDSHOTS" ' \
                    'And soi.is_archived = false ' \
                    'And ur.role_id = 3 ' \
                    'And soi.is_archived = false ' \
                    'And u.id in (Select user_id From t_user_medshot_project) '
    dfAllUser = pd.read_sql(text(queryAllUser), dbConnection)

    ## Total Users
    # arr = dfAllUser["user_id"].to_numpy()
    # arr = list(set(arr))
    totUserO = len(dfAllUser["id"].to_numpy())
    totUserO = len(dfAllUser["id"].to_numpy())
    ##Total App Users
    ## Total Android
    totMobAndO = len(dfAllUser[dfAllUser['current_device'].str.contains("ANDROID", regex=False, na=False)].index)
    ## Total IOS
    totMobIosO = len(dfAllUser[dfAllUser['current_device'].str.contains("IOS", regex=False, na=False)].index)

    queryAllUser = ''
    queryAllUser += 'Select distinct(u.id), u.current_device From t_user u ' \
                    'Join t_user_speciality_of_interest soi On soi.user_id = u.id ' \
                    'Join t_user_role ur On ur.user_id = u.id ' \
                    'Where soi.type = "MEDSHOTS" ' \
                    'And soi.is_archived = false ' \
                    'And ur.role_id = 3 ' \
                    'And u.id not in (Select user_id From t_user_medshot_project) '

    dfAllUser = pd.read_sql(text(queryAllUser), dbConnection)

    ## Total Users
    # arr = dfAllUser["user_id"].to_numpy()
    # arr = list(set(arr))
    totUserIn = len(dfAllUser["id"].to_numpy())
    ##Total App Users
    ## Total Android
    totMobAndIn = len(
        dfAllUser[dfAllUser['current_device'].str.contains("ANDROID", regex=False, na=False)].index)
    ## Total IOS
    totMobIosIn = len(dfAllUser[dfAllUser['current_device'].str.contains("IOS", regex=False, na=False)].index)


    x = {
        "organic": {
            "app": {
                "android": totMobAndO,
                "ios": totMobIosO
            },
            "web": totUserO - (totMobAndO + totMobIosO),
            "tot": totUserO,
        },
        "in_organic": {
            "app": {
                "android": totMobAndIn,
                "ios": totMobIosIn
            },
            "web": totUserIn - (totMobAndIn + totMobIosIn),
            "tot": totUserIn,
        }
    }

    return x

@app.route('/cogent/v2/dashboard/spec', methods=['POST'])
# @cross_origin()
def analyticsTotSpec():
    request_data = request.json
    # data = json.loads(request_data)
    result = None
    sqlEngine = create_engine(
        db_url,
        pool_recycle=36000)
    dbConnection = sqlEngine.connect()

    querySpeciality = 'SELECT ' \
                      'd.`name`, ' \
                      'COUNT(DISTINCT a.id) count ' \
                      'FROM t_user a ' \
                      'JOIN t_user_role b ' \
                      'JOIN t_user_speciality_of_interest c ' \
                      'ON a.id = b.user_id  ' \
                      'AND c.user_id = a.id ' \
                      'AND b.role_id = 3 ' \
                      'LEFT JOIN m_speciality d ON c.speciality_id = d.id ' \
                      'WHERE ' \
                      'c.type = "MEDSHOTS" ' \
                      'GROUP BY d.id'
    # print(querySpeciality)

    dfSpeciality = pd.read_sql(text(querySpeciality), dbConnection)
    dfTop = dfSpeciality.sort_values(['count'],ascending=False)
    specInfoTot = []
    for index, row in dfTop.iterrows():
        specInfoTot.append({"name": row["name"], "count": int(row["count"])})

    querySpeciality = 'SELECT ' \
                      'd.`name`, ' \
                      'COUNT(DISTINCT a.id) count ' \
                      'FROM t_user a ' \
                      'JOIN t_user_role b ' \
                      'JOIN t_user_speciality_of_interest c ' \
                      'ON a.id = b.user_id  ' \
                      'AND c.user_id = a.id ' \
                      'AND b.role_id = 3 ' \
                      'LEFT JOIN m_speciality d ON c.speciality_id = d.id ' \
                      'WHERE ' \
                      'c.type = "MEDSHOTS" ' \
                      'And a.id In (Select user_id From t_user_medshot_project where is_archived = false)' \
                      'GROUP BY d.id'
    # print(querySpeciality)

    dfSpeciality = pd.read_sql(text(querySpeciality), dbConnection)
    dfTop = dfSpeciality.sort_values(['count'],ascending=False)
    specInfoIn = []
    for index, row in dfTop.iterrows():
        specInfoIn.append({"name": row["name"], "count": int(row["count"])})

    querySpeciality = 'SELECT ' \
                      'd.`name`, ' \
                      'COUNT(DISTINCT a.id) count ' \
                      'FROM t_user a ' \
                      'JOIN t_user_role b ' \
                      'JOIN t_user_speciality_of_interest c ' \
                      'ON a.id = b.user_id  ' \
                      'AND c.user_id = a.id ' \
                      'AND b.role_id = 3 ' \
                      'LEFT JOIN m_speciality d ON c.speciality_id = d.id ' \
                      'WHERE ' \
                      'c.type = "MEDSHOTS" ' \
                      'And a.id Not In (Select user_id From t_user_medshot_project where is_archived = false)' \
                      'GROUP BY d.id'
    # print(querySpeciality)

    dfSpeciality = pd.read_sql(text(querySpeciality), dbConnection)
    dfTop = dfSpeciality.sort_values(['count'],ascending=False)
    specInfoO = []
    for index, row in dfTop.iterrows():
        specInfoO.append({"name": row["name"], "count": int(row["count"])})
    #
    # ######
    # querySpeciality = 'SELECT ' \
    #                   'ms.`name` name, ' \
    #                   'COUNT(DISTINCT tusi.user_id) count ' \
    #                   'FROM ' \
    #                   'm_speciality ms ' \
    #                   'LEFT JOIN t_user_speciality_of_interest tusi ON ms.id = tusi.speciality_id ' \
    #                   'LEFT JOIN t_user_article_tracker tuat ON tusi.user_id = tuat.user_id ' \
    #                   'WHERE ' \
    #                   'ms.enable_feed = 1 ' \
    #                   'And tusi.user_id NOT IN (SELECT user_id FROM `t_user_medshot_project` where is_archived = false) ' \
    #                   'GROUP BY ms.id'
    # # print(querySpeciality)
    #
    # dfSpeciality = pd.read_sql(text(querySpeciality), dbConnection)
    # dfTop = dfSpeciality.sort_values(['count'], ascending=False)
    # specInfoO = []
    # for index, row in dfTop.iterrows():
    #     specInfoO.append({"name": row["name"], "count": int(row["count"])})
    #
    #
    # querySpeciality = 'SELECT ' \
    #                   'ms.`name` name, ' \
    #                   'COUNT(DISTINCT tusi.user_id) count ' \
    #                   'FROM ' \
    #                   'm_speciality ms ' \
    #                   'LEFT JOIN t_user_speciality_of_interest tusi ON ms.id = tusi.speciality_id ' \
    #                   'LEFT JOIN t_user_article_tracker tuat ON tusi.user_id = tuat.user_id ' \
    #                   'WHERE ' \
    #                   'ms.enable_feed = 1 ' \
    #                   'And tusi.user_id IN (SELECT user_id FROM `t_user_medshot_project` where is_archived = false) ' \
    #                   'GROUP BY ms.id'
    #
    # dfSpeciality = pd.read_sql(text(querySpeciality), dbConnection)
    # dfTop = dfSpeciality.sort_values(['count'], ascending=False)
    # specInfoIn = []
    # for index, row in dfTop.iterrows():
    #     specInfoIn.append({"name": row["name"], "count": int(row["count"])})
    specInfoO.sort(key=lambda x: x["count"], reverse=True)
    specInfoIn.sort(key=lambda x: x["count"], reverse=True)
    totalActiveData = {
        "total": specInfoTot,
        "organic": specInfoO,
        "in_organic": specInfoIn
    }

    return totalActiveData


@app.route('/cogent/v2/dashboard', methods=['POST'])
# @cross_origin()
# @cross_origin(supports_credentials=False)
def analyticsV2():
    request_data = request.json
    # data = json.loads(request_data)
    print("**************************************")
    print(request)
    print(format(request_data))
    print("**************************************")
    result = None
    sqlEngine = create_engine(
        db_url,
        pool_recycle=36000)
    dbConnection = sqlEngine.connect()

    queryAllUser = ''
    queryAllUser += 'SELECT uat.user_id,u.current_device,u.device_token,u.uninstalled, uat.created_at, ' \
                    'r.name as region ' \
                    'FROM t_user_activity_tracker uat ' \
                    'Join t_user u On u.id = uat.user_id ' \
                    'Left Join m_region r On u.registration_region_id = r.id ' \
                    'Where uat.is_archived = 0 ' \
                    'And u.is_archived = 0 ' \
                    'Group By uat.user_id ' \
                    'Order by uat.created_at desc'

    # if 'range_start' in request_data:
    #     query+='And ump.created_at between \"'+request_data['range_start']+'\" And \"'+request_data['range_end'] +'\" '
    # elif 'interval_months' in request_data:
    #     query += 'And ump.created_at >= DATE_ADD(NOW(), INTERVAL -' + str(request_data['interval_months']) + ' Month) '
    # elif 'interval_days' in request_data:
    #     query += 'And ump.created_at >= DATE_ADD(NOW(), INTERVAL -' + str(request_data['interval_days']) + ' Day) '
    #
    # query+='group by ump.user_id'

    # print (query)


    dfMedshotUser = pd.read_sql(queryAllUser, dbConnection)
    # print("------------------------------------------------------------")
    # print(len(dfMedshotUser.index))
    # print("------------------------------------------------------------")
    # print(dfMedshotUser)

    ## Total Users
    arr = dfMedshotUser["user_id"].to_numpy()
    arr = list(set(arr))
    totUser = len(arr)

    ## Total Web User
    totWeb = dfMedshotUser["current_device"].isnull().sum()
    # print(totWeb)

    ##Total App Users
    ## Total Android
    totMobAnd = len(dfMedshotUser[dfMedshotUser['current_device'].str.contains("ANDROID", regex=False, na=False)].index)
    ## Total IOS
    totMobIos = len(dfMedshotUser[dfMedshotUser['current_device'].str.contains("IOS", regex=False, na=False)].index)
    ## Total Mobile
    totApp = totMobAnd + totMobIos
    print(totApp)
    ## Total App Downloaded
    appDownload = dfMedshotUser["device_token"].notnull().sum()


    string_ints = [str(int) for int in arr]

    # queryActiveUser = ''
    # # 'where user_id in ('+(",".join(string_ints)) +') ' \
    # queryActiveUser += 'SELECT * FROM t_user_article_tracker ' \
    #                    'where is_archived = 0 '
    # if 'range_start' in request_data:
    #     query+='And created_at between \"'+request_data['range_start']+'\" And \"'+request_data['range_end'] +'\" '
    # elif 'interval_months' in request_data:
    #     query += 'And created_at >= DATE_ADD(NOW(), INTERVAL -' + str(request_data['interval_months']) + ' Month) '
    #
    # queryActiveUser += 'group by user_id'
    # # print(queryActiveUser)
    #
    # dfActiveUser = pd.read_sql(queryActiveUser, dbConnection)
    # # print(dfActiveUser)

    querySpeciality = ''
    querySpeciality += 'SELECT uat.*,s.id as speciality_id,s.name as speciality_name, r.name as region FROM omnicuris.t_user_article_tracker uat ' \
                       'Join t_article_speciality asp On asp.article_id = uat.article_id ' \
                       'Join m_speciality s On s.id = asp.speciality_id ' \
                       'Join t_user u On uat.user_id = u.id ' \
                       'Left Join m_region r On u.registration_region_id = r.id ' \
                       'where uat.is_archived = 0 ' \
                       'And s.is_archived = 0 '
    if 'range_start' in request_data:
        querySpeciality += 'And uat.created_at between \"' + request_data['range_start'] + '\" And \"' + request_data['range_end'] + '\" '
    elif 'interval_months' in request_data:
        querySpeciality += 'And uat.created_at >= DATE_ADD(NOW(), INTERVAL -' + str(request_data['interval_months']) + ' Month)  '
    elif 'interval_days' in request_data:
        querySpeciality += 'And uat.created_at >= DATE_ADD(NOW(), INTERVAL -' + str(request_data['interval_days']) + ' Day)  '

    # print(querySpeciality)

    dfSpeciality = pd.read_sql(text(querySpeciality), dbConnection)
    # print(dfSpeciality)

    ## Top Speciality
    # dfTop = dfSpeciality.groupby(['speciality_name'])['name'].describe()[['count']]
    dfTop = dfSpeciality.groupby('speciality_name').size().sort_values(ascending=False)
    # print (dfTop.to_json(orient ='columns'))
    sepcInfo = []
    # dfTop['percent'] = (dfTop[1] /
    #                   dfTop[1].sum()) * 100
    # dfTop = dfTop / dfTop.groupby(level=[0]).transform("sum")
    # for x in dfTop.to_json(orient ='columns'):
    dct = json.loads(dfTop.to_json(orient ='columns'))
    print(dct)
    for key in dct.keys():
        sepcInfo.append({"name": key, "count": dct[key] })
    # print(sepcInfo)

    ## State wise
    dfState = dfSpeciality.groupby('region').size().sort_values(ascending=False)
    # print(dfState)
    # print(dfState.to_json(orient='columns'))
    stateInfo = []
    # for x in dfTop.to_json(orient ='columns'):
    dct = json.loads(dfState.to_json(orient='columns'))
    for key in dct.keys():
        stateInfo.append({"x": key, "y": dct[key]})
    # print(sepcInfo)

    ## Active Users
    activeUser = len(dfSpeciality['user_id'].unique())

    x = {
        "user_info":{
            "total":{
                "user": int(totUser),
                "web": int(totWeb),
                "app": {
                    "tot": int(totApp),
                    "android": int(totMobAnd),
                    "ios": int(totMobIos)
                },
            },
            "app_downloaded": int(appDownload),
            "active_user": int(activeUser),
        },
        "speciality": {
            "info": sepcInfo,
            "state_wise": stateInfo
        }
    }
    return x


