import pyodbc
from re import sub
import numpy as np
import pandas as pd
import configparser
from datetime import datetime

import warnings
warnings.simplefilter("ignore", UserWarning)
warnings.filterwarnings("ignore", category=FutureWarning)

def connect_database(var):
    config = configparser.ConfigParser()
    config.read('config_file.ini')

    db_server = config.get(var, "SERVER")
    db_database = config.get(var, "DATABASE")
    db_user = config.get(var, "UID")
    db_password = config.get(var, "PWD")

    conn = pyodbc.connect(driver='{ODBC Driver 17 for SQL Server}',server=db_server, database=db_database,user=db_user, password=db_password)

    return conn

#db = connect_database("prod")

def camel_case(s):
  s = sub(r"(_|-)+", " ", s).title().replace(" ", "")
  return ''.join([s[0], s[1:]])

def camel_case_dict(d):
   if isinstance(d, list):
      return [t_dict(i) if isinstance(i, (dict, list)) else i for i in d]
   return {camel_case(a):t_dict(b) if isinstance(b, (dict, list)) else b for a, b in d.items()}

def fn(a):
    return ''.join(word.title() for word in a.split('_'))


def username_phone(policy):
    '''
    A helper function to fetch Fname,Mobile number of specific policy_no
    returns:
    {'First_Name': 'Fname',
    'Mobile_Number': 9876543210
    }
    '''
    db = connect_database("prod")
    cur = db.cursor()
    query = "SELECT [First_Name],[Mobile_Number] from [dbo].[customer_table] where [Policy_No]=?;"
    cur.execute(query, (policy))
    columns = [d[0] for d in cur.description]
    out = [dict(zip(columns, row)) for row in cur.fetchall()]
    cur.close()
    if len(out)==0:
        return {'First_Name': 'NotFound',
                'Mobile_Number': 'NotFound'
                }
    return out[0]

def customer_care_insert(Customer_Name,Mobile_Number,Policy_Number, Language,Customer_Care_Response):
    print(Customer_Name,Mobile_Number,Policy_Number, Language,Customer_Care_Response)
    db = connect_database("prod")
    cur = db.cursor()
    now = datetime.now()
    formatted_date = now.strftime('%Y-%m-%d %H:%M:%S')

    query = '''
         INSERT INTO [customer_care] ([Customer_Name], [Mobile_Number], [Policy_Number], [Language], [Date_time],[Customer_Care_Response])
             VALUES (?, ?, ?, ?, ?,?)
         '''
    print("before running query ")
    a = cur.execute(query, (Customer_Name,Mobile_Number,Policy_Number, Language, formatted_date,Customer_Care_Response))
    print(a)
    #data = cur.fetchone()
    #print(data)
    print("completed")
    db.commit()
    return "Inserted"



def user_info(policy_no):
    db = connect_database("prod")
    cur = db.cursor()
    query = "SELECT * from [dbo].[customer_table] where [Policy_No]=?;"
    cur.execute(query, (policy_no))

    columns = [d[0] for d in cur.description]
    out = [dict(zip(columns, row)) for row in cur.fetchall()]
    cur.close()

    out = camel_case_dict(out[0])
    out['Dob'] = out['Dob'].strftime('%d/%m/%Y')
    
    return out

def policy_info(policy_no):
    db = connect_database("prod")
    cur = db.cursor()
    query = "SELECT * from [dbo].[policy_table] where [Policy_No]=?;"
    cur.execute(query, (policy_no))

    columns = [d[0] for d in cur.description]
    out = [dict(zip(columns, row)) for row in cur.fetchall()]
    cur.close()

    out = camel_case_dict(out[0])
    out['StartDate'] = out['StartDate'].strftime('%d/%m/%Y')
    out['EndDate'] = out['EndDate'].strftime('%d/%m/%Y')
    if out['PolicyPortingFlag'] == "Yes":
    	out['PolicyPortingFlag'] = True
    else:
        out['PolicyPortingFlag'] = False
    return out

def insured_info_change(data):
    df = pd.DataFrame(data)
    
    df['Customer_ID'] = df[df.columns[1:9]].values.tolist()
    df['Relation_with_policy_holder'] = df[df.columns[9:17]].values.tolist()
    df['First_Name'] = df[df.columns[17:25]].values.tolist()
    df['Last_Name'] = df[df.columns[25:33]].values.tolist()

    df['Gender'] = df[df.columns[33:41]].values.tolist()
    df['DOB'] = df[df.columns[41:49]].values.tolist()
    df['HMB'] = df[df.columns[49:57]].values.tolist()
    df['Health_Disclosure'] = df[df.columns[57:65]].values.tolist()

    test_df = df.iloc[:, [0, 80, 81, 82, 83, 84, 85, 86, 87]]
    
    final_df = test_df.set_index('Policy_No').apply(lambda x: x.apply(pd.Series).stack()).reset_index()
    
    final_df['Dob'] =  final_df['DOB'].dt.strftime('%d/%m/%Y')
    
    final_df = final_df[final_df['Customer_ID'].notnull()]    
    final_df.columns = list(map(fn, final_df.columns))
    final_df['CustomerId'] = final_df['CustomerId'].astype(float)
    final_df['CustomerId'] = final_df['CustomerId'].apply(lambda x: '%.0f' % x)
            
    return final_df

def insured_info(policy_no):
    db = connect_database("prod")
    cur = db.cursor()
    query = "SELECT * FROM [dbo].[insured_table] where [Policy_No]=?;"
    cur.execute(query, (policy_no))
    
    desc = cur.description
    column_names = [col[0] for col in desc]
    
    data = [dict(zip(column_names, row)) for row in cur.fetchall()]
    cur.close()
    
    final_df =  insured_info_change(data)
    final_df = final_df.fillna('')
    
    return final_df.to_dict('records')


def ported_info_change(data):
    df = pd.DataFrame(data)
    
    df['Insured_from'] = df[df.columns[3:11]].values.tolist()
    df['PREV_POLICY_NUMBER'] = df[df.columns[11:19]].values.tolist()
    df['CUMULATIVE_BONUS'] = df[df.columns[19:27]].values.tolist()

    df['Waiting_period_waived_off'] = df[df.columns[27:35]].values.tolist()
    df['Breakup_of_ported_Sum_Insured'] = df[df.columns[35:43]].values.tolist()

    test_df = df.iloc[:, [0, 1, 2, 43, 44, 45, 46, 47]]
    
    final_df = test_df.set_index(['Policy_No', 'Name_of_Insured', 'Insurer_name']).apply(lambda x: x.apply(pd.Series).stack()).reset_index()

    # final_df['InsuredFrom'] =  final_df['Insured_from'].dt.strftime('%d/%m/%Y')

    final_df = final_df[final_df['Insured_from'].notnull()]    
    final_df.columns = list(map(fn, final_df.columns))
    final_df['PrevPolicyNumber'] = final_df['PrevPolicyNumber'].apply(str)
            
    return final_df


def ported_policy_info(policy_no):
    db = connect_database("prod")
    cur = db.cursor()
    query = "SELECT * FROM [dbo].[ported_policy_table] where [Policy_No]=?;"
    cur.execute(query, (policy_no))
    
    desc = cur.description
    column_names = [col[0] for col in desc]
    data = [dict(zip(column_names, row)) for row in cur.fetchall()]
    cur.close()
    
    final_df = ported_info_change(data)
    final_df = final_df.fillna('')

    return final_df.to_dict('records')


def user_disagree_db(policy_no, page_name, consent):
    db = connect_database("prod")
    cur = db.cursor()
    now = datetime.now()
    formatted_date = now.strftime('%Y-%m-%d %H:%M:%S')
    
    query = """
         IF EXISTS (SELECT 1 FROM user_journey_table WHERE Policy_No = ?)
         BEGIN UPDATE user_journey_table 
             SET {} = ?, Modified_Date = ?
             WHERE Policy_No = ?
         END
         ELSE
         BEGIN
             INSERT INTO [dbo].[user_journey_table] (Policy_No, {}, Created_Date, Modified_Date)
             VALUES (?, ?, ?, ?)
         END
         """.format(page_name, page_name)
    
    cur.execute(query, (policy_no, consent, formatted_date, policy_no, policy_no, consent, formatted_date, formatted_date))
    db.commit()

    return "Inserted"


def user_journey_flag(policy_no):
    db = connect_database("prod")
    cur = db.cursor()
    journey_flag = 1
    query = '''
             UPDATE [dbo].[user_journey_table] 
             SET [journey_flag] = ?
             WHERE [Policy_No] = ?;  '''
    
    cur.execute(query, ( journey_flag, policy_no))
    db.commit()
    print("Journey_flag_updated")
    return "Journey_flag_updated"





def feedback(policy_no, feedback):
    db = connect_database("prod")
    cur = db.cursor()
    now = datetime.now()
    formatted_date = now.strftime('%Y-%m-%d %H:%M:%S')

    query = '''
         IF EXISTS (SELECT 1 FROM [dbo].[Customer_Feedback] WHERE [Policy_Number] = ?)
         BEGIN
             UPDATE [dbo].[Customer_Feedback] 
             SET [Feedback] = ?, [Inserted_DT] = ?
             WHERE [Policy_Number] = ?;
         END
         ELSE
         BEGIN
             INSERT INTO [dbo].[Customer_Feedback] ([Policy_Number], [Feedback], [Inserted_DT])
             VALUES (?, ?, ?)
         END
         '''
    
    cur.execute(query, (policy_no, feedback, formatted_date, policy_no, policy_no, feedback, formatted_date))
    db.commit()

    return "Inserted"


def journey_check(policy_no, uid):
    
    now = datetime.now()
    formatted_date = now.strftime('%Y-%m-%d %H:%M:%S')
    db = connect_database("prod")    
    cur = db.cursor()
    insert_query = '''
         IF NOT EXISTS(SELECT 1 FROM [dbo].[user_journey_table] WHERE [Policy_No] = ?)
            INSERT INTO [dbo].[user_journey_table] ([Policy_No], [UID], [Created_Date], [Modified_Date]) 
                                VALUES (?, ?, ?, ?);
        '''
    cur.execute(insert_query, (policy_no, policy_no, uid, formatted_date, formatted_date))
    db.commit()
    
    query = "select journey_flag FROM [dbo].[user_journey_table] where Policy_No = ?;"
    cur.execute(query, policy_no)
    flag = cur.fetchone()
    
    cur.close()
    return flag

def login_check(uid):
    db = connect_database("prod")
    cur = db.cursor()
    query = "SELECT Dob, Policy_No from [dbo].[customer_table] where [UID]=?;"
    cur.execute(query, (uid))
    final = cur.fetchone()

    if final == None:
        return None, False

    cur.close()
    flag = journey_check(final[1], uid)

    return final, flag[0]
