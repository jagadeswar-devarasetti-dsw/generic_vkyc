
import uuid
import pyodbc
import numpy as np
import pandas as pd
import configparser

def connect_database(var):
    config = configparser.ConfigParser()
    config.read('backend/config_file.ini')

    db_server = config.get(var, "SERVER")
    db_database = config.get(var, "DATABASE")
    db_user = config.get(var, "UID")
    db_password = config.get(var, "PWD")

    conn = pyodbc.connect(driver='{ODBC Driver 17 for SQL Server}',server=db_server, database=db_database,user=db_user, password=db_password)

    return conn

db = connect_database("prod")

def customer_func(df):
    customer_df = df
    customer_df = customer_df.where(pd.notnull(df), None)
    
    customer_df['Master_Policy_Number'] = None
    customer_df['Member_ID'] = None 
    customer_df['COI_Number'] = None
    customer_df['LAN_Number'] = None

    final_cols = ['First_Name', 'Last_Name', 'Gender', 'DOB', 'OWNER_OCCUPATION', 'Email_ID', 
                  'Mobile_Number', 'Address', 'Policy_No', 'UIN', 'UID',
                  'Master_Policy_Number', 'Member_ID', 'COI_Number', 'LAN_Number']

    customer_df = customer_df.reindex(columns=final_cols)
    customer_df['UID'] = customer_df['UID'].map(lambda cid: str(uuid.uuid4().hex[:8]))

    cur = db.cursor()
    
    # cur.execute("DROP TABLE [dbo].[customer_table];")
    # db.commit()
    create_table = '''
        IF  NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[customer_table]') AND type in ('U'))
        BEGIN
            CREATE TABLE [dbo].[customer_table](
                First_Name varchar(50), Last_Name varchar(50), Gender varchar(50), DOB date, Owner_Occupation varchar(50), 
                Email_Id varchar(MAX), Mobile_Number bigint, Address varchar(MAX), Policy_no varchar(50), UIN varchar(50), 
                UID varchar(30), Master_Policy_Number varchar(30), Member_Id varchar(30), 
                COI_Number varchar(30), LAN_Number varchar(30)) 
        END
    '''
    
    cur.execute(create_table)
    db.commit()

    insert_query = '''
         IF NOT EXISTS(SELECT 1 FROM [dbo].[customer_table] WHERE [Policy_No] = ?)
            INSERT INTO [dbo].[customer_table] VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        '''
    
    for index, row in customer_df.iterrows():
        row_values = list(row)
        cur.execute(insert_query, row_values[8], *row_values)
        db.commit()
    cur.close()

    return "Inserted"


def policy_func(df):
    
    policy_df = df
    policy_df = policy_df.astype(object).where(pd.notnull(df), None)
    
    cur = db.cursor()
    
    # cur.execute("DROP TABLE [dbo].[policy_table];")
    # db.commit()
    
    create_table = '''
        IF  NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[policy_table]') AND type in ('U'))
        BEGIN
            CREATE TABLE [dbo].[policy_table](
                Policy_no varchar(50), Application_No varchar(50), Product_Name varchar(Max), Policy_Type varchar(50), 
                Start_Date date, End_Date date, Policy_Term int, Premium_Amount float, No_of_Insured int, 
                Policy_Porting_Flag varchar(30),Health_Disclosure varchar(Max), Policy_Status varchar(30), 
                Parent_Agency_ID varchar(50), Report_Channel varchar(50), SUM_INSURED float) 
        END
    '''
    cur.execute(create_table)
    db.commit()

    insert_query = '''
         IF NOT EXISTS(SELECT 1 FROM [dbo].[policy_table] WHERE [Policy_No] = ?)
            INSERT INTO [dbo].[policy_table] VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        '''
    
    for index, row in policy_df.iterrows():
        row_values = list(row)
        # print(index)
        cur.execute(insert_query, (row_values[0], *row_values))
        db.commit()
    cur.close()

    return "Inserted"

def ported_policy_func(df):
    
    ported_df = df
    ported_df = ported_df.astype(object).where(pd.notnull(df), None)
    
    cur = db.cursor()
    # cur.execute("DROP TABLE [dbo].[ported_policy_table];")
    # db.commit()
    
    create_table = '''
        IF  NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[ported_policy_table]') AND type in ('U'))
        BEGIN
            CREATE TABLE [dbo].[ported_policy_table](
            [Policy_No] varchar(50), [Name_of_Insured] varchar(Max), [Insurer_name] varchar(Max), 
            [Insured_from_01] date,[Insured_from_02] date, [Insured_from_03] date, 
            [Insured_from_04] date, [Insured_from_05] date, [Insured_from_06] date, 
            [Insured_from_07] date, [Insured_from_08] date,
            [PREV_POLICY_NUMBER_01] varchar(Max), [PREV_POLICY_NUMBER_02] varchar(Max), 
            [PREV_POLICY_NUMBER_03] varchar(Max), [PREV_POLICY_NUMBER_04] varchar(Max), 
            [PREV_POLICY_NUMBER_05] varchar(Max), [PREV_POLICY_NUMBER_06] varchar(Max),
            [PREV_POLICY_NUMBER_07] varchar(Max), [PREV_POLICY_NUMBER_08] varchar(Max),
            [CUMULATIVE_BONUS_01] float, [CUMULATIVE_BONUS_02] float, 
            [CUMULATIVE_BONUS_03] float, [CUMULATIVE_BONUS_04] float,
            [CUMULATIVE_BONUS_05] float, [CUMULATIVE_BONUS_06] float, 
            [CUMULATIVE_BONUS_07] float,	[CUMULATIVE_BONUS_08] float,
            [Waiting_period_waived_off_01] varchar(50), [Waiting_period_waived_off_02] varchar(50), 
            [Waiting_period_waived_off_03] varchar(50), [Waiting_period_waived_off_04] varchar(50), 
            [Waiting_period_waived_off_05] varchar(50), [Waiting_period_waived_off_06] varchar(50),
            [Waiting_period_waived_off_07] varchar(50), [Waiting_period_waived_off_08] varchar(50), 
            [Breakup_of_ported_Sum_Insured_01] varchar(50), [Breakup_of_ported_Sum_Insured_02] varchar(50), 
            [Breakup_of_ported_Sum_Insured_03] varchar(50), [Breakup_of_ported_Sum_Insured_04] varchar(50),
            [Breakup_of_ported_Sum_Insured_05] varchar(50), [Breakup_of_ported_Sum_Insured_06] varchar(50),
            [Breakup_of_ported_Sum_Insured_07] varchar(50), [Breakup_of_ported_Sum_Insured_08] varchar(50)
            )
        END
    '''
    cur.execute(create_table)
    db.commit()

    insert_query = '''
         IF NOT EXISTS(SELECT 1 FROM [dbo].[ported_policy_table] WHERE [Policy_No] = ?)
            INSERT INTO [dbo].[ported_policy_table] VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        '''
    
    for index, row in ported_df.iterrows():
        row_values = list(row)
        # print(index)
        cur.execute(insert_query, (row_values[0], *row_values))
        db.commit()
    cur.close()

    return "Inserted"


def insured_func(df):
    
    ported_df = df
    ported_df = ported_df.astype(object).where(pd.notnull(df), None)
    
    cur = db.cursor()
    
    # cur.execute("DROP TABLE [dbo].[insured_table];")
    # db.commit()
    
    create_table = '''
        IF  NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[insured_table]') AND type in ('U'))
        BEGIN
            CREATE TABLE [dbo].[insured_table](
                [Policy_No] varchar(max),
                [Customer_ID_01] varchar(max), [Customer_ID_02] varchar(max), [Customer_ID_03] varchar(max), 
                [Customer_ID_04] varchar(max), [Customer_ID_05] varchar(max), [Customer_ID_06] varchar(max),
                [Customer_ID_07] varchar(max), [Customer_ID_08] varchar(max),
                [Relation_with_policy_holder_01] varchar(max), [Relation_with_policy_holder_02] varchar(max),
                [Relation_with_policy_holder_03] varchar(max), [Relation_with_policy_holder_04] varchar(max),
                [Relation_with_policy_holder_05] varchar(max), [Relation_with_policy_holder_06] varchar(max),
                [Relation_with_policy_holder_07] varchar(max), [Relation_with_policy_holder_08] varchar(max),
                [Insurer_First_Name_01] varchar(max), [Insurer_First_Name_02] varchar(max), [Insurer_First_Name_03] varchar(max), 
                [Insurer_First_Name_04] varchar(max), [Insurer_First_Name_05] varchar(max), [Insurer_First_Name_06] varchar(max),
                [Insurer_First_Name_07] varchar(max), [Insurer_First_Name_08] varchar(max),
                [Insurer_Last_Name_01] varchar(max), [Insurer_Last_Name_02] varchar(max), [Insurer_Last_Name_03] varchar(max), 
                [Insurer_Last_Name_04] varchar(max), [Insurer_Last_Name_05] varchar(max), [Insurer_Last_Name_06] varchar(max),
                [Insurer_Last_Name_07] varchar(max), [Insurer_Last_Name_08] varchar(max),
                [Insurer_Gender_01] varchar(max), [Insurer_Gender_02] varchar(max), [Insurer_Gender_03] varchar(max), 
                [Insurer_Gender_04] varchar(max), [Insurer_Gender_05] varchar(max), [Insurer_Gender_06] varchar(max),
                [Insurer_Gender_07] varchar(max), [Insurer_Gender_08] varchar(max),
                [Insurer_DOB_01] [datetime], [Insurer_DOB_02] [datetime], [Insurer_DOB_03] [datetime], [Insurer_DOB_04] [datetime],
                [Insurer_DOB_05] [datetime], [Insurer_DOB_06] [datetime], [Insurer_DOB_07] [datetime], [Insurer_DOB_08] [datetime],
                [HMB_01] float, [HMB_02] float, [HMB_03] float, [HMB_04] float,
                [HMB_05] float, [HMB_06] float, [HMB_07] float, [HMB_08] float,
                [Health_Disclosure_01] varchar(max), [Health_Disclosure_02] varchar(max),
                [Health_Disclosure_03] varchar(max), [Health_Disclosure_04] varchar(max),
                [Health_Disclosure_05] varchar(max), [Health_Disclosure_06] varchar(max),
                [Health_Disclosure_07] varchar(max), [Health_Disclosure_08] varchar(max),
                [SUM_INSURED_01] float, [SUM_INSURED_02] float, [SUM_INSURED_03] float, [SUM_INSURED_04] float,
                [SUM_INSURED_05] float, [SUM_INSURED_06] float, [SUM_INSURED_07] float, [SUM_INSURED_08] float,
                [SUM_INSURED_09] float, [SUM_INSURED_10] float, [SUM_INSURED_11] float, [SUM_INSURED_12] float,
                [SUM_INSURED_13] float, [SUM_INSURED_14] float, [SUM_INSURED_15] float, [TOT_SUM_INSURED] float 
            )
        END
    '''
    cur.execute(create_table)
    db.commit()

    insert_query = '''
         IF NOT EXISTS(SELECT 1 FROM [dbo].[insured_table] WHERE [Policy_No] = ?)
            INSERT INTO [dbo].[insured_table] VALUES 
           (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        '''
    
    for index, row in ported_df.iterrows():
        row_values = list(row)
        # print(index)
        cur.execute(insert_query, (row_values[0], *row_values))
        db.commit()
    cur.close()

    return "Inserted"

def user_journey_func():

    cur = db.cursor()
    
    query = '''
    IF  NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[user_journey_table]') AND type in ('U'))
        BEGIN
            CREATE TABLE [dbo].[user_journey_table](
                [Policy_No] [varchar](50) NOT NULL,
                [UID] [varchar](10), [PersonalDetailsPage] bit,
                [PolicyDetailsPage] bit, [PortedPolicyPage] bit,
                [InsuredDetailsPage] bit, [journey_flag] bit DEFAULT 0,
                [Created_Date] datetime, [Modified_Date] datetime 
            )
        END
    '''

    cur.execute(query)
    db.commit()

    cur.close()

    return "Inserted"


if __name__ == '__main__':
	df = pd.read_excel("test.xlsx")
	df = df.tail(50)
	print(df)
	cols = list(df.columns)

	customer_table_cols = cols[:1] + cols[2:11]
	policy_table_cols = cols[9:10] + cols[11:24] + cols[130:131]
	ported_policy_cols = cols[9:10] + cols[24:66]
	insured_cols = cols[9:10] + cols[66:]

	customer_func(df[customer_table_cols])
	policy_func(df[policy_table_cols])
	ported_policy_func(df[ported_policy_cols])
	insured_func(df[insured_cols])
	user_journey_func()
