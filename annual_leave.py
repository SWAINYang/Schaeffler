import shutil
import pyodbc
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.header import Header
from email.utils import formataddr
import logging
from pathlib import Path
import math
import sqlalchemy
import pandas as pd
import base64
import datetime
import pymysql
import comtypes.client
import win32com.client as win32
import os
import json

# define list to save year
list_year_yyyy = [str(i) for i in range(2023, 2124, 1)]

# define list to save month
list_english_month = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
list_numerical_month = [str(i).rjust(2, "0") for i in range(1, 13, 1)]
dict_english_numerical_month = dict(zip(list_english_month, list_numerical_month))

# define list to save 26 english letters
list_letters_upper = [chr(i) for i in range(65, 91, 1)]
list_letters_lower = [chr(i).lower() for i in range(65, 91, 1)]

# define list to save year month
list_year_month = []
for year in list_year_yyyy:
    for month in list_numerical_month:
        year_month = "".join([year, month])
        list_year_month.append(year_month)

# suffix of files
excel_suffix = [".xlsm", ".xlsx", ".xlsb", ".xls"]
picture_suffix = [".png", ".jpeg", "", ".jpg", ".gif", ".svg"]
pdf_suffix = [".pdf"]

config_data = {
    "mssql": {
        "server": "WS007238",
        "user": "SCA_Admin",
        "password": "JAdmin!2309!",
        "database": "SCA_Digital"
    }
}

# define one object to send email
class SendEmail:
    """
    One object to send email
    """

    def __init__(self, sender_name, sender_address, receiver, cc, subject, content):
        """
        :param sender_name:  str | sender_name,
        :param sender_address:  str | sender_address,
        :param receiver: list | email address of receiver,
        :param cc: list | email address of Cc,
        :param subject: str | email title
        :param content: str | content of email, can only be normal text
        """
        self.sender_name = sender_name
        self.sender_address = sender_address
        self.receiver = receiver
        self.cc = cc
        self.subject = subject
        self.content = content

    def send_email_with_text(self):
        # create connection to Email Serve
        email_server = smtplib.SMTP(host="mail-de-hza.schaeffler.com", port=25)

        # create email object
        msg = MIMEMultipart()

        # create subject
        title = Header(s=self.subject, charset="utf-8").encode()
        msg["Subject"] = title

        # set sender
        msg["From"] = formataddr((self.sender_name, self.sender_address))

        # set receiver
        msg["To"] = ",".join(self.receiver)

        # set Cc
        msg["Cc"] = ",".join(self.cc)

        # add content
        text = MIMEText(_text=self.content, _subtype="plain", _charset="utf-8")
        msg.attach(text)

        # extend receiver list
        to_list = msg["To"].split(",")
        cc_list = msg["Cc"].split(",")
        to_list.extend(cc_list)

        # send email
        email_server.sendmail(from_addr=msg["From"], to_addrs=to_list, msg=msg.as_string())
        email_server.quit()

    def send_email_with_html(self):
        # create connection to Email Serve
        email_server = smtplib.SMTP(host="mail-de-hza.schaeffler.com", port=25)

        # create email object
        msg = MIMEMultipart()

        # create subject
        title = Header(s=self.subject, charset="utf-8").encode()
        msg["Subject"] = title

        # set sender
        msg["From"] = formataddr((self.sender_name, self.sender_address))

        # set receiver
        msg["To"] = ",".join(self.receiver)

        # set Cc
        msg["Cc"] = ",".join(self.cc)

        # add content
        html = MIMEText(_text=self.content, _subtype="html", _charset="utf-8")
        msg.attach(html)

        # extend receiver list
        to_list = msg["To"].split(",")
        cc_list = msg["Cc"].split(",")
        to_list.extend(cc_list)

        # send email
        email_server.sendmail(from_addr=msg["From"], to_addrs=to_list, msg=msg.as_string())
        email_server.quit()

class Logger:
    """
    define one logger for common usage
    """

    def __init__(self, level, file_name):
        """
        :param level: logging level
        :param file_name: absolute directory of log file
        """

        self.level = level
        self.file_name = file_name

    def basic_configuration(self):
        """
        :return: basic configuration for logging
        """
        return logging.basicConfig(level=self.level,
                                   filename=self.file_name,
                                   filemode="w",
                                   format="%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s")


class MSSQL:
    """
    define one object to get connection with MS SQL Server
    """

    def __init__(self, server, user, password, database):
        """
        :param server: str | host name of MS SQL Server
        :param user: str | user to log in MS SQL Server
        :param password: str | password to log in MS SQL Server
        :param database: str | database name in MS SQL Server
        """
        self.server = server
        self.user = user
        self.database = database
        self.password = password

    def pyodbc_connection(self):
        """
        :return: pyodbc connection
        """
        if not self.database:
            raise (NameError, "Incorrect configuration of MS SQL Server !")

        # create connection to SQL Server
        connection_string = f'DRIVER={{SQL Server}};SERVER={self.server};DATABASE={self.database};UID={self.user};PWD={self.password}'
        try:
            pyodbc_con = pyodbc.connect(
                connection_string,
                fast_executemany=True
            )

            return pyodbc_con

        except Exception:
            raise Exception("Failed to connect to MS SQL Server !")

    def sqlalchemy_connection(self):
        """
        :return: SQLAlchemy + pyodbc connection
        """

        # create connection to SQL Server
        try:
            engine = sqlalchemy.create_engine(
                'mssql+pyodbc://{}:{}@{}/{}?driver=ODBC+Driver+17+for+SQL+Server'.format(self.user,
                                                                                         self.password,
                                                                                         self.server,
                                                                                         self.database),
                fast_executemany=True)

            sqlalchemy_con = engine.connect()

            return sqlalchemy_con

        except Exception:
            raise Exception("Failed to connect to MS SQL Server !")

    def add_table_property(self, table_name, table_desc):
        """
        :param table_name: table name in MS SQL Server
        :return:
        """

        # create sql string
        sql = f"""
        EXEC sp_addextendedproperty   
                @name = N'MS_Description',
                @value = N'{table_desc}',
                @level0type = N'Schema',
                @level0name = N'dbo',
                @level1type = N'Table',
                @level1name = N'{table_name}';
        """

        # get cursor
        con = self.pyodbc_connection()
        cursor = con.cursor()

        # execute sql string
        cursor.execute(sql)

        # submit and close
        con.commit()
        con.close()

    def update_table_property(self, table_name, table_desc):
        """
        :param table_name: table name in MS SQL Server
        :return:
        """

        # create sql string
        sql = f"""
        EXEC sp_updateextendedproperty   
                @name = N'MS_Description',
                @value = N'{table_desc}',
                @level0type = N'Schema',
                @level0name = N'dbo',
                @level1type = N'Table',
                @level1name = N'{table_name}';
        """

        # get cursor
        con = self.pyodbc_connection()
        cursor = con.cursor()

        # execute sql string
        cursor.execute(sql)

        # submit and close
        con.commit()
        con.close()

    def execute_sql_query(self, sql):
        """
        :param sql: sql query string
        :return: None
        """

        # get cursor
        con = self.pyodbc_connection()
        cursor = con.cursor()

        # execute sql query
        cursor.execute(sql)

        # commit and close
        con.commit()
        con.close()

def determine_function(row):
    org_unit_text = row['OrgUnit_text']
    if org_unit_text in ['OP/SCA-PII', 'OP/SCA-PIIE', 'OP/SCA-PIID', 'OP/SCA-PII1', 'OP/SCA-PIIP']:
        return 'Basic IE & Digitalization'
    elif org_unit_text in ['OP/SCA-PIX', 'OP/SCA-PIXA']:
        return 'PMO & Automation'
    elif org_unit_text == 'OP/SCA-PIS':
        return '6 sigma'
    elif org_unit_text == 'OP/SCA-PIC':
        return 'Cost'
    else:
        return 'Industrial Engineering'

def add_function_column(df):
    df['function'] = df.apply(determine_function, axis=1)

def calculate_level(row, df, level_dict):
    if row['employee_ID'] in level_dict:
        return level_dict[row['employee_ID']]

    level = 1

    matching_rows = df[df['employee_ID'] == row['Pers_No_of_superior_OM']]

    if not matching_rows.empty:
        for _, matching_row in matching_rows.iterrows():
            matching_level = calculate_level(matching_row, df, level_dict)
            level = max(level, matching_level + 1)

    level_dict[row['employee_ID']] = level
    return level

def add_level_column(df):
    level_dict = {}
    for index, row in df.iterrows():
        df.at[index, 'level'] = calculate_level(row, df, level_dict)

def send_emails_based_on_level(df):
    # Prepare and send emails
    for index, row in df.iterrows():
        if row['level'] == 1:
            # Send all data for level 1
            send_all_data_email(df, row)
        elif row['level'] == 2:
            # Send specific data for level 2
            send_specific_data_email(df, row)
        elif row['level'] == 3:
            # Send only this row's data for level 3
            send_single_data_email(df, row)


def send_all_data_email(df, row):
    # Add the level column to the DataFrame
    add_level_column(df)
    
    # Ensure 'level' can be converted to numeric
    df['level'] = pd.to_numeric(df['level'], errors='coerce')

    # Define the new column order
    selected_columns = ['function', 'employee_ID', 'Chinese_Pyinyin', 'total_remained_days']
    
    # Handle missing values in 'function' column
    df['function'].fillna('No Function Specified', inplace=True)  
    
    # Custom sorting function that prioritizes level 1
    def sort_function_level(x):
        # Separate level 1 records
        level_1_records = x[x['level'] == 1]
        # Exclude level 1 records from the rest
        other_records = x[x['level'] != 1]
        # Sort the rest of the records by 'level'
        other_records_sorted = other_records.sort_values(by='level')
        # Concatenate level 1 records with sorted others
        return pd.concat([level_1_records, other_records_sorted]).sort_index()
    
    df_sorted = df.groupby('function').apply(sort_function_level).reset_index(drop=True)
    
    # Select the desired columns
    df_selected = df_sorted[selected_columns]

    # Rename columns for the HTML table
    column_renames = {
        'function': 'Function',
        'employee_ID': 'Employee ID',
        'Chinese_Pyinyin': 'Chinese Name',
        'total_remained_days': 'Remaining Annual Leave'
    }
    df_selected.rename(columns=column_renames, inplace=True)

    css_styles = """
    <style>
        table {
            border-collapse: collapse;
            width: 100%;
        }
        th, td {
            border: 1px solid #ddd;
            text-align: center;
            padding: 8px;
        }
        th {
            background-color: #f2f2f2;
        }
    </style>
    """

    email_content = f"""<html>
    <head>{css_styles}</head>
    <body>
        <p>Dear {row['Chinese_Pyinyin']},</p>
        <p>Here is the annual leave information for all employees up to {row['due_date']}.</p>
        {df_selected.to_html(index=False, classes='styled-table')}
    </body>
    </html>"""

    # Create and send the email
    sender_name = "Plant 3 IE/PM"
    sender_address = "yangsyu@schaeffler.com"
    # receiver = ["yangsyu@schaeffler.com"]
    receiver = [row['Email_address']]
    cc = []
    subject = "Annual Leave Remaining Days Update (All Employees)"
    
    email = SendEmail(sender_name, sender_address, receiver, cc, subject, email_content)
    email.send_email_with_html()

def send_specific_data_email(df, row):
    # Define the new column order
    selected_columns = ['function', 'employee_ID', 'Chinese_Pyinyin', 'total_remained_days']

    # Filter rows based on level and superior
    if row['level'] == 2:
        current_row = pd.DataFrame([row])[selected_columns]
    else:
        current_row = pd.DataFrame([])

    matching_level_3_rows = df[(df['level'] == 3) & (df['Pers_No_of_superior_OM'] == row['employee_ID'])][selected_columns]

    combined_data = pd.concat([current_row, matching_level_3_rows], ignore_index=True)

    # Rename columns for the HTML table
    column_renames = {
        'function': 'Function',
        'employee_ID': 'Employee ID',
        'Chinese_Pyinyin': 'Chinese Name',
        'total_remained_days': 'Remaining Annual Leave'
    }
    combined_data.rename(columns=column_renames, inplace=True)

    css_styles = """
    <style>
        table {
            border-collapse: collapse;
            width: 100%;
        }
        th, td {
            border: 1px solid #ddd;
            text-align: center;
            padding: 8px;
        }
        th {
            background-color: #f2f2f2;
        }
    </style>
    """


    email_content = f"""<html>
    <head>{css_styles}</head>
    <body>
        <p>Dear {row['Chinese_Pyinyin']},</p>
        <p>Here is your annual leave information and that of your subordinates up to {row['due_date']}:</p>
        {combined_data.to_html(index=False, classes='styled-table')}
    </body>
    </html>"""

    # Create and send the email
    sender_name = "Plant 3 IE/PM"
    sender_address = "yangsyu@schaeffler.com"
    # receiver = ["yangsyu@schaeffler.com"]
    receiver = [row['Email_address']]
    cc = []
    subject = "Annual Leave Remaining Days Update (You and Your Subordinates)"

    email = SendEmail(sender_name, sender_address, receiver, cc, subject, email_content)
    email.send_email_with_html()

def send_single_data_email(df, row):
    # Format the email content
    email_content = f"""<html>
    <body>
        <p>Dear {row['Chinese_Pyinyin']},您好</p>
        <p>注意合理安排您的时间，适当的休息和放松有助于提高工作效率！</p>
        <p>截至:  {row['due_date']}</p>
        <p>您的年假余额为: {row['total_remained_days']}天</p>
        <p>Best Regards!</p>
    </body>
    </html>"""
    # Create and send the email
    sender_name = "Plant 3 IE/PM"
    sender_address = "yangsyu@schaeffler.com"
    # receiver = ["yangsyu@schaeffler.com"]
    receiver = [row['Email_address']]
    cc = []
    subject = "Annual Leave Remaining Days Update"
    
    email = SendEmail(sender_name, sender_address, receiver, cc, subject, email_content)
    email.send_email_with_html()

def send_annual_leave_emails(config_data):
    # Define the SQL query
    query = """
WITH CTE_NameList AS (
    SELECT 
        personal_number,
        Email_address,
        OrgUnit_text,  
        Pers_No_of_superior_OM,  
        MAX(download_date) AS download_date
    FROM 
        dim_name_list
    WHERE 
        OrgUnit_text LIKE 'OP/SCA-PI%'
    GROUP BY 
        personal_number,
        Email_address,
        OrgUnit_text,  
        Pers_No_of_superior_OM  
),
CTE_LatestAnnualLeave AS (
    SELECT 
        employee_ID,
        due_date,
        total_remained_days,
        Chinese_Pyinyin,
        ROW_NUMBER() OVER (
            PARTITION BY employee_ID
            ORDER BY due_date DESC
        ) AS RowNum
    FROM 
        fact_HR_annual_leave
)
SELECT 
    a.employee_ID,
    a.due_date AS due_date,
    a.total_remained_days AS total_remained_days,
    a.Chinese_Pyinyin AS Chinese_Pyinyin,
    n.Email_address,
    n.OrgUnit_text,  
    n.Pers_No_of_superior_OM,  
    n.download_date AS download_date
FROM 
    CTE_LatestAnnualLeave AS a
JOIN 
    CTE_NameList AS n ON a.employee_ID = n.personal_number
WHERE 
    a.RowNum = 1
    AND n.download_date = (
        SELECT 
            MAX(download_date)
        FROM 
            dim_name_list
        WHERE 
            OrgUnit_text LIKE 'OP/SCA-PI%'
    );
    """

    # Establish a connection using pyodbc
    connection_string = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + config_data["mssql"]["server"] + ';DATABASE=' + config_data["mssql"]["database"] + ';UID=' + config_data["mssql"]["user"] + ';PWD=' + config_data["mssql"]["password"]
    conn = pyodbc.connect(connection_string)

    try:
        # Use pandas to read the SQL query into a DataFrame
        df = pd.read_sql_query(query, conn)
        # Add level and function columns
        add_level_column(df)
        add_function_column(df)
        # Send emails based on level
        send_emails_based_on_level(df)
    finally:
        # Ensure the connection is closed
        conn.close()

send_annual_leave_emails(config_data)

