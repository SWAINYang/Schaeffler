import pandas as pd
import os
import sqlalchemy
from sqlalchemy import create_engine, inspect, text
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header
from email.utils import formataddr
import logging
import re
from datetime import datetime

class Logger:

    def __init__(self, level, file_name):
        self.level = level
        self.file_name = file_name

    def basic_configuration(self):
        return logging.basicConfig(level=self.level,
                                   filename=self.file_name,
                                   filemode="w",
                                   format="%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s")

file_path = r'C:\Users\yangsyu\Schaeffler Technologies AG & Co.KG\Plant3 IE&PM - 文档\General\99_Sharing\02_Projects\202211_MES Project_Zhu Yihong\MES项目管理.xlsx'
worksheet_name = '01_MES Rollout Detail list'

if not os.path.exists(file_path):
    print("File not found. Please check the path.")
else:
    excel_data = pd.read_excel(file_path, sheet_name=worksheet_name, engine='openpyxl', header=1)
    excel_data = excel_data.drop(columns=[col for col in excel_data.columns if 'Unnamed' in col])

    # Updated new_column_names to match the second code
    new_column_names = [
        'Department', 'PV', 'Machine', 'Description',
        'APP应用', 'MS0生产准备',
        'MS1_SDS安装', 'MS2_SDS配置', 'MS3_NATS连接', 'MS4_APP配置',
        'MS5_APP培训上线', 'MS6_APP_To_SAP', 'MS7_写PLC', 'Group_组设备', 'APP_Type', 'Connectivity连接方式', 'Status'
    ]

    excel_data.columns = new_column_names
    print(excel_data.head())

    if excel_data.empty:
        print("The loaded worksheet is empty.")
    else:
        config_data = {
            "mssql": {
                "server": "WS007238",
                "user": "SCA_Admin",
                "password": "JAdmin!2309!",
                "database": "SCA_Digital"
            }
        }

        table_name = 'Fact_Machine_Con_Overview'

        # 定义清空表的函数
        def clear_table(engine, table_name):
            try:
                with engine.begin() as conn:
                    conn.execute(text(f"DELETE FROM [{table_name}]"))
                print(f"Table '{table_name}' has been cleared.")
            except Exception as e:
                print(f"Failed to clear table '{table_name}': {e}")

        # 创建数据库引擎
        engine = create_engine(
            f'mssql+pyodbc://{config_data["mssql"]["user"]}:{config_data["mssql"]["password"]}@'
            f'{config_data["mssql"]["server"]}/{config_data["mssql"]["database"]}?driver=ODBC+Driver+17+for+SQL+Server',
            fast_executemany=True
        )

        # 清空表
        clear_table(engine, table_name)

        # 获取当前时间并添加为新的一列
        run_time = datetime.now()
        excel_data['Run Time'] = run_time

        # 映射列名
        column_mapping = {
            'Department': 'Department',
            'PV': 'PV',
            'Machine': 'Machine',
            'Description': 'Description',
            'Connectivity连接方式': 'Connectivity',  
            'APP应用': 'APP',
            'APP_Type': 'APP Type',
            'MS0生产准备': 'MS0',
            'MS1_SDS安装': 'MS1',
            'MS2_SDS配置': 'MS2',
            'MS3_NATS连接': 'MS3',
            'MS4_APP配置': 'MS4',
            'MS5_APP培训上线': 'MS5',
            'MS6_APP_To_SAP': 'MS6',
            'MS7_写PLC': 'MS7',
            'Group_组设备': 'Group',
            'Status': 'Status',
            'Run Time': 'Run Time'
        }

        # 重命名列
        excel_data = excel_data.rename(columns=column_mapping)
        excel_data = excel_data.where(pd.notnull(excel_data), None)

        # 清洗字符串数据
        string_columns = ['Department', 'PV', 'Machine', 'Description', 'APP', 'MS0', 'MS1', 'MS2', 'MS3', 'MS4', 'MS5', 'MS6', 'MS7', 'APP Type', 'Connectivity', 'Status']

        # 确保 Group 是整数
        excel_data['Group'] = pd.to_numeric(excel_data['Group'], errors='coerce').astype('Int64')

        # 确保 APP Type 长度不超过 5
        excel_data['APP Type'] = excel_data['APP Type'].astype(str).str[:5]

        # 确保 Connectivity 长度不超过 10
        excel_data['Connectivity'] = excel_data['Connectivity'].astype(str).str[:10]

        # 清洗字符串数据中的非法字符
        for col in string_columns:
            excel_data[col] = excel_data[col].apply(lambda x: re.sub(r'[^\x00-\x7F]', '', str(x)) if pd.notnull(x) else None)

        # 获取数据库表的列信息
        inspector = inspect(engine)
        columns_info = inspector.get_columns(table_name)
        db_column_names = [col['name'] for col in columns_info]
        db_column_types = {col['name']: col['type'] for col in columns_info}

        # 对数据类型进行转换，匹配数据库表的列类型
        for db_col, db_type in db_column_types.items():
            if db_col in excel_data.columns:
                if isinstance(db_type, sqlalchemy.types.String):
                    excel_data[db_col] = excel_data[db_col].astype(str)
                elif isinstance(db_type, sqlalchemy.types.Float):
                    excel_data[db_col] = pd.to_numeric(excel_data[db_col], errors='coerce').astype(float)
                elif isinstance(db_type, sqlalchemy.types.Integer):
                    excel_data[db_col] = pd.to_numeric(excel_data[db_col], errors='coerce').fillna(0).astype(int)
                elif isinstance(db_type, sqlalchemy.types.DateTime):
                    excel_data[db_col] = pd.to_datetime(excel_data[db_col], errors='coerce')

        try:
            excel_data.to_sql(name=table_name, con=engine, if_exists='append', index=False)
            print(f"Data has been successfully written to the database table '{table_name}'.")
        except Exception as e:
            print(f"An error occurred while writing data to the database: {e}")

            log_file = 'error.log'
            logger = Logger(logging.ERROR, log_file)
            logger.basic_configuration()
            logging.error(f"An error occurred while writing data to the database: {e}")

            sender_name = 'Database Importer Error'
            sender_address = 'OR-Taicang-Plant3-IE-and-PM@schaeffler.com'
            
            receiver = ['yangsyu@schaeffler.com']
            cc = []
            
            subject = '202211_MES Project_Zhu Yihong'
            content = f"An error occurred while writing data to the database: {e}\n\nLog file attached."

            # Create the email message
            msg = MIMEMultipart()
            msg.attach(MIMEText(content, 'plain', 'utf-8'))

            # Attach the log file
            with open(log_file, 'rb') as f:
                attachment = MIMEText(f.read(), 'base64', 'utf-8')
                attachment.add_header('Content-Disposition', 'attachment', filename=log_file)
                msg.attach(attachment)

            email_server = smtplib.SMTP(host="mail-de-hza.schaeffler.com", port=25)
            msg["Subject"] = Header(s=subject, charset="utf-8").encode()
            msg["From"] = formataddr((sender_name, sender_address))
            msg["To"] = ",".join(receiver)
            msg["Cc"] = ",".join(cc)

            to_list = msg["To"].split(",") + msg["Cc"].split(",")

            email_server.sendmail(from_addr=msg["From"], to_addrs=to_list, msg=msg.as_string())
            email_server.quit()

            print("Error logged and email sent.")

        finally:
            engine.dispose()


