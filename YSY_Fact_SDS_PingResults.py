import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, text
import subprocess
import time
from datetime import datetime
import re
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header
from email.utils import formataddr

class Logger:

    def __init__(self, level, file_name):
        self.level = level
        self.file_name = file_name

    def basic_configuration(self):
        return logging.basicConfig(level=self.level,
                                   filename=self.file_name,
                                   filemode="w",
                                   format="%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s")
    

log_file = 'error.log'
logger = Logger(logging.ERROR, log_file)
logger.basic_configuration()


db_configs = {
    "epaper_sca_gc": {
        "server": "WS007238",
        "user": "epaper_sca",
        "password": "9xp9WJrA",
        "database": "EPAPER_SCA_GC"
    },
    "sca_digital": {
        "server": "WS007238",
        "user": "SCA_Admin",
        "password": "JAdmin!2309!",
        "database": "SCA_Digital"
    }
}

db_column_types = {
    'Id': sqlalchemy.types.NVARCHAR(length=50),  
    'PingTime': sqlalchemy.types.DATETIME(),
    'IPAddress': sqlalchemy.types.NVARCHAR(length=50),
    'Success': sqlalchemy.types.Boolean(),
    'ResponseTime': sqlalchemy.types.FLOAT()
}


def send_email(subject, content, log_file):
    sender_name = 'Fact_SDS_PingResults Error'
    sender_address = 'OR-Taicang-Plant3-IE-and-PM@schaeffler.com'
    
    # receiver = ['yangsyu@schaeffler.com']
    # cc = ['yangsyu@schaeffler.com']


    receiver = ['huangjon@schaeffler.com']
    cc = ['zhuyih@schaeffler.com']
    

    with open(log_file, 'rb') as f:
        attachment = MIMEText(f.read(), 'base64', 'utf-8')
        attachment.add_header('Content-Disposition', 'attachment', filename=log_file)
        msg = MIMEMultipart()
        msg.attach(attachment)

    msg["Subject"] = Header(s=subject, charset="utf-8").encode()
    msg["From"] = formataddr((sender_name, sender_address))
    msg["To"] = ",".join(receiver)
    msg["Cc"] = ",".join(cc)

    to_list = msg["To"].split(",") + msg["Cc"].split(",")

    email_server = smtplib.SMTP(host="mail-de-hza.schaeffler.com", port=25)
    email_server.sendmail(from_addr=msg["From"], to_addrs=to_list, msg=msg.as_string())
    email_server.quit()


def create_db_engine(config):
    return create_engine(
        f'mssql+pyodbc://{config["user"]}:{config["password"]}@{config["server"]}/{config["database"]}?driver=ODBC+Driver+17+for+SQL+Server',
        fast_executemany=True
    )

def get_sds_ips(engine):
    query = """
    SELECT id, ip_address
    FROM basicdatamodule$sds
    """
    return pd.read_sql(query, engine)

def ping_ip(ip):
    response = subprocess.run(['ping', '-n', '1', ip], capture_output=True, text=True)
    success = response.returncode == 0
    response_time = float('inf')

    if success:
        try:
            match = re.search(r'平均 = (\d+)ms', response.stdout)
            if match:
                response_time = int(match.group(1))
        except Exception as e:
            print(f"Error parsing ping response: {e}")
            response_time = float('inf')
    
    return success, response_time

def ping_ip(ip):
    response = subprocess.run(['ping', '-n', '1', ip], capture_output=True, text=True)
    success = response.returncode == 0
    response_time = float('inf')

    if success:
        try:
            match = re.search(r'Average=(\d+)ms', response.stdout)
            if match:
                response_time = int(match.group(1))
        except Exception as e:
            print(f"Error parsing ping response: {e}")
            response_time = float('inf')
    
    return success, response_time

def clean_data(df, db_column_types):
    for db_col, db_type in db_column_types.items():
        if db_col in df.columns:
            if isinstance(db_type, sqlalchemy.types.String):
                df[db_col] = df[db_col].astype(str)
            elif isinstance(db_type, sqlalchemy.types.Float):
                df[db_col] = pd.to_numeric(df[db_col], errors='coerce').replace([float('inf'), float('-inf'), float('nan')], None).astype(float)
            elif isinstance(db_type, sqlalchemy.types.Integer):
                df[db_col] = pd.to_numeric(df[db_col], errors='coerce').fillna(0).astype(int)
            elif isinstance(db_type, sqlalchemy.types.DateTime):
                df[db_col] = pd.to_datetime(df[db_col], errors='coerce')
            elif isinstance(db_type, sqlalchemy.types.Boolean):
                df[db_col] = df[db_col].astype(bool)
    return df

def update_or_insert(engine, table_name, record, unique_key, update_columns):
    with engine.begin() as conn:
        existing_record = conn.execute(text(f"SELECT * FROM {table_name} WHERE {unique_key} = :uk"), {"uk": record[unique_key]}).fetchone()
        
        if existing_record:
            set_clause = ', '.join([f"{col} = :{col}" for col in update_columns])
            update_stmt = text(f"UPDATE {table_name} SET {set_clause} WHERE {unique_key} = :uk")
            conn.execute(update_stmt, {**record, "uk": record[unique_key]})
        else:
            for key in record:
                if record[key] is None and isinstance(db_column_types.get(key), sqlalchemy.types.Float):
                    record[key] = 0.0  # Replace None with 0.0 
            
            insert_stmt = text(f"INSERT INTO {table_name} ({', '.join(record.keys())}) VALUES ({', '.join([f':{k}' for k in record.keys()])})")
            conn.execute(insert_stmt, record)

def save_ping_results(engine, results):
    now = datetime.now()
    results_with_pingtime = [
        [str(result[0]), result[1], int(result[2]), float(result[3]), now] for result in results
    ]
    results_df = pd.DataFrame(results_with_pingtime, columns=['Id', 'IPAddress', 'Success', 'ResponseTime', 'PingTime'])
    

    print("Before cleaning:")
    print(results_df)
    
    results_df = clean_data(results_df, db_column_types)
    

    print("After cleaning:")
    print(results_df)
    
    for _, row in results_df.iterrows():
        record = row.to_dict()
        try:
            update_or_insert(engine, 'Fact_SDS_PingResults', record, 'Id', ['Success', 'ResponseTime', 'PingTime'])
        except Exception as e:
            error_message = f"Failed to write to database: {e}"
            logger.error(error_message)
            send_email("Database Write Failure", error_message, log_file)

# def main():
#     epaper_engine = create_db_engine(db_configs["epaper_sca_gc"])
#     sca_engine = create_db_engine(db_configs["sca_digital"])

    
#     while True:
#         sds_ips = get_sds_ips(epaper_engine)
#         ping_results = []
        

#         for index, row in sds_ips.iterrows():
#             ip = row['ip_address']
#             id = row['id']
#             success, response_time = ping_ip(ip)
#             ping_results.append([id, ip, int(success), response_time])
        
        

#         save_ping_results(sca_engine, ping_results)
#         time.sleep(300)  

def main():
    epaper_engine = create_db_engine(db_configs["epaper_sca_gc"])
    sca_engine = create_db_engine(db_configs["sca_digital"])

    try:
        sds_ips = get_sds_ips(epaper_engine)
        ping_results = []

        for index, row in sds_ips.iterrows():
            ip = row['ip_address']
            id = row['id']
            success, response_time = ping_ip(ip)
            ping_results.append([id, ip, int(success), response_time])

        save_ping_results(sca_engine, ping_results)
    except Exception as e:
        error_message = f"An error occurred during the main process: {e}"
        logger.error(error_message)
        send_email("Main Process Failure", error_message, log_file)

if __name__ == "__main__":
    main()