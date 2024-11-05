import os
import glob
import pandas as pd
from sqlalchemy import create_engine, text
from openpyxl import load_workbook
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header
from email.utils import formataddr
import logging
import shutil

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

config_data = {
    "mssql": {
        "server": "WS007238",
        "user": "SCA_Admin",
        "password": "JAdmin!2309!",
        "database": "SCA_Digital"
    }
}

table_name_default = 'fact_im_submit_data'
table_name_closed = 'fact_im_closed_data'

engine = create_engine(
    f'mssql+pyodbc://{config_data["mssql"]["user"]}:{config_data["mssql"]["password"]}@'
    f'{config_data["mssql"]["server"]}/{config_data["mssql"]["database"]}?driver=ODBC+Driver+17+for+SQL+Server',
    fast_executemany=True
)


def send_email(subject, content, log_file):
    sender_name = 'Database Importer Error'
    sender_address = 'OR-Taicang-Plant3-IE-and-PM@schaeffler.com'
    
    # receiver = ['yangsyu@schaeffler.com']
    # cc = ['yangsyu@schaeffler.com']


    receiver = ['huangjon@schaeffler.com']
    cc = ['zhaixia@schaeffler.com']
    
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

def clear_table(engine, table_name):
    try:
        with engine.begin() as conn:
            conn.execute(text(f"TRUNCATE TABLE [{table_name}]"))
        print(f"Table '{table_name}' has been cleared.")
    except Exception as e:
        print(f"Failed to clear table '{table_name}': {e}")
        logging.error(f"An error occurred while processing file {file}: {e}")
        send_email('Clear data stored in the SQL Error', f"An error occurred while processing file {file}: {e}", log_file)


def read_excel(file):
    return pd.read_excel(file, header=0, skiprows=1, engine='openpyxl')

def get_file_metadata(file):
    file_name = os.path.basename(file)
    creation_time = os.path.getctime(file)
    last_modified_time = os.path.getmtime(file)
    return file_name, creation_time, last_modified_time

def clean_and_prepare_df(df, column_mapping, table_name, file_name, creation_time, last_modified_time):
    df = df.rename(columns=column_mapping)
    
    # 添加或更新 'file_name', 'creation_time', 'last_modified_time' 列
    df['file_name'] = file_name
    df['creation_time'] = pd.to_datetime(creation_time, unit='s')
    df['last_modified_time'] = pd.to_datetime(last_modified_time, unit='s')
    
    if table_name == table_name_default:
        df['rationalization_id'] = df['rationalization_id'].astype(str).str[:50]
        df['title'] = df['title'].astype(str).str[:510]
        df['category'] = df['category'].astype(str).str[:510]
        df['submitter_id'] = df['submitter_id'].astype(str).str[:510]
        df['submitter_title'] = df['submitter_title'].astype(str).str[:510]
        df['submitter_last_name'] = df['submitter_last_name'].astype(str).str[:510]
        df['submitter_first_name'] = df['submitter_first_name'].astype(str).str[:510]
        df['submitter_organization'] = df['submitter_organization'].astype(str).str[:510]
        df['special_contribution_factor'] = df['special_contribution_factor'].replace('', pd.NA).astype(float, errors='ignore')
        df['share'] = df['share'].replace('', pd.NA).astype(float, errors='ignore')
        df['status'] = df['status'].astype(str).str[:510]
        df['editor_id'] = df['editor_id'].astype(str).str[:510]
        df['editor_title'] = df['editor_title'].astype(str).str[:510]
        df['editor_last_name'] = df['editor_last_name'].astype(str).str[:510]
        df['editor_first_name'] = df['editor_first_name'].astype(str).str[:510]
        df['editor_organization'] = df['editor_organization'].astype(str).str[:510]
        df['result'] = df['result'].astype(str).str[:510]
        df['calculated_net_benefit'] = df['calculated_net_benefit'].replace('', pd.NA).astype(float, errors='ignore')
        df['estimated_net_benefit'] = df['estimated_net_benefit'].replace('', pd.NA).astype(float, errors='ignore')
        df['personnel_area'] = df['personnel_area'].astype(str).str[:510]
    elif table_name == table_name_closed:
        df['rationalization_id'] = df['rationalization_id'].astype(str).str[:50]
        df['title'] = df['title'].astype(str).str[:255]
        df['category'] = df['category'].astype(str).str[:50]
        df['submission_date'] = pd.to_datetime(df['submission_date'], errors='coerce')
        df['assessment_date'] = pd.to_datetime(df['assessment_date'], errors='coerce')
        df['implementation_date'] = pd.to_datetime(df['implementation_date'], errors='coerce')
        df['completion_date'] = pd.to_datetime(df['completion_date'], errors='coerce')
        df['result'] = df['result'].astype(str).str[:255]
        df['calculated_net_benefit'] = df['calculated_net_benefit'].replace('', pd.NA).astype(float, errors='ignore')
        df['estimated_net_benefit'] = df['estimated_net_benefit'].replace('', pd.NA).astype(float, errors='ignore')
        df['submitter_id'] = df['submitter_id'].astype(str).str[:50]
        df['submitter_title'] = df['submitter_title'].astype(str).str[:50]
        df['submitter_last_name'] = df['submitter_last_name'].astype(str).str[:50]
        df['submitter_first_name'] = df['submitter_first_name'].astype(str).str[:50]
        df['submitter_organization'] = df['submitter_organization'].astype(str).str[:100]
        df['share'] = df['share'].replace('', pd.NA).astype(float, errors='ignore')
        df['special_contribution_factor'] = df['special_contribution_factor'].replace('', pd.NA).astype(float, errors='ignore')
        df['reward'] = df['reward'].replace('', pd.NA).astype(float, errors='ignore')
        df['calculated_net_benefit_increase_value'] = df['calculated_net_benefit_increase_value'].replace('', pd.NA).astype(float, errors='ignore')
        df['calculated_net_benefit_increase_currency'] = df['calculated_net_benefit_increase_currency'].astype(str).str[:10]
        df['estimated_net_benefit_increase_value'] = df['estimated_net_benefit_increase_value'].replace('', pd.NA).astype(float, errors='ignore')
        df['estimated_net_benefit_increase_currency'] = df['estimated_net_benefit_increase_currency'].astype(str).str[:10]
        df['personnel_area'] = df['personnel_area'].astype(str).str[:100]
        df['attribute_value'] = df['attribute_value'].astype(str).str[:255]

    return df

def write_to_db(df, table_name):
    try:
        df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
        print(f"Data has been successfully written to the database table '{table_name}'.")
    except Exception as e:
        print(f"Failed to write data to table '{table_name}': {e}")
        logging.error(f"An error occurred while processing file {file}: {e}")
        send_email('Write to SQL Error', f"An error occurred while processing file {file}: {e}", log_file)


def process_excel_default(file):
    try:
        file_name, creation_time, last_modified_time = get_file_metadata(file)
        df = read_excel(file)
        
        column_mapping = {
            '合理化建议编号': 'rationalization_id',
            '合理化建议标题': 'title',
            '类别': 'category',
            '提交日期': 'submission_date',
            '提交者 (权限)': 'submitter_id',
            '提交者 (职称)': 'submitter_title',
            '提交者 (姓)': 'submitter_last_name',
            '提交者 (名)': 'submitter_first_name',
            '提交者 (组织架构)': 'submitter_organization',
            '特殊贡献因子': 'special_contribution_factor',
            '份额': 'share',
            '状态': 'status',
            '编辑者 (权限)': 'editor_id',
            '编辑者 (职称)': 'editor_title',
            '编辑者 (姓)': 'editor_last_name',
            '编辑者 (名)': 'editor_first_name',
            '编辑者 (组织架构)': 'editor_organization',
            '结果': 'result',
            '可计算的净收益 (今日报告货币）': 'calculated_net_benefit',
            '预估的净收益 (今日报告货币）': 'estimated_net_benefit',
            '人员领域': 'personnel_area'
        }

        df = clean_and_prepare_df(df, column_mapping, table_name_default, file_name, creation_time, last_modified_time)
        
        clear_table(engine, table_name_default)
        write_to_db(df, table_name_default)
    except Exception as e:
        print(f"An error occurred while processing file {file}: {e}")
        logging.error(f"An error occurred while processing file {file}: {e}")
        send_email('Read Excel Error', f"An error occurred while processing file {file}: {e}", log_file)


def process_excel_closed(file):
    try:
        file_name, creation_time, last_modified_time = get_file_metadata(file)
        df = read_excel(file)
        
        column_mapping = {
            '合理化建议编号': 'rationalization_id',
            '合理化建议标题': 'title',
            '类别': 'category',
            '提交日期': 'submission_date',
            '评估日期': 'assessment_date',
            '实施日期': 'implementation_date',
            '结束日期': 'completion_date',
            '结果': 'result',
            '可计算的净收益 (今日报告货币）': 'calculated_net_benefit',
            '预估的净收益 (今日报告货币）': 'estimated_net_benefit',
            '提交者 (权限)': 'submitter_id',
            '提交者 (职称)': 'submitter_title',
            '提交者 (姓)': 'submitter_last_name',
            '提交者 (名)': 'submitter_first_name',
            '提交者 (组织架构)': 'submitter_organization',
            '份额': 'share',
            '特殊贡献因子': 'special_contribution_factor',
            '支付的奖励 (今日报告货币）': 'reward',
            '可计算的净收益 (今日报告货币） (增加) (值)': 'calculated_net_benefit_increase_value',
            '可计算的净收益 (今日报告货币） (增加) (货币)': 'calculated_net_benefit_increase_currency',
            '预估的净收益 (今日报告货币） (增加) (值)': 'estimated_net_benefit_increase_value',
            '预估的净收益 (今日报告货币） (增加) (货币)': 'estimated_net_benefit_increase_currency',
            '人员领域': 'personnel_area',
            '属性值': 'attribute_value'
        }

        df = clean_and_prepare_df(df, column_mapping, table_name_closed, file_name, creation_time, last_modified_time)
        
        clear_table(engine, table_name_closed)
        write_to_db(df, table_name_closed)
    except Exception as e:
        print(f"An error occurred while processing file {file}: {e}")
        logging.error(f"An error occurred while processing file {file}: {e}")
        send_email('Data Cleaning Error', f"An error occurred while processing file {file}: {e}", log_file)


def move_processed_files(source_dir, destination_dir):

    files_to_move = glob.glob(os.path.join(source_dir, "*.xlsx"))
    if not files_to_move:
        print("No files to move in the source directory.")
        return


    if not os.path.exists(destination_dir):
        os.makedirs(destination_dir)


    for file in files_to_move:
        try:
            shutil.move(file, destination_dir)
            print(f"Moved: {file} -> {destination_dir}")
        except Exception as e:
            print(f"Failed to move file {file}: {e}")
            logging.error(f"Failed to move file {file}: {e}")

network_path = r'\\schaeffler.com\taicang\Data\OP-SCA-PI\PII\08_Private Database\37_IM_Database'
xlsx_files = glob.glob(os.path.join(network_path, "*.xlsx"))

latest_files = {}
for file in xlsx_files:
    if not os.path.basename(file).startswith('~$'):
        if '提交的合理化建议自己员工_' in os.path.basename(file):
            latest_files.setdefault(table_name_default, []).append((os.path.getmtime(file), file))
        elif '完成的合理化建议自己的员工_' in os.path.basename(file):
            latest_files.setdefault(table_name_closed, []).append((os.path.getmtime(file), file))

latest_files = {k: max(v, key=lambda x: x[0])[1] for k, v in latest_files.items()}

if latest_files:  
    engine = create_engine(
        f'mssql+pyodbc://{config_data["mssql"]["user"]}:{config_data["mssql"]["password"]}@'
        f'{config_data["mssql"]["server"]}/{config_data["mssql"]["database"]}?driver=ODBC+Driver+17+for+SQL+Server',
        fast_executemany=True
    )


    clear_table(engine, table_name_default)
    clear_table(engine, table_name_closed)

    for target_table, file in latest_files.items():
        try:
            if target_table == table_name_default:
                process_excel_default(file)
            elif target_table == table_name_closed:
                process_excel_closed(file)
        except Exception as e:
            print(f"Failed to process file {file}: {e}")
            logging.error(f"An error occurred while processing file {file}: {e}")
            send_email('Path Error', f"An error occurred while processing file {file}: {e}", log_file)

    move_processed_files(network_path, r'\\schaeffler.com\taicang\Data\OP-SCA-PI\PII\08_Private Database\37_IM_Database\00_History')

    print("All files processed and moved.")
else:
    print("No files to process.")
