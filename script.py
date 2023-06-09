# Timing the script execution
import time
start_time = time.time()

# importing necessary libraries
import pandas as pd
import psycopg2
import datetime
import numpy as np
import psycopg2.extras as extras


# create database 'sample_db'
user='postgres' 
password='2907' 
host='127.0.0.1' 
port='5432'

conn = psycopg2.connect(database="postgres", user=user, password=password, host=host, port=port)
  
conn.autocommit = True
cursor = conn.cursor()

sqls = ['''DROP DATABASE IF EXISTS milestone1_db;''',
'''CREATE database milestone1_db;''']

for sql in sqls:
    cursor.execute(sql)
print("Database created successfully........")

#Closing the connection
cursor.close()
conn.close()

# read data from csv to df
df_loan = pd.read_csv("loan.csv", sep=';')
df_district = pd.read_csv("district.csv", sep=';')
df_disp = pd.read_csv("disp.csv", sep=';')
df_account = pd.read_csv("account.csv", sep=';')
df_trans = pd.read_csv("trans.csv", sep=';')
df_client = pd.read_csv("client.csv", sep=';')
df_card = pd.read_csv("card.csv", sep=';')
df_order = pd.read_csv("order.csv", sep=';')

# date formatting
df_loan['date'] = df_loan['date'] + 19000000
df_loan['date'] = pd.to_datetime(df_loan['date'], format='%Y%m%d')
df_trans['date'] = df_trans['date'] + 19000000
df_trans['date'] = pd.to_datetime(df_trans['date'], format='%Y%m%d')
df_account['date'] = df_account['date'] + 19000000
df_account['date'] = pd.to_datetime(df_account['date'], format='%Y%m%d')

''' creating gender column for client dataframe
As per data source:   
YYMMDD - Men
YYMM50+DD - Women '''

df_client['gender'] = '?'
for ind, row in df_client.iterrows():
    x = str(row['birth_number'])
    month = int(x[2:4])
    if month > 12:
        month -= 50
        if month >= 10:
            y = x[:2] + str(month) + x[4:]
            df_client.iloc[ind, 1] = int(y) + 19000000
            df_client.iloc[ind, 3] = 'Female'
        else:
            y = x[:2] + '0' + str(month) + x[4:]
            df_client.iloc[ind, 1] = int(y) + 19000000
            df_client.iloc[ind, 3] = 'Female'
    else:
        df_client.iloc[ind, 1] += 19000000
        df_client.iloc[ind, 3] = 'Male'
        
df_client['birth_number'] = pd.to_datetime(df_client['birth_number'], format='%Y%m%d')    


# creating date table
df_date = pd.DataFrame(pd.date_range(start='1911-08-20', end='1998-12-31'), columns=['date'])
df_date['year'] = pd.DatetimeIndex(df_date['date']).year
df_date['month'] = pd.DatetimeIndex(df_date['date']).month
df_date['day'] = pd.DatetimeIndex(df_date['date']).day

# data cleaning (translations)
df_account['frequency'] = df_account['frequency'].replace({'POPLATEK MESICNE': 'MONTHLY ISSUANCE (MI)', 
                                                           'POPLATEK TYDNE':'WEEKLY ISSUANCE (WI)', 
                          'POPLATEK PO OBRATU':'ISSUANCE AFTER TRANSACTION (TI)'})
df_disp['type'] = df_disp['type'].replace({'DISPONENT':'USER'})
df_trans['type'] = df_trans['type'].replace({'PRIJEM':'Credit', 'VYDAJ':'Withdrawl','VYBER':'Withdrawal in Cash (WC)'})
df_trans['operation'] = df_trans['operation'].replace({'VYBER':'Withdrawal in Cash (WC)', 'PREVOD NA UCET':'Remittance to Another Bank (RAB)',
                              'VKLAD':'Credit in Cash (CRC)','PREVOD Z UCTU':'Collection from Another Bank (CAB)',
                              'VYBER KARTOU':'Credit Card Withdrawal (CCW)'})
df_trans['k_symbol'] = df_trans['k_symbol'].replace({'UROK':'Interest Credited (ICR)', 'SLUZBY':'Payment on Statement (PS)',
                                                    'SIPO':'Household (H)','DUCHOD':'Old-age Pension (OP)',
                                                    'POJISTNE':'Insurance Payment (IP)','UVER':'Loan Payment (LP)',
                                                    'SANKC. UROK':'Sanction Interest (SI)'})

for ind, row in df_card.iterrows():
    date = int(row['issued'].split(' ')[0])
    date = date + 19000000
    df_card.iloc[ind, 3] = date
df_card['issued'] = pd.to_datetime(df_card['issued'], format='%Y%m%d')

# proper headers for demographics df
districts_cols = ['dist_id','dist_name','region_name','no_inhab','no_muncp_lt_499','no_muncp_500_1499','no_muncp_2000_9999','no_muncp_gt_10000','no_cities','ratio_urban_inhab','avg_sal','unemploy_rate_1995','unemploy_rate_1996','entrep_per_1000_inhab','no_crimes_1995','no_crimes_1996']
df_district.columns = districts_cols

# data cleaning (datatypes)
df_district.loc[(df_district['unemploy_rate_1995'] == '?'),'unemploy_rate_1995']='0.0'
df_district.loc[(df_district['no_crimes_1995'] == '?'),'no_crimes_1995']='0'
df_district['unemploy_rate_1995'] = df_district['unemploy_rate_1995'].astype('float64')
df_district['no_crimes_1995'] = df_district['no_crimes_1995'].astype('int64')

create_table_sqls = [
'''
CREATE TABLE IF NOT EXISTS DEMOGRAPHICS(
    dist_id   integer PRIMARY KEY,
    dist_name varchar,
    region_name varchar,
    no_inhab integer,
    no_muncp_lt_499 integer,
    no_muncp_500_1499 integer,
    no_muncp_2000_9999 integer,
    no_muncp_gt_10000 integer,
    no_cities integer,
    ratio_urban_inhab real,
    avg_sal real,
    unemploy_rate_1995 real,
    unemploy_rate_1996 real,
    entrep_per_1000_inhab real,
    no_crimes_1995 integer,
    no_crimes_1996 integer
     );                    
''',
'''
CREATE TABLE IF NOT EXISTS DATE(
    date date PRIMARY KEY,
    year int,
    month int,
    day int
);
''',
'''
CREATE TABLE IF NOT EXISTS ACCOUNT(
    account_id integer PRIMARY KEY,
    district_id integer NOT NULL,
    frequency varchar,
    date Date NOT NULL,
    CONSTRAINT fk_account_demographics foreign key(district_id) references DEMOGRAPHICS (dist_id),
    CONSTRAINT fk_account_date FOREIGN KEY(date) REFERENCES DATE(date)
);
''',
'''
CREATE TABLE IF NOT EXISTS LOAN(
    loan_id     integer PRIMARY KEY,
    account_id  integer NOT NULL,
    date        date NOT NULL,
    amount      integer,
    duration    integer,
    payments    real,
    status      varchar,
    CONSTRAINT fk_loan_account FOREIGN KEY(account_id) REFERENCES ACCOUNT(account_id),
    CONSTRAINT fk_loan_date FOREIGN KEY(date) REFERENCES DATE(date)
);
''',

'''
CREATE TABLE IF NOT EXISTS PERMANENT_ORDER(
    order_id  integer PRIMARY KEY,
    account_id integer NOT NULL,
    bank_to varchar,
    account_to integer,
    amount real,
    k_symbol varchar,
    CONSTRAINT fk_order_account FOREIGN KEY(account_id) REFERENCES ACCOUNT(account_id)
);
''',                                   
'''
CREATE TABLE IF NOT EXISTS CLIENT(
    client_id integer PRIMARY KEY,
    birth_number date NOT NULL,
    district_id integer NOT NULL,
    gender varchar,
    CONSTRAINT fk_client_demographics foreign key(district_id) references DEMOGRAPHICS (dist_id),
    CONSTRAINT fk_client_date FOREIGN KEY(birth_number) REFERENCES DATE(date)
);
''',
    
'''
CREATE TABLE IF NOT EXISTS DISPOSITION(
    disp_id integer PRIMARY KEY,
    client_id integer NOT NULL,
    account_id integer NOT NULL,
    type varchar,
    CONSTRAINT fk_disp_client foreign key(client_id) references CLIENT(client_id),
    CONSTRAINT fk_disp_acc foreign key(account_id) references ACCOUNT(account_id)
);
''',
    
'''
CREATE TABLE IF NOT EXISTS CARD(
    card_id integer PRIMARY KEY,
    disp_id integer NOT NULL,
    type varchar,
    issued date NOT NULL,
    CONSTRAINT fk_card_disp foreign key(disp_id) references DISPOSITION (disp_id),
    CONSTRAINT fk_card_date FOREIGN KEY(issued) REFERENCES DATE(date)
);
''',

'''
CREATE TABLE IF NOT EXISTS TRANSACTIONS(
    trans_id integer PRIMARY KEY,
    account_id integer NOT NULL,
    date DATE NOT NULL,
    type varchar,
    operation varchar,
    amount real,
    balance real,
    k_symbol varchar,
    bank varchar,
    account real,
    CONSTRAINT fk_trans_acc foreign key(account_id) references ACCOUNT(account_id),
    CONSTRAINT fk_trans_date FOREIGN KEY(date) REFERENCES DATE(date)
);
''',
    
'''
CREATE TABLE IF NOT EXISTS USER_TABLE(
    login_id SERIAL PRIMARY KEY,
    username TEXT UNIQUE NOT NULL,
    password TEXT NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('admin', 'manager', 'employee', 'board_member', 'IT'))
);
''',
    
'''
CREATE TABLE IF NOT EXISTS ACTIVITY(
    id SERIAL PRIMARY KEY,
    username VARCHAR(255),
    query TEXT,
    execution_time TIMESTAMP,
    CONSTRAINT fk_act_user foreign key(username) references USER_TABLE(username)
);
'''
                    ]

# Creating connection to our postgres DB 'milestone1_db'
conn = psycopg2.connect(database="milestone1_db", user=user, password=password, host=host, port=port)
  
conn.autocommit = True
cursor = conn.cursor()

for sql in create_table_sqls:
    cursor.execute(sql)

cursor.execute("INSERT INTO USER_TABLE(username, password, role) values ('postgres','2907','admin');")
cursor.close()
conn.commit()

# defining function to create insert statements in a loop
def execute_values(conn, df, table, page_size = 1000):
    print(f"Loading data for table {table}")
    tuples = [tuple(x) for x in df.to_numpy()]
    cols = ','.join(list(df.columns))
    num_cols = len(df.columns)
    vals_str = "%s,"*num_cols
    # SQL query to execute
    query = f"INSERT INTO {table}({cols}) VALUES ({vals_str[:-1]})"
    cursor = conn.cursor()
    inserted_count = 0
    for i in range(0, len(tuples), page_size):
        batch = tuples[i:i+page_size]
        try:
            extras.execute_batch(cursor, query, batch)
            inserted_count += len(batch)
        except psycopg2.errors.ForeignKeyViolation as fk_error:
            print(fk_error)
            print(f"inserted count: {inserted_count}")
            conn.rollback()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            print(f"inserted count: {inserted_count}")
            conn.rollback()
            cursor.close()
            return 1
    
    conn.commit()
    cursor.close()
    
    print(f"{inserted_count} rows inserted into {table} table")
    

execute_values(conn, df_district, 'demographics')
execute_values(conn, df_date, 'date')
execute_values(conn, df_account, 'account')
execute_values(conn, df_loan, 'loan')
execute_values(conn, df_order, 'permanent_order')
execute_values(conn, df_client, 'client')
execute_values(conn, df_disp, 'disposition')
execute_values(conn, df_card, 'card')
execute_values(conn, df_trans, 'transactions')
conn.close()



# Get the end time
end_time = time.time()
elapsed_time = end_time - start_time

def format_time(seconds):
    minutes, secs = divmod(seconds, 60)
    if minutes > 0:
        print(f"{minutes} minute{'s' if minutes > 1 else ''} and {secs} second{'s' if secs > 1 else ''}")
    else:
        print(f"{secs} second{'s' if secs > 1 else ''}")
    
format_time(elapsed_time)
