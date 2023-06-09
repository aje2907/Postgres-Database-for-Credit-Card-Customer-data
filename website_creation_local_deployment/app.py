from flask import Flask, render_template, request, flash, session, make_response
import pandas as pd
import psycopg2
import shutil
import os
import datetime
import webbrowser


app = Flask(__name__)
app.secret_key = 'secret_key'

@app.route('/', methods=['POST','GET'])
def home():
    session.clear()
    return render_template('home.html')

@app.route('/user_check', methods=['POST','GET'])
def user_check():
    user_check = request.form['user_check']
    if user_check == "Existing User":
        return render_template('login_page.html')
    elif user_check == "New User":
        return render_template('registration.html')
    
@app.route('/go_to_register',methods=['POST','GET'])
def go_to_register():
    return render_template('registration.html')
    
@app.route('/go_to_login',methods=['POST','GET'])
def go_to_login():
    return render_template('login_page.html')

@app.route('/login', methods=['POST'])
def login():
    try:
        database = "milestone1_db"
        user='postgres' 
        password='2907' 
        host='127.0.0.1' 
        port='5432'
        conn = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)

        entry_username = str(request.form['username']).strip()
        entry_password = str(request.form['password']).strip()

        cursor = conn.cursor()
        sql = f"select * from user_table where username = '{entry_username}' and password = '{entry_password}';"
        cursor.execute(sql)
        entry = cursor.fetchone()
        cursor.close()
        conn.close()

        if entry is not None:
            session['username'] = entry[1]
            session['password'] = entry[2]
            return render_template('main_page.html')
        else:
            flash('Invalid username or password')
            return render_template('login_page.html')
    except Exception as e:
        error_message = str(e)
        cursor.close()
        conn.close()
        return render_template('error_page.html', error_message = error_message)


@app.route('/register', methods=['POST','GET'])
def register():
    try:
        database = "milestone1_db"
        user='postgres' 
        password='2907' 
        host='127.0.0.1' 
        port='5432'

        entry_username = str(request.form['username']).strip()
        entry_password = str(request.form['password']).strip()
        entry_role = str(request.form['role'])

        if entry_password == '' or entry_username == '' or entry_role == '':
            error_message = "Please enter all fields for new user registration."
            return render_template('error_page.html', error_message = error_message)

        conn = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
        cursor = conn.cursor()
        sql = f"INSERT INTO USER_TABLE (USERNAME, PASSWORD, ROLE) VALUES ('{entry_username}', '{entry_password}', '{entry_role}');"
        cursor.execute(sql)
        create_user = f"CREATE USER {entry_username} WITH PASSWORD '{entry_password}';"
        cursor.execute(create_user)
        grant_sql = f'GRANT {entry_role} TO {entry_username};'
        cursor.execute(grant_sql)
        conn.commit()
        cursor.close()
        conn.close()
        flash('New User created. Please login.')
        return render_template('login_page.html')
    
    except Exception as e:
        error_message = str(e)
        cursor.close()
        conn.close()
        return render_template('error_page.html', error_message = error_message)

@app.route('/logout', methods=['POST'])
def logout():
    session.clear()
    return render_template('home.html')

@app.route('/operation', methods=['POST'])
def operation():
    try:
        operation_name = request.form['operation']
        session['operation_name'] = operation_name
        if operation_name == 'read':
           return render_template('read.html')
        
        else:
            return render_template('table_choose.html')

    except Exception as e:
        error_message = str(e)
        return render_template('error_page.html', error_message = error_message)

@app.route('/read', methods = ['POST'])
def read():
    database = "milestone1_db"
    username = session.get('username')
    password = session.get('password')
    host='127.0.0.1' 
    port='5432'
    table = request.form['table']
    rows = request.form['rows']
    conn = psycopg2.connect(database=database, user=username, password=password, host=host, port=port)
    try:
        if rows == 'all':
            sql = f'SELECT * FROM {table};'
        else:
            rows = int(request.form['num_rows']) 
            sql = f'SELECT * FROM {table} LIMIT {rows}'
        df = pd.read_sql_query(sql, conn)
        # html_table = df.to_string(index=False)
        conn.close()
        df_styled = df.style.set_properties(           
                **{"text-align": "center", "font-weight": "bold"}
            ).hide(axis="index")
        
        if os.path.exists('templates/myFile.html'):
            os.remove('templates/myFile.html')
        # export html with style
        df_styled.to_html("templates/myFile.html")
        chrome_path = r"C:\Program Files\Google\Chrome\Application\chrome.exe"
        webbrowser.register('chrome', None, webbrowser.BackgroundBrowser(chrome_path))

        # Open file in Chrome
        file_path = r"C:\Users\aje29\OneDrive\Documents\MS Course work\EAS 560 - Data Models and Query Language\Project\Front_end_2\templates\myFile.html"
        new_tab = 2  # opens the file in a new browser tab
        webbrowser.get('chrome').open("file://" + file_path, new=new_tab)
        return render_template('read.html')

    except Exception as e:
        error_message = str(e)
        conn.close()
        return render_template('error_page.html', error_message = error_message)       

@app.route('/table_choose', methods = ['POST'])
def table_choose():
    operation_name = session.get('operation_name')
    table = request.form['table']
    session['table'] = table
    if operation_name != 'update':
        return render_template(f'{table}_modified.html')
    else:
        return render_template(f'{table}_update.html')



@app.route('/delete_update_insert', methods = ['POST'])
def delete_update_insert():
    operation_name = session.get('operation_name')
    table = session.get('table')
    if operation_name == 'delete':
        return delete()
    elif operation_name == 'insert':
        return insert()
    elif operation_name == 'update':
        return update()
     

def delete():
    try:
        database = "milestone1_db"
        username = session.get('username')
        password = session.get('password')
        host='127.0.0.1' 
        port='5432'
        conn = psycopg2.connect(database=database, user=username, password=password, host=host, port=port)
        cursor = conn.cursor()
        table = session.get('table')
        operation_name = session.get('operation_name')

        sql1 = f'DELETE FROM {table} WHERE '

        columns = request.form.getlist('column[]')
        values = {}
        data_types = {}

        cursor.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table}'")
        for row in cursor.fetchall():
            column_name, data_type = row
            data_types[column_name] = data_type

        for column in columns:
            value = request.form.get(column)
            # Convert value to correct data type
            if data_types[column] == 'integer':
                value = int(value)
                values[column] = value
                sql1 = sql1 + f"{column} = {value} AND "
            elif data_types[column] == 'date':
                value = datetime.datetime.strptime(value, '%Y-%m-%d').date()
                values[column] = value
                sql1 = sql1 + f"{column} = '{value}' AND "
            else:
                values[column] = value
                sql1 = sql1 + f"{column} = '{value}' AND "
            
        query = sql1[:-5]
            
        cursor.execute(query)
        conn.commit()
        cursor.close()
        conn.close()
        # flash(f"{operation_name} operation successful. You are still performing {operation_name} operation currently.")
        # return render_template(f'{table}_modified.html')
        message = f"{operation_name} operation successful. You are still performing {operation_name} operation currently."
        return render_template(f'{table}_modified.html', message=message)
    
    except Exception as e:
        error_message = str(e)
        conn.close()
        return render_template('error_page.html', error_message = error_message)      

def insert():
    try:
        database = "milestone1_db"
        user='postgres' 
        password='2907' 
        host='127.0.0.1' 
        port='5432'
        conn = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
        cursor = conn.cursor()
        operation_name = session.get('operation_name')
        table = session.get('table')
        columns = request.form.getlist('column[]')
        values = []
        # Get data type information for each column
        data_types = {}
        cursor.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table}'")
        for row in cursor.fetchall():
            column_name, data_type = row
            data_types[column_name] = data_type
        cursor.close()
        conn.close()
        
        columns_joined = ', '.join(columns)
        values_holder = []
        for column in columns:
            value = request.form.get(column)
            # Convert value to correct data type
            if data_types[column] == 'integer':
                value = int(value)
            elif data_types[column] == 'date':
                value = datetime.datetime.strptime(value, '%Y-%m-%d').date()
            values.append(value)
            values_holder.append('%s')
        values_holder = ', '.join(values_holder)

        database = "milestone1_db"
        username = session.get('username')
        password = session.get('password')
        host='127.0.0.1' 
        port='5432'
        conn = psycopg2.connect(database=database, user=username, password=password, host=host, port=port)
        cursor = conn.cursor()
        query = f"INSERT INTO {table} ({columns_joined}) VALUES ({values_holder});"
        cursor.execute(query, values)
        conn.commit()
        cursor.close()
        conn.close()
        # flash(f"{operation_name} operation successful. You are still performing {operation_name} operation currently.")
        message = f"{operation_name} operation successful. You are still performing {operation_name} operation currently."
        # return render_template(f'{table}_modified.html')
        return render_template(f'{table}_modified.html', message = message)

        # flash('Row inserted successfully!', 'success')

    except Exception as e:
        error_message = str(e)
        conn.close()
        return render_template('error_page.html', error_message = error_message)       

def update():
    try:
        database = "milestone1_db"
        user='postgres' 
        password='2907' 
        host='127.0.0.1' 
        port='5432'
        conn = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
        cursor = conn.cursor()
        operation_name = session.get('operation_name')
        table = session.get('table')
        data_types = {}
        cursor.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table}'")
        for row in cursor.fetchall():
            column_name, data_type = row
            data_types[column_name] = data_type
        cursor.close()
        conn.close()

        where_columns = request.form.getlist('column_where[]')
        where_dict = {}
        where = " WHERE "
        for where_column in where_columns:
            value = request.form.get(f'{where_column}_where')
            if data_types[where_column] == 'integer':
                value = int(value)
                where += f"{where_column} = {value} AND "
            elif data_types[where_column] == 'date':
                value = datetime.datetime.strptime(value, '%Y-%m-%d').date()
                where += f"{where_column} = '{value}' AND "
            else:
                where += f"{where_column} = '{value}' AND "
            where_dict[where_column] = value
        where = where[:-5]

        set_columns = request.form.getlist('column_new_value[]')
        set_dict = {}
        set = "SET "
        for set_column in set_columns:
            value = request.form.get(f'{set_column}_new_value')
            if data_types[set_column] == 'integer':
                value = int(value)
                set += f"{set_column} = {value}, "
            elif data_types[set_column] == 'date':
                value = datetime.datetime.strptime(value, '%Y-%m-%d').date()
                set += f"{set_column} = '{value}', "
            else:
                set += f"{set_column} = '{value}', "
            set_dict[set_column] = value
        set = set[:-2]

        database = "milestone1_db"
        username = session.get('username')
        password = session.get('password')
        host='127.0.0.1' 
        port='5432'
        conn = psycopg2.connect(database=database, user=username, password=password, host=host, port=port)
        cursor = conn.cursor()

        query = f"UPDATE {table} " + set + where

        cursor.execute(query)
        conn.commit()
        cursor.close()
        conn.close()
        # flash(f"{operation_name} operation successful. You are still performing {operation_name} operation currently.")
        # return render_template(f'{table}_update.html')
        message = f"{operation_name} operation successful. You are still performing {operation_name} operation currently."
        return render_template(f'{table}_update.html', message = message)

    except Exception as e:
        error_message = str(e)
        conn.close()
        return render_template('error_page.html', error_message = error_message)      


@app.route('/go_to_page', methods = ['POST'])
def go_to_page():
    page = request.form['page']
    return render_template(f'{page}')
    

if __name__ == '__main__':
    app.run(debug=True)


