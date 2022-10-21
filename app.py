import os
import psycopg2
import jinja2
from flask import Flask, redirect, request, render_template, url_for
from urllib.parse import urlparse


app = Flask(__name__)

def get_db_connection():
    result = urlparse(os.environ['DATABASE_URL'])
    username = result.username
    password = result.password
    database = result.path[1:]
    hostname = result.hostname
    port = result.port
    connection = psycopg2.connect(
        database=database,
        user=username,
        password=password,
        host=hostname,
        port=port
    )

    return connection


@app.route('/<path:text>', methods=['GET', 'POST'])
def all_routes(text):
    if text.startswith('pages') or text.startswith('sections'):
        return render_template(text)


@app.route('/', methods=['GET'])
def hello_world():  # put application's code here
    conn = get_db_connection()
    # create a cursor
    cur = conn.cursor()

    # execute a statement
    print('PostgreSQL database version:')
    cur.execute('SELECT version()')

    # display the PostgreSQL database server version
    db_version = cur.fetchone()
    print(db_version[0])

    # close the communication with the PostgreSQL
    cur.close()
    return render_template('index.html', version=db_version[0])

def index():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('SELECT * FROM membership_dimension;')
    members = cur.fetchall()
    cur.close()
    conn.close()
    return render_template('index.html', members=members)

def query(): #template for some query
    if request.method == 'GET':
        start_date = request.form['startDate']
        end_date = request.form['endDate']
        table = request.form['table']
        sum_value = request.form['value']
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(('SELECT sum(sum_value) FROM table, date_dimension WHERE date BETWEEN startDate AND endDate;')) #insert query here
        table = cur.fetchall() #fetches query and put into object
        cur.close() #closes query
        conn.close() #closes connection to db
    return render_template('index.html', table=table)

if __name__ == '__main__':
    app.run()
