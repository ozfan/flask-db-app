import os
import psycopg2
from flask import Flask, redirect, render_template, url_for
from urllib.parse import urlparse


app = Flask(__name__)
DATABASE_URL = postgres://vjnlwhwwpapkra:808600f376cc3c4efe8176d44786f56a73f6ecd73eb37b3d1709efedbb64be57@ec2-54-158-247-210.compute-1.amazonaws.com:5432/d2n0pg84ihotrn

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


@app.route('/')
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

def query(): #template for some query
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(('SELECT * FROM database;')) #insert query here
    books = cur.fetchall() #fetches query and put into object
    cur.close() #closes query
    conn.close() #closes connection to db
    return render_template('index.html', books=books)

if __name__ == '__main__':
    app.run()
