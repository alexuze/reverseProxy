from asyncore import write
# from crypt import methods
from email.policy import default
from fileinput import filename
from functools import cache
from time import sleep
from flask import Flask, request, jsonify, url_for
from celery import Celery
import sys
import requests
import time
import os
# import threading
from ratelimiter import RateLimiter

import ast


time_to_keep_file = 60

app = Flask(__name__)
app.config['CELERY_BROKER_URL'] = 'redis://localhost/0'
app.config['CELERY_RESULT_BACKEND'] = 'redis://localhost/0'

celery = Celery(
    app.name, broker=app.config['CELERY_BROKER_URL'], backend=app.config['CELERY_RESULT_BACKEND'])


def create_file_name(url):
    suffix = url.split("/")[1:]  # get the url without http:
    delimiter = "_"
    filename = delimiter.join(suffix)
    return "cache"+filename+".txt"


@app.route("/", methods=['GET', 'POST'])
@RateLimiter(max_calls=10, period=60)
@RateLimiter(max_calls=1000, period=86400)
def handle_req():
    method = request.method
    if method == "GET":
        return handle_get()
    return handle_post()


def handle_get():
    url = request.args.get(
        'url', default="*", type=str)
    filename = create_file_name(url)
    # print(suffix)
    res_from_cache = fetch_from_cache(filename)
    if res_from_cache:
        print("Got result from cache")
        return res_from_cache
    else:
        task = execute_get.delay(url)
        return jsonify({
            'res': {},
            'status': '202',
            'Location': url_for('task_status_get', task_id=task.id)
        })


def handle_post():
    url = request.args.get(
        'url', default="*", type=str)
    data = request.get_json()
    print(data)
    task = execute_post.delay(url, data)
    return jsonify({
        'res': {},
        'status': '202',
        'Location': url_for('task_status_post', task_id=task.id)
    })


@celery.task(bind=True)
def execute_post(self, url, data):
    sleep(20)  # testing
    res = requests.post(url, data).json()
    return {'current': 100, 'total': 100, 'status': 'Task completed!',
            'result': res}


@celery.task(bind=True)
def execute_get(self, url):
    sleep(20)   # testing
    res = requests.get(url).json()
    print("received res from remote server")
    save_res_to_cache(res, url)
    return {'current': 100, 'total': 100, 'status': 'Task completed!',
            'result': res}


@app.route('/status_get/<task_id>')
def task_status_get(task_id):
    task = execute_get.AsyncResult(task_id)
    if task.state == 'PENDING':
        # job did not start yet
        response = {
            'state': task.state,
            'current': 0,
            'total': 1,
            'status': 'Pending...'
        }
    elif task.state != 'FAILURE':
        response = {
            'state': task.state,
            'current': task.info.get('current', 0),
            'total': task.info.get('total', 1),
            'status': task.info.get('status', '')
        }
        if 'result' in task.info:
            response['result'] = task.info['result']
    else:
        # something went wrong in the background job
        response = {
            'state': task.state,
            'current': 1,
            'total': 1,
            'status': str(task.info),  # this is the exception raised
        }
    return jsonify(response)


@app.route('/status_post/<task_id>')
def task_status_post(task_id):
    task = execute_post.AsyncResult(task_id)
    if task.state == 'PENDING':
        # job did not start yet
        response = {
            'state': task.state,
            'current': 0,
            'total': 1,
            'status': 'Pending...'
        }
    elif task.state != 'FAILURE':
        response = {
            'state': task.state,
            'current': task.info.get('current', 0),
            'total': task.info.get('total', 1),
            'status': task.info.get('status', '')
        }
        if 'result' in task.info:
            response['result'] = task.info['result']
    else:
        # something went wrong in the background job
        response = {
            'state': task.state,
            'current': 1,
            'total': 1,
            'status': str(task.info),  # this is the exception raised
        }
    return jsonify(response)


def fetch_from_cache(filename):
    try:
        # Check if we have this file locally
        fin = open(filename)
        content = fin.read()
        fin.close()
        if check_file_validity(filename):
            # If we have it, let's send it
            return ast.literal_eval(content)
        else:
            delete_file(filename)
            return None
    except IOError:
        return None


def save_res_to_cache(res, url):
    filename = create_file_name(url)
    new_file = open(filename, 'w')
    writable_res = str(res)
    new_file.write(writable_res)
    new_file.close()


def check_file_validity(filename):
    file_creation_time = os.path.getctime(filename)
    curr_time = time.time()
    return curr_time - file_creation_time <= time_to_keep_file


def delete_file(filename):
    try:
        os.remove(filename)
        return True
    except Exception as e:
        print(e)
        return None


if __name__ == '__main__':
    if len(sys.argv) > 1:
        time_to_keep_file = int(sys.argv[1])
    app.run()
