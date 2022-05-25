from email.policy import default
from functools import cache
from time import sleep
from flask import Flask, request, jsonify, url_for
from celery import Celery
import sys
import requests
from multiprocessing import Manager


# manager = Manager()
# serviceLock = manager.Lock()
# serviceStatusDict = manager.dict()
cache = {}

app = Flask(__name__)
app.config['CELERY_BROKER_URL'] = 'redis://localhost/0'
app.config['CELERY_RESULT_BACKEND'] = 'redis://localhost/0'

celery = Celery(
    app.name, broker=app.config['CELERY_BROKER_URL'], backend=app.config['CELERY_RESULT_BACKEND'])


@app.get("/")
def handle_get():
    url = request.args.get(
        'url', default="*", type=str)
    if url in cache:
        return cache[url]
    task = execute_get.delay(url)
    return jsonify({
        'res': {},
        'status': '202',
        'Location': url_for('task_status', task_id=task.id)
    })


@app.post("/")
def handle_post():
    url = request.args.get(
        'url', default="*", type=str)
    data = {"name": "morpheus", "job": "leader"}
    return "True"


@celery.task(bind=True)
def execute_get(self, url):
    sleep(20)   # testing
    res = requests.get(url).json()
    print("received res from remote server")
    # serviceLock.acquire()
    cache[url] = res
    print("the cache[{}] is = {}".format(url, cache[url]))
    # serviceLock.release()
    return {'current': 100, 'total': 100, 'status': 'Task completed!',
            'result': res}


@app.route('/status/<task_id>')
def task_status(task_id):
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


if __name__ == '__main__':
    app.run()
