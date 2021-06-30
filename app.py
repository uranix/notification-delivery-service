import logging
import re
import time

from flask import Flask, request
from flask.json import jsonify

from sender import SendQueue, SenderThread


logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)8s] %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)
send_queue = SendQueue(maxsize=300)
SenderThread(send_queue).start()

filters = {}
filter_id = 0

def to_model(filter):
    return {
        'id': filter['id'],
        'pattern': filter['pattern'],
    }

@app.route("/filters", methods=['GET'])
def list_filters():
    return jsonify([to_model(filter) for filter in filters.values()])

@app.route("/filters", methods=['POST'])
def add_filter():
    if not request.json or not 'pattern' in request.json:
        return jsonify({'message': 'field "pattern" is missing from json request'}), 400
    pattern = request.json['pattern']
    try:
        compiled = re.compile(pattern)
    except Exception as e:
        return jsonify({'message': str(e)}), 400

    logger.info('Adding filter with pattern %s', pattern)
    global filter_id
    filter_id += 1
    filters[filter_id] = {
        'id': filter_id,
        'pattern': pattern,
        'compiled': compiled
    }
    return jsonify(to_model(filters[filter_id]))

@app.route("/filters/<int:filter_id>", methods=['GET'])
def get_filter(filter_id):
    filter = filters.get(filter_id)
    if filter is None:
        return jsonify({'message': 'no filter with id=' + str(filter_id)}), 404
    return jsonify(to_model(filters[filter_id]))

@app.route("/filters/<int:filter_id>", methods=['DELETE'])
def del_filter(filter_id):
    if filter_id not in filters:
        return jsonify({'message': 'no filter with id=' + str(filter_id)}), 404
    filter = filters.get(filter_id)
    logger.info('Removing filter with pattern %s', filter['pattern'])
    del filters[filter_id]
    return jsonify(to_model(filter)), 204

@app.route("/send", methods=["POST"])
def send():
    if not request.json or not 'body' in request.json:
        return jsonify({'message': 'field "body" is missing from json request'}), 400
    body = request.json['body']
    for filter in filters.values():
        id = filter['id']
        compiled = filter['compiled']
        if compiled.matches() is not None:
            return jsonify({'message': 'forbidden by filter id=' + str(id)}), 400
    if not send_queue.accept(time.monotonic(), body):
        return jsonify({'message': 'the queue is full'}), 429
    return jsonify({}), 202
