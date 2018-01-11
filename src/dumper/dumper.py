from requests import get
import json
from queue import Queue
from threading import Thread
import multiprocessing
from time import time

from src.consts import config
from src.consts import urls
from src.db.mongo import DB


def dump_by_first_id(first_id):
    ids = ",".join([str(x) for x in range(first_id, first_id + 100)])
    url = urls.account_info_url.format(token=config.token, ids=ids)
    response = get(url)
    return json.loads(response.text)


table_name = "table1"
collection_name = "collection1"


def process_id(id):
    db = DB(table_name, collection_name)
    if id % 100000 == 0:
        print("start with id: {}, thread name: {}, secs from start: {}".format(id, multiprocessing.current_process().name, time() - start_time))
    response = dump_by_first_id(id)
    if response["status"] != "ok":
        print("ERROR with id: " + str(id) + " " + response["error"]["message"] + " " + str(response))
        return
    try:
        data = [x for x in response["data"].values() if x]
    except KeyError:
        print("ERROR KeyError with id: " + str(id) + " " + str(response))
        return
    if data:
        db.put_many(data)


start_time = time()
pool = multiprocessing.Pool(processes=20)
pool_output = pool.map(process_id, range(10000000, 100000000, 100))
pool.close()
pool.join()
print("FINISHED in " + str(time() - start_time))
