import multiprocessing
from time import time
from requests import get
import json

from src.consts import config
from src.consts import urls
from src.db.mongo import DB

table_name = "table1"
collection_name = "collection1"
db = DB(table_name, collection_name)
collection_name = "full_data"


def dump_full_data_by_id(id):
    url = urls.tank_stats_url.format(token=config.token2, id=id)
    response = get(url)
    return json.loads(response.text)


def process_id(id):
    db = DB(table_name, collection_name)
    if id % 1000 == 0:
        print("start with id: {}, thread name: {}, secs from start: {}".format(id, multiprocessing.current_process().name, time() - start_time))
    response = dump_full_data_by_id(id)
    try:
        data = response["data"][str(id)]
    except KeyError:
        print("ERROR KeyError with id: " + str(id) + " " + str(response))
        failed_ids.append(id)
        return
    if data:
        db.put_many(data)


start_time = time()

ids = [x["account_id"] for x in db.get_ids()]
failed_ids = []
pool = multiprocessing.Pool(processes=20)
print("start application")
pool_output = pool.map(process_id, ids)
pool_output2 = pool.map(process_id, failed_ids)
pool.close()
pool.join()
print("FINISHED in " + str(time() - start_time))
