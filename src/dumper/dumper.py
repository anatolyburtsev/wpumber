from requests import get
import json

from src.consts import config
from src.consts import urls


def dump_by_first_id(first_id):
    ids = ",".join([str(x) for x in range(first_id, first_id + 100)])
    url = urls.account_info_url.format(token = config.token, ids = ids)
    response = get(url)
    return json.loads(response.text)

print(dump_by_first_id(1000)["data"])