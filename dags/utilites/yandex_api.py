import requests
import datetime
import json
import io

FROM_DATE = (
    datetime.datetime.now() - datetime.timedelta(days=7)
).strftime("%Y-%m-%d %H:%M:%S")

URL = "https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/%s?"
PATH = f"from={FROM_DATE}&sort_field=_id&sort_direction=asc&limit=50&offset=%d"


class YandexApi:
    def __init__(self, nickname: str, cohort: str, api_key: str) -> None:
        self.nickname = nickname
        self.cohort = cohort
        self.api_key = api_key

    def make_headers(self) -> dict:
        return {
            "X-Nickname": self.nickname,
            "X-Cohort": self.cohort,
            "X-API-KEY": self.api_key
        }

    def get_requests(self, field: str):
        buffer = io.StringIO()
        d = {"objects": []}
        try:
            session = requests.session()
            offset = 0
            while True:
                response = session.get(
                    url=URL % field + PATH % (offset),
                    headers=self.make_headers()
                ).json()

                if len(response) == 0:
                    break

                for i in response:
                    d["objects"].append(i)
                offset += len(response)

            json.dump(d, buffer, sort_keys=True, ensure_ascii=False)
            return buffer
        except requests.exceptions.RequestException as error:
            raise error
