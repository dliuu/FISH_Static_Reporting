import requests
import json
from datetime import datetime

class BubbleAPI:
    def __init__(self, raw_url: str, apikey: str):
        self.raw_url = raw_url
        self.apikey = apikey
        self.headers = {
            'Content-type': 'application/json',
            'Authorization': f'Bearer {apikey}',
        }

    def merge_constraints(self, key_list, type_list, value_list):
        if len(key_list) != len(type_list) or len(key_list) != len(value_list):
            raise ValueError("Input lists must have the same length")

        merged_dicts = []
        for i in range(len(key_list)):
            merged_dicts.append({
                "key": key_list[i],
                "constraint_type": type_list[i],
                "value": value_list[i]
            })
        return merged_dicts

    def get_datetime(self):
        return datetime.today().strftime('%Y-%m-%d')

    def GET_single_object(self, obj: str, **kwargs):
        url = f"{self.raw_url}/{obj}"
        r = requests.get(url)
        response = r.json()
        return response

    def GET_all_objects(self, obj: str, cursor=0, **kwargs,):
        url = f"{self.raw_url}/{obj}"

        if kwargs:
            key_list = kwargs.get('key_list')
            type_list = kwargs.get('type_list')
            value_list = kwargs.get('value_list')

            constraints = self.merge_constraints(key_list, type_list, value_list)

            if len(constraints) == 0:
                r = requests.get(url, headers=self.headers)
                return r.json()
            else:
                url = f"{url}?constraints={constraints}"
                r = requests.get(url, headers=self.headers)
                return r.json()
        else:
            if cursor > 0:
                return_json = {'response': {'results': []}}
                remaining_items = 1

                while remaining_items > 0:

                    r = requests.get(url= f"{url}?cursor={cursor}")
                    data = r.json()

                    if "response" in data:
                        if len(return_json['response']['results']) == 0:
                            return_json['response']['results'] = data['response']['results']
                        else:
                            return_json['response']['results'] += data['response']['results']

                    if data['response']['remaining'] > 0:
                        remaining_items = data['response']['remaining']
                        if remaining_items >= 100:
                            cursor+=100
                        else:
                            cursor+=data['response']['remaining']
                    else:
                        return json.dumps(return_json)

            else:
                r = requests.get(url, headers=self.headers)
                return r.json()
    

    def write_to_file(self, obj: str, api_type: str, filename: str):
        if api_type == 'single':
            raw_obj = self.GET_single_object(obj)
        elif api_type == 'all':
            raw_obj = self.GET_all_objects(obj)
        else:
            print('Error: Please input a valid api_type')
            return api_type

        json_obj = json.dumps(raw_obj, indent=4)
        day = self.get_datetime()

        with open(f"{day}-{filename}.json", "w") as outfile:
            outfile.write(json_obj)


raw_url = 'https://ifish.tech/version-test/api/1.1/wf'
apikey = '3d83175353e3af62cc0d4dd5c167a855'
bubble_api = BubbleAPI(raw_url, apikey)

print(bubble_api.GET_all_objects('reporting_colchis_report'))

