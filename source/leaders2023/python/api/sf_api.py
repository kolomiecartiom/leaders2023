import requests
import json
import urllib3
from os import getenv




urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class SmartFormsAPI:
    def __init__(self, visiology_conn) -> None:
        self.__login = visiology_conn['login']
        self.__password = visiology_conn['pass']
        self.__dc_host = visiology_conn['host'] + 'datacollection/api'
        self.__identity_host = visiology_conn['host'] + 'idsrv/connect/token'
        self.__headers = self._get_headers()
        self.dimentions_data = {}

    def _get_headers(self) -> dict:
        """Возвращает словарь с хедерами для работы с Smart Forms."""

        auth_data = {
            'grant_type': 'password',
            'scope': 'openid profile email roles viewer_api core_logic_facade',
            'password': self.__password,
            'username': self.__login,
            'response_type': 'id_token token'
        }
        authorization_headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Authorization': 'Basic cm8uY2xpZW50OmFtV25Cc3B9dipvfTYkSQ=='
        }
        authorization_response = requests.post(
            self.__identity_host,
            data=auth_data,
            headers=authorization_headers,
            verify=False)

        authorization_json_response = authorization_response.json()
        if authorization_response.status_code != 200:
            raise Exception(
                f'Error {authorization_response.status_code}',
                str(authorization_json_response))
        authorization_token = authorization_json_response['access_token']
        authorization_headers = {
            'X-API-VERSION': '2.0',
            'Content-Type': 'application/json charset=UTF-8',
            'Authorization': 'Bearer ' + authorization_token
        }
        return authorization_headers

    def get_measuregroups(self) -> dict:
        """Возвращает список групп показателей"""
        url = f'{self.__dc_host}/measuregroups/?getAll=true'
        response = requests.get(url, headers=self.__headers, verify=False)
        json_response = response.json()
        if response.status_code != 200:
            raise Exception(f'Error {response.status_code}', str(json_response))
        return json_response

    def create_measuregroup_elements(self, mg_name: str, mg_elements: list) -> dict:
        '''Создает элементы группы показателей.
           Возвращает ответ сервера о количестве созданных элементов.
           :rtype: object'''

        if not mg_name.startswith('measureGroup_'):
            mg_name = 'measureGroup_' + mg_name
        url = f'{self.__dc_host}/measuregroups/{mg_name}/elements'
        response = requests.post(url, headers=self.__headers, data=json.dumps(mg_elements), verify=False)
        json_response = response.json()
        if response.status_code != 200:
            raise Exception(f'Error {response.status_code}', str(json_response))
        return json_response

    def get_measuregroup_elements(self, mg_name: str, filters: dict = None) -> list:
        '''Возращает все элементы группы показателей.'''

        if not mg_name.startswith('measureGroup_'):
            mg_name = 'measureGroup_' + mg_name

        url = f'{self.__dc_host}/measuregroups/{mg_name}/elements?getAll=true'

        if filters:
            response = requests.get(url, headers=self.__headers, verify=False, data=json.dumps(filters))
        else:
            response = requests.get(url, headers=self.__headers, verify=False)
        json_response = response.json()

        if response.status_code != 200:
            raise Exception(f'Error {response.status_code}', str(json_response))
        return json_response.get('elements', [])

    def delete_measuregroup_elements(self, mg_name: str, filters: dict = None) -> dict:
        '''Удаляет элементы группы показателей по указанному фильтру.
           Если не указан фильтр, то удаляет все элементы.'''

        if not mg_name.startswith('measureGroup_'):
            mg_name = 'measureGroup_' + mg_name
        url = f'{self.__dc_host}/measuregroups/{mg_name}/elements'

        if filters:
            response = requests.delete(url, headers=self.__headers, data=json.dumps(filters), verify=False)
        else:
            response = requests.delete(url, headers=self.__headers, verify=False)

        json_response = response.json()
        if response.status_code != 200:
            raise Exception(f'Error {response.status_code}', str(json_response))

        return json_response

    def create_dimension_elements(self, dim_name: str, dim_elements: list) -> dict:
        '''Создает элементы измерения.'''

        if not dim_name.startswith('dim_'):
            dim_name = 'dim_' + dim_name

        url = f'{self.__dc_host}/dimensions/{dim_name}/elements'
        response = requests.post(url, headers=self.__headers, data=json.dumps(dim_elements), verify=False)
        json_response = response.json()

        if response.status_code != 200:
            raise Exception(f'Error {response.status_code}', str(json_response))

        return json_response

    def get_dimension_elements(self, dim_name: str) -> list:
        """Возращает все элементы измерения."""

        if not dim_name.startswith('dim_'):
            dim_name = 'dim_' + dim_name

        url = f'{self.__dc_host}/dimensions/{dim_name}/elements?getAll=true'
        response = requests.get(url, headers=self.__headers, verify=False)
        json_response = response.json()

        if response.status_code != 200:
            raise Exception(f'Error {response.status_code}', str(json_response))

        return json_response.get('elements', [])

    def update_dimension_elements(self, dim_name: str, update_options: list) -> dict:
        '''Обновляет элементы измерения.'''

        if not dim_name.startswith('dim_'):
            dim_name = 'dim_' + dim_name

        url = f'{self.__dc_host}/dimensions/{dim_name}/elements'
        response = requests.put(url, headers=self.__headers, data=json.dumps(update_options), verify=False)
        json_response = response.json()

        if response.status_code != 200:
            raise Exception(f'Error {response.status_code}', str(json_response))

        return json_response

    def delete_dimension_elements(self, dim_name: str, filters: dict = None) -> dict:
        '''Удаляет элементы измерения по указанному фильтру.
           Если не указан фильтр, то удаляет все элементы.'''

        if not dim_name.startswith('dim_'):
            dim_name = 'dim_' + dim_name

        url = f'{self.__dc_host}/dimensions/{dim_name}/elements'

        if filters:
            response = requests.delete(url, headers=self.__headers, data=json.dumps(filters), verify=False)
        else:
            response = requests.delete(url, headers=self.__headers, verify=False)

        json_response = response.json()
        if response.status_code != 200:
            raise Exception(f'Error {response.status_code}', str(json_response))
        return json_response

    def get_dimension_attributes(self, dim_name: str) -> list:
        """Возращает все атрибуты измерения."""

        if not dim_name.startswith('dim_'):
            dim_name = 'dim_' + dim_name

        url = f'{self.__dc_host}/dimensions/{dim_name}/attributes?getAll=true'
        response = requests.get(url, headers=self.__headers, verify=False)
        json_response = response.json()

        if response.status_code != 200:
            raise Exception(f'Error {response.status_code}', str(json_response))

        return json_response.get('attributes', [])

    def get_dimension_element_attribute(self, dim_name: str, element_id: int, attribute_name: str = None):
        '''Возвращает значение атрибута элемента измерения по указанному id элемента.'''

        if not dim_name.startswith('dim_'):
            dim_name = 'dim_' + dim_name

        if attribute_name is not None and not attribute_name.startswith('attr_'):
            attribute_name = 'attr_' + attribute_name

        if dim_name not in self.dimentions_data:
            self.dimentions_data[dim_name] = self.get_dimension_elements(dim_name)

        for element in self.dimentions_data[dim_name]:
            if element['id'] == element_id:
                if attribute_name is None:
                    return element['name']
                else:
                    for attr in element['attributes']:
                        if attr['attributeId'] == attribute_name:
                            return attr['value']
        return None

    def get_dimension_element_id(self, dim_name: str, element_name: str, data_source: str = None):
        '''Возвращает значение id элемента измерения по значению заголовочного атрибута.'''
        if not dim_name.startswith('dim_'):
            dim_name = 'dim_' + dim_name
        if dim_name not in self.dimentions_data:
            self.dimentions_data[dim_name] = self.get_dimension_elements(dim_name)
        for element in self.dimentions_data[dim_name]:
            if element['name'] != element_name: continue
            if data_source:
                for fl in element['path']:
                    if fl['folderName'] == data_source:
                        return element['id']
            else:
                return element['id']
        return None

    def get_dimension_element_id_by_attribute(self, dim_name: str, attribute_name: str, attribute_value: str):
        '''Возвращает значение id элемента измерения по значению выбранного атрибута.'''
        if attribute_value is None:
            return None

        if not dim_name.startswith('dim_'):
            dim_name = 'dim_' + dim_name

        if not attribute_name.startswith('attr_'):
            attribute_name = f'attr_{attribute_name}'

        if dim_name not in self.dimentions_data:
            self.dimentions_data[dim_name] = self.get_dimension_elements(dim_name)

        for element in self.dimentions_data[dim_name]:
            for attribute in element['attributes']:
                if attribute['attributeId'] == attribute_name and attribute['value'] == str(attribute_value):
                    return element['id']
        return None

    def get_measuregroup_forms(self, mg_name: str) -> list:
        '''Возращает список форм группы показателей.'''

        if not mg_name.startswith('measureGroup_'):
            mg_name = 'measureGroup_' + mg_name

        url = f'{self.__dc_host}/measuregroups/{mg_name}/forms'
        response = requests.get(url, headers=self.__headers, verify=False)
        json_response = response.json()

        if response.status_code != 200:
            raise Exception(f'Error {response.status_code}', str(json_response))

        return json_response.get('forms', [])

    def get_measuregroup_form_state(self, mg_name: str, form_id: str) -> list:
        '''Возращает статусы данных формы группы показателей.'''

        if not mg_name.startswith('measureGroup_'):
            mg_name = 'measureGroup_' + mg_name

        url = f'{self.__dc_host}/measuregroups/{mg_name}/forms/{form_id}/states'
        tries = 5
        response_try = 1
        while response_try < tries:
            try:
                response = requests.get(url, headers=self.__headers, verify=False, timeout=10)
                json_response = response.json()
                if response.status_code != 200:
                    raise Exception(f'Error {response.status_code}', str(json_response))
                return json_response.get('states', [])
            except requests.exceptions.Timeout:
                print(url, 'has timeout, try: ', response_try)
                response_try += 1
        else:
            raise Exception(url, 'has max tries, but not get')

    def set_measuregroup_form_state(self, mg_name: str, form_id: str, state: dict) -> bool:
        '''Возращает статусы данных формы группы показателей.'''

        if not mg_name.startswith('measureGroup_'):
            mg_name = 'measureGroup_' + mg_name

        url = f'{self.__dc_host}/measuregroups/{mg_name}/forms/{form_id}/states'

        response = requests.put(url, headers=self.__headers, data=json.dumps(state), verify=False)
        if response.status_code != 200:
            raise Exception(f'Error {response.status_code}', response.text)
        return True

    def get_business_processes(self):
        url = f'{self.__dc_host}/businessprocesses/?getAll=true'
        response = requests.get(url, headers=self.__headers, verify=False)
        json_response = response.json()

        if response.status_code != 200:
            raise Exception(f'Error {response.status_code}', str(json_response))

        return json_response.get("businessProcesses", [])
