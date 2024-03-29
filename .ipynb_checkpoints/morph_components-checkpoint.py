from xai_components.base import InArg, OutArg, InCompArg, Component, BaseComponent, secret, xai_component
import os
import requests
import shutil


@xai_component
class MorphAPIConnect(Component):
    """
    Stores the Morph API configuration for use in other components.
    """
    team_slug: InArg[str]
    database_id: InArg[str]
    table_slug: InArg[str]
    api_key: InArg[str]

    def execute(self, ctx) -> None:
        ctx['morph_api_config'] = {
            'team_slug': self.team_slug.value,
            'database_id': self.database_id.value,
            'table_slug': self.table_slug.value,
            'api_key': self.api_key.value
        }
        

@xai_component
class MorphFindRecords(Component):
    """
    Finds records in a table in the Morph database based on filter criteria and sort directions.
    """
    api_config: InArg[dict]
    select_fields: InCompArg[list]
    limit: InArg[int]
    filter_criteria: InCompArg[dict]
    sort_criteria: InCompArg[list]
    records: OutArg[list]

    def execute(self, ctx) -> None:
        api_config = self.api_config.value
        url = f"https://{api_config['team_slug']}.api.morphdb.io/v0/record/{api_config['database_id']}/{api_config['table_slug']}/query"
        headers = {
            'Content-Type': 'application/json',
            'client-type': 'widget',
            'x-api-key': api_config['api_key']
        }
        data = {
            'select': self.select_fields.value,
            'limit': self.limit.value,
            'filter': self.filter_criteria.value,
            'sort': self.sort_criteria.value
        }
        resp = requests.post(url, headers=headers, json=data)
        if resp.status_code == 200:
            self.records.value = resp.json()
        else:
            self.records.value = {
                'error': True,
                'message': f'Failed to find records: {resp.text}',
                'status_code': resp.status_code
            }


@xai_component
class MorphAddRecord(Component):
    """
    Adds a record to a table in the Morph database.
    """
    api_config: InArg[dict]
    record_data: InCompArg[dict]
    response: OutArg[dict]

    def execute(self, ctx) -> None:
        api_config = self.api_config.value
        headers = {
            "Content-Type": "application/json",
            "client-type": "widget",
            "x-api-key": api_config['api_key']
        }
        url = f"https://{api_config['team_slug']}.api.morphdb.io/v0/record/{api_config['database_id']}/{api_config['table_slug']}/create"
        data = {
            "fixedValue": [],
            "values": self.record_data.value
        }
        
        resp = requests.post(url, headers=headers, json=data)
        if resp.status_code == 200:
            self.response.value = resp.json()
        else:
            self.response.value = {
                'error': True,
                'message': f'Failed to add record: {resp.text}',
                'status_code': resp.status_code
            }


@xai_component
class MorphUpdateRecord(Component):
    """
    Updates records in a table in the Morph database based on filter criteria.
    """
    api_config: InArg[dict]
    update_values: InCompArg[list]
    filter_criteria: InCompArg[dict]
    response: OutArg[dict]

    def execute(self, ctx) -> None:
        api_config = self.api_config.value
        url = f"https://{api_config['team_slug']}.api.morphdb.io/v0/record/{api_config['database_id']}/{api_config['table_slug']}/update"
        headers = {
            'Content-Type': 'application/json',
            'client-type': 'widget',
            'x-api-key': api_config['api_key']
        }
        data = {
            'fixedValue': [],
            'values': self.update_values.value,
            'filter': self.filter_criteria.value
        }
        resp = requests.post(url, headers=headers, json=data)
        if resp.status_code == 200:
            self.response.value = resp.json()
        else:
            self.response.value = {
                'error': True,
                'message': f'Failed to update record: {resp.text}',
                'status_code': resp.status_code
            }


@xai_component
class MorphDeleteRecord(Component):
    """
    Deletes records from a table in the Morph database based on filter criteria.
    """
    api_config: InArg[dict]
    filter_criteria: InCompArg[dict]
    response: OutArg[dict]

    def execute(self, ctx) -> None:
        api_config = self.api_config.value
        url = f"https://{api_config['team_slug']}.api.morphdb.io/v0/record/{api_config['database_id']}/{api_config['table_slug']}/delete"
        headers = {
            'Content-Type': 'application/json',
            'client-type': 'widget',
            'x-api-key': api_config['api_key']
        }
        data = {
            'filter': self.filter_criteria.value
        }
        resp = requests.post(url, headers=headers, json=data)
        if resp.status_code == 200:
            self.response.value = resp.json()
        else:
            self.response.value = {
                'error': True,
                'message': f'Failed to delete record: {resp.text}',
                'status_code': resp.status_code
            }
            

            

@xai_component
class MorphGetTableStructure(Component):
    """
    Retrieves the structure (list of fields) of a table in the Morph database.
    """
    api_config: InArg[dict]
    table_structure: OutArg[list]

    def execute(self, ctx) -> None:
        api_config = self.api_config.value
        headers = {
            "Content-Type": "application/json",
            "client-type": "widget",
            "x-api-key": api_config['api_key']
        }
        url = f"https://{api_config['team_slug']}.api.morphdb.io/v0/structure/{api_config['database_id']}/{api_config['table_slug']}"
        
        resp = requests.get(url, headers=headers)
        if resp.status_code == 200:
            self.table_structure.value = resp.json()
        else:
            self.table_structure.value = {
                'error': True,
                'message': f'Failed to get table structure: {resp.text}',
                'status_code': resp.status_code
            }
            

@xai_component
class MorphUploadFile(Component):
    """
    Uploads a file to a bucket in Morph Storage.
    """
    api_config: InArg[dict]
    bucket_name: InArg[str]
    file_path: InArg[str]
    file_key: OutArg[str]

    def execute(self, ctx) -> None:
        api_config = self.api_config.value
        url = f"https://{api_config['team_slug']}.api.morphdb.io/v0/storage/object/{self.bucket_name.value}"
        headers = {
            'x-api-key': api_config['api_key']
        }
        files = {
            'key': (None, self.file_path.value.split('/')[-1]),
            'file': (self.file_path.value.split('/')[-1], open(self.file_path.value, 'rb'))
        }
        response = requests.post(url, headers=headers, files=files)
        if response.status_code == 200:
            self.file_key.value = response.json().get('key')
        else:
            self.file_key.value = {
                'error': True,
                'message': f'Failed to upload file: {response.text}',
                'status_code': response.status_code
            }
            
            
@xai_component
class MorphGetFile(Component):
    """
    Retrieves the object of the uploaded file in Morph Storage.
    """
    api_config: InArg[dict]
    bucket_name: InArg[str]
    file_key: InArg[str]
    file_object: OutArg[dict]

    def execute(self, ctx) -> None:
        api_config = self.api_config.value
        url = f"https://{api_config['team_slug']}.api.morphdb.io/v0/storage/public/{self.bucket_name.value}/{self.file_key.value}"
        headers = {
            'x-api-key': api_config['api_key']
        }
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            self.file_object.value = response.headers
        else:
            self.file_object.value = {
                'error': True,
                'message': f'Failed to get file object: {response.text}',
                'status_code': response.status_code
            }


@xai_component
class MorphGetSignedURL(Component):
    """
    Retrieves a signed URL for the file uploaded in Morph Storage with expiration date.
    """
    api_config: InArg[dict]
    bucket_name: InArg[str]
    file_key: InArg[str]
    signed_url: OutArg[str]

    def execute(self, ctx) -> None:
        api_config = self.api_config.value
        url = f"https://{api_config['team_slug']}.api.morphdb.io/v0/storage/url/sign/{self.bucket_name.value}/{self.file_key.value}"
        headers = {
            'x-api-key': api_config['api_key']
        }
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            self.signed_url.value = response.json().get('url')
        else:
            self.signed_url.value = {
                'error': True,
                'message': f'Failed to get signed URL: {response.text}',
                'status_code': response.status_code
            }
            

