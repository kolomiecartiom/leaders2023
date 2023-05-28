from df.common.helpers.general import get_decoded_connection
from os import getenv

from source.leaders2023.python.api.sf_api import SmartFormsAPI
from source.leaders2023.python.api.db_postgre import DBPostgresql

if __name__ == '__main__':
    visiology_conn = get_decoded_connection(getenv('AF_VISIOLOGY_CONNECTION'))
    db_dwh_conn = get_decoded_connection(getenv('AF_DWH_DB_CONNECTION'))

    sf_api = SmartFormsAPI(visiology_conn)
    db_conn = DBPostgresql(db_dwh_conn)

    db_data = db_conn.select_as_dict(
        '''
        select 
        	prescription_name_doc,
	        prescription_name_pack 
        from ds.d_prescription
        
        except
        
        select 
        	prescription_name_doc,
	        prescription_name_pack 
        from stg1_sf.matches_prescription
        '''
    )

    dim_protocol = sf_api.get_dimension_elements('Naznacheniya_iz_protokola')
    dim_standart = sf_api.get_dimension_elements('Naznacheniya_iz_standarta')

    mg_elem = [
        {
            "value": None,
            "comment": '',
            "systemInfo": None,
            "measure": None,
            "calendars": [],
            "attributes": [
                {
                    "id": "attr_Naznachenie_iz_sta",
                    "value": [elem['id'] for elem in dim_standart if elem['name'] == line['prescription_name_pack']][0]
                }
            ],
            "dimensions": [
                {
                    'id': 'dim_Naznacheniya_iz_protokola',
                    'elementId': [elem['id'] for elem in dim_protocol if elem['name'] == line['prescription_name_doc']][0]
                }
            ],
        } for line in db_data
    ]

    if mg_elem:
        sf_api.create_measuregroup_elements(mg_name='Sootvetstviya', mg_elements=mg_elem)