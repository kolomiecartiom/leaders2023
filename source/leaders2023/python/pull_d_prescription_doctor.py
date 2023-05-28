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
        select distinct 
            doctor_prescription 
        from ds.prescriptions
        where doctor_prescription <> ''
        '''
    )

    loaded_elements = sf_api.get_dimension_elements('Naznacheniya_iz_protokola')
    loaded_elements = [elem['name'] for elem in loaded_elements]

    dim_elements = []

    for line in db_data:
        if line['doctor_prescription'] not in loaded_elements:
            element_new = {
                "Name": line['doctor_prescription'],
                "Path": [],
                "Attributes": [
                ]
            }
            dim_elements.append(element_new)

    if dim_elements:
        sf_api.create_dimension_elements(
            'Naznacheniya_iz_protokola',
            dim_elements=dim_elements
        )