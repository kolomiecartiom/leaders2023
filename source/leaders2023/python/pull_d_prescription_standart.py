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
            t.prescription_subcategory,
            t.prescription_name
        from (
            select distinct 
                prescription_subcategory_shortname as prescription_subcategory, 
                prescription_name 
            from ds.d_standart_prescription
            
            union all 
    
            select distinct 
                prescription_subcategory, 
                prescription_name 
            from ds.d_package_prescription    
        ) t 
        '''
    )

    loaded_elements = sf_api.get_dimension_elements('Naznacheniya_iz_standarta')
    loaded_elements = [(elem['name'], elem['attributes'][0]['value']) for elem in loaded_elements]

    dim_elements = []

    for line in db_data:
        if (line['prescription_name'], line['prescription_subcategory']) not in loaded_elements:
            element_new = {
                "Name": line['prescription_name'],
                "Path": [],
                "Attributes": [
                    {"AttributeId": "attr_Tip_naznacheniya", "Value": line['prescription_subcategory']},
                ]
            }
            dim_elements.append(element_new)

    print(dim_elements)
    if dim_elements:
        sf_api.create_dimension_elements(
            'Naznacheniya_iz_standarta',
            dim_elements=dim_elements
        )