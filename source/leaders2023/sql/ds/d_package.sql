truncate table ds.d_package;

insert into ds.d_package (
    dt_decree,
    diagnosis_category,
    diagnosis_subcategory,
    mkb_code
)
select distinct
    '2021-01-01'::date as dt_decree,
    upper(substring(section from 1 for 1)) || lower(substring(section from 2 for length(section))) as diagnosis_category,
    subsection as diagnosis_subcategory,
    string_to_array(replace(mkb_code, ' ', ''), ',') as mkb_code
from stg1_xls.d_package_prescription
