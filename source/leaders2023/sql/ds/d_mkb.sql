delete from ds.d_mkb ds_d
using stg1_xls.doc_prescriptions stg1_d
where ds_d.mkb_code = stg1_d.mkb_code
  and ds_d.mkb_name = stg1_d.mkb_name;

insert into ds.d_mkb (
    mkb_code,
    mkb_name
)
select distinct
    mkb_code,
    mkb_name
from stg1_xls.doc_prescriptions