delete from ds.d_mkb_standart ds_d
using stg1_pdf.d_mkb_standart stg1_d
where ds_d.standart_name = stg1_d.standart_name;

insert into ds.d_mkb_standart(
    standart_id,
    standart_name,
    mkb_code
)
select
	d.standart_id,
	d_mkb.standart_name,
	d_mkb.mkb_code
from stg1_pdf.d_mkb_standart d_mkb
left join ds.d_standart d
	on d.standart_name = d_mkb.standart_name