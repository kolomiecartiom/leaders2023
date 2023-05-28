delete from ds.d_standart_prescription ds_d
using stg1_pdf.d_standart_prescription stg1_d
where ds_d.standart_name = stg1_d.standart_name;

insert into ds.d_standart_prescription (
    standart_id,
    standart_name,
    prescription_category,
    prescription_subcategory_shortname,
    prescription_subcategory_fullname,
    prescription_code,
    prescription_name,
    frequency_avg,
    multiplicity_avg
)

select
	d.standart_id,
	dsp.standart_name,
	prescription_category,
    case
        when prescription_subcategory in (
            'Лабораторные в методы исследования',
            'Лабораторные методы исследования',
            'Лабораторнье методы исследования'
        ) then 'ЛИ'
        when lower(prescription_subcategory) like 'прием%'
            then 'Консультация'
        when prescription_subcategory in (
            'Инструментальные методы исследования',
            'Инструментаментальные методы исследования'
        ) or lower(prescription_subcategory) like 'хирургические%'
            then 'ИИ'
        else 'Иные'
    end as prescription_subcategory_shortname,
	prescription_subcategory as prescription_subcategory_fullname,
	prescription_code,
	prescription_name,
	case
		when frequency_avg = '' then null::numeric
		else replace(frequency_avg, ',', '.')::numeric
	end as frequency_avg,
	case
		when multiplicity_avg = '' then null::numeric
		else replace(multiplicity_avg, ',', '.')::numeric
	end as multiplicity_avg
from stg1_pdf.d_standart_prescription dsp
left join ds.d_standart d
	on d.standart_name = dsp.standart_name