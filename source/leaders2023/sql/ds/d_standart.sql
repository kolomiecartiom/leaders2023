delete from ds.d_standart ds_d
using stg1_pdf.d_standart stg1_d
where ds_d.standart_name = stg1_d.standart_name;

insert into ds.d_standart (
    standart_name,
    dt_decree,
    dt_reg_decree,
    decree_number,
    patient_age_category,
    patient_gender,
    mkb_phase,
    mkb_stage,
    mkb_complication,
    med_care_type,
    med_care_condition,
    med_care_form,
    therapy_duration_avg
)
with month_num (name, num) as (
	values
		('января', 1),
		('февраля', 2),
		('марта', 3),
		('апреля', 4),
		('мая', 5),
		('июня', 6),
		('июля', 7),
		('августа', 8),
		('сентября', 9),
		('октября', 10),
		('ноября', 11),
		('декабря', 12)
)
select
    standart_name,
    to_date(
        concat(
            (regexp_matches(dt_decree, '(\d{1,2}).+(\d{4})'))[1],
            '_',
            mn1.num::text,
            '_',
            (regexp_matches(dt_decree, '(\d{1,2}).+(\d{4})'))[2]
        ), 'DD_MM_YYYY'
    ) as dt_decree,
    to_date(
        concat(
            (regexp_matches(dt_reg_decree, '(\d{1,2}).+(\d{4})'))[1],
            '_',
            mn2.num::text,
            '_',
            (regexp_matches(dt_reg_decree, '(\d{1,2}).+(\d{4})'))[2]
        ), 'DD_MM_YYYY'
    ) as dt_reg_decree,
    decree_number::int,
    patient_age_category,
    patient_gender,
    mkb_phase,
    mkb_stage,
    mkb_complication,
    med_care_type,
    med_care_condition,
    med_care_form,
    therapy_duration_avg::int
from stg1_pdf.d_standart
left join month_num mn1
	on dt_decree like '%' || mn1.name || '%'
left join month_num mn2
	on dt_reg_decree like '%' || mn2.name || '%'