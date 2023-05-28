truncate table dm.prescriptions;

insert into dm.prescriptions(
    dt_prescription,
    med_institution,
    department,
    protocol_id,
    doctor_name,
    patient_id,
    patient_gender,
    mkb_code,
    mkb_name,
    prescription_subcategory,
    prescription,
    prescription_category,
    is_mandatory
)
with concat_mkb_code as (
	select
		coalesce(p.mkb_code, standart.mkb_code, package.mkb_code) as mkb_code,
		ds_p.doctor_prescription as prescription_name_protocol,
		standart.prescription_name as prescription_name_standart,
		package.prescription_name as prescription_name_package,
		coalesce(standart.prescription_subcategory_shortname, package.prescription_subcategory) as prescription_subcategory,
		package.prescription_mandatory
	from ds.protocols p
	left join ds.prescriptions ds_p
		on ds_p.protocol_id = p.protocol_id
	left join ds.d_prescription ds_dp
		on ds_dp.prescription_name_doc = ds_p.doctor_prescription
	full join (
		select
			prescription_name,
			d_mkb_s.mkb_code,
			prescription_subcategory_shortname
		from ds.d_standart_prescription dsp
		left join ds.d_standart d_s
			on d_s.standart_id = dsp.standart_id
		left join ds.d_mkb_standart d_mkb_s
			on d_mkb_s.standart_id = d_s.standart_id
	) standart
		on standart.mkb_code = p.mkb_code
   	   and standart.prescription_name = ds_dp.prescription_name_pack
   	full join (
		select
			prescription_name,
			prescription_subcategory,
			prescription_mandatory,
			unnest(dp.mkb_code) as mkb_code
		from ds.d_package_prescription  dpp
		left join ds.d_package dp
			on dp.package_id = dpp.package_id
	) package
		on p.mkb_code = package.mkb_code
   	   and package.prescription_name = ds_dp.prescription_name_pack
)
select
	dt_prescription,
	med_institution_name as med_institution,
	case
		when doctor_position = 'врач-кардиолог' then 'Кардиология'
		when doctor_position = 'врач-оториноларинголог' then 'Отолорингология'
		when doctor_position = 'врач-невролог' then 'Неврология'
		else 'Иное'
	end as department,
	p.protocol_id,
	doctor_name,
	patient_id,
	p.patient_gender,
	p.mkb_code,
	d_mkb.mkb_name,
	cmc.prescription_subcategory,
	coalesce(cmc.prescription_name_protocol, cmc.prescription_name_standart, cmc.prescription_name_package) as prescription,
	case
		when cmc.prescription_name_protocol is not null
				and (cmc.prescription_name_standart is not null
				or cmc.prescription_name_package is not null)
			then 'Есть в рекомендациях, назначено'
		when cmc.prescription_name_protocol is not null
				and (cmc.prescription_name_standart is null
				and cmc.prescription_name_package is null)
			then 'Нет в рекомендациях, назначено'
		when cmc.prescription_name_protocol is null
				and (cmc.prescription_name_standart is not null
				or cmc.prescription_name_package is not null)
			then 'Есть в рекомендациях, не назначено'
	end prescription_category,
	cmc.prescription_mandatory as is_mandatory
from ds.protocols p
left join ds.d_mkb
	on d_mkb.mkb_code = p.mkb_code
left join concat_mkb_code cmc
	on cmc.mkb_code = p.mkb_code