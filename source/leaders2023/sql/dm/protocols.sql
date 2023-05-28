truncate table dm.protocols;

insert into dm.protocols (
    dt_prescription,
    med_institution,
    department,
    protocol_id,
    doctor_name,
    patient_id,
    patient_gender,
    dt_patient_birth,
    mkb_code,
    mkb_name,
    prescriptions_doc_list,
    prescriptions_pack_list,
    prescriptions_notdoc_cnt,
    prescriptions_notpack_cnt,
    protocol_category,
    prescriptions_cnt
)
with dm_pres as (
	select
		protocol_id,
		prescription_category,
		count(prescription_category) as amount_pres
	from dm.prescriptions
	group by
		protocol_id,
		prescription_category
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
    p.dt_patient_birth,
	p.mkb_code,
	d_mkb.mkb_name,
	doc_pres.prescriptions_doc_list,
	pack_pres.prescriptions_pack_list,
	dm_pres1.amount_pres as prescriptions_notdoc_cnt,
	dm_pres2.amount_pres as prescriptions_notpack_cnt,
	case
		when pack_pres.prescriptions_pack_list is null then 'Нет стандарта'
		when dm_pres1.amount_pres = 0 and dm_pres2.amount_pres = 0 then 'Соответствует'
		when dm_pres1.amount_pres <> 0 then 'Частично'
		when dm_pres2.amount_pres <> 0 then 'Доп. назначения'
	end as protocol_category,
	amount_pres.amount_prescription as prescriptions_cnt
from ds.protocols p
left join ds.d_mkb
	on d_mkb.mkb_code = p.mkb_code
left join (
	select
		protocol_id,
		string_agg(doctor_prescription, '\n') as prescriptions_doc_list
	from ds.prescriptions
	group by protocol_id
) doc_pres
	on doc_pres.protocol_id = p.protocol_id
left join (
	select distinct
		t.mkb_code,
		string_agg(t.prescription_name, '\n') as prescriptions_pack_list
	from (
		select
			prescription_name,
			d_mkb_s.mkb_code
		from ds.d_standart_prescription dsp
		left join ds.d_standart d_s
			on d_s.standart_id = dsp.standart_id
		left join ds.d_mkb_standart d_mkb_s
			on d_mkb_s.standart_id = d_s.standart_id
		union all
		select
			prescription_name,
			unnest(dp.mkb_code) as mkb_code
		from ds.d_package_prescription  dpp
		left join ds.d_package dp
			on dp.package_id = dpp.package_id
	) t
	group by
		t.mkb_code
) pack_pres
	on pack_pres.mkb_code = p.mkb_code
left join dm_pres dm_pres1
	on dm_pres1.protocol_id = p.protocol_id
   and dm_pres1.prescription_category = 'Есть в рекомендациях, не назначено'
left join dm_pres dm_pres2
	on dm_pres2.protocol_id = p.protocol_id
   and dm_pres2.prescription_category = 'Нет в рекомендациях, назначено'
left join dm_pres dm_pres3
	on dm_pres3.protocol_id = p.protocol_id
   and dm_pres3.prescription_category = 'Есть в рекомендациях, назначено'
left join (
	select
		protocol_id,
		count(doctor_prescription) as amount_prescription
	from ds.prescriptions
	group by protocol_id
) amount_pres
	on amount_pres.protocol_id = p.protocol_id