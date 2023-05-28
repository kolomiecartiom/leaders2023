delete from ds.protocols ds_p
using stg1_xls.doc_prescriptions stg1_d
where ds_p.patient_id = stg1_d.patient_id::int
  and ds_p.mkb_code = stg1_d.mkb_code
  and ds_p.dt_prescription = stg1_d.dt_prescription
  and ds_p.doctor_position = to_date(substring(stg1_d.dt_service, '\d{1,2}\.\d{1,2}\.\d{4}'), 'DD_MM_YYYY')
  and ds_p.doctor_name = stg1_d.doctor_name;

insert into ds.protocols (
    patient_gender,
    dt_patient_birth,
    patient_id,
    mkb_code,
    dt_prescription,
    doctor_position,
    doctor_prescription,
    med_institution_name,
    doctor_id,
    doctor_name
)
select
	patient_gender,
	to_date(substring(dt_patient_birth, '\d{1,2}\.\d{1,2}\.\d{4}'), 'DD_MM_YYYY') as dt_patient_birth,
	patient_id::int,
	mkb_code,
	to_date(substring(dt_service, '\d{1,2}\.\d{1,2}\.\d{4}'), 'DD_MM_YYYY') as dt_prescription,
	doctor_position,
	doctor_prescription,
	med_institution_name,
	doctor_id::int,
	doctor_name
from stg1_xls.doc_prescriptions