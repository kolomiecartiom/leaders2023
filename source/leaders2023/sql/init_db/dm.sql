create schema if not exists dm;

create table if not exists dm.prescriptions (
	dt_prescription date,
	med_institution text,
	department text,
	protocol_id int4,
	doctor_name text,
	patient_id int4,
	patient_gender text,
	mkb_code text,
	mkb_name text,
	prescription_subcategory text,
	prescription text,
	prescription_category text,
	is_mandatory text
);

create table if not exists dm.protocols (
	dt_prescription date,
	med_institution text,
	department text,
	protocol_id int4,
	doctor_name text,
	patient_id int4,
	patient_gender text,
	dt_patient_birth date,
	mkb_code text,
	mkb_name text,
	prescriptions_doc_list text,
	prescriptions_pack_list text,
	prescriptions_notdoc_cnt int4,
	prescriptions_notpack_cnt int4,
	protocol_category text,
	prescriptions_cnt int4
);