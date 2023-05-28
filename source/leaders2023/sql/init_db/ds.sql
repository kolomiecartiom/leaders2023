create schema if not exists ds;


create table if not exists ds.cfg_category_prescription (
	category_id int4,
	category_description text,
	category_name text
);


create table if not exists ds.cfg_category_protocol (
	category_id int4,
	category_description text,
	category_name text
);

create table if not exists ds.d_mkb (
	mkb_code text,
	mkb_name text
);

create table if not exists ds.d_mkb_standart (
	standart_id int4,
	standart_name text,
	mkb_code text
);

create table if not exists ds.d_package (
	package_id serial4,
	dt_decree date,
	diagnosis_category text,
	diagnosis_subcategory text,
	mkb_code text[]
);

create table if not exists ds.d_package_prescription (
	package_id int4,
	prescription_subcategory text,
	prescription_name text,
	prescription_mandatory text,
	criteria text
);

create table if not exists ds.d_prescription (
	prescription_name_doc text,
	prescription_name_pack text
);

create table if not exists ds.d_standart (
	standart_id serial4,
	standart_name text,
	dt_decree date,
	dt_reg_decree date,
	decree_number int4,
	patient_age_category text,
	patient_gender text,
	mkb_phase text,
	mkb_stage text,
	mkb_complication text,
	med_care_type text,
	med_care_condition text,
	med_care_form text,
	therapy_duration_avg int4
);

create table if not exists ds.d_standart_prescription (
	standart_id int4,
	standart_name text,
	prescription_category text,
	prescription_subcategory_shortname text,
	prescription_subcategory_fullname text,
	prescription_code text,
	prescription_name text,
	frequency_avg numeric,
	multiplicity_avg numeric
);

create table if not exists ds.prescriptions (
	protocol_id int4,
	doctor_prescription text
);

create table if not exists ds.protocols (
	protocol_id serial4,
	load_id int4,
	patient_gender text,
	dt_patient_birth date,
	patient_id int4,
	mkb_code text,
	dt_prescription date,
	doctor_position text,
	doctor_prescription text,
	med_institution_name text,
	doctor_id int4,
	doctor_name text
);