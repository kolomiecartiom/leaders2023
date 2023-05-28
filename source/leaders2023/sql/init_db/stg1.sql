create schema if not exists stg1_pdf;

create table if not exists stg1_pdf.d_mkb_standart (
    filename text,
    id int4,
    standart_name text,
    mkb_code text,
    mkb_name text 
);

create table if not exists stg1_pdf.d_standart (
    filename text,
    id int4,
    standart_name text,
    dt_decree text,
    dt_reg_decree text,
    decree_number text,
    patient_age_category text,
    patient_gender text,
    mkb_phase text,
    mkb_stage text,
    mkb_complication text,
    med_care_type text,
    med_care_condition text,
    med_care_form text,
    therapy_duration_avg text 
);

create table if not exists stg1_pdf.d_standart_prescription (
    id int4,
    filename text,
    standart_name text,
    prescription_category text,
    prescription_subcategory text,
    prescription_code text,
    prescription_name text,
    frequency_avg text,
    multiplicity_avg text 
 );

create schema if not exists stg1_sf;

create table if not exists stg1_sf.matches_prescription (
    prescription_name_doc text,
    prescription_name_pack text 
);

create schema if not exists stg1_xls;

create table if not exists stg1_xls.d_package_prescription (
    "section" text,
    subsection text,
    mkb_code text,
    prescription_subcategory text,
    prescription_name text,
    prescription_mandatory text,
    criteria text,
    id int4,
    filename text 
);

create table if not exists stg1_xls.doc_prescriptions (
    filename text,
    protocol_id int4,
    patient_gender text,
    dt_patient_birth text,
    patient_id text,
    mkb_code text,
    mkb_name text,
    dt_service text,
    doctor_position text,
    doctor_prescription text,
    med_institution_name text,
    doctor_id text,
    doctor_name text 
);