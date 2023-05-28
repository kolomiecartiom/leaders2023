truncate table ds.d_prescription;

insert into ds.d_prescription (
	prescription_name_doc,
	prescription_name_pack
)
select distinct
    doctor_prescription,
    coalesce(mp.prescription_name_doc, dpp.prescription_name, dsp.prescription_name) as prescription_name_pack
from ds.prescriptions p
left join stg1_sf.matches_prescription mp
    on mp.prescription_name_doc = p.doctor_prescription
left join ds.d_package_prescription dpp
    on dpp.prescription_name = p.doctor_prescription
left join ds.d_standart_prescription dsp
    on dsp.prescription_name = p.doctor_prescription
where coalesce(mp.prescription_name_doc, dpp.prescription_name, dsp.prescription_name) is not null