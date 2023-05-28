truncate table ds.prescriptions;

insert into ds.prescriptions (
    protocol_id,
    doctor_prescription
)
with prescription as (
	select
		protocol_id,
		unnest(string_to_array(doctor_prescription, ';')) as doctor_prescription
	from (
		select
			protocol_id,
			unnest(string_to_array(doctor_prescription, E'\n')) as doctor_prescription
		from ds.protocols
	) t
)
select
	protocol_id,
	trim(doctor_prescription, ' ')
from prescription
where doctor_prescription <> ''