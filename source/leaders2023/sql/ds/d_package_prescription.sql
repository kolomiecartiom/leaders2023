delete from ds.d_package_prescription ds_d
using stg1_xls.d_package_prescription stg1_d
where ds_d.prescription_subcategory = stg1_d.prescription_subcategory
  and ds_d.prescription_name = stg1_d.prescription_name
  and ds_d.prescription_mandatory = stg1_d.prescription_mandatory
  and ds_d.criteria = stg1_d.criteria;

insert into ds.d_package_prescription (
    package_id,
    prescription_subcategory,
    prescription_name,
    prescription_mandatory,
    criteria
)
select
	d.package_id,
	dpp.prescription_subcategory,
	dpp.prescription_name,
	dpp.prescription_mandatory,
	dpp.criteria
from stg1_xls.d_package_prescription dpp
left join ds.d_package d
	on d.diagnosis_subcategory = dpp.subsection