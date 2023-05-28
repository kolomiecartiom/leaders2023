select 
    protokol."Naimenovanie", 
    standart."Naimenovanie" 
from "data"."measureGroup_Sootvetstviya" mg
left join "data"."dim_Naznacheniya_iz_protokola" protokol
    on mg."id_dim_Naznacheniya_iz_protokola" = protokol."Id"
left join "data"."dim_Naznacheniya_iz_standarta" standart
    on mg."attr_Naznachenie_iz_sta" = standart."Id"
where mg."IsDeleted"  is false