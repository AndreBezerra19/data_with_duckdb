
{{ config(materialized='table') }}

select 
    p.ID,
    p.NAME,
    t.type_1 as MAIN_TYPE,
    t.type_2 as SEC_TYPE 
from {{source('main', 'b_pokemons')}} p
left join {{source('main', 'b_types')}} t
    on p.ID = t.id