
{{ config(materialized='table') }}

select 
    b_pokemons.ID,
    b_pokemons.NAME,
    b_types.type_1 as MAIN_TYPE,
    b_types.type_2 as SEC_TYPE 
from {{source('pokemon_db', 'b_pokemons')}} 
join b_types 
    on b_pokemons.ID = b_types.id