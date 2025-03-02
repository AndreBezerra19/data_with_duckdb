{{ config(materialized='table', schema='main') }}

SELECT 
    p.id,  
    p.name,
    t.type_1 as main_type,
    t.type_2 as sec_type 
FROM {{ source('main', 'b_pokemons') }} p
LEFT JOIN {{ source('main', 'b_types') }} t
    ON p.id = t.id