-- import

with source as (
    select
        "Data",
        "Close",
        "Simbolo"
    from 
        {{ source ('dbsales_7llw', 'commodities') }}
),
--aqui seria a parte de tranformação
renamed as (

    select
        cast("Data" as date) as data,
        "Close" as valor_fechamento,
        "Simbolo" as simbolo
    from
        source
)

select * from renamed