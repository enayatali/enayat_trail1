{{ config(
    materialized="incremental",
    incremental_strategy="merge",
    on_schema_change='append_new_columns',
    unique_key = ['category','business_name','date'],
    tags=["run_every_2_min"]
) }}

select 
array_to_string(array_distinct(team_array), ',') as category,
date,
business_name,
count(distinct id) as leads_touched,
count(distinct case when flag_converted = 1 then id else null end) as leads_converted,
sum(call_duration) as call_duration,
sum(case when flag_converted = 1 then call_duration else 0 end) as converted_call_duration

from {{ref('itd_bpo_conv')}} as base
where 1=1
{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
and base.date >= (select max(date :: date) - interval '1 week' from {{ this }})
 
{% endif %}

group by 1,2,3
