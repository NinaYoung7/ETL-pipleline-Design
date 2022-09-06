CREATE VIEW NEW_VIEW 
AS 

with temp as (
select 'New_York' AS type,"Test_Date"::date as tese_dates,"County",(replace("Test_Positive",'%','')::float)/100  as ratio
from "New_York" ny 
union ALL
select 'LA' AS type, ep_date::date as tese_dates,geo_merge as "County",cases_14day::FLOAT / (CASE WHEN population ='NA' THEN 0.00 ELSE population::FLOAT END ) as ratio
from "LA"
where   population <>'NA'  AND ep_date <>'NA'
),
t1 as (
select type,"County" ,max(tese_dates) as dates,(max(tese_dates) - INTERVAL '14 days')::Date  as dates_day14
from temp 
WHERE type='New_York'
group by "County",type
union all 
select type,"County" ,max(tese_dates) as dates,(max(tese_dates) - INTERVAL '14 days')::Date  as dates_day14
from temp 
WHERE type='LA'
group by "County",type
),
t2 as (
select temp.type,temp."County",
avg(ratio)::FLOAT*100 as Positive14day,
dense_rank ()over (order by avg(ratio) desc ) as rn 
from temp
join t1 on temp."County"=t1."County" and  temp.tese_dates between dates_day14 and dates
where  temp.type='New_York'
group by temp."County",temp.type
union all
select temp.type,temp."County",
avg(ratio)::FLOAT*100 as Positive14day,
dense_rank ()over (order by avg(ratio) desc ) as rn 
from temp
join t1 on temp."County"=t1."County" and  temp.tese_dates between dates_day14 and dates
where  temp.type='LA'
group by temp."County",temp.type
)
select 
type,"County",positive14day,condition,
dense_rank()over(partition by condition,type  order by positive14day) as rn 
from(
select type,"County",Positive14day,
case when  rn <((select max(rn) from t2 where type='New_York')*0.25) then 'High'
     when  rn between ((select max(rn) from t2 where type='New_York')*0.25)  and ((select max(rn) from t2 where type='New_York')*0.5) then 'Medium High'
     when  rn between ((select max(rn) from t2 where type='New_York')*0.5)  and ((select max(rn) from t2 where type='New_York')*0.75) then 'Medium Low'
     when  rn >((select max(rn) from t2 where type='New_York')*0.75) then 'Low' end  condition
from t2
where type='New_York' and Positive14day>0
) tt 
union all
select 
type,"County",positive14day,condition,
dense_rank()over(partition by condition,type  order by positive14day) as rn 
from(
select type,"County",Positive14day,
case when  rn <((select max(rn) from t2 where type='LA')*0.25) then 'High'
     when  rn between ((select max(rn) from t2 where type='LA')*0.25)  and ((select max(rn) from t2 where type='LA')*0.5) then 'Medium High'
     when  rn between ((select max(rn) from t2 where type='LA')*0.5)  and ((select max(rn) from t2 where type='LA')*0.75) then 'Medium Low'
     when  rn >((select max(rn) from t2 where type='LA')*0.75 ) then 'Low' end  condition
from t2
where type='LA' and Positive14day>0
) tt ;
