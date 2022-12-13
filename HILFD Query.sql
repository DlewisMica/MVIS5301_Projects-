Use [HILFD]
go

--filtering hospital table to Maryland and surrounding areas
IF OBJECT_ID('tempdb..#hosp', 'U') IS NOT NULL drop table #hosp;
select *
into #hosp
from [dbo].[HOSPITALS]
where [state] in ('OH','KY','TN','NC','VA','WV','MD','DC','DE','NJ','PA')
and [TYPE] not in ('LONG TERM CARE','MILITARY','PSYCHIATRIC','REHABILITATION','WOMEN','SPECIAL')
and [STATUS]='OPEN'
;
GO

--Calulate Median bed counts for missing values (i.e., BEDS = -999)
--There are 38 beds in dataset missing
IF OBJECT_ID('tempdb..#median_beds', 'U') IS NOT NULL drop table #median_beds;
select distinct
 [STATE]
,[TYPE]
,ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY BEDS) OVER (PARTITION BY [STATE],[TYPE]),0) AS MedianCont
into #median_beds
from #hosp
where BEDS>0
order by
 [STATE]
,[TYPE];
GO

alter table #hosp
add imputation int, BEDS2 int;
GO

update #hosp
set imputation =
case when BEDS<0 then 1 else 0 end;
GO

update A
set A.BEDS2= case when A.BEDS<0 then B.MedianCont else A.BEDS end
from
#hosp A left join #median_beds B
on A.[STATE]=B.[STATE] and A.[TYPE]=B.[TYPE];
GO


--creating primary key just in case data is too big and takes a while to use in future queries
alter table #hosp
alter column [ID] varchar(20) not null;
GO

alter table #hosp
add primary key(ID);



--filtering population table to Maryland and surrounding areas
IF OBJECT_ID('tempdb..#pop', 'U') IS NOT NULL drop table #pop;
select *
into #pop
from [dbo].[Population_Table]
where [state_code] in ('OH','KY','TN','NC','VA','WV','MD','DC','DE','NJ','PA')

--creating primary key just in case data is too big and takes a while to use in future queries
alter table #pop
alter column GEOID varchar(20) not null;
GO

alter table #pop
add primary key(GEOID);


--filter origin-destination matrix to capture neighborhood-hosptial combinations within 60 minutes that are within Maryland and the surrounding areas
IF OBJECT_ID('tempdb..#matrix', 'U') IS NOT NULL drop table #matrix;
select
 A.*
--using logic that hospitals further away are less accessible than closer hospitals
,CASE
 WHEN Total_TravelTime <= 20 then 1.00 --i.e., full accessible
 WHEN (Total_TravelTime>20 AND Total_TravelTime<=30) then 0.60 --i.e., partially accessible  
 WHEN (Total_TravelTime>30 AND Total_TravelTime<=45) then 0.30 --i.e., lowly accessible
 else 0 end as [Decay]
into #matrix
from [dbo].[HILFD_OD_MATRIX] A
inner join
#pop B
on A.GEOID=B.GEOID
inner join
#hosp C
on A.hosp_ID=C.ID
where Total_TravelTime<=60;
GO

alter table #matrix
alter column GEOID varchar(20) not null;
GO

alter table #matrix
alter column hosp_ID varchar(20) not null;
GO

alter table #matrix
ADD PRIMARY KEY(GEOID_Hospital_key);
GO

CREATE INDEX idx_GEOID_Hosp
ON #matrix(GEOID, hosp_ID);
GO


--Estimate the closest hospital to each community (i.e., US Census Block Group or GEOID)
IF OBJECT_ID('tempdb..#closest', 'U') IS NOT NULL drop table #closest;
select distinct
 A.GEOID
,isnull(B.hospital_beds_20Min,0) as hospital_beds_20Min
,isnull(B.[hospitals_20Min],0) as [hospitals_20Min]
,isnull(B.hospital_beds_30Min,0) as hospital_beds_30Min
,isnull(B.[hospitals_30Min],0) as [hospitals_30Min]
,isnull(B.hospital_beds_45Min,0) as hospital_beds_45Min
,isnull(B.[hospitals_45Min],0) as [hospitals_45Min]
,isnull(B.[minutes_closest_hospital],70) as [minutes_closest_hospital]
into #closest
from
#pop A
left join
(
select
 A.GEOID
,sum(case when Total_TravelTime<=20 then B.BEDS2 end) as [hospital_beds_20Min]
,sum(case when Total_TravelTime<=30 then B.BEDS2 end) as [hospital_beds_30Min]
,sum(case when Total_TravelTime<=45 then B.BEDS2 end) as [hospital_beds_45Min]
,count(distinct case when Total_TravelTime<=20 then hosp_ID end) as [hospitals_20Min]
,count(distinct case when Total_TravelTime<=30 then hosp_ID end) as [hospitals_30Min]
,count(distinct case when Total_TravelTime<=45 then hosp_ID end) as [hospitals_45Min]
,min(A.Total_TravelTime) as [minutes_closest_hospital]
from #matrix A
left join
#hosp B
on A.hosp_ID=B.ID
group by
A.GEOID
)B
on A.GEOID=B.GEOID;
GO

--Generate Acceesibility Estimates
--Base table
IF OBJECT_ID('tempdb..#base', 'U') IS NOT NULL drop table #base;
select distinct
 A.*
,B.Total_Pop
,C.BEDS2
INTO #base
from #matrix A
left join
#pop B
on A.GEOID=B.GEOID
left join
#hosp C
on A.hosp_ID=C.[ID]
where Decay>0;
GO

--STEP 1: Access
IF OBJECT_ID('tempdb..#init_estimates', 'U') IS NOT NULL drop table #init_estimates;
select distinct
 hosp_ID
, ((BEDS2*1.00)/inital2)*2500 as inital --Estimates will be interpreted as beds per 2,500 people
into #init_estimates
from(
select
 hosp_ID
,BEDS2
,sum(Total_Pop*Decay) as inital2
from #base
group by
 hosp_ID
,BEDS2)A

--Rejoin initial estimates on Base Table
IF OBJECT_ID('tempdb..#base2', 'U') IS NOT NULL drop table #base2;
select
 A.*
,B.inital
into #base2
from #base A
left join
#init_estimates B
on A.hosp_ID=B.hosp_ID;
GO

--STEP 2: ACCESS
IF OBJECT_ID('tempdb..#STEP2', 'U') IS NOT NULL drop table #STEP2;
select distinct
 A.GEOID
,isnull(B.Beds_per_2_5K,0) as Beds_per_2_5K
into #STEP2
from
#pop A
left join
(
select
 GEOID
,sum(inital*DECAY) as Beds_per_2_5K
from #base2
group by
GEOID
)B
on A.GEOID=B.GEOID
order by GEOID



IF OBJECT_ID('dbo.MARYLAND_ACCESS', 'U') IS NOT NULL drop table dbo.MARYLAND_ACCESS;
select distinct
 A.*
,B.hospital_beds_20Min
,B.hospitals_20Min
,B.hospital_beds_30Min
,B.hospitals_30Min
,B.hospital_beds_45Min
,B.hospitals_45Min
,B.[minutes_closest_hospital]
,C.Beds_per_2_5K
into dbo.MARYLAND_ACCESS
from #pop A
left join
#closest B
on A.GEOID=B.GEOID
left join
#STEP2 C
on A.GEOID=C.GEOID
where
A.State_Code='MD'
order by GEOID;
GO

IF OBJECT_ID('dbo.MARYLAND_AREA_HOSPITALS', 'U') IS NOT NULL drop table dbo.MARYLAND_AREA_HOSPITALS;
select distinct
*
into dbo.MARYLAND_AREA_HOSPITALS
from #hosp
where [STATE] in ('MD','DC','PA','DE','VA','WV');
GO


select * from dbo.MARYLAND_ACCESS order by GEOID