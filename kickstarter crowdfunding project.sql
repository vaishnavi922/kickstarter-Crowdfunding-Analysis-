create database crowdfunding;
use crowdfunding;

create table projects(
				id bigint,
				state varchar(10),
                name text,
                country varchar(20),
                creator_id bigint,
                location_id bigint,
                category_id bigint,
                created_at bigint,
                deadline_at bigint,
                updated_at bigint,
                state_changed_at bigint,
                successful_at bigint,
                launched_at bigint,
                goal bigint,
                pledged bigint,
                currency varchar(10),
                currency_symbol varchar(10),
                usd_pledged bigint,
                static_usd_rate int,
                backers_count int,
                spotlight boolean,
                staff_pick boolean,
                blurb text,
                currency_trailing boolean,
                disable_communication boolean);
desc projects;

load data infile "C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\crowdfunding_project.csv"
into table projects
fields terminated by ','
LINES TERMINATED BY '\n'
ignore 1 rows;

create table location(id bigint primary key,
					  displayable_name varchar(100),
                      type varchar(50),
                      name varchar(100),
                      state varchar(50),
                      short_name varchar(100),
                      is_root boolean,
                      country varchar(100));
load data infile "C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\crowdfunding_location.csv"
into table location
fields terminated by ','
LINES TERMINATED BY '\n'
ignore 1 rows;

create table category(category_id int primary key,
					  category_name varchar(100),
                      parent_id int,
                      position_id int);
load data infile "C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\crowdfunding_Category.csv"
into table category
fields terminated by ','
LINES TERMINATED BY '\n'
ignore 1 rows;
drop table creator;
create table creator(creator_id int primary key,creator_name varchar(200));
load data infile "C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\crowdfunding_Creator.csv"
into table creator
fields terminated by ','
LINES TERMINATED BY '\n'
ignore 1 rows;

select count(*) from projects;
select count(*) from location;
select count(*) from category;
select count(*) from creator;
select concat(round(count(*)/1000),"K") as total_projects from projects;
select count(*) as successful_projects from projects where state="successful";
select concat(round(sum(backers_count)/1000000,2),"M") as Total_backers  from projects where state="successful";
select concat("$",round(sum(usd_pledged)/100000000,2),"B") as Total_amount_raised from projects where state="successful";
select concat(round(100*(sum(case when state="successful" then 1 else 0 end)/count(*)),2),"%") as success_percentage from projects;

alter table projects add column project_duration decimal(10,2);
set sql_safe_updates=0;
update projects set project_duration=datediff(deadline_date,created_date);
select ceiling(avg(project_duration)) from projects where state="successful";

alter table projects add column created_date date,
					add column  deadline_date date,
					add column  updated_date date,
					add column state_changed date,
				    add column successful_date date,
					add column launched_date date;
							
update projects set created_date=date(from_unixtime(created_at)),
					deadline_date=date(from_unixtime(deadline_at)),
                    updated_date=date(from_unixtime(updated_at)),
                    state_changed=date(from_unixtime(state_changed_at)),
                    successful_date=date(from_unixtime(successful_at)),
                    launched_date=date(from_unixtime(launched_at));
                    
select state,count(*) as total_projects from projects
group by state
order by total_projects desc;

select count(p.id) as total_projects,c.category_name as category_name
from projects p join category c on p.category_id=c.category_id
group by c.category_name
order by total_projects desc;

select count(p.id) as total_projects,l.country as country
from projects p join location l on p.location_id=l.id
group by l.country
order by total_projects desc;

select id,sum(backers_count) as Total_backers
from projects where state="successful"
group by id
order by total_backers desc;

select id,sum(usd_pledged) as Total_amount_raised
from projects where state="successful"
group by id
order by Total_amount_raised desc;

select c.category_name,round((sum(case when p.state="successful" then 1 else 0 end)/count(id))*100,2) as success_percentage 
from projects p join category c on p.category_id=c.category_id
group by c.category_name
order by round((sum(case when p.state="successful" then 1 else 0 end)/count(id))*100,2) desc;

select case when (goal*static_usd_rate)<=10000 then "$0-10k"
			when (goal*static_usd_rate)<=100000 then "$10k-100k"
            when (goal*static_usd_rate)<=1000000 then "$100k-1M"
            when (goal*static_usd_rate)<=10000000 then "$1M-10M"
            else "$10M+"
            end as "goal_range",
            round(100*(sum(case when state="successful" then 1 else 0 end)/count(id)),2) as sucess_percentage
            from projects
            group by goal_range
            order by sucess_percentage desc;
            
create table calendar(
	calendar_date DATE primary key,
	year smallint not null,
    month tinyint not null,
    month_name varchar(10) not null,
    quarter tinyint not null,
    day_of_week tinyint not null,
    day_name varchar(10) not null,
    fiscal_month varchar(10) not null,
    fiscal_quater varchar(10) not null);
    
set @@cte_max_recursion_depth=10000;
insert into calendar(calendar_date,year,month,month_name,quarter,day_of_week,day_name,fiscal_month,fiscal_quater)
with recursive dates as(
	select date(min(created_date)) as calendar_date
    from projects
    union all
    select date_add(calendar_date,interval 1 day)
    from dates
    where calendar_date<(Select date(max(created_date)) from projects)
)
select
	d.calendar_date,
    year(d.calendar_date) as year,
    month(d.calendar_date) as month,
    monthname(d.calendar_date) as month_name,
    quarter(d.calendar_date) as quarter,
    weekday(d.calendar_date)+1 as day_of_week,
    dayname(d.calendar_date) as day_name,
    case when month(d.calendar_date)>=4
		 then concat("FM",month(d.calendar_date)-3) 
         else concat("FM",month(d.calendar_date)+9)
	end as financial_month,
    case when month(d.calendar_date) between 4 and 6 then "FQ1"
		 when month(d.calendar_date) between 7 and 9 then "FQ2"
         when month(d.calendar_date) between 10 and 12 then "FQ3"
         else "FQ4"
	end as fiscal_quater
 from dates d;   

select *from calendar;

select c.year,c.quarter,c.month_name,count(p.id) as total_projects
from projects p join calendar c on p.created_date=c.calendar_date
group by c.year,c.quarter,c.month_name 
order by c.year asc;

select c.year,c.quarter,c.month_name,round(100*(sum(case when state="successful" then 1 else 0 end)/count(id)),2) as sucess_percentage
from projects p join calendar c on p.created_date=c.calendar_date
group by c.year,c.quarter,c.month_name 
order by c.year asc;
