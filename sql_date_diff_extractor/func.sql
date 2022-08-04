--Удаляем таблицу с выходными и праздниками, если есть
drop table if exists work_minutes;

--Создаем таблицу в которой генерируем поминутный диапазон дат за 2022
create table work_minutes ( work_minute timestamp primary key );

--Генерируем поминутный диапазон дат за 2022
insert into work_minutes
select work_minute
from (
select
    generate_series(timestamp '2022-01-01 00:00:00', timestamp '2022-12-31 11:59:00', '1 minute') as work_minute
) t
where extract(isodow from work_minute) < 6 -- захардкодим субботу и воскресенье
  and cast(work_minute as time) between time '09:30' and time '18:30' --захардкодим рабочие часы
;

--Удаляем табличку с праздничными днями
drop table if exists t_rest_day;

--Создаем табличку с праздничными днями
create table t_rest_day(dt date);

--Вставляем значения с праздиками
insert into t_rest_day(dt)
select generate_series(date '2022-01-01', date '2022-01-09', '1 day');
--
insert into t_rest_day(dt)
select generate_series(date '2022-02-23', date '2022-02-23', '1 day');
--
insert into t_rest_day(dt)
select generate_series(date '2022-03-07', date '2022-03-08', '1 day');
--
insert into t_rest_day(dt)
select generate_series(date '2022-05-02', date '2022-05-03', '1 day');
--
insert into t_rest_day(dt)
select generate_series(date '2022-06-13', date '2022-06-13', '1 day');
--
insert into t_rest_day(dt)
select generate_series(date '2022-11-04', date '2022-11-04', '1 day');


--Создаем функцию
create or replace function work_hour_extrator(start_date timestamp, end_date timestamp)
returns decimal as $$
    select
        count(*) / 60.0
    from work_minutes
    where work_minute between start_date and end_date
    and start_date::date not in (
        select dt from t_rest_day
        )
    and end_date:: date not in (
        select dt from t_rest_day
        )
    ;
$$ language sql;

 --Тестовая функция
select work_hour_extrator('2022-01-10 09:30:00.0', '2022-01-10 18:01:00.0');