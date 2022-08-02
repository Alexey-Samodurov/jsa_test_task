--Удаляем функцию если уже создана
drop function if exists count_work_day(startTime timestamp,endTime timestamp);

--Удаляем таблицу с выходными и праздниками, есть есть
drop table if exists t_rest_day;

--Создаем таблицу с выходными и праздниками, есть нету
create table if not exists t_rest_day(
    id serial primary key,
    start_time timestamp,
    end_time timestamp
);

--Вставляем значения с праздиками и выходными
insert into t_rest_day(start_time, end_time)
values('2022-01-01'::date, '2022-01-03'::date);
--И так далее, общий смысл перебрать все даты с выходными
insert into t_rest_day(start_time, end_time)
values('2022-01-04'::date, '2022-01-05'::date);

 --Создаем функцию
create or replace function count_work_day(startTime timestamp, endTime timestamp)
returns interval as $hour$
declare
	start_date timestamp := date_trunc('day',startTime);
	end_date timestamp := date_trunc('day',endTime);
	rest_cost interval := '0 day';
	count_day int := 0;
    count_day_temp int :=0;
	start_time_temp timestamp := startTime;
	end_time_temp timestamp := endTime;
begin
	--тот же день
	if(start_date = end_date) then
		select count(1) into count_day from t_rest_day t where t.start_time <= startTime and t.end_time > endTime;
		if(count_day > 0) then
			return '0 day';
		else
			return endTime - startTime;
		end if;
	end if;
	 -- И время начала, и время окончания находятся в периоде выходного.
	select count(t.id) into count_day_temp from t_rest_day t where t.start_time <= startTime and t.end_time > endTime;
	if(count_day_temp = 1) then
		return '0 day';
	end if;
	 -- Не в тот же день
	while date_trunc('day',start_time_temp) <= end_time_temp loop
		select count(1) into count_day from t_rest_day t where t.start_time <= start_time_temp and t.end_time > start_time_temp;
		if(count_day > 0) then
			if(date_trunc('day',start_time_temp) = start_date) then
				rest_cost = rest_cost + (start_date + '1 day' - startTime);
			elseif(date_trunc('day',start_time_temp) = end_date) then
				rest_cost = rest_cost + (endTime - end_date);
			else
				rest_cost = rest_cost + '1 day';
			end if;
			--raise notice 'rest_cost :%',rest_cost;
		end if;
		start_time_temp = start_time_temp + '1 day';
		--raise notice 'start_time_temp :%',start_time_temp;
	end loop;
	return age(endTime,startTime) - rest_cost;
end
$hour$ LANGUAGE plpgsql;

 --Тестовая функция
select extract(epoch from count_work_day('2022-01-02 02:00:00.0','2022-01-02 14:01:00.0'))/3600 as date_diff;