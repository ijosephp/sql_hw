--Immanuel Ponminissery
--Assignment 5
--ETL

--Q1
--First name, last name, email, id

--DDL to create client_dw

CREATE TABLE client_dw
(
    client_id NUMBER,
    first_name varchar(50) not null,
    last_name varchar(50),
    email varchar(50),
    status varchar(50),
    data_source varchar(50),
    constraint client_dw_pk primary key (client_id,data_source)
);

--DDL to create Views

create view curr_user_view as
    SELECT user_id as client_id, 
               first_name, 
               last_name,
               email,
               cc_flag as status,
               'CURR' as data_source
            from curr_user_table;

create view prospective_view as
    SELECT prospective_id as client_id, 
               pc_first_name, 
               pc_last_name,
               email,
               'N' as status,
               'PROS' as data_source
            from prospective_user;

--Statements to create procedure
/*Using Path 2 for Update whereby entire datawarehouse is truncated and new info is reinserted*/
/*Please note that I usd delete instead of truncate because truncate can only be used to clear the entire table. The requirement
in the question was that we create 2 update statements. That's why I used delete with the where clause.*/

create or replace
PROCEDURE user_etl_proc
as
begin
    insert into client_dw(client_id,first_name,last_name,email,status,data_source)
        select client_id,pc_first_name,pc_last_name,email,status,data_source
        from prospective_view
    where not exists (select client_id, data_source from client_dw where (client_dw.client_id = prospective_view.client_id) and (client_dw.data_source = prospective_view.data_source));
    
    insert into client_dw(client_id,first_name,last_name,email,status,data_source)
        select client_id,first_name,last_name,email,status,data_source
        from curr_user_view
    where not exists (select client_id, data_source from client_dw where (client_dw.client_id = curr_user_view.client_id) and (client_dw.data_source = curr_user_view.data_source));
    
    DELETE FROM client_dw where data_source='PROS';
    insert into client_dw(client_id,first_name,last_name,email,status,data_source)
        select client_id,pc_first_name,pc_last_name,email,status,data_source
        from prospective_view
    where not exists (select client_id, data_source from client_dw where (client_dw.client_id = prospective_view.client_id) and (client_dw.data_source = prospective_view.data_source));
    
    DELETE FROM client_dw where data_source='CURR';
    insert into client_dw(client_id,first_name,last_name,email,status,data_source)
        select client_id,first_name,last_name,email,status,data_source
        from curr_user_view
    where not exists (select client_id, data_source from client_dw where (client_dw.client_id = curr_user_view.client_id) and (client_dw.data_source = curr_user_view.data_source));

end;
