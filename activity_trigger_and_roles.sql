-- Function and triggers to log the insert update and delete activity into activity table

CREATE OR REPLACE FUNCTION log_activity() RETURNS TRIGGER AS $$
BEGIN
	IF TG_TABLE_NAME != 'activity' THEN
		BEGIN
      		INSERT INTO activity (username, query, execution_time)
      		VALUES (current_user, current_query(), statement_timestamp());
      		RETURN NEW;
		END;
	END IF;
END;
$$ LANGUAGE plpgsql;	
	
CREATE TRIGGER activity_trigger_loan
AFTER INSERT OR UPDATE OR DELETE ON loan
FOR EACH STATEMENT EXECUTE FUNCTION log_activity();

CREATE TRIGGER activity_trigger_transactions
AFTER INSERT OR UPDATE OR DELETE ON transactions
FOR EACH STATEMENT EXECUTE FUNCTION log_activity();

CREATE TRIGGER activity_trigger_permanent_order
AFTER INSERT OR UPDATE OR DELETE ON permanent_order
FOR EACH STATEMENT EXECUTE FUNCTION log_activity();

CREATE TRIGGER activity_trigger_date
AFTER INSERT OR UPDATE OR DELETE ON date
FOR EACH STATEMENT EXECUTE FUNCTION log_activity();

CREATE TRIGGER activity_trigger_account
AFTER INSERT OR UPDATE OR DELETE ON account
FOR EACH STATEMENT EXECUTE FUNCTION log_activity();

CREATE TRIGGER activity_trigger_card
AFTER INSERT OR UPDATE OR DELETE ON card
FOR EACH STATEMENT EXECUTE FUNCTION log_activity();

CREATE TRIGGER activity_trigger_client
AFTER INSERT OR UPDATE OR DELETE ON client
FOR EACH STATEMENT EXECUTE FUNCTION log_activity();

CREATE TRIGGER activity_trigger_Disposition
AFTER INSERT OR UPDATE OR DELETE ON Disposition
FOR EACH STATEMENT EXECUTE FUNCTION log_activity();

CREATE TRIGGER activity_trigger_Demographics
AFTER INSERT OR UPDATE OR DELETE ON Demographics
FOR EACH STATEMENT EXECUTE FUNCTION log_activity();

-- Create roles
CREATE ROLE admin;
CREATE ROLE board_member;
CREATE ROLE manager;
CREATE ROLE employee;
CREATE ROLE IT;

-- Grant and revoke privileges
GRANT all ON ALL TABLES IN SCHEMA public TO admin WITH GRANT OPTION;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE transactions, account, loan, permanent_order, card, client, disposition, date, demographics TO board_member WITH GRANT OPTION;
GRANT SELECT ON TABLE user_table, activity TO board_member WITH GRANT OPTION;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE transactions, account, loan, permanent_order, card, client, disposition TO manager WITH GRANT OPTION;
GRANT SELECT ON TABLE date, user_table, activity, demographics TO manager WITH GRANT OPTION;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO employee;
REVOKE SELECT ON TABLE user_table, activity FROM employee;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE user_table, activity TO IT;
GRANT INSERT ON TABLE activity TO admin, board_member, manager, employee, IT;
GRANT USAGE, SELECT ON SEQUENCE activity_id_seq TO admin, board_member, manager, employee, IT;

-- Grant login privilege to all roles
ALTER ROLE admin LOGIN;
ALTER ROLE board_member LOGIN;
ALTER ROLE manager LOGIN;
ALTER ROLE employee LOGIN;
ALTER ROLE IT LOGIN;
	
	
-- DROP FUNCTION log_activity() CASCADE;

-- Function and trigger to enter date into date table based on client birth date

CREATE OR REPLACE FUNCTION insert_new_client() 
RETURNS TRIGGER AS $$
BEGIN
  -- Check if the birth date exists in the date table
  IF NOT EXISTS (SELECT 1 FROM date WHERE date = NEW.birth_number) THEN
    -- If it does not exist, insert the new birth date into the date table
    INSERT INTO date (date, year, month, day) 
    VALUES (NEW.birth_number, EXTRACT(year FROM NEW.birth_number), 
            EXTRACT(month FROM NEW.birth_number), EXTRACT(day FROM NEW.birth_number));
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER new_client_trigger
BEFORE INSERT ON client
FOR EACH ROW
EXECUTE FUNCTION insert_new_client();

-- Function to calculate age
DROP FUNCTION IF EXISTS calculate_age(date);
CREATE FUNCTION calculate_age(birth_date date)
RETURNS int AS $$

  SELECT date_part('year', age('1999-01-01'::date, birth_date::date));

$$ LANGUAGE SQL;

--- stored procedure to get clients who have negative balance as on given date

create or replace procedure get_clients_with_current_negative_balance (current_day date)
language plpgsql
as $$ 
declare prev_month DATE := current_day::date - INTERVAL '1 month';
begin
	drop table if exists temp_table;
	create table temp_table(client_id int, last_transaction_date date, end_balance numeric, start_date date, end_date date); 
	insert into temp_table 
	select dp.client_id, iq.date as last_transaction_date, iq.balance as ending_bal, current_day, prev_month from disposition dp, ( 
		select t.account_id, t.date, t.balance from transactions t,(
			select account_id, max(date) as mdt
			from transactions
			where date < '1993-10-10' and date > '1993-09-10'
			group by 1) sq where t.account_id = sq.account_id and t.date = sq.mdt and balance < 0 limit 1) iq
			where iq.account_id = dp.account_id;
end;
$$;

