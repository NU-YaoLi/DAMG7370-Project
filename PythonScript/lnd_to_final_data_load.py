import sys
import psycopg2
from awsglue.utils import getResolvedOptions

# fetching parameters from the glue job configuration
args = getResolvedOptions(sys.argv, ['DB_HOST', 'DB_NAME', 'DB_USER', 'DB_PWD'])

def run_etl():
    conn_params = {
        "host": args['DB_HOST'],
        "database": args['DB_NAME'],
        "user": args['DB_USER'],
        "password": args['DB_PWD']
    }
    
    conn = None
    try:
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()

        # unique constraints are added to enable the upsert logic
        create_tables_queries = [
            """
            create table if not exists house_value (
                region_id int4, 
                size_rank int4, 
                region_name text,  
                region_type text, 
                state text, 
                metro text, 
                county_name text, 
                avg_house_value float8, 
                metric_date date, 
                "year" int4, 
                inserted_at timestamp default current_timestamp, 
                updated_at timestamp default current_timestamp,
                unique(region_id, metric_date)
            );
            """,
            """
            create table if not exists monthly_payment (
                region_id int4,
                size_rank int4,
                region_name text,
                region_type text,
                state text,
                monthly_payment float8,
                metric_date date,
                "year" int4,
                inserted_at timestamp default current_timestamp,
                unique(region_id, metric_date)
            );
            """,
            """
            create table if not exists rental_income (
                region_id int4,
                size_rank int4,
                region_name text,
                region_type text,
                state text,
                city text,
                metro text,
                county_name text,
                rental_income float8,
                metric_date date,
                "year" int4,
                inserted_at timestamp default current_timestamp,
                unique(region_id, metric_date)
            );
            """
        ]

        for query in create_tables_queries:
            cur.execute(query)

        # 2. upsert logic for house_value (using your specific aggregation)
        upsert_house_value = """
        insert into house_value (
            region_id, size_rank, region_name, region_type, state, metro, 
            county_name, avg_house_value, metric_date, year
        )
        select 
            cast(region_id as integer), 
            cast(size_rank as integer), 
            region_name, 
            region_type, 
            state, 
            metro, 
            county_name,
            avg(house_value) as avg_house_value,
            metric_date, 
            year
        from lnd_house_value 
        group by 
            region_id, size_rank, region_name, region_type, state, metro, 
            county_name, metric_date, year
        on conflict (region_id, metric_date) 
        do update set 
            avg_house_value = excluded.avg_house_value,
            updated_at = current_timestamp;
        """

        # upsert logic for monthly_payment
        upsert_monthly = """
        insert into monthly_payment (
            region_id, size_rank, region_name, region_type, state, 
            monthly_payment, metric_date, year
        )
        select 
            region_id, size_rank, region_name, region_type, state_name, 
            monthly_payment, metric_date, year
        from lnd_monthly_payment
        on conflict (region_id, metric_date) 
        do update set 
            monthly_payment = excluded.monthly_payment;
        """

        # upsert logic for rental_income
        upsert_rental = """
        insert into rental_income (
            region_id, size_rank, region_name, region_type, state, 
            city, metro, county_name, rental_income, metric_date, year
        )
        select 
            region_id, size_rank, cast(region_name as text), region_type, state_name, 
            city, metro, county_name, rental_income, metric_date, year
        from lnd_rental_income
        on conflict (region_id, metric_date) 
        do update set 
            rental_income = excluded.rental_income;
        """

        # execute upserts
        cur.execute(upsert_house_value)
        cur.execute(upsert_monthly)
        cur.execute(upsert_rental)

        conn.commit()
        print("database setup and upsert operations completed successfully.")

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"an error occurred during the etl process: {e}")
        raise e
    finally:
        if conn:
            cur.close()
            conn.close()


run_etl()