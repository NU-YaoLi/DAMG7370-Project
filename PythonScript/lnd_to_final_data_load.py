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

        # 1. Create tables: dim_geography and the three fact tables
        create_tables_queries = [
            """
            create table if not exists dim_geography (
                geo_key serial primary key,
                metro text unique not null,
                state text,
                county_name text,
                inserted_at timestamp default current_timestamp,
                updated_at timestamp default current_timestamp
            );
            """,
            """
            create table if not exists fact_house_value (
                house_value_id serial primary key,
                geo_key int4 references dim_geography(geo_key), 
                avg_house_value float8, 
                metric_date date, 
                "year" int4, 
                inserted_at timestamp default current_timestamp, 
                updated_at timestamp default current_timestamp,
                unique(geo_key, metric_date)
            );
            """,
            """
            create table if not exists fact_monthly_payment (
                monthly_payment_id serial primary key,
                geo_key int4 references dim_geography(geo_key),
                monthly_payment float8,
                metric_date date,
                "year" int4,
                inserted_at timestamp default current_timestamp,
                updated_at timestamp default current_timestamp,
                unique(geo_key, metric_date)
            );
            """,
            """
            create table if not exists fact_rental_income (
                rental_income_id serial primary key,
                geo_key int4 references dim_geography(geo_key),
                rental_income float8,
                metric_date date,
                "year" int4,
                inserted_at timestamp default current_timestamp,
                updated_at timestamp default current_timestamp,
                unique(geo_key, metric_date)
            );
            """
        ]

        for query in create_tables_queries:
            cur.execute(query)

        # 2. Populate dim_geography first
        upsert_dim_geography = """
        insert into dim_geography (metro, state, county_name)
        select distinct on (metro) metro, state, county_name
        from (
            select 
                case 
                    when metro = 'Seattle, WA' then 'Seattle-Tacoma-Bellevue, WA' 
                    else metro 
                end as metro, 
                state, 
                county_name 
            from landing.lnd_house_value 
            where metro is not null
            
            union
            
            select 
                case 
                    when region_name = 'Seattle, WA' then 'Seattle-Tacoma-Bellevue, WA' 
                    else region_name 
                end as metro, 
                state_name as state, 
                null as county_name 
            from landing.lnd_monthly_payment 
            where region_name is not null
            
            union
            
            select 
                case 
                    when metro = 'Seattle, WA' then 'Seattle-Tacoma-Bellevue, WA' 
                    else metro 
                end as metro, 
                state_name as state, 
                county_name 
            from landing.lnd_rental_income 
            where metro is not null
        ) as combined_geos
        order by metro, county_name nulls last
        on conflict (metro) do update set 
            state = excluded.state,
            county_name = coalesce(excluded.county_name, dim_geography.county_name),
            updated_at = current_timestamp;
        """
        
        cur.execute(upsert_dim_geography)

        # 3. Upsert logic for fact tables linking to dim_geography via geo_key
        upsert_house_value = """
        insert into fact_house_value (
            geo_key, avg_house_value, metric_date, year
        )
        select 
            g.geo_key,
            avg(l.house_value) as avg_house_value,
            l.metric_date, 
            l.year
        from landing.lnd_house_value l
        join dim_geography g on l.metro = g.metro
        where l.metro is not null
        group by g.geo_key, l.metric_date, l.year
        on conflict (geo_key, metric_date) 
        do update set 
            avg_house_value = excluded.avg_house_value,
            updated_at = current_timestamp;
        """

        upsert_monthly = """
        with clean_lnd_monthly as (
            select 
                case 
                    when region_name = 'Seattle, WA' then 'Seattle-Tacoma-Bellevue, WA' 
                    else region_name 
                end as region_name,
                monthly_payment,
                metric_date,
                year
            from landing.lnd_monthly_payment
            where region_name is not null
        )
        insert into fact_monthly_payment (
            geo_key, monthly_payment, metric_date, year
        )
        select 
            g.geo_key,
            avg(l.monthly_payment) as monthly_payment,
            l.metric_date, 
            l.year
        from clean_lnd_monthly l
        join dim_geography g on l.region_name = g.metro
        group by g.geo_key, l.metric_date, l.year
        on conflict (geo_key, metric_date) 
        do update set 
            monthly_payment = excluded.monthly_payment,
            updated_at = current_timestamp;
        """

        upsert_rental = """
        insert into fact_rental_income (
            geo_key, rental_income, metric_date, year
        )
        with clean_lnd_rental as (
            select 
                case when metro = 'Seattle, WA' then 'Seattle-Tacoma-Bellevue, WA' else metro end as metro,
                rental_income,
                metric_date,
                year
            from landing.lnd_rental_income
            where metro is not null
        )
        select 
            g.geo_key,
            avg(l.rental_income) as rental_income,
            l.metric_date, 
            l.year
        from clean_lnd_rental l
        join dim_geography g on l.metro = g.metro
        group by g.geo_key, l.metric_date, l.year
        on conflict (geo_key, metric_date) 
        do update set 
            rental_income = excluded.rental_income,
            updated_at = current_timestamp;
        """

        # Execute fact upserts
        cur.execute(upsert_house_value)
        cur.execute(upsert_monthly)
        cur.execute(upsert_rental)

        conn.commit()
        print("Dimensional database setup and upsert operations completed successfully.")

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"An error occurred during the ETL process: {e}")
        raise e
    finally:
        if conn:
            cur.close()
            conn.close()

run_etl()