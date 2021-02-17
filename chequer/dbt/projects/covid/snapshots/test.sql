{% snapshot covid_snapshot %}
    {{
        config(
	  target_schema="snapshots",
          unique_key="date",
	  strategy='timestamp',
          updated_at='date'
        )
    }}	
	select * from airflow_database.test.covid 
{% endsnapshot %}
