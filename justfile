google_login:
    gcloud auth application-default login

only_build_views:
    dbt run --select config.materialized:view
