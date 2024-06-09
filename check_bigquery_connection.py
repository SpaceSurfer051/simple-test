from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from airflow.utils.dates import days_ago

# 기본 설정 정의
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# DAG 정의
with DAG(
    'check_bigquery_connection',
    default_args=default_args,
    description='DAG to check BigQuery connection',
    schedule_interval='@daily',  # 매일 실행되도록 스케줄링
    start_date=days_ago(1),  # 어제 날짜부터 시작
    tags=['example'],
) as dag:

    # BigQuery 프로젝트 및 데이터셋 테이블
    project_id = 'f1-data-3rd'  # GCP 프로젝트 ID
    dataset_table = 'basic.trainer'  # BigQuery 데이터셋 및 테이블

    # BigQuery에서 간단한 쿼리 실행
    check_bq_query = BigQueryCheckOperator(
        task_id='check_bq_query',
        sql=f'SELECT COUNT(*) FROM `{project_id}.{dataset_table}`',  # 테이블에서 행 수를 세는 쿼리
        use_legacy_sql=False,  # 표준 SQL 사용
        gcp_conn_id='google_cloud_default',  # GCP 연결 ID
        dag=dag,
    )

    # 작업 순서 정의
    check_bq_query

