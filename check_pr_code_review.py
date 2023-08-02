"""DAG: cg playable api"""
import json
import logging
from datetime import date, datetime
from tempfile import NamedTemporaryFile

import requests
import pandas as pd
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.S3_hook import S3Hook
from helpers.dag_helpers import DagGenerator
from helpers.connection_helpers import fetch_connection

S3_BUCKET = "jetkwy-cg-source-data"
S3_PREFIX = "leadfamly"
REDSHIFT_AIRFLOW_CONNECTION = "redshift-cg"
PLAYABLE_API_CONN = "playable-web-api"
S3_CONNECTION = "s3-airflow"


def get_access_token(api_conn):
    """
    does a POST api call to playable api with different scopes for an authentication token
    """
    conn = fetch_connection(api_conn)
    response = requests.request(
        "POST",
        f"{conn.host}/oauth/token",
        headers={"Content-Type": "application/json"},
        data=json.dumps(
            {
                "grant_type": "client_credentials",
                "client_id": conn.login,
                "client_secret": conn.password,
                "scope": "campaigns.list campaigns.view campaigns.bulk-prizes.list",
            },
        ),
        timeout=10,
    )
    if response.status_code != 200:
        logging.info("could not retrieve the authorization token")
        raise Exception(
            f"response code:{response.status_code} reason:{response.reason}"
        )
    access_token = json.loads(response.text)["access_token"]
    return access_token, conn.host


def get_campaigns(api_conn, **context):
    """Retrieve campaigns from Playable API"""
    token, host = get_access_token(api_conn)

    url = f"{host}/v1/campaigns"
    df_columns = [
        "id",
        "name",
        "timezone",
        "type",
        "created_on",
        "active",
        "active_from",
        "active_to",
    ]
    campaign_lists_df = pd.DataFrame(columns=df_columns)
    payload = {"filter_display": "active"}
    while str(url) != "None":
        campaign_df = pd.json_normalize(playable_api_data_fetch(url, token, payload)["data"])
        url = playable_api_data_fetch(url, token, payload)["links"].get("next", "None")
        campaign_lists_df = pd.concat(
            [campaign_lists_df, campaign_df],
            ignore_index=True,
        )
    # push campaign ids to xcom
    context["ti"].xcom_push(
        key="playable_campaign_ids", value=campaign_lists_df.id.values.tolist()
    )
    # upload file to S3
    remote_path = "{}/list_of_campaigns/list_of_campaigns_{}.csv".format(
        S3_PREFIX, context["ds_nodash"]
    )
    with NamedTemporaryFile("w", suffix=".csv") as temp_file:
        logging.info(f"Upload Campaign List to `s3://{S3_BUCKET}/{remote_path}`")
        campaign_lists_df[df_columns].to_csv(temp_file.name, header=True, index=False)
        client = S3Hook(aws_conn_id=S3_CONNECTION).get_conn()
        client.upload_file(temp_file.name, S3_BUCKET, remote_path)

    return remote_path


def get_campaign_session_stats(api_conn, **context):
    """this function calls playable api to get campaign session statistics data"""
    token, host = get_access_token(api_conn)
    campaign_id_list = context["ti"].xcom_pull(
        task_ids="task_get_campaigns_list", key="playable_campaign_ids"
    )
    column_list = ["date", "desktop", "tablet", "mobile", "campaign_id"]
    campaign_stats_df = pd.DataFrame(columns=column_list)

    for campaign_id in campaign_id_list:
        stats_campaign_url = f"{host}/v1/campaign/{campaign_id}/statistics/sessions"
        stats_of_campaign_df_iterate = pd.DataFrame(
            playable_api_data_fetch(stats_campaign_url, token)
        )
        if not stats_of_campaign_df_iterate.empty:
            stats_of_campaign_df_iterate["campaign_id"] = campaign_id
            campaign_stats_df = pd.concat(
                [campaign_stats_df, stats_of_campaign_df_iterate], ignore_index=True
            )

    # upload file to S3
    remote_path = (
        "{}/session_stats_of_campaigns/session_stats_of_campaigns_{}.csv".format(
            S3_PREFIX, context["ds_nodash"]
        )
    )
    with NamedTemporaryFile("w", suffix=".csv") as temp_file:
        logging.info(f"Upload Campaign Stats to `s3://{S3_BUCKET}/{remote_path}`")
        campaign_stats_df[column_list].to_csv(temp_file.name, header=True, index=False)
        client = S3Hook(aws_conn_id=S3_CONNECTION).get_conn()
        client.upload_file(temp_file.name, S3_BUCKET, remote_path)

    return remote_path


def get_campaign_bulk_prizes(api_conn, **context):
    """this function calls playable api to get campaign bulk prizes data"""
    token, host = get_access_token(api_conn)
    campaign_id_list = context["ti"].xcom_pull(
        task_ids="task_get_campaigns_list", key="playable_campaign_ids"
    )
    column_list = [
        "id",
        "name",
        "description",
        "type",
        "display_criteria",
        "is_instantwin",
        "date_range",
        "time_range",
        "esp_identifier",
        "message",
        "total_prizes",
        "winners",
        "created_on",
        "campaign_id",
    ]
    campaign_bulk_prizes_df = pd.DataFrame(columns=column_list)

    for campaign_id in campaign_id_list:
        bulk_prizes_campaign_url = f"{host}/v1/campaign/{campaign_id}/bulk-prizes"
        bulk_prizes_campaign_df_iterate = pd.DataFrame(
            playable_api_data_fetch(bulk_prizes_campaign_url, token)["data"]
        )
        if not bulk_prizes_campaign_df_iterate.empty:
            bulk_prizes_campaign_df_iterate["campaign_id"] = campaign_id
            campaign_bulk_prizes_df = pd.concat(
                [campaign_bulk_prizes_df, bulk_prizes_campaign_df_iterate],
                ignore_index=True,
            )

    remote_path = "{}/bulk_prizes_of_campaigns/bulk_prizes_of_campaigns_{}.csv".format(
        S3_PREFIX, context["ds_nodash"]
    )
    with NamedTemporaryFile("w", suffix=".csv") as temp_file:
        logging.info(f"Upload Campaign Prizes to `s3://{S3_BUCKET}/{remote_path}`")
        campaign_bulk_prizes_df[column_list].to_csv(
            temp_file.name, header=True, index=False
        )
        client = S3Hook(aws_conn_id=S3_CONNECTION).get_conn()
        client.upload_file(temp_file.name, S3_BUCKET, remote_path)

    return remote_path


def get_campaign_full_stats(api_conn, **context):
    """this function calls playable api to get full campaign statistics data"""
    token, host = get_access_token(api_conn)
    campaign_id_list = context["ti"].xcom_pull(
        task_ids="task_get_campaigns_list", key="playable_campaign_ids"
    )
    column_list = [
        "sessions",
        "registrations",
        "unique_registrations",
        "conversion",
        "time_spent_average",
        "total_time_spent",
    ]
    campaign_stats_df = pd.DataFrame(columns=column_list)
    for campaign_id in campaign_id_list:
        stats_campaign_url = f"{host}/v1/campaign/{campaign_id}/statistics"
        dict_response = playable_api_data_fetch(stats_campaign_url, token)
        if dict_response:
            fields_to_remove = [
                "realtime",
                "internal_links",
                "sent_to_esp",
                "tip_a_friend",
                "devices",
                "facebook",
                "funnel_statistics",
            ]
            for field in fields_to_remove:
                dict_response.pop(field, None)
            dict_response["campaign_id"] = campaign_id
            dict_response["time_spent_average"] = dict_response["engagement"][
                "time_spent_average"
            ]
            dict_response["total_time_spent"] = dict_response["engagement"][
                "total_time_spent"
            ]
            dict_response.pop("engagement", None)
            stats_of_campaign_df_iterate = pd.DataFrame(dict_response, index=[0])
            campaign_stats_df = pd.concat(
                [campaign_stats_df, stats_of_campaign_df_iterate], ignore_index=False
            )
    #check if campaign id is present in df
    if "campaign_id" in campaign_stats_df.columns:
     campaign_stats_df["campaign_id"] = campaign_stats_df["campaign_id"].astype('int')
    # upload file to S3
    remote_path = "{}/full_stats_of_campaigns/full_stats_of_campaigns_{}.csv".format(
        S3_PREFIX, context["ds_nodash"]
    )
    with NamedTemporaryFile("w", suffix=".csv") as temp_file:
        logging.info(f"Upload Campaign full Stats to `s3://{S3_BUCKET}/{remote_path}`")
        campaign_stats_df.to_csv(temp_file.name, header=True, index=False)
        client = S3Hook(aws_conn_id=S3_CONNECTION).get_conn()
        client.upload_file(temp_file.name, S3_BUCKET, remote_path)
    return remote_path


def playable_api_data_fetch(url, token, payload={}):
    """
    sends a get call to fetch data from playable api with bearer authentication token
    """
    #
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {token}",
    }
    response = requests.request("GET", url, headers=headers, params=payload, timeout=10)
    if response.status_code != 200:
        logging.info("could not retrieve the json response")
        raise Exception(
            f"response code:{response.status_code} reason:{response.reason}"
        )
    response_text = json.loads(response.text)
    return response_text


dg: DagGenerator = DagGenerator(
    dag_id="cg_retention_playable_data",
    schedule_interval="0 7 * * *",
    start_date=datetime(2022, 11, 14),
    max_active_runs=1,
    catchup=False,
    retries=2,
)
dag: DAG = dg.generate()

task_get_campaigns_list = PythonOperator(
    task_id="task_get_campaigns_list",
    python_callable=get_campaigns,
    op_kwargs={"api_conn": PLAYABLE_API_CONN},
    provide_context=True,
    dag=dag,
)

task_get_campaign_session_stats_data = PythonOperator(
    task_id="task_get_campaign_session_stats_data",
    python_callable=get_campaign_session_stats,
    op_kwargs={"api_conn": PLAYABLE_API_CONN},
    provide_context=True,
    dag=dag,
)

task_get_campaign_full_stats_data = PythonOperator(
    task_id="task_get_campaign_full_stats_data",
    python_callable=get_campaign_full_stats,
    op_kwargs={"api_conn": PLAYABLE_API_CONN},
    provide_context=True,
    dag=dag,
)


task_get_campaign_bulk_prizes_data = PythonOperator(
    task_id="task_get_campaign_bulk_prizes_data",
    python_callable=get_campaign_bulk_prizes,
    op_kwargs={"api_conn": PLAYABLE_API_CONN},
    provide_context=True,
    dag=dag,
)

task_insert_playable_details_data = PostgresOperator(
    task_id="task_insert_playable_details_data",
    sql="sql/insert_playable_details_data.sql",
    params={"schema": "datamart_retention", "s3_bucket": S3_BUCKET},
    dag=dag,
    postgres_conn_id=REDSHIFT_AIRFLOW_CONNECTION,
)

(
    task_get_campaigns_list
    >> task_get_campaign_session_stats_data
    >> task_get_campaign_bulk_prizes_data
    >> task_get_campaign_full_stats_data
    >> task_insert_playable_details_data
)
