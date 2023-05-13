import datetime
import os

import oci
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime

import dataflow_constants as const

# Read dataflow related configurations
dataflow_config = Variable.get(const.DATAFLOW_CONFIG, deserialize_json=const.TRUE)
app_name = Variable.get(const.DATAFLOW_APP_NAMES).split(const.LIST_SEP)[4]
dag_app_owner = dataflow_config[const.DAG_APP_OWNER]

region_list = Variable.get(const.DATAFLOW_REGIONS)
region = region_list.split(const.LIST_SEP)[0]

dag_config = Variable.get(const.DAG_CONFIG, deserialize_json=const.TRUE)
date_format = dag_config[const.DATE]
no_of_hours = dag_config[const.HOURS]

if date_format is None or date_format is '':
    date_format = const.DATE_FORMAT

if no_of_hours is None or no_of_hours is '':
    no_of_hours = const.NO_OF_HOURS

compartment_id = dataflow_config[const.COMPARTMENT_ID]
display_name = const.AIRFLOW_TESTING_FOR_DF + datetime.now().strftime(date_format)

region_code = Variable.get(const.REGION_CODE, deserialize_json=const.TRUE)

scheduler_time = Variable.get(const.SCHEDULED_TESTING_TIME, deserialize_json=const.TRUE)
dag_start_date = Variable.get(const.DAG_START_DATE, deserialize_json=const.TRUE)
dag_date_list = dag_start_date[const.DF_TEST.lower()].split(const.LIST_SEP)

# Set proxy settings
os.environ['http_proxy'] = ''
os.environ['HTTP_PROXY'] = ''
os.environ['https_proxy'] = ''
os.environ['HTTPS_PROXY'] = ''

start_year = int(dag_date_list[0])
start_month = int(dag_date_list[1])
start_day = int(dag_date_list[2])

# default_args = {
#     const.START_DATE: datetime.datetime(start_year, start_month, start_day),
#     const.END_DATE: datetime.datetime(start_year, start_month, start_day ) + datetime.timedelta(1),
#     const.EMAIL_ON_FAILURE: const.FALSE,
#     const.EMAIL_ON_RETRY: const.FALSE,
#     const.SLA: datetime.timedelta(hours=no_of_hours)
# }
start_date = datetime.now() - timedelta(1)
end_date = start_date + timedelta(minutes=5)
default_args = {
    'owner': 'amritdas',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 10, 7, 42, 0), # Scheduled start time
    # 'start_date' : start_date,
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
    # 'end_date': datetime(2023, 5, 10, 7, 48, 0) # Scheduled end time
    # 'end_date' : end_date
}


dag = DAG(
    "airflow_test_for_dataflow",
    default_args=default_args,
    # schedule_interval=scheduler_time[region_code[region] + const.FIELD_SEP + const.VB_ACCESS.lower()],
    # schedule_interval=scheduler_time['iad_test'],
    schedule = "0,15,30,45 * * * *",
    description='AirFlow Dag Testing',
    # schedule=timedelta(minutes=3),
    catchup=const.FALSE
)


def get_airflow_test_config():
    '''
     to create airflow test configuration
    :return:
    '''
    config = { const.TENANCY: dataflow_config[const.TENANCY],
            #   const.TENANCY : 'ocid1.tenancy.oc1..aaaaaaaagkbzgg6lpzrf47xzy4rjoxg4de6ncfiq2rncmjiujvy2hjgxvziq',
              const.COMPARTMENT_ID: compartment_id,
              const.REGION: region,
              const.NAMESPACE: dataflow_config[const.NAMESPACE]
              }
    print(f'config : {config}')
    return config


def airflow_test_for_dataflow():
    '''
    to start airflow test for dataflow app
    :return:
    '''
    # config = get_airflow_test_config()
    ip_based_sp_signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
    data_flow_client = oci.data_flow.DataFlowClient(config={}, signer=ip_based_sp_signer)
    list_app_response = data_flow_client.list_applications(compartment_id=compartment_id, display_name=app_name)
    for app in list_app_response.data:
       # if app.owner_user_name == dag_app_owner:
        print("Creating the airFlow_Testing_for_DF Data Flow Run for the region " + region)
        create_run_details = oci.data_flow.models.CreateRunDetails(
            compartment_id=compartment_id,
            application_id=app.id,
            display_name=display_name,
            max_duration_in_minutes = 5)
        run = data_flow_client.create_run(create_run_details=create_run_details)
        if run.status != 200:
            print("Failed to create airFlow_Testing_for_DF Data Flow Run for the region " + region)
            print(run.data)
        else:
            print("airFlow_Testing_for_DF Data Flow Run ID is " + run.data.id + " for the region " + region)


df_run = PythonOperator(
    task_id='run_airflow_testing_for_df_start', 
    python_callable=airflow_test_for_dataflow, 
    dag=dag)

df_run
