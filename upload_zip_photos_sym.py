"""
### SSH into MyDaemen, zip up jpg files, SFTP zipped files to Symplicity's 
### server (get and put), send email notifications and remove zipped file.
"""

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.sftp.operators.sftp import SFTPOperator
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime
import pendulum

local_tz = pendulum.timezone("US/Eastern")

ssh_conn_myd = "mydaemen_ssh_prod"

mydaemen_remote_path = '/home/nginx/html/people/colleague_user_photos/'
local_path = '/dags/my_daemen/exports/'
symplicity_remote_path = '/home/daemendrop/'
zip_file_name = 'DaemenStudentIDPhotos.zip'

to = [
	'John Zaepfel <jzaepfel@daemen.edu>',
	'Miguel Rodriguez <mrodrig1@daemen.edu>',
	'Kerry Spicer <kspicer@daemen.edu>',
]

default_args = {
    'owner': 'jzaepfel',
    'depends_on_past': False,
	'start_date': datetime(2021, 9, 19, tzinfo=local_tz),
    'email': ['jzaepfel@daemen.edu'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'depends_on_past': True,
}

dag = DAG(
	'upload_zip_photos_sym', 
	default_args=default_args, 
	schedule_interval="1 0 1 * *",
	tags=['MyDaemen','Symplicity']
)

# Use SSH hook to get a count of the photo files
def count_mydaemen_photos():
	
	ssh_hook = SSHHook(ssh_conn_id=ssh_conn_myd)
	
	client = ssh_hook.get_conn()
	
	stdin, stdout, stderr = client.exec_command('ls ' + mydaemen_remote_path + ' | wc -l')
	
	for line in stdout:
		
		print(line)
	
	client.close()

	return line

cmp = PythonOperator(
	task_id = 'count_mydaemen_photos',
	python_callable = count_mydaemen_photos,
	do_xcom_push = True,
	dag = dag,
)

# SSH into MyDaemen and gzip up all the ID photos into one file
smzp = SSHOperator(
	task_id='ssh_mydaemen_zip_photos',
	ssh_conn_id=ssh_conn_myd,
	command='zip -j ' + mydaemen_remote_path + zip_file_name + ' ' + mydaemen_remote_path + '*.jpg',
	dag=dag,
)

# SFTP the compressed file from MyDaemen to the Airflow server
sfmgz = SFTPOperator(
    task_id="sftp_mydaemen_get_zip",
	ssh_conn_id=ssh_conn_myd,
    remote_filepath = mydaemen_remote_path + zip_file_name,
    local_filepath =  local_path + zip_file_name,
    operation = 'get',
    dag=dag,
)

# SFTP the compressed file from Airflow to Symplicity's drop server
sfspz = SFTPOperator(
    task_id="sftp_symplicity_put_zip",
    ssh_conn_id = 'ssh_symplicity_key',
    local_filepath =  local_path + zip_file_name,
    remote_filepath = symplicity_remote_path + zip_file_name,
    operation = 'put',
    dag=dag,
)

# Remove the gzipped file on MyDaemen
smrzf = SSHOperator(
	task_id='ssh_mydaemen_rm_zip_file',
	ssh_conn_id=ssh_conn_myd,
	command='rm -f ' + mydaemen_remote_path + zip_file_name,
	dag=dag,
)

# Remove the gzipped file on Airflow
brzf = BashOperator(
    task_id='bash_rm_zip_file',
	bash_command='rm -f ' + local_path + zip_file_name,
# 	bash_command='echo ' + local_path + zip_file_name,
    dag=dag,
)

# report message emailed
message = """
Photos sent to Symplicity Insight and Advocate<br/>
<br/>
{{ ti.xcom_pull(task_ids='count_mydaemen_photos') }} zipped photos in the file from:<br/>
 - {{ params.mydaemen_remote_path + params.zip_file_name }} -> {{ params.local_path + params.zip_file_name }}<br/>
 - {{ params.local_path + params.zip_file_name }} -> {{ params.symplicity_remote_path + params.zip_file_name }}<br/>
<br/>
Execution Time: {{ next_execution_date.in_timezone('US/Eastern') }}<br/>
Owner: {{ dag.owner }}<br/>
DAG: <a href="https://airflow.daemen.edu/admin/airflow/graph?dag_id={{ dag.dag_id }}">{{ dag.dag_id }}</a> - {{ ti.task_id }}<br/>
<br/>
"""

er = EmailOperator(
    task_id='email_report',
	to = to,
	subject = "Monthly Photos to Syplicity",
	html_content = message,
	params={
		'mydaemen_remote_path': mydaemen_remote_path, 
		'local_path': local_path, 
		'symplicity_remote_path': symplicity_remote_path, 
		'zip_file_name': zip_file_name,
	},
	dag=dag,
)


cmp >> smzp >> sfmgz >> sfspz >> [smrzf,brzf,er]
