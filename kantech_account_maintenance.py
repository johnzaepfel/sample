"""
### Kantech Account Maintenance  
"""
from airflow import DAG
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.hooks.http import HttpHook
from hooks.adldap_hook import AdLdapHook
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.utils import timezone
from urllib.parse import quote

from datetime import datetime, timedelta
import base64, json
import xml.etree.ElementTree as ET
import html
import pendulum

kantech_task_text_color = '#990000' #redish
ad_task_text_color = '#000099' #bluish
update_task_text_color = '#009900' #greenish
shortcircuit_text_color = '#999900' #yellowish

local_tz = pendulum.timezone("US/Eastern")

simulate = False


default_args = {
	'owner': 'jzaepfel',
	'depends_on_past': False,
# 	'start_date': days_ago(1),
	'start_date': datetime(2021, 9, 16, tzinfo=local_tz),
	'email': ['jzaepfel@daemen.edu'],
	'email_on_failure': True,
	'email_on_retry': True,
	'retries': 1,
	'retry_delay': timedelta(minutes=5),
}

dag = DAG(
	'kantech_account_maintenance', 
	default_args=default_args, 
 	schedule_interval='55 08,10,12,14,16,18 * * *',
# 	schedule_interval=None,
	tags=['Kantech','Active Directory']
)

# 
# Active Directory Settings
# 

adldap_conn = "adldap_prime"
adldap_hook =  AdLdapHook(adldap_conn_id = adldap_conn)
ous_list = [
	'Faculty',
	'Staff',
	'Students',
	'StudentsRemote',
	'StudentEmployees',
	'FirstYear',
]


"""
MySQL Settings
"""
mysql_conn = "prod_maria_db"
mysql_hook = MySqlHook(mysql_conn_id=mysql_conn)

"""
Generic MySQL query and automatically push into xcom (this a default 
task option or xcom_push=True
"""
def get_rows_and_push(sql,**context):
	#This lets the sql field be set with out context or **op_kargs or provide_context=False
	
	ti = context['ti']
	
	#GET records based on the sql sent
	rows = mysql_hook.get_records(sql)
	
	print(rows)

	ti.xcom_push(key='key_id', value=rows[0][0])
	ti.xcom_push(key='key_value', value=rows[0][1])
	
	return True


"""
Get one program key from the MyDaemen table. Results are pushed into XCom.
"""
mysql_get_one_program_key = """
	SELECT
		*
	FROM
		`kantech_program_keys`
	ORDER BY 
		`id` ASC
	LIMIT 1
"""

gopk = PythonOperator(
	task_id='get_one_program_key',
	python_callable=get_rows_and_push,
	op_kwargs={
		"sql": mysql_get_one_program_key
	},
	provide_context = True,
	dag=dag
)


"""
Remove the selected program key from MyDaemen table.
"""
mysql_del_current_program_key = """
	DELETE FROM
		`kantech_program_keys`
	WHERE
		`id` = {{ ti.xcom_pull(task_ids='get_one_program_key',key='key_id') }}
"""

dpk = MySqlOperator(
    task_id='delete_program_key',
	sql=mysql_del_current_program_key, 
	mysql_conn_id=mysql_conn, 
	dag=dag,
)


def GET_session_key(**context):
	"""
	Uses the Kantech API credentials and returns a token to the XCom
	"""
	
	SessionKey = None
	
	# The ADP API credentials 
	username = Variable.get("KANTECH_USERNAME")
	password = Variable.get("KANTECH_PASSWORD")

	ti = context['ti']
	program_key = ti.xcom_pull(task_ids="get_one_program_key",key='key_value')
		
	# Set the connection hook
	http_conn = HttpHook(
		http_conn_id = "kantech_api",
		method = "GET",
	)
	
	token_request_params = '?encrypted=1&userName='+quote(username)+'&password='+quote(password)+'&ConnectedProgram='+program_key
	
	# Run connetion to generate a result 
	result = http_conn.run(
		endpoint = "SmartService/Login" + token_request_params,
	)
	
	root = ET.fromstring(result.text)
	
	for child in root:
		print(child.tag, child.text)
		if child.tag and child.tag=="SessionKey":
			print(child.tag, child.text)
			SessionKey = child.text

	if SessionKey == None:
		message= "The sessions key has not been retrieved successfully."
		raise ValueError(message)

	# Get the json text from the result object and convert to a dictionary 
	#result_dict = json.loads(result.text)
	
	# Return just the access token to xcom
	return SessionKey


Gsesskey = PythonOperator(
	task_id = 'GET_session_key',
	python_callable = GET_session_key,
	provide_context = True,
	do_xcom_push = True,
	dag = dag,
)
Gsesskey.ui_fgcolor = kantech_task_text_color


# GET ALL CARD TYPES IN KANTECH AND SET THE DESCRIPTION AS KEY AND PKDATA (KANTECH CARD TYPE ID) AS THE VALUE
def GET_card_types(**context) :
	
	ti = context['ti']
	session_key = ti.xcom_pull(task_ids='GET_session_key')
		
	# Set the connection hook
	http_conn = HttpHook(
		http_conn_id = "kantech_api",
		method = "GET",
	)
	
	url_request_params = '/?sdKey='+session_key
	
	# Run connetion to generate a result 
	result = http_conn.run(
		endpoint = "SmartService/CardTypes"+url_request_params,
	)
	
# 	print(result.text)
	xml = ET.fromstring(result.text)
	
	card_types = {}
	
	for cardtype in xml :
		
		#user: SmartLinkDataRow, 0: item, 1: value, 0: SmartLinkDataValue, 0: Value
		pkdata = cardtype[0][1][0][0].text
		
		#user: SmartLinkDataRow, 2: item, 1: value, 0: SmartLinkDataValue, 0: Value
		description = cardtype[2][1][0][0].text
# 		print("user DESCRIPTION: ",DESCRIPTION)
		
		card_types[description] = pkdata;
		
	return card_types


Gct = PythonOperator(
	task_id = 'GET_card_types',
	python_callable = GET_card_types,
	provide_context = True,
	do_xcom_push = True,
	dag = dag,
)
Gct.ui_fgcolor = kantech_task_text_color


# GET ALL GATEWAY SITES IN KANTECH AND SET THE DESCRIPTION AS KEY AND GATEWAY SITE ID AS THE VALUE
def GET_gateway_sites(**context) :
	
	ti = context['ti']
	session_key = ti.xcom_pull(task_ids='GET_session_key')
		
	# Set the connection hook
	http_conn = HttpHook(
		http_conn_id = "kantech_api",
		method = "GET",
	)
	
	url_request_params = '?sdKey='+session_key
	
	# Run connetion to generate a result 
	result = http_conn.run(
		endpoint = "SmartService/GatewaysSites"+url_request_params,
	)
	
# 	print(result.text)
	xml = ET.fromstring(result.text)
	
	gateway_sites = {}
	
	for gatewaysite in xml :
		
		#gatewaysite: LightComponentOfGatewaySite, 1: ID
		id = gatewaysite[1].text
		
		#gatewaysite: LightComponentOfGatewaySite, 2: ID
		description = gatewaysite[2].text
# 		print("user Description: ",description)
		
		gateway_sites[description] = id;
		
	return gateway_sites


Ggs = PythonOperator(
	task_id = 'GET_gateway_sites',
	python_callable = GET_gateway_sites,
	provide_context = True,
	do_xcom_push = True,
	dag = dag,
)
Ggs.ui_fgcolor = kantech_task_text_color


# GET ALL ACCESS LEVELS IN KANTECH AND SET THE OWNER AND DESCRIPTION AS KEYS AND ACCESS LEVEL ID AS THE VALUE
def GET_access_levels(**context) :
	
	ti = context['ti']
	session_key = ti.xcom_pull(task_ids='GET_session_key')
		
	# Set the connection hook
	http_conn = HttpHook(
		http_conn_id = "kantech_api",
		method = "GET",
	)
	
	url_request_params = '?sdKey='+session_key
	
	# Run connetion to generate a result 
	result = http_conn.run(
		endpoint = "SmartService/AccessLevels"+url_request_params,
	)
	
	print(result.text)
	xml = ET.fromstring(result.text)
	
	access_levels = {}
	
	for accesslevel in xml :
		
		#accesslevel: LightComponentOfAccessLevel, 1: ID
		id = accesslevel[1].text
		
		#accesslevel: LightComponentOfAccessLevel, 2: Description
		description = accesslevel[2].text
# 		print("user Description: ",description)
		
		#accesslevel: LightComponentOfAccessLevel, 4: Owner
		owner = accesslevel[4].text
# 		print("user Owner: ",owner)
		
		if( owner not in access_levels ) :
			access_levels[owner] = {}
			
		access_levels[owner][description] = id
		
	return access_levels


Gal = PythonOperator(
	task_id = 'GET_access_levels',
	python_callable = GET_access_levels,
	provide_context = True,
	do_xcom_push = True,
	dag = dag,
)
Gal.ui_fgcolor = kantech_task_text_color



# RETURN ALL USERNAMES IN KANTECH and set the pkdata (KANTECH USER ID) as key
# KANTECH HAS RETURN LIMIT OF 4096, MUST LOOP AND CONTINUE FROM PKDATA/ID OF LAST ENTRY RETURNED
# extendedFields=CARDINFO10 and OMIT is !=
# item[0] is pkdata (KANTECH USER ID)
# item[2] is Daemen username
# item[4] is CARDINFO10 used for OMIT cards

def GET_users(**context):
	
	ti = context['ti']
	session_key = ti.xcom_pull(task_ids='GET_session_key')
		
	# Set the connection hook
	http_conn = HttpHook(
		http_conn_id = "kantech_api",
		method = "GET",
	)
	
	loop = True
	last_key = ''
	cardstates_by_pkdata = {}
	fullnames_by_pkdata = {}
	usernames_by_pkdata = {}
	cardnumber_by_pkdata = {}
	pkdata_by_usernames = {}
	pkdata_by_usernames_singles = {}
	pkdata_by_usernames_dups = {}
	omit_usernames_by_pkdata = {}
	omit_pkdata_by_usernames = {}
	colleagueid_by_pkdata = {}
	
	while loop :

		cardinfo10 = None
		username = None
		pkdata = None

		if(last_key == ''):
			url_request_params = '?sdKey='+session_key+'&contains=0&extendedFields=CARDINFO10,CARDINFO2,CARDINFO1'
		else :
			url_request_params = '?sdKey='+session_key+'&contains=0&listStartValue='+str(last_key)+'&extendedFields=CARDINFO10,CARDINFO2,CARDINFO1'
		
		# Run connetion to generate a result 
		result = http_conn.run(
			endpoint = "SmartService/Cards"+url_request_params,
		)
		
# 		print(result.text)
		xml = ET.fromstring(result.text)
		
		c = 0
		for user in xml :
			#count the number of users processed
			c += 1
			
			#user: SmartLinkDataRow, 0: item, 1: value, 0: SmartLinkDataValue, 0: Value
			pkdata = user[0][1][0][0].text
			
			#user: SmartLinkDataRow, 1: item, 1: value, 0: SmartLinkDataValue, 0: Value
			cardnumber = user[1][1][0][0].text
			
			#user: SmartLinkDataRow, 2: item, 1: value, 0: SmartLinkDataValue, 0: Value
			username = user[2][1][0][0].text
# 			print("user USERNAME: ",username)

			#user: SmartLinkDataRow, 3: item, 1: value, 0: SmartLinkDataValue, 0: Value
			cardstate = user[3][1][0][0].text
# 			print("user cardstate: ",cardstate)
			
			#user: SmartLinkDataRow, 4: item, 1: value, 0: SmartLinkDataValue, 0: Value
			cardinfo10 = user[4][1][0][0].text
# 			print("user cardinfo10: ",cardinfo10)

			colleagueid = user[5][1][0][0].text
# 			print("user colleagueid: ",colleagueid)
			
			fullname = user[6][1][0][0].text
# 			print("user fullname: ",fullname)
			
			cardstates_by_pkdata[pkdata] = cardstate
			fullnames_by_pkdata[pkdata] = fullname
			colleagueid_by_pkdata[pkdata] = colleagueid
			
			
			if cardinfo10 != 'OMIT' :
				usernames_by_pkdata[pkdata] = username
				
				#could be multiple pkdatas for a username!
				if( username in pkdata_by_usernames ) :
					pkdata_by_usernames[username].append(pkdata)
				else :
					pkdata_by_usernames[username] = [pkdata]
					
			else :
				omit_usernames_by_pkdata[pkdata] = username
				
				#could be multiple pkdatas for a username!
				if( username in omit_pkdata_by_usernames ) :
					omit_pkdata_by_usernames[username].append(pkdata)
				else :
					omit_pkdata_by_usernames[username] = [pkdata]
				
		print(c)
		#if the total number of processed user in the last request is 4096, loop again
		if c == 4096 :
			loop = True
			#use the last set pkdata as the start value in the next request
			last_key = pkdata
		else :
			#that was the last loop
			loop = False
			
	#handling multiple pkdata records per username
	for username in pkdata_by_usernames :	
		if( len(pkdata_by_usernames[username]) > 1 ) :
			#if there is more than one pkdata...
			print("multiple pks for: ",username," > ",pkdata_by_usernames[username])
			pkdata_by_usernames_dups[username] = pkdata_by_usernames[username]
		else :
			pkdata_by_usernames_singles[username] = pkdata_by_usernames[username][0]
			
	#handling multiple pkdata records per username
	for username in omit_pkdata_by_usernames :	
		if( len(omit_pkdata_by_usernames[username]) > 1 ) :
			#if there is more than one pkdata...
			print("multiple pks for: ",username," > ",omit_pkdata_by_usernames[username])
			pkdata_by_usernames_dups[username] = pkdata_by_usernames[username]
		else :
			omit_pkdata_by_usernames[username] = omit_pkdata_by_usernames[username][0]
			
			
	print("PKData Count: ", len(pkdata_by_usernames))
	print("PKData Singles Count: ", len(pkdata_by_usernames_singles))
	print("PKData Duplicates Counts: ", len(pkdata_by_usernames_dups))
	print("Omit Count: ", len(omit_pkdata_by_usernames))
	print("Usernames Count: ", len(usernames_by_pkdata))
	print("Card State Count: ", len(cardstates_by_pkdata))		
	print("ColleagueIDs Count: ", len(colleagueid_by_pkdata))
	
	ti.xcom_push(key='cardstates_by_pkdata', value=cardstates_by_pkdata)
	ti.xcom_push(key='fullnames_by_pkdata', value=fullnames_by_pkdata)
	ti.xcom_push(key='usernames_by_pkdata', value=usernames_by_pkdata)
	ti.xcom_push(key='pkdata_by_usernames', value=pkdata_by_usernames_singles)
	ti.xcom_push(key='pkdata_by_usernames_dups', value=pkdata_by_usernames_dups)
	ti.xcom_push(key='omit_pkdata_by_usernames', value=omit_pkdata_by_usernames)
	ti.xcom_push(key='colleagueid_by_pkdata', value=colleagueid_by_pkdata)

	return True
	

Gu = PythonOperator(
	task_id = 'GET_users',
	python_callable = GET_users,
	provide_context = True,
	dag = dag,
)
Gu.ui_fgcolor = kantech_task_text_color




#ADD CARD TO KANTECH
def add_card(session_key, kantech_card_types, username, email, firstname, lastname, card_type, colleagueid) :
	
	# Set the connection hook
	http_conn = HttpHook(
		http_conn_id = "kantech_api",
		method = "POST",
	)
	
# 	print(kantech_card_types)
# 	print(card_type)
	
	if(card_type == 'Student') :
		input_xml = '''<Card>
	<UserName>'''+username+'''</UserName>
	<CardInfo1>'''+firstname+''' '''+lastname+'''</CardInfo1>
	<CardInfo2>'''+colleagueid+'''</CardInfo2>
	<CardInfo3>'''+email+'''</CardInfo3>
	<CardType>'''+kantech_card_types[card_type]+'''</CardType>
	<State>Added</State>
</Card>'''
	else :
		input_xml = '''<Card>
	<UserName>'''+username+'''</UserName>
	<CardInfo1>'''+firstname+''' '''+lastname+'''</CardInfo1>
	<CardInfo3>'''+email+'''</CardInfo3>
	<CardType>'''+kantech_card_types[card_type]+'''</CardType>
	<State>Added</State>
</Card>'''
	
	url_request_params = '/0?sdKey='+session_key
	
	# Run connetion to generate a result 
	result = http_conn.run(
		endpoint = "SmartService/Cards"+url_request_params,
		data = input_xml,
		headers = {'Content-Type':'application/xml'},
	)
	
	#posible methods on http_hook result:
	# result.status_code
	# result.headers['content-type']
	# result.encoding
	# result.text
	# result.json()
	
# 	print( "result Text: ", result.text )
	xml = ET.fromstring(result.text)
# 	print( "xml tag: ", xml.tag)
# 	print( "xml attrib: ", xml.attrib)
# 	print( "xml text: ", xml.text)
	
	if(result.status_code == 200 and xml.text) :
		return xml.text
	else :
		return False



#GET CARD DETAILS
def get_card(session_key, pkdata) :
	
	# Set the connection hook
	http_conn = HttpHook(
		http_conn_id = "kantech_api",
		method = "GET",
	)
	
	url_request_params = '/'+pkdata+'?sdKey='+session_key
	
	# Run connetion to generate a result 
	result = http_conn.run(
		endpoint = "SmartService/Cards"+url_request_params,
	)
	
# 	print(result.text)
	xml = ET.fromstring(result.text)
# 	print(xml.text)
	
	if( xml ) :
		return xml
	else :
		return False


#DELETES CARD PERMANENTLY
def remove_card(session_key, pkdata) :
	
	# Set the connection hook
	http_conn = HttpHook(
		http_conn_id = "kantech_api",
		method = "DELETE",
	)
	
	url_request_params = '/'+pkdata+'?sdKey='+session_key
	
	# Run connetion to generate a result 
	result = http_conn.run(
		endpoint = "SmartService/Cards"+url_request_params,
	)
	
# 	print(result.url)
	result_text = result.text
# 	print(result_text)
	
	if( result_text.find('Error deleting card.') != -1 ) :
		return False
	else :
		return True


#UPDATE COLLEAGUE ID IN KANTECH
def update_card(session_key, pkdata, attribute, value) :
	
	# Set the connection hook
	http_conn = HttpHook(
		http_conn_id = "kantech_api",
		method = "PUT",
	)
	
	input_xml = '''<Card>
	<'''+attribute+'''>'''+value+'''</'''+attribute+'''>
	<ID>'''+pkdata+'''</ID>
	<State>Modified</State>
</Card>'''
	
	url_request_params = '/'+pkdata+'?sdKey='+session_key
	
	# Run connetion to generate a result 
	result = http_conn.run(
		endpoint = "SmartService/Cards"+url_request_params,
		data = input_xml,
		headers = {'Content-Type':'application/xml'},
	)
	
	#posible methods on http_hook result:
	# result.status_code
	# result.headers['content-type']
	# result.encoding
	# result.text
	# result.json()
	
	if(result.status_code == 200) :
		return True
	else :
		return False


#CHANGE A CARDS STATE IN KANTECH
def change_card_state(session_key, pkdata, state) :
	
	# Set the connection hook
	http_conn = HttpHook(
		http_conn_id = "kantech_api",
		method = "PUT",
	)
	
	if( state == 'Valid' ) :
		UsingEndDate = '''	<UsingEndDate>False</UsingEndDate>
'''
	else :
		UsingEndDate = ''
	
	input_xml = '''<Card>
	<CardState>'''+state+'''</CardState>
	<CardStateAsString>'''+state+'''</CardStateAsString>'''+UsingEndDate+'''
	<ID>'''+pkdata+'''</ID>
	<State>Modified</State>
</Card>'''
# 	print( input_xml )
	
	url_request_params = '/'+pkdata+'?sdKey='+session_key
	
	# Run connetion to generate a result 
	result = http_conn.run(
		endpoint = "SmartService/Cards"+url_request_params,
		data = input_xml,
		headers = {'Content-Type':'application/xml'},
	)
	
	#posible methods on http_hook result:
	# result.status_code
	# result.headers['content-type']
	# result.encoding
	# result.text
	# result.json()
	
# 	print( result.text )
	
	if(result.status_code == 200) :
		return True
	else :
		return False


#ADD ACCESS LEVEL TO CARDS
def add_access_level_to_card(session_key, gateway_sites, access_levels, pkdata, site_name, access_level) :
	
	site_id = gateway_sites[site_name]
	
	access_level_id = access_levels[site_id][access_level]
	
	input_xml = '''<Card>
	<CardAccessLevels>
		<CardAccessLevel>
			<GatewaySiteID>'''+site_id+'''</GatewaySiteID>
			<AccessLevelID>'''+access_level_id+'''</AccessLevelID>
			<State>Modified</State>
		</CardAccessLevel>
	</CardAccessLevels>
	<ID>'''+pkdata+'''</ID>
	<State>Modified</State>
</Card>'''
	
	url_request_params = '/'+pkdata+'?sdKey='+session_key
	
	# Run connetion to generate a result 
	result = http_conn.run(
		endpoint = "SmartService/Cards"+url_request_params,
		data = input_xml,
		headers = {'Content-Type':'application/xml'},
	)
	
	#posible methods on http_hook result:
	# result.status_code
	# result.headers['content-type']
	# result.encoding
	# result.text
	# result.json()
	
	if(result.status_code == 200) :
		return True
	else :
		return False

	
#REMOVES ACCESS LEVEL TO CARD - SETS TO DISABLED ACCESS LEVEL
def remove_access_level_from_card(session_key, gateway_sites, access_levels, pkdata, site_name, username) :
	
	site_id = gateway_sites[site_name]
	
	disabled_id = access_levels[site_id]['Disabled']
	
	input_xml = '''<Card>
	<UserName>'''+username+'''</UserName>
	<CardAccessLevels>
		<CardAccessLevel>
			<GatewaySiteID>'''+site_id+'''</GatewaySiteID>
			<AccessLevelID>'''+disabled_id+'''</AccessLevelID>
			<State>Modified</State>
		</CardAccessLevel>
	</CardAccessLevels>
	<ID>'''+pkdata+'''</ID>
	<State>Modified</State>
</Card>'''
	
	url_request_params = '/'+pkdata+'?sdKey='+session_key
	
	# Run connetion to generate a result 
	result = http_conn.run(
		endpoint = "SmartService/Cards"+url_request_params,
		data = input_xml,
		headers = {'Content-Type':'application/xml'},
	)
	
	#posible methods on http_hook result:
	# result.status_code
	# result.headers['content-type']
	# result.encoding
	# result.text
	# result.json()
	
	if(result.status_code == 200) :
		return True
	else :
		return False		



# Search AD by specific OUs and return the user accounts
def AD_search_ou_all_users():
	"""
	Get all the users in an OU. Any OU can be used to retrieve records. 
	The filter restricts to just the person objects. Using the custom 
	adldap_hook method search_multiple. This pages allowing for an 
	unlimited number of objects.
	"""
	
	all_entries = {}
	
	for ou in ous_list:
	
		entry_list = adldap_hook.search_multi(
			search_base = 'ou=Users,ou='+ou+',dc=daemen,dc=college',
			search_filter = '(&(objectClass=person)(!(userAccountControl:1.2.840.113556.1.4.803:=2)))',
			attributes=['uid', 'sn', 'givenName', 'mail', 'wWWHomePage'],
		)
		
		print(ou+': ', len(entry_list))
		
		all_entries[ou] = entry_list
	
# 	print('All: ', len(all_entries))
	
	return all_entries


suau = PythonOperator(
	task_id = 'AD_search_ou_all_users',
	python_callable = AD_search_ou_all_users,
	do_xcom_push = True,
	dag=dag
)
suau.ui_fgcolor = ad_task_text_color




def account_maintenance(**context):
	"""
	This task adds card, updates name, adds or updates Colleague ID, 
	changes card status from valid to invalue and invalid or expired to valid, 
	and lists duplicates by username. The output text and other various 
	results are passed to the email task.
	""" 
	
	ti = context['ti']

	session_key = ti.xcom_pull(task_ids='GET_session_key')

	cardstates_by_pkdata = ti.xcom_pull(task_ids="GET_users",key='cardstates_by_pkdata')
	fullnames_by_pkdata = ti.xcom_pull(task_ids="GET_users",key='fullnames_by_pkdata')
	usernames_by_pkdata = ti.xcom_pull(task_ids="GET_users",key='usernames_by_pkdata')
	pkdata_by_usernames = ti.xcom_pull(task_ids="GET_users",key='pkdata_by_usernames')
	pkdata_by_usernames_dups = ti.xcom_pull(task_ids="GET_users",key='pkdata_by_usernames_dups')
	omit_pkdata_by_usernames = ti.xcom_pull(task_ids="GET_users",key='omit_pkdata_by_usernames')
	colleagueid_by_pkdata = ti.xcom_pull(task_ids="GET_users",key='colleagueid_by_pkdata')

	card_types = ti.xcom_pull(task_ids="GET_card_types")
	
	all_ad_entries_by_ou = ti.xcom_pull(task_ids="AD_search_ou_all_users")
		
	output_list = []
	output = ''
	fail_output_list = []
	fail_output = ''
	dup_list = []
	change_ivlist = []
	change_vlist = []
	change_invalid = 0
	change_valid = 0
	added = 0
	name_updated = 0
	cid_added = 0
	cid_updated = 0
	all_ad_uids = {}
	
	# ======== Adding and Updating Cards =======
	#go through each OU
	for ou in all_ad_entries_by_ou :
	
		ou = ou.strip()
	
		#go through each user in the OU
		for ad_user in all_ad_entries_by_ou[ou] :
			uid = ad_user['attributes']['uid'][0]
			givenName_ad = html.unescape(ad_user['attributes']['givenName'])
			sn_ad = html.unescape(ad_user['attributes']['sn'])
			colleagueID_ad = ad_user['attributes']['wWWHomePage']
			
			#print(uid)
			
			#If they are not in Kantech (single or duplicate) and not ommited, add the card 
			if( uid not in pkdata_by_usernames.keys() and uid not in pkdata_by_usernames_dups.keys() and uid not in omit_pkdata_by_usernames.keys() ) :
				
				if( simulate == False ) :
					
					if( colleagueID_ad ) :
						colleagueID = colleagueID_ad
					else :
						colleagueID = '<em>none</em>'
					
# 					print( "ou: ", ou )
# 					print( "test: ", ou == 'StudentsRemote' )
					
					if( ou == 'Students' or ou == 'StudentsRemote' or ou == 'FirstYear') :
						card_type = 'Student'
					elif( ou == 'StudentEmployees') :
						card_type = 'Staff'
					else :
						card_type = ou
					
# 					print( "ou: ", ou, "card_type: ", card_type )
					
					pkdata = add_card(session_key, card_types, uid, ad_user['attributes']['mail'], givenName_ad, sn_ad, card_type, colleagueID)
					
					if( pkdata ) :
						pkdata_by_usernames[uid] = pkdata
						fullnames_by_pkdata[pkdata] = givenName_ad+' '+sn_ad
						colleagueid_by_pkdata[pkdata] = colleagueID
						output_list.append(' + '+givenName_ad+' '+sn_ad+' ('+uid+') ['+colleagueID+'] Not in Kantech'+' - Card Added.')
						added += 1
					else :
						fail = True
						fail_output_list.append(' + '+givenName_ad+' '+sn_ad+' - add_card() FAILED!')
					
				else :
					pkdata_by_usernames[uid] = '99999'
					fullnames_by_pkdata['99999'] = 'Sim Card'
					colleagueid_by_pkdata['99999'] = '9999999'
					output_list.append(' + '+givenName_ad+' '+sn_ad+' ('+uid+') ['+colleagueID+'] Not in Kantech'+' - Card Added. (Simulated)')
					added += 1

						
			#If they are in Kantech (singles) (and not ommitted), check for changes...
			if( uid in pkdata_by_usernames.keys() ) :
				
				pkdata = pkdata_by_usernames[uid]
				
				# ---- if their names exists and is not null in LDAP ----
				if( 'sn' in ad_user['attributes'] and ad_user['attributes']['sn'] ) :
					#if it does exist in Kantech and not equal to LDAP:  Update it
					if( fullnames_by_pkdata[pkdata_by_usernames[uid]].strip() != givenName_ad.strip()+' '+sn_ad.strip()) :
						print( 'fullnames_by_pkdata[pkdata_by_usernames[uid]].strip()', fullnames_by_pkdata[pkdata_by_usernames[uid]].strip() , ' ? ', givenName_ad.strip()+' '+sn_ad.strip(), "givenName_ad.strip()+' '+sn_ad.strip()") 
						
						if( simulate == False ) :
						
							update_cid_result = update_card(session_key, pkdata, 'CardInfo1', givenName_ad.strip()+' '+sn_ad.strip())
							
							if( update_cid_result ) :
								output_list.append(' * '+givenName_ad+' '+sn_ad+' ('+uid+'): Updated name from '+fullnames_by_pkdata[pkdata]+' to '+givenName_ad+' '+sn_ad)
								name_updated += 1
							
							else :
								fail = True
								fail_output_list.append(' * '+givenName_ad+' '+sn_ad+' ('+uid+') - update_cid_card() FAILED!')
							
						else :
							output_list.append(' * '+givenName_ad+' '+sn_ad+' ('+uid+'): Updated name from '+fullnames_by_pkdata[pkdata]+' to '+givenName_ad+' '+sn_ad+' (Simulated)')
							name_updated += 1
							

				# ---- if their CID exists and is not null in LDAP ----
				if( 'wWWHomePage' in ad_user['attributes'] and ad_user['attributes']['wWWHomePage'] ) :
					#Colleague ID exists in AD and is not null in Kantech:  Add it
# 					print( 'colleagueid_by_pkdata[pkdata]:', colleagueid_by_pkdata[pkdata], type(colleagueid_by_pkdata[pkdata]) )
# 					print( 'pkdata not in colleagueid_by_pkdata:',pkdata not in colleagueid_by_pkdata )
					
					if( pkdata not in colleagueid_by_pkdata.keys() ) :
						
						if( simulate == False ) :
						
							update_cid_result = update_card(session_key, pkdata, 'CardInfo2', ad_user['attributes']['wWWHomePage'])
							
							if( update_cid_result ) :
							
								output_list.append(' # '+givenName_ad+' '+sn_ad+' ('+uid+'): Added Colleague ID')
								cid_added += 1
							
							else :
								fail = True
								fail_output_list.append(' # '+givenName_ad+' '+sn_ad+' ('+uid+') - update_cid_card() FAILED!')
							
						else :
							
							output_list.append(' # '+givenName_ad+' '+sn_ad+' ('+uid+'): Added Colleague ID'+' (Simulated)')
							cid_added += 1
						
					#Colleague ID in Kantech is not equal to AD:  Update it
					elif(colleagueid_by_pkdata[pkdata] != ad_user['attributes']['wWWHomePage']) :
						
						if( simulate == False ) :
						
							update_cid_result = update_card(session_key, pkdata, 'CardInfo2', ad_user['attributes']['wWWHomePage'])
							
							if( update_cid_result ) :
								output_list.append(' # '+givenName_ad+' '+sn_ad+' ('+uid+'): Updated Colleague ID from '+str(colleagueid_by_pkdata[pkdata])+' to '+ad_user['attributes']['wWWHomePage'])
								cid_updated += 1
							
							else :
								fail = True
								fail_output_list.append(' # '+givenName_ad+' '+sn_ad+' ('+uid+') - update_cid_card() FAILED!')
							
						else :
							print( 'givenName_ad:', givenName_ad, type(givenName_ad) )
							print( 'sn_ad:', sn_ad, type(sn_ad) )
							print( 'uid:', uid, type(uid) )
							print( 'colleagueid_by_pkdata[pkdata]:', colleagueid_by_pkdata[pkdata], type(colleagueid_by_pkdata[pkdata]) )
							print( 'ad_user[attributes][wWWHomePage]:', ad_user['attributes']['wWWHomePage'], type(ad_user['attributes']['wWWHomePage']) )
							
							output_list.append(' # '+givenName_ad+' '+sn_ad+' ('+uid+'): Updated Colleague ID from '+str(colleagueid_by_pkdata[pkdata])+' to '+ad_user['attributes']['wWWHomePage']+' (Simulated)')
							cid_updated += 1
			
			all_ad_uids[pkdata] = uid
			
					
		if( len(output_list) > 0 ) :
			
			output += '<br/>'+ou+':'+'<br/>'+'====================='+'<br/>'
			output += '<br/>'.join(output_list)
			output += '<br/>'
			
		output_list = []	
			
		if( len(fail_output_list) > 0 ) :
			
			fail_output += '<br/>'+'FAILED '+ou+':'+'<br/>'+'====================='+'<br/>'
			fail_output += '<br/>'.join(fail_output_list)
			fail_output += '<br/>'
		
		fail_output_list = []	
	

	# ======== Change Card Status =======
	
	for cardstate_pkdata in cardstates_by_pkdata :
	
		# Change to Invalid cards
		#If a card is in Kantech and not in AD lists AND not already invalid (1) AND not expired (4) AND not in OMITed  
		if( cardstate_pkdata not in all_ad_uids.keys() and cardstates_by_pkdata[cardstate_pkdata] != '1' and cardstates_by_pkdata[cardstate_pkdata] != '4' and cardstate_pkdata not in omit_pkdata_by_usernames.values() ) :

			print('PKData: ', cardstate_pkdata, ' State: ', cardstates_by_pkdata[cardstate_pkdata], ' Name: ', fullnames_by_pkdata[cardstate_pkdata] )
			
			card_data = get_card(session_key, cardstate_pkdata)
			#get card data about a pkdata and add it to the report
			
			if( simulate == False ) :
				change_card_state(session_key, cardstate_pkdata, 'Invalid')
				
				change_ivlist.append(' < '+fullnames_by_pkdata[cardstate_pkdata]+' ('+usernames_by_pkdata[cardstate_pkdata]+') - State: '+card_data.find('CardState').text+' -> Invalid')
			
			else:
			
				change_ivlist.append(' < '+fullnames_by_pkdata[cardstate_pkdata]+' ('+usernames_by_pkdata[cardstate_pkdata]+') - State: '+card_data.find('CardState').text+' -> Invalid  (Simulated)')
				
			print('CardState: ', card_data.find('CardState').text, ' to Invalid')

			change_invalid += 1
		
	
		# Change to Valid cards
		#If they are in the AD list AND they are NOT marked as valid (0) AND not in OMITed
		if( cardstate_pkdata in all_ad_uids.keys() and cardstates_by_pkdata[cardstate_pkdata] != '0' and cardstate_pkdata not in omit_pkdata_by_usernames.values() ) :

			#print('PKData: ', cardstate_pkdata, ' State: ', cardstates_by_pkdata[cardstate_pkdata], ' Name: ', fullnames_by_pkdata[cardstate_pkdata] )
			
			card_data = get_card(session_key, cardstate_pkdata)
			#get card data about a pkdata and add it to the report

			#print('CardState: ', card_data.find('CardState').text, ' to Valid')
			
			if( simulate == False ) :
				change_card_state(session_key, cardstate_pkdata, 'Valid')
				
				change_vlist.append(' > '+fullnames_by_pkdata[cardstate_pkdata]+' ('+usernames_by_pkdata[cardstate_pkdata]+') - State: '+card_data.find('CardState').text+' -> Valid')
			
			else:
			
				change_vlist.append(' > '+fullnames_by_pkdata[cardstate_pkdata]+' ('+usernames_by_pkdata[cardstate_pkdata]+') - State: '+card_data.find('CardState').text+' -> Valid (Simulated)')
				

			change_valid += 1
		
	
	if( len(change_ivlist) > 0 ) :
		
		output += '<br/>Change to Invalid ('+str(change_invalid)+')<br/>'+'====================='+'<br/>'
		output += '<br/>'.join(change_ivlist)
		output += '<br/>'
		
	
	print( 'Change to Invalid: ', change_invalid )
	
	if( len(change_vlist) > 0 ) :
		
		output += '<br/>Change to Valid ('+str(change_valid)+')<br/>'+'====================='+'<br/>'
		output += '<br/>'.join(change_vlist)
		output += '<br/>'
		
	
	print( 'Change to Valid: ', change_valid )
	
	
	# ======== Finding Duplicate Cards =======
	if( len(pkdata_by_usernames_dups) > 0 ) :
	
		output += '<br/>Duplicates ('+str(len(pkdata_by_usernames_dups))+')<br/>'+'====================='+'<br/>'
		
		#go through the dict of duplicate UIDs
		for uid in pkdata_by_usernames_dups :
			
			dup_list.append(' ? '+uid+' - Duplicate, more details:')
			
			#go through each duplicate card pkdata under a UID
			for dup_pkdata in pkdata_by_usernames_dups[uid] :
				
				card_data = get_card(session_key, dup_pkdata)
				#get card data about a pkdata and add it to the report
				if( card_data ) :
# 					print('--- ',dup_pkdata,'<br/>------ Name: ',str(card_data.find('CardInfo1').text),'<br/>------ CardState: ',card_data.find('CardState').text,'<br/>------ CreationDate: ',card_data.find('CreationDate').text,'<br/>------ LastModificationDate: ',card_data.find('LastModificationDate').text,'<br/>------ PictureDefined: ',card_data.find('PictureDefined').text )
					dup_list.append('--- '+dup_pkdata+'<br/>------ Name: '+str(card_data.find('CardInfo1').text)+'<br/>------ CardState: '+card_data.find('CardState').text+'<br/>------ CreationDate: '+card_data.find('CreationDate').text+'<br/>------ LastModificationDate: '+card_data.find('LastModificationDate').text+'<br/>------ PictureDefined: '+card_data.find('PictureDefined').text)
			
		output += '<br/>'.join(dup_list)
		
		output += '<br/>'
		
					
	if( output == '' ) :
		output = '<br/>No changes.<br/>'


	
	
	
# 	print('change_added: ',len(change_added))
# 	print('change_updated: ',len(change_updated))
# 	print('no_daemen_email: ',len(no_daemen_email))
	
	ti.xcom_push(key='output', value=output)
	ti.xcom_push(key='fail_output', value=fail_output)
	ti.xcom_push(key='added', value=added)
	ti.xcom_push(key='name_updated', value=name_updated)
	ti.xcom_push(key='cid_added', value=cid_added)
	ti.xcom_push(key='cid_updated', value=cid_updated)
	
	return True


am = PythonOperator(
	task_id = 'account_maintenance',
	python_callable = account_maintenance,
	provide_context = True,
	dag = dag,
)
am.ui_fgcolor = update_task_text_color


emails_message = """
Kantech Entrapass Account Maintenance<br/>
<br>
Users Added: {{ ti.xcom_pull(task_ids='account_maintenance',key='added') }}<br/>
Names Updated: {{ ti.xcom_pull(task_ids='account_maintenance',key='name_updated') }}<br/>
IDs Added: {{ ti.xcom_pull(task_ids='account_maintenance',key='cid_added') }}<br/>
IDs Updated: {{ ti.xcom_pull(task_ids='account_maintenance',key='cid_updated') }}<br/>
{{ ti.xcom_pull(task_ids='account_maintenance',key='output') }}
<br/>
Execution Time: {{ next_execution_date.in_timezone('US/Eastern') }}<br/>
Owner: {{ dag.owner }}<br/>
DAG: <a href="https://airflow.daemen.edu/admin/airflow/graph?dag_id={{ dag.dag_id }}">{{ dag.dag_id }}</a> - {{ ti.task_id }}<br/>
<br/>
"""

if simulate == True :
	ram_to = [
		'jzaepfel@daemen.edu',
	]
else :
	ram_to = [
		'jzaepfel@daemen.edu', 
		'cpack@daemen.edu', 
		'rmeadcol@daemen.edu', 
		'kdubrey@daemen.edu', 
		'twoj@daemen.edu',
	]
	
ram = EmailOperator(
	task_id = 'report_account_maintenance',
	to = ram_to, # (list or string (comma or semicolon delimited)) – list of emails to send the email to. (templated)
	subject = 'Kantech Entrapass Account Maintenance', # (str) – subject line for the email. (templated)
	html_content = emails_message, #(str) – content of the email, html markup is allowed. (templated)
	dag = dag,
)








gopk >> [dpk, Gsesskey]

Gsesskey >> [Gu, Gct, Ggs, Gal] >> am

suau >> am

am >> ram

