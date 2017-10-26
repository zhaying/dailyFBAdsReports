#BGN IMPORTS----------------------------------------------------------------------

# SYS IMPORT
import sys

# TIME IMPORT
from datetime import date, timedelta

# SQL IMPORT
import pyodbc 

# XML IMPORT
import json
import urllib
import dicttoxml

# FB IMPORT
from facebookads.adobjects.adaccount import AdAccount
from facebookads.adobjects.adset import AdSet
from facebookads.adobjects.adsinsights import AdsInsights
from facebookads.api import FacebookAdsApi

#END IMPORTS----------------------------------------------------------------------


#PARAMETERS 
app_environment = sys.argv[1]
#the_year = sys.argv[2]
#the_month = sys.argv[3]
#the_sday = sys.argv[4]
#the_eday = sys.argv[5]


#VARIABLES
the_path = "C:\\dir\\fb\\in\\"


#BGN TIME
today = date.today()
yesterday = today.day - 1
yesterday_date = date(int(today.year), int(today.month), int(yesterday))
#print (yesterday) 
#print (yesterday_date) 
d1 = today # start date
d2 = today # end date
date_of_today = d1
#print (date_of_today)

delta = d2 - d1         # timedelta
#END TIME


#BGN SQL----------------------------------------------------------------------
#MOVE ALL PASSWORDS AND KEYS TO ENV VARIABLES PENDING, THIS IS ONLY A TEST
#SQL AUTHENTICATION
 #SQL PARAMETERS
driver		= '{ODBC Driver 13 for SQL Server}' 
server 		= 'localhostNAME' 
database 	= 'databaseNAME' 
username 	= 'userNAME' 
password 	= 'passwordNAME' 
 #SQL CONNECTION AND CURSOR
cnxn 		= pyodbc.connect('DRIVER='+driver+';SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password +';MultipleActiveResultSets=True')
cursor 		= cnxn.cursor()

# FB AUTHENTICATION
 #FB PARAMETERS
access_token 	= 'accessTokenNAME'
app_secret 		= 'accessSecretNAME'
 #FB API INITIALIZATION
FacebookAdsApi.init(access_token=access_token)


 #FB DATA BEING REQUESTED
async = False
fields = [
	'account_id',
	'account_name',
	'campaign_id',
	'campaign_name',
	'adset_id',
	'adset_name',
	'ad_id',
	'ad_name',
	'actions',
	'action_values',
	'total_actions',
	'reach', #unique_impression
	'impressions',
	'clicks',
	'spend',
	'social_reach', #unique_social_impressions
	'social_clicks',
	'cost_per_total_action',
	'cost_per_unique_click',
	'cost_per_action_type',
	'frequency',
	'unique_social_clicks',
	'unique_clicks',
	'unique_ctr',
	'total_unique_actions',
	'cpc',
	'ctr'
]

for i in range(delta.days + 1):
	date_since = str(yesterday_date)
	date_until = str(yesterday_date)
	the_date = str(yesterday_date)
	
	platform_params = {'limit':			3000,
	'level': 			'ad',
	'filtering': 		[],
	'breakdowns': 		['publisher_platform', 'device_platform'],
	'time_increment': 	1,
	'time_range': 		{	'since':	date_since, 'until':	date_until }
	}

	age_params = {'limit':			3000,
	'level': 			'ad',
	'filtering': 		[],
	'breakdowns': 		[	'age', 'gender' ],
	'time_increment': 	1,
	'time_range': 		{	'since':	date_since, 'until':	date_until },
	}

	country_params = {'limit':			4000,
	'level': 			'ad',
	'filtering': 		[],
	'breakdowns': 		['country'],
	'time_increment': 	1,
	'time_range': 		{	'since':	date_since, 'until':	date_until },
	}
# BGN MANIPULATE GRAPH to JSON
# END MANIPULATE GRAPH to JSON
#SQL TEST QUERY
#cursor.execute("SELECT TOP 5 [COLUMN1],[COLUMN2],[COLUMN3],[COLUMN4],[COLUMN5],[COLUMN6] FROM [DATABASENAME].[dbo].[TABLENAME]")
	cursor.execute("SELECT COLUMN1, COLUMN2, COLUMN3, REPLACE(COLUMN4,'-','') AS COLUMN4 FROM TABLENAME WHERE COLUMN5=26  AND COLUMNS7='Active' AND COLUMN5 IS NOT NULL ORDER BY COLUMN1, COLUMN2, COLUMN3")
	for row in cursor.fetchall():
		print (row)
		print (row[0], row[1], row[2], row[3])
		ad_account_id = row[3]
		#row = cursor.fetchmany(size=cursor.arraysize)
		#FB CLIENT BEING REQUESTED
		account			= AdAccount(ad_account_id)
		#    PLATFORM
		myedges_platform	= str(AdAccount(ad_account_id).get_insights( fields =	fields,
			params	=	platform_params,
			async	=	async ))
		#print myedges_platform
		fixedBgnplatform	= myedges_platform.replace("[<AdsInsights>", '{"AdsInsights":[')
		fixedcontentplatform	= fixedBgnplatform.replace("<AdsInsights>", "")
		fixed_platform			= fixedcontentplatform.replace("}]", "}]}")
		#print fixed_platform
		
		#	AGE
		myedges_age			= str(AdAccount(ad_account_id).get_insights( fields	=	fields, 
			params	=	age_params,
			async	=	async ))
		fixedBgnAge			= myedges_age.replace("[<AdsInsights>", '{"AdsInsights":[')
		fixedcontentAge		= fixedBgnAge.replace("<AdsInsights>", "")
		fixed_age			= fixedcontentAge.replace("}]", "}]}")
		#print fixed_age
		#	/AGE		
		
		#    COUNTRY
		myedges_country			= str(AdAccount(ad_account_id).get_insights(
			fields	=	fields,
			params	=	country_params,
			async	=	async ))
		fixedBgnCountry			= myedges_country.replace("[<AdsInsights>", '{"AdsInsights":[')
		fixedcontentCountry		= fixedBgnCountry.replace("<AdsInsights>", "")
		fixed_country			= fixedcontentCountry.replace("}]", "}]}")
		#print fixed_country
		#    /COUNTRY
		
		obj_platform	=	json.loads(fixed_platform)
		obj_age			=	json.loads(fixed_age)
		obj_country		=	json.loads(fixed_country)
#print obj


#BGN XML----------------------------------------------------------------------
#XML CONVERT JSON TO XML
		xml_platform				= 	dicttoxml.dicttoxml(
										obj_platform,
										attr_type	=	False
								)

		file_name_platform			=	"report_FB_platform_" + ad_account_id + "_" + the_date + ".xml"
		file_platform				= 	open(the_path + file_name_platform, 'wb')
		file_platform.write(xml_platform)
		file_platform.close() 

		xml_age						= 	dicttoxml.dicttoxml(
										obj_age, 
										attr_type	=	False
							)
		file_name_age				=	"report_FB_age_" + ad_account_id + "_" + the_date + ".xml"
		file_age					= 	open(the_path + file_name_age, 'wb')
		file_age.write(xml_age)
		file_age.close() 

		xml_country					= 	dicttoxml.dicttoxml(
										obj_country, 
										attr_type	=	False
							)
		file_name_country			=	"report_FB_country_" + ad_account_id + "_" + the_date + ".xml"
		file_country				= 	open(the_path + file_name_country, 'wb') 
		file_country.write(xml_country)
		file_country.close() 

#END XML----------------------------------------------------------------------

# MSSQL BULK INSERT 
		sql_syntax_platform 					= "{call INSERT_FB_PLATFORM (?, ?, ?, ?)}"
		params 						= (ad_account_id, the_path + file_name_platform, date_since, date_until)
		#print (sql_syntax_platform, params)
		try:
			cnxn.execute(sql_syntax_platform, params)
			cnxn.commit()
			#return_value_platform = cursor.fetchval()
		except pyodbc.Error as err:
			print ('Error !!!!! %s' % err)
		print ("\nResults :" )

		sql_syntax_age 					= "{call INSERT_FB_AGE_GENDER (?, ?, ?, ?)}"
		params 						= (ad_account_id, the_path + file_name_age, date_since, date_until)
		#print (sql_syntax_age, params)
		try:
			cnxn.execute(sql_syntax_age, params)
			cnxn.commit()
			#return_value_age = cursor.fetchval()
		except pyodbc.Error as err:
			print ('Error !!!!! %s' % err)
		print ("\nResults :" )

		sql_syntax_country 					= "{call INSERT_FB_COUNTRY (?, ?, ?, ?)}"
		params 						= (ad_account_id, the_path + file_name_country, date_since, date_until)
		#print (sql_syntax_country, params)
		try:
			cnxn.execute(sql_syntax_country, params)
			cnxn.commit()
			#return_value_country = cursor.fetchval()
		except pyodbc.Error as err:
			print('Error !!!!! %s' % err)
		print ("\nResults :" )
		
print ("\n\nComplete.")

# Close and delete cursor
cursor.close()
del cursor

# Close Connection
cnxn.close()
#row = cursor.fetchone()
