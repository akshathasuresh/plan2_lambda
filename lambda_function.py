import tempfile
import numpy as np
import pandas as pd
import json
import boto3
import os
import csv
import sys
import traceback
from io import StringIO
from src.awslambda.Generic_function import logger, s3_get_object, s3_put_object, s3_Dataset_put_object, \
	s3_Dataset_get_object, s3_intermediate_files_put_object, db2_conn_test, s3_intermediate_files_get_object
from src.awslambda.plan_parameters import if_pl_src_header_col, HEADER_UPDATED_ROWS_IF_PL_SRC, col_dtype_if_pl_src_col, \
	TOCDC_IF_PL_SRC, \
	SELECT_STATEMENT_IF_PL_SRC, tgt_if_pl_src_update, SELECT_STATEMENT_UNIVL_PL_SRC, cdc_univl_pl_src, \
	cdc_univl_pl_src1, final_col, col_datatype, \
	header_univl_pl_src, col_list, HEADER_UPDATED_ROWS, tgt_unvil_pl_update, new_dataset_Col, \
	col_dtype_univl_pl_ind_y_col, df_Y_src_col, newplaninsert_ds_Col, HEADER_DS

Conn = db2_conn_test()
file_data_lst = []
file_data_Y_lst = []
file_data_N_Y_lst = []
universal_pl = ''


def lambda_handler(event, context):
	print("Starting point of plan load sequence 2")
	# logger.debug("Received event: " + json.dumps(event, indent=2))
	# if 'Records' in event:
	# 	recordkeepercode = event['Records'][0]
	# 	# cycledate = event['Records'][0]
	#
	# 	print(".....recordkeeper code....")
	# 	print(recordkeepercode)
	print("Entered into plan  sequence 2  ")
	print(
		'In plan sequence2 read the  dataset based on the condition and then  filter the rows and send to diffrent targets')

	# STEP1 this function to read DS_41_PLAN.txt and divide the file N,Y,NOT_EQL_N_Y contained files
	with tempfile.NamedTemporaryFile(mode='w+b', delete=False) as tmp:

		bucket_name, file_key, s3 = s3_Dataset_get_object()
		file_key = file_key + 'DS_41_PLAN.txt'
		response = s3.get_object(Bucket=bucket_name, Key=file_key)
		existing_data_DS_41_PLAN_DS = response['Body'].read().decode('utf-8')
		# print(existing_data_DS_41_PLAN_DS)
		with open(tmp.name, 'w', encoding='utf-8') as csv_file:
			csv_file.write(existing_data_DS_41_PLAN_DS)
		df_DS_41_PLAN = pd.read_csv(tmp)
		DS_41_PLAN = df_DS_41_PLAN.copy()
		# print(df)

		with tempfile.NamedTemporaryFile(suffix='txt', delete=False) as tmp1:

			# NEW_IND_N = 'NEW_IND_N.txt'
			# tmp1.name = NEW_IND_N
			with open(tmp1.name, 'w') as fn:

				fn.write(str(HEADER_DS).strip('[]'))
				fn.write('\n')
				for index, row in df_DS_41_PLAN.iterrows():

					# print(row['NEW_IND'])
					if row['NEW_IND'] == 'N':
						row = row.to_list()
						print('row', row)
						file_data_lst.append(row)
						fn.write(str(row).strip('[]'))
				# fn.write('\n')
				print(file_data_lst)

			NEW_IND_N_csv_buffer = StringIO()
			col = ['RCDKPER_CD', 'NEW_IND', 'NAME', 'COMPANY_NAME', 'COMPANY_ADDR_1', 'COMPANY_ADDR_2', 'COMPANY_CITY ',
				   'COMPANY_STATE', 'COMPANY_ZIP ', 'TYPE ', 'CONTRACT_STATE', 'YEAR_END', 'FISCAL_YEAR_END', 'IRS_ID',
				   'IFTP_PLAN_NUM', 'TP_NAME', 'TP_PHONE', 'TP_WEB_URL', 'TP_WEB_NAME', 'TP_CALL_CENTER',
				   'TP_CALL_CENTER_HRS', 'TP_PHONE_IMPAIRED', 'TP_PHONE_NON_US', 'ZIP_CD_SFX']
			print(file_data_lst)
			pd.options.display.width = 1000
			df_NEW_IND_N = pd.DataFrame(file_data_lst, columns=col)

			'''code to save file to s3 bucket'''
			df_NEW_IND_N.to_csv(NEW_IND_N_csv_buffer, index=False)
			bucket_name, file_key, s3 = s3_intermediate_files_put_object()
			file_key = file_key + "NEW_IND_N.txt"
			# file_str = output.read()
			# body = file_str
			result = s3.put_object(Bucket=bucket_name, Key=file_key, Body=NEW_IND_N_csv_buffer.getvalue())
			res = result.get('ResponseMetadata')
			if res.get('HTTPStatusCode') == 200:

				print("file uploaded successfully")
			else:
				print("file not uploaded")

			NEW_IND_N = df_NEW_IND_N.copy()
			read_NEW_IND_N(NEW_IND_N, DS_41_PLAN)

		"-----------------------------------"
		with tempfile.NamedTemporaryFile(suffix='txt', delete=False) as tmp_y:

			# NEW_IND_N = 'NEW_IND_N.txt'
			# tmp1.name = NEW_IND_N
			with open(tmp_y.name, 'w') as fn:

				fn.write(str(HEADER_DS).strip('[]'))
				fn.write('\n')
				for index, row in DS_41_PLAN.iterrows():

					# print(row['NEW_IND'])
					if row['NEW_IND'] == 'Y':
						row = row.to_list()
						print('row', row)
						file_data_Y_lst.append(row)
						fn.write(str(row).strip('[]'))
			# fn.write('\n')

			csv_buffer1 = StringIO()
			col = ['RCDKPER_CD', 'NEW_IND', 'NAME', 'COMPANY_NAME', 'COMPANY_ADDR_1', 'COMPANY_ADDR_2', 'COMPANY_CITY ',
				   'COMPANY_STATE', 'COMPANY_ZIP ', 'TYPE ', 'CONTRACT_STATE', 'YEAR_END', 'FISCAL_YEAR_END', 'IRS_ID',
				   'IFTP_PLAN_NUM', 'TP_NAME', 'TP_PHONE', 'TP_WEB_URL', 'TP_WEB_NAME', 'TP_CALL_CENTER',
				   'TP_CALL_CENTER_HRS', 'TP_PHONE_IMPAIRED', 'TP_PHONE_NON_US', 'ZIP_CD_SFX']

			pd.options.display.width = 1000
			df_NEW_IND_Y = pd.DataFrame(file_data_Y_lst, columns=col)

			df_NEW_IND_Y.to_csv(csv_buffer1, index=False)
			bucket_name, file_key, s3 = s3_intermediate_files_put_object()
			file_key = file_key + "NEW_IND_Y.txt"
			# file_str = output.read()
			# body = file_str
			result = s3.put_object(Bucket=bucket_name, Key=file_key, Body=csv_buffer1.getvalue())
			res = result.get('ResponseMetadata')
			if res.get('HTTPStatusCode') == 200:

				print("NEW_IND_Y.txt file uploaded successfully")
			else:
				print("NEW_IND_Y.txt file not uploaded")

			'''step2 calling new_ind_y file '''
			NEW_IND_Y = df_NEW_IND_Y.copy()

		read_NEW_IND_Y(NEW_IND_Y)

		with tempfile.NamedTemporaryFile(suffix='txt', delete=False) as tmp_n_y:

			# NEW_IND_N = 'NEW_IND_N.txt'
			# tmp1.name = NEW_IND_N
			with open(tmp_n_y.name, 'w') as fn:

				fn.write(str(HEADER_DS).strip('[]'))
				fn.write('\n')
				for index, row in DS_41_PLAN.iterrows():

					# print(row['NEW_IND'])
					if row['NEW_IND'] != 'N' and row['NEW_IND'] != 'Y':
						row = row.to_list()
						print('row', row)
						file_data_N_Y_lst.append(row)
						fn.write(str(row).strip('[]'))
			# fn.write('\n')

			csv_buffer3 = StringIO()
			col = ['RCDKPER_CD', 'NEW_IND', 'NAME', 'COMPANY_NAME', 'COMPANY_ADDR_1', 'COMPANY_ADDR_2', 'COMPANY_CITY ',
				   'COMPANY_STATE', 'COMPANY_ZIP ', 'TYPE ', 'CONTRACT_STATE', 'YEAR_END', 'FISCAL_YEAR_END', 'IRS_ID',
				   'IFTP_PLAN_NUM', 'TP_NAME', 'TP_PHONE', 'TP_WEB_URL', 'TP_WEB_NAME', 'TP_CALL_CENTER',
				   'TP_CALL_CENTER_HRS', 'TP_PHONE_IMPAIRED', 'TP_PHONE_NON_US', 'ZIP_CD_SFX']

			pd.options.display.width = 1000
			df_not_eql_n_y = pd.DataFrame(file_data_N_Y_lst, columns=col)

			df_not_eql_n_y.to_csv(csv_buffer3, index=False)
			bucket_name, file_key, s3 = s3_intermediate_files_put_object()
			file_key = file_key + "NEW_IND_NOT_EQL_N_Y.txt"
			# file_str = output.read()
			# body = file_str
			result = s3.put_object(Bucket=bucket_name, Key=file_key, Body=csv_buffer3.getvalue())
			res = result.get('ResponseMetadata')
			if res.get('HTTPStatusCode') == 200:

				print("NEW_IND_N_Y.txt file uploaded successfully")
			else:
				print("NEW_IND_N_Y.txt file not uploaded")
			# df_not_eql_n_y=df_not_eql_n_y.copy()
			''''calling df_not_eql_n_y function'''
			NEW_IND_NOT_EQL_Y_N = df_not_eql_n_y.copy()
		read_NEW_IND_NOT_EQL_Y_N(NEW_IND_NOT_EQL_Y_N)


# STEP2
def read_NEW_IND_N(NEW_IND_N, DS_41_PLAN_DS):
	if NEW_IND_N.empty:

		print('The NEW_IND_N dataframe  is empty')
		return
	else:
		print('The NEW_IND_N dataframe is not empty is not empty')

	# df = pd.read_csv(NEW_IND_N)
	df = NEW_IND_N.copy()
	df.columns = df.columns.str.replace("'", '')
	df.replace("'", '', regex=True, inplace=True)
	# Remove leading and trailing spaces from all column names
	df.columns = df.columns.str.strip(" ")

	def year_end_value_extract(year_end):
		# x = 19011231
		year_end = str(year_end)
		year_end = year_end[4:]
		return year_end

	# Apply a function to 'col1'
	# df['YEAR_END'] = df['YEAR_END'].apply(lambda x: str(x)[4:])
	df['YEAR_END'] = df['YEAR_END'].apply(year_end_value_extract)

	# copying the original columns dataframe for other process cdc_univl_pl_src

	df_cdc_univl_pl_src = df.copy()
	df_cdc_univl_pl_src1 = df.copy()

	# rename the columns to send the data to TOCDC_IF_PL_SRC link
	df.rename(columns=TOCDC_IF_PL_SRC, inplace=True)
	# Data send to TOCDC_IF_PL_SRC
	df_cdc_if_pl_src = df[
		['RCDKPER_CD', 'IFTP_CO_NM', 'PLAN_TY_CD', 'ST_JURIS_CD', 'PLAN_YR_END_DT', 'TAX_IRS_NUM',
		 'IFTP_PLAN_NUM', 'IFTP_RCDKPER_NM', 'IFTP_RCDKPER_800_NUM', 'IFTP_WEB_URL_ADDR', 'IFTP_WEBSITE_NM',
		 'IFTP_CALL_CNTR_NM',
		 'IFTP_CALL_CNTR_HR_TXT', 'IFTP_HRING_IMPRD_TEL_NUM', 'IFTP_OUT_USA_TEL_NUM']]

	# df_cdc_if_pl_src.to_csv(CDC_IF_PL_SRC, index=False)
	CDC_IF_PL_SRC = df_cdc_if_pl_src.copy()
	DS_41_PLAN_DS = DS_41_PLAN_DS.copy()

	'''step4 sub process function calling in main function'''
	# calling function which process the logic step by step passing file CDC_UNIVL_PL_SRC
	try:

		new_ind_n_cdc_if_pl_src(CDC_IF_PL_SRC, DS_41_PLAN_DS)
	except Exception as e:
		print(f"Error occured while processing new_ind_n_cdc_if_pl_src function inside plan seq two:{e}")

		sys.exit(21)
	# Data send to cdc_univl_pl_src
	print(df_cdc_univl_pl_src.columns)
	print(df_cdc_univl_pl_src1['COMPANY_ZIP'])
	df_cdc_univl_pl_src1['COMPANY_ZIP'] = df_cdc_univl_pl_src['COMPANY_ZIP'].apply(lambda x: str(x)[5:])
	print(df_cdc_univl_pl_src1['COMPANY_ZIP'])
	# COMPANY_STATE[1,2]
	# COMPANY_ZIP[1,5]
	# COMPANY_ZIP[6,4]
	df_cdc_univl_pl_src1 = df_cdc_univl_pl_src1[
		['RCDKPER_CD', 'NEW_IND', 'NAME', 'COMPANY_NAME', 'COMPANY_ADDR_1',
		 'COMPANY_ADDR_2', 'COMPANY_CITY', 'COMPANY_STATE', 'ZIP_CD_SFX', 'TYPE',
		 'CONTRACT_STATE', 'YEAR_END', 'FISCAL_YEAR_END', 'IRS_ID',
		 'IFTP_PLAN_NUM', 'TP_NAME', 'TP_PHONE', 'TP_WEB_URL', 'TP_WEB_NAME',
		 'TP_CALL_CENTER', 'TP_CALL_CENTER_HRS', 'TP_PHONE_IMPAIRED', 'TP_PHONE_NON_US']]
	df_cdc_univl_pl_src1.rename(columns=cdc_univl_pl_src1, inplace=True)

	df_cdc_univl_pl_src.rename(columns=cdc_univl_pl_src, inplace=True)

	# df_cdc_univl_pl_src1= df_cdc_univl_pl_src1[]

	# Apply a function to 'company_state',company_zip,company_zip
	df_cdc_univl_pl_src['STE_CD'] = df_cdc_univl_pl_src['STE_CD'].apply(lambda x: str(x)[0:])
	df_cdc_univl_pl_src['ZIP_CD'] = df_cdc_univl_pl_src['ZIP_CD'].apply(lambda x: str(x)[0:5])
	# df_cdc_univl_pl_src['ZIP_CD_SFX'] = df_cdc_univl_pl_src['ZIP_CD_SFX'].apply(lambda x: str(x)[6:4])

	df_cdc_univl_pl_src = df_cdc_univl_pl_src[['RCDKPER_CD', 'NEW_IND', 'UNIVL_PLAN_NM', 'COMPANY_NAME',
											   'FRST_ST_ADDR_LN', 'SEC_ST_ADDR_LN', 'CITY_NM', 'STE_CD',
											   'ZIP_CD',
											   'TYPE', 'CONTRACT_STATE', 'YEAR_END', 'FISCAL_YEAR_END',
											   'IRS_ID',
											   'IFTP_PLAN_NUM', 'TP_NAME', 'TP_PHONE', 'TP_WEB_URL',
											   'TP_WEB_NAME',
											   'TP_CALL_CENTER', 'TP_CALL_CENTER_HRS', 'TP_PHONE_IMPAIRED',
											   'TP_PHONE_NON_US']]

	# Concatenate the 'C' column of df2 to df1
	df_cdc_univl_pl_src1 = pd.concat([df_cdc_univl_pl_src, df_cdc_univl_pl_src1['ZIP_CD_SFX']], axis=1)
	# df_cdc_univl_pl_src1.to_csv('a.csv', index=False)

	df_cdc_univl_pl_src1 = df_cdc_univl_pl_src1[
		['UNIVL_PLAN_NM', 'FRST_ST_ADDR_LN', 'SEC_ST_ADDR_LN', 'CITY_NM', 'STE_CD', 'ZIP_CD', 'RCDKPER_CD',
		 'IFTP_PLAN_NUM', 'ZIP_CD_SFX']]

	CDC_UNIVL_PL_SRC = df_cdc_univl_pl_src1.copy()

	# step 3 sub process function calling in main function
	# calling function which process the logic step by step passing file CDC_UNIVL_PL_SRC
	try:

		new_ind_n_cdc_univl_pl_src(CDC_UNIVL_PL_SRC, DS_41_PLAN_DS)
	except Exception as e:

		print(f"Error occured at plan level two while processing new_ind_n_cdc_univl_pl_src :{e}")
		sys.exit(22)




'''this function inside step3 function'''


def srccheck_universal_pl_tgt():
	print("function call in step 3 function inside")
	universal_pl = pd.read_sql(SELECT_STATEMENT_UNIVL_PL_SRC, Conn)
	#
	# except Exception as e:
	# 	# handle the exception
	# 	error_msg = traceback.format_exc()
	#     logger.error("error occured while excecuting the DB2 ststement for TPODS.STE_CD :{}".format(e))
	#     print(f"Traceback:\n{error_msg}")
	#     sys.exit(8)

	return universal_pl


# step3
def new_ind_n_cdc_univl_pl_src(CDC_UNIVL_PL_SRC, DS_41_PLAN_DS):
	updated_list3 = []
	HEADER_COMM_ROWS = HEADER_UPDATED_ROWS
	HEADER_NEW_ROWS = HEADER_UPDATED_ROWS
	# HEADER_DEL_ROWS=HEADER_UPDATED_ROWS
	df_updated_rows = pd.DataFrame(columns=HEADER_UPDATED_ROWS)  # change_code=3
	df_common_rows = pd.DataFrame(columns=HEADER_COMM_ROWS)  # change_code=0
	df_new_rows = pd.DataFrame(columns=HEADER_NEW_ROWS)  # change_code=1

	df_univl_pl_src = CDC_UNIVL_PL_SRC.copy()

	# Set 'RCDKPER_CD', 'IFTP_PLAN_NUM' as the primary key
	df_univl_pl_src.set_index(['RCDKPER_CD', 'IFTP_PLAN_NUM'], inplace=True)

	# Refrence table data
	universal_pl_tgt = srccheck_universal_pl_tgt()

	if universal_pl_tgt.empty:
		print("universal_pl_tgt is empty, skipping processing")
		return

	universal_pl_tgt['IFTP_PLAN_NUM'] = universal_pl_tgt['IFTP_PLAN_NUM'].apply(lambda x: str(x).strip())
	# universal_pl_tgt.drop('Unnamed: 0', axis=1, inplace=True)
	universal_pl_tgt.set_index(['RCDKPER_CD', 'IFTP_PLAN_NUM'], inplace=True)

	for row in df_univl_pl_src.index:
		updated_list1 = []
		updated_list2 = []

		if (row in universal_pl_tgt.index) and (row in df_univl_pl_src.index):

			print("row exist in both place", row)
			# print(universal_pl_tgt.loc[row].values)
			data = universal_pl_tgt.loc[row].values
			# print(df_univl_pl_src.loc[row].values)
			data1 = df_univl_pl_src.loc[row].values

			# strip of the space
			for v, v2 in zip(data, data1):
				updated_list1.append(str(v).strip())
				updated_list2.append(str(v2).strip())
			# This index position is to add recorkpr_cd,iftp-num column in 6,7 position
			index_pos = [6, 7]
			for index, t1 in zip(index_pos, row):
				updated_list1.insert(index, t1)
				updated_list2.insert(index, t1)

			# this will print when all values similar in both list
			if all(x in updated_list1 for x in updated_list2):
				print(row, "similar records", updated_list1, updated_list2)
				updated_list1.append('0')

				# New data as pandas.DataFrame
				new = pd.DataFrame(columns=df_common_rows.columns, data=[updated_list1])
				# Overwrite original dataframe
				df_common_rows = pd.concat([df_common_rows, new], axis=0, ignore_index=True)

			# this will print when any of the values found similar in both list
			elif any(x in updated_list1 for x in updated_list2):
				print(row, "updated record", updated_list1, updated_list2)

				# Here change code value is 3 for updated rows
				updated_list1.append('3')
				# New data as pandas.DataFrame
				new = pd.DataFrame(columns=df_updated_rows.columns, data=[updated_list1])
				# Overwrite original dataframe
				df_updated_rows = pd.concat([df_updated_rows, new], axis=0, ignore_index=True)

		else:
			print(row, "new rows exist in transformer data but not available in refrence table")
			updated_list3.append(row)

	change_code_new_row = ['1']
	for row in updated_list3:
		val = list(row)

		print("new records coming from source", df_univl_pl_src.loc[row].to_list())
		row = df_univl_pl_src.loc[row].to_list()
		row = row[:6] + val + row[6:] + change_code_new_row

		# New data as pandas.DataFrame
		new = pd.DataFrame(columns=df_new_rows.columns, data=[row])
		# Overwrite original dataframe
		df_new_rows = pd.concat([df_new_rows, new], axis=0, ignore_index=True)

	final_result = pd.concat([df_updated_rows, df_common_rows, df_new_rows])

	# filter the DataFrame based on a condition with change code is = 3
	To_Tgt_UNIVL_PL = final_result.loc[(final_result['CHANGE_CODE'] == '3')]

	from datetime import datetime
	now = datetime.now()
	LAST_ACTY_DTM = str(now.strftime("%Y-%m-%d %H:%M:%S ")).strip()
	for index, row in To_Tgt_UNIVL_PL.iterrows():
		# print(row[6])
		# print(row[7])
		# print(list(row))
		# print(list(row)[:6])
		RCDKPER_CD = row[6]
		IFTP_PLAN_NUM = row[7]
		list_of_values = list(row)
		list_to_update = list_of_values[:6] + list_of_values[8:9]
		list_to_update1 = [LAST_ACTY_DTM, RCDKPER_CD, IFTP_PLAN_NUM]

		final_list_to_update = list_to_update + list_to_update1
		Conn = db2_conn_test()
		try:
			# execute the statement
			cur = Conn.cursor()
			SQL_UPDATE = tgt_unvil_pl_update
			cur.execute(SQL_UPDATE, final_list_to_update)
		except Exception as e:
			# handle the exception
			error_msg = traceback.format_exc()
			print(f"An error occurred while loading data into TPODS.UNIVL_PLAN: {e}")
			print(f"Traceback:\n{error_msg}")
			sys.exit(11)

	# filter the DataFrame based on a condition with change code is = 3,1,0
	To_LKPLAN_NM = final_result.loc[(final_result['CHANGE_CODE'] == '3') | (final_result['CHANGE_CODE'] == '0') | (
			final_result['CHANGE_CODE'] == '1')]
	To_LKPLAN_NM = To_LKPLAN_NM[['UNIVL_PLAN_NM', 'RCDKPER_CD', 'IFTP_PLAN_NUM', 'CHANGE_CODE']]
	lkplan_nm_dataset_buffer = StringIO()
	To_LKPLAN_NM.to_csv(lkplan_nm_dataset_buffer, index=False)

	# with open(LKPLAN_NM_DATASET, 'r') as output:
	bucket_name, file_key, s3 = s3_Dataset_put_object()
	file_key = file_key + "LKPLAN_NM_DATASET_UNIVL_PL.txt"

	result = s3.put_object(Bucket=bucket_name, Key=file_key, Body=lkplan_nm_dataset_buffer.getvalue())
	res = result.get('ResponseMetadata')
	if res.get('HTTPStatusCode') == 200:

		print("file LKPLAN_NM_DATASET_UNIVL_PL uploaded successfully")
	else:
		print("LKPLAN_NM_DATASET_UNIVL_PL file not uploaded")

	# reading DS_41_PLAN_DS file  for plan number
	DS_41_PLAN_Dataset = DS_41_PLAN_DS.copy()  # pd.read_csv(DS_41_PLAN_DS)

	# reading data from LKPLAN_NUM step to check with recordkp_cd and iftp_number with dataset DS_41_PLAN_DS
	LKPLAN_NM_DATA = To_LKPLAN_NM.copy()  # pd.read_csv(LKPLAN_NM_DATASET)

	new_dataset_Col1 = LKPLAN_NM_DATA.columns
	New_DS_41_PLAN_Dataset = pd.DataFrame(columns=new_dataset_Col)
	new_ds_lkplan_nm_dataset = pd.DataFrame(columns=new_dataset_Col1)
	# new_ds_lkplan_nm_dataset.drop('Unnamed: 0', axis=1, inplace=True)

	print(new_ds_lkplan_nm_dataset.columns)

	LKPLAN_NM_DATA.set_index(['RCDKPER_CD', 'IFTP_PLAN_NUM'], inplace=True)

	DS_41_PLAN_Dataset.set_index(['RCDKPER_CD', 'IFTP_PLAN_NUM'], inplace=True)

	for row in LKPLAN_NM_DATA.index:

		if (row in LKPLAN_NM_DATA.index) and (row in DS_41_PLAN_Dataset.index):
			row_index_lst = list(row)
			row = DS_41_PLAN_Dataset.loc[row].values
			row = list(row) + row_index_lst

			# New data as pandas.DataFrame
			new = pd.DataFrame(columns=new_dataset_Col, data=[row])
			# Overwrite original dataframe
			New_DS_41_PLAN_Dataset = pd.concat([New_DS_41_PLAN_Dataset, new], axis=0, ignore_index=True)

	NEW_DS_41_PLAN_Dataset = New_DS_41_PLAN_Dataset[['NAME', 'TP_NAME', 'RCDKPER_CD', 'IFTP_PLAN_NUM']]
	NEW_DS_41_PLAN_Dataset.set_index(['RCDKPER_CD', 'IFTP_PLAN_NUM'], inplace=True)
	# LKPLAN_NM_DATA.drop('Unnamed: 0', axis=1, inplace=True)
	LKPLAN_NM_DF = LKPLAN_NM_DATA[['CHANGE_CODE']]
	DS_41_PLAN_LKPLAN_NM_DATA = pd.merge(NEW_DS_41_PLAN_Dataset, LKPLAN_NM_DF, left_index=True, right_index=True)
	DS_41_PLAN_LKPLAN_NM_DATA.reset_index(drop=False, inplace=True)

	DS_UNIVL_RPT_LOG41_3_ds = DS_41_PLAN_LKPLAN_NM_DATA.loc[(DS_41_PLAN_LKPLAN_NM_DATA['CHANGE_CODE'] == 3)]
	DS_UNIVL_RPT_LOG41_3_ds.rename(columns={'IFTP_PLAN_NUM': 'NUMBER'})
	DS_UNIVL_RPT_LOG41_3_ds.insert(3, 'DESCRIPTION', 2)

	'''DS_UNIVL_RPT_LOG41_3_ds dataset generating'''
	DS_UNIVL_RPT_LOG41_3_BUFFER = StringIO()
	DS_UNIVL_RPT_LOG41_3_ds.to_csv(DS_UNIVL_RPT_LOG41_3_BUFFER, index=False)
	bucket_name, file_key, s3 = s3_Dataset_put_object()
	file_key = file_key + "DS_UNIVL_RPT_LOG41_3.txt"

	result = s3.put_object(Bucket=bucket_name, Key=file_key, Body=DS_UNIVL_RPT_LOG41_3_BUFFER.getvalue())
	res = result.get('ResponseMetadata')
	if res.get('HTTPStatusCode') == 200:

		print("file LKPLAN_NM_DATASET uploaded successfully")
	else:
		print("file not uploaded")

	# DS_UNIVL_RPT_LOG#jRcdkper_cd#_4.ds
	DS_UNIVL_RPT_LOG41_4_ds = DS_41_PLAN_LKPLAN_NM_DATA.loc[
		(DS_41_PLAN_LKPLAN_NM_DATA['CHANGE_CODE'] == 3) | (DS_41_PLAN_LKPLAN_NM_DATA['CHANGE_CODE'] == 0) | (
				DS_41_PLAN_LKPLAN_NM_DATA['CHANGE_CODE'] == 1)]
	DS_UNIVL_RPT_LOG41_4_ds.rename(columns={'IFTP_PLAN_NUM': 'NUMBER'})
	DS_UNIVL_RPT_LOG41_4_ds.insert(3, 'DESCRIPTION', 5)

	DS_UNIVL_RPT_LOG41_4_BUFFER = StringIO()
	DS_UNIVL_RPT_LOG41_4_ds.to_csv(DS_UNIVL_RPT_LOG41_4_BUFFER, index=False)

	'''DS_UNIVL_RPT_LOG41_4_ds dataset generating'''
	# with open(DS_UNIVL_RPT_LOG41_3, 'r') as output:
	bucket_name, file_key, s3 = s3_Dataset_put_object()
	file_key = file_key + "DS_UNIVL_RPT_LOG41_4.txt"

	result = s3.put_object(Bucket=bucket_name, Key=file_key, Body=DS_UNIVL_RPT_LOG41_4_BUFFER.getvalue())
	res = result.get('ResponseMetadata')
	if res.get('HTTPStatusCode') == 200:

		print("DS_UNIVL_RPT_LOG41_4 file uploaded successfully")
	else:
		print("DS_UNIVL_RPT_LOG41_4file not uploaded")


def srccheck_IF_PL_SRC_tgt():
	print("function call inside step 4 function inside")

	try:
		if_pl_src = pd.read_sql(
			f"SELECT RCDKPER_CD, IFTP_CO_NM, PLAN_TY_CD, ST_JURIS_CD, PLAN_YR_END_DT, TAX_IRS_NUM, IFTP_PLAN_NUM, IFTP_RCDKPER_NM, IFTP_RCDKPER_800_NUM, IFTP_WEB_URL_ADDR, IFTP_WEBSITE_NM, IFTP_CALL_CNTR_NM, IFTP_CALL_CNTR_HR_TXT, IFTP_HRING_IMPRD_TEL_NUM, IFTP_OUT_USA_TEL_NUM FROM TPODS.IF_PLAN where RCDKPER_CD='41' ORDER BY RCDKPER_CD, IFTP_PLAN_NUM",
			Conn)
	except Exception as e:
		# error_msg = traceback.format_exc()
		print("error occured while excecuting the DB2 ststement for TPODS.STE_CD :{}".format(e))
		# print(f"Traceback:\n{error_msg}")
		sys.exit(9)

	return if_pl_src


# #step 4
# # step to process the steps for  IF_PL_SRC
def new_ind_n_cdc_if_pl_src(CDC_IF_PL_SRC, DS_41_PLAN_DS):
	updated_list3 = []
	HEADER_COMM_ROWS = HEADER_UPDATED_ROWS_IF_PL_SRC
	HEADER_NEW_ROWS = HEADER_UPDATED_ROWS_IF_PL_SRC
	HEADER_UPDATED_ROWS = HEADER_UPDATED_ROWS_IF_PL_SRC
	# HEADER_DEL_ROWS=HEADER_UPDATED_ROWS
	df_updated_rows = pd.DataFrame(columns=HEADER_UPDATED_ROWS)  # change_code=3
	df_common_rows = pd.DataFrame(columns=HEADER_COMM_ROWS)  # change_code=0
	df_new_rows = pd.DataFrame(columns=HEADER_NEW_ROWS)  # change_code=1

	# Transformer_data
	df_if_pl_src = CDC_IF_PL_SRC.copy()  # pd.read_csv(CDC_IF_PL_SRC, index_col=False, dtype=col_datatype)
	# print(df.columns)
	# print(df['RCDKPER_CD'])
	df_if_pl_src.replace(np.nan, ' ', inplace=True)
	# Set 'RCDKPER_CD', 'IFTP_PLAN_NUM' as the primary key
	df_if_pl_src.set_index(['RCDKPER_CD', 'IFTP_PLAN_NUM'], inplace=True)
	print(df_if_pl_src.index)

	# Refrence table data
	if_pl_src_tgt = srccheck_IF_PL_SRC_tgt()

	if if_pl_src_tgt.empty:
		print("if_pl_src_tgt is empty, skipping processing")
		return

	if_pl_src_tgt.replace(np.nan, ' ', inplace=True)
	if_pl_src_tgt['IFTP_PLAN_NUM'] = if_pl_src_tgt['IFTP_PLAN_NUM'].apply(lambda x: str(x).strip())
	if_pl_src_tgt.set_index(['RCDKPER_CD', 'IFTP_PLAN_NUM'], inplace=True)

	for row in df_if_pl_src.index:
		updated_list1 = []
		updated_list2 = []

		if (row in df_if_pl_src.index) and (row in if_pl_src_tgt.index):

			print("row exist in both place", row)
			# print(if_pl_src_tgt.loc[row].values)

			data = if_pl_src_tgt.loc[row].values

			# print(df_if_pl_src.loc[row].values)
			data1 = df_if_pl_src.loc[row].values

			# strip of the space

			for v, v2 in zip(data, data1):
				updated_list1.append(str(v).strip())
				updated_list2.append(str(v2).strip())

			# This index position is to add recorkpr_cd,iftp-num column in 6,7 position

			index_pos = [0, 6]
			for index, t1 in zip(index_pos, row):
				# print(t1)
				updated_list1.insert(index, t1)
				updated_list2.insert(index, t1)
			print("updated_list1", updated_list1)
			print("updated_list2", updated_list2)

			# this will print when all values similar in both list
			if all(x in updated_list1 for x in updated_list2):
				print(row, "similar records", updated_list1, updated_list2)
				updated_list1.append('0')

				# New data as pandas.DataFrame
				new = pd.DataFrame(columns=df_common_rows.columns, data=[updated_list1])
				# Overwrite original dataframe
				df_common_rows = pd.concat([df_common_rows, new], axis=0, ignore_index=True)

			# this will print when any of the values found similar in both list
			elif any(x in updated_list1 for x in updated_list2):
				print(row, "updated record", updated_list1, updated_list2)

				# Here change code value is 3 for updated rows
				updated_list1.append('3')
				# New data as pandas.DataFrame
				new = pd.DataFrame(columns=df_updated_rows.columns, data=[updated_list1])
				# Overwrite original dataframe
				df_updated_rows = pd.concat([df_updated_rows, new], axis=0, ignore_index=True)

		else:
			print(row, "new rows exist in transformer data not available in refrence table")
			updated_list3.append(row)
	change_code_new_row = ['1']
	for row in updated_list3:
		val = list(row)
		print("val", val)
		IFTP_NUM = val[1]
		R_CD = val[0]

		print("new records coming from source", df_if_pl_src.loc[row].values)

		row = list(df_if_pl_src.loc[row].values)
		row = [R_CD] + row[0:5] + [IFTP_NUM] + row[5:] + change_code_new_row

		# New data as pandas.DataFrame
		new = pd.DataFrame(columns=df_new_rows.columns, data=[row])
		# Overwrite original dataframe
		df_new_rows = pd.concat([df_new_rows, new], axis=0, ignore_index=True)

	final_result = pd.concat([df_updated_rows, df_common_rows, df_new_rows])

	# filter the DataFrame based on a condition with change code is = 3
	To_Tgt_IF_PL = final_result.loc[(final_result['CHANGE_CODE'] == '3')]

	from datetime import datetime
	now = datetime.now()
	LAST_ACTY_DTM = str(now.strftime("%Y-%m-%d %H:%M:%S ")).strip()
	for index, row in To_Tgt_IF_PL.iterrows():

		RCDKPER_CD = row[0]
		print(RCDKPER_CD)
		IFTP_PLAN_NUM = row[6]
		print(IFTP_PLAN_NUM)
		# print(type(row))

		list_of_values = list(row)
		print(list_of_values)
		list_to_update = list_of_values[1:6] + list_of_values[7:15]
		print(list_to_update)

		list_to_update1 = [LAST_ACTY_DTM, RCDKPER_CD, IFTP_PLAN_NUM]
		print(list_to_update + list_to_update1)
		final_list_to_update = list_to_update + list_to_update1
		Conn = db2_conn_test()
		try:
			# execute the statement
			cur = Conn.cursor()
			SQL_UPDATE = tgt_if_pl_src_update
			cur.execute(SQL_UPDATE, final_list_to_update)
		except Exception as e:
			# handle the exception
			error_msg = traceback.format_exc()
			print(f"Traceback:\n{error_msg}")
			print(f"An error occurred while loading data into TPODS.UNIVL_PLAN: {e}")
			sys.exit(13)

	# filter the DataFrame based on a condition with change code is = 3,1,0
	To_LKPLAN_NM = final_result.loc[(final_result['CHANGE_CODE'] == '3') | (final_result['CHANGE_CODE'] == '0') | (
			final_result['CHANGE_CODE'] == '1')]

	To_LKPLAN_NM = To_LKPLAN_NM[['RCDKPER_CD', 'IFTP_RCDKPER_NM', 'IFTP_PLAN_NUM', 'CHANGE_CODE']]
	print("To_LKPLAN_NM dataframe", To_LKPLAN_NM)

	# with tempfile.NamedTemporaryFile(suffix='.ds', delete=False) as tmp1:
	#
	#     LKPLAN_NM_DATASET_IF_PL = 'LKPLAN_NM_DATASET_IF_PL'
	#     tmp1.name =LKPLAN_NM_DATASET_IF_PL
	#     LKPLAN_NM_DATASET_IF_PL=tmp1.name
	LKPLAN_NM_DATASET_IF_PL_BUFFER = StringIO()

	To_LKPLAN_NM.to_csv(LKPLAN_NM_DATASET_IF_PL_BUFFER, index=False)

	# with open(LKPLAN_NM_DATASET_IF_PL, 'r') as output:
	bucket_name, file_key, s3 = s3_Dataset_put_object()
	file_key = file_key + "LKPLAN_NM_DATASET_IF_PL.txt"

	result = s3.put_object(Bucket=bucket_name, Key=file_key, Body=LKPLAN_NM_DATASET_IF_PL_BUFFER.getvalue())
	res = result.get('ResponseMetadata')
	if res.get('HTTPStatusCode') == 200:

		print("file uploaded successfully")
	else:
		print("file not uploaded")

	# reading DS_41_PLAN_DS file  for plan number
	DS_41_PLAN_Dataset = DS_41_PLAN_DS.copy()  # pd.read_csv(DS_41_PLAN_DS)

	# reading data from LKPLAN_NUM step to check with recordkp_cd and iftp_number with dataset DS_41_PLAN_DS
	lkplan_nm_data = To_LKPLAN_NM.copy()  # pd.read_csv(LKPLAN_NM_DATASET_IF_PL)

	new_dataset_Col1 = lkplan_nm_data.columns
	New_DS_41_PLAN_Dataset = pd.DataFrame(columns=new_dataset_Col)
	new_ds_lkplan_nm_dataset = pd.DataFrame(columns=new_dataset_Col1)
	# new_ds_lkplan_nm_dataset.drop('Unnamed: 0', axis=1, inplace=True)

	print("lkplan_nm_data columns", new_ds_lkplan_nm_dataset.columns)

	lkplan_nm_data.set_index(['RCDKPER_CD', 'IFTP_PLAN_NUM'], inplace=True)

	DS_41_PLAN_Dataset.set_index(['RCDKPER_CD', 'IFTP_PLAN_NUM'], inplace=True)
	data_list1 = []
	for row in lkplan_nm_data.index:

		if (row in lkplan_nm_data.index) and (row in DS_41_PLAN_Dataset.index):
			row_index_lst = list(row)
			row = DS_41_PLAN_Dataset.loc[row].values
			row = list(row) + row_index_lst
			print(row)
			data_list1.append(row)
			# New data as pandas.DataFrame
			new = pd.DataFrame(columns=new_dataset_Col, data=[row])
			# Overwrite original dataframe
			New_DS_41_PLAN_Dataset = pd.concat([New_DS_41_PLAN_Dataset, new], axis=0, ignore_index=True)
			print(New_DS_41_PLAN_Dataset)
	New_DS_41_PLAN_Dataset = New_DS_41_PLAN_Dataset[['NAME', 'TP_NAME', 'RCDKPER_CD', 'IFTP_PLAN_NUM']]
	New_DS_41_PLAN_Dataset.set_index(['RCDKPER_CD', 'IFTP_PLAN_NUM'], inplace=True)
	# lkplan_nm_data.drop('Unnamed: 0', axis=1, inplace=True)
	lkplan_nm_df = lkplan_nm_data[['CHANGE_CODE']]
	DS_41_PLAN_lkplan_nm_data = pd.merge(New_DS_41_PLAN_Dataset, lkplan_nm_df, left_index=True, right_index=True)
	DS_41_PLAN_lkplan_nm_data.reset_index(drop=False, inplace=True)
	# print("lkplan_nm_data", DS_41_PLAN_lkplan_nm_data)

	# DS_PLAN_RPT_LOG  # jRcdkper_cd#_3.ds

	ds_if_pl_rpt_LOG41_3_ds = DS_41_PLAN_lkplan_nm_data.loc[(DS_41_PLAN_lkplan_nm_data['CHANGE_CODE'] == 3)]
	ds_if_pl_rpt_LOG41_3_ds.rename(columns={'IFTP_PLAN_NUM': 'NUMBER'})
	ds_if_pl_rpt_LOG41_3_ds.insert(3, 'DESCRIPTION', 2)
	# print("ds_if_pl_rpt_LOG41_3_ds", ds_if_pl_rpt_LOG41_3_ds)

	ds_if_pl_rpt_LOG41_3_buffer = StringIO()
	ds_if_pl_rpt_LOG41_3_ds.to_csv(ds_if_pl_rpt_LOG41_3_buffer, index=False)

	bucket_name, file_key, s3 = s3_Dataset_put_object()
	file_key = file_key + "DS_PLAN_RPT_LOG41_3.txt"

	result = s3.put_object(Bucket=bucket_name, Key=file_key, Body=ds_if_pl_rpt_LOG41_3_buffer.getvalue())
	res = result.get('ResponseMetadata')
	if res.get('HTTPStatusCode') == 200:

		print("DS_IF_PL_RPT_LOG41_3 file uploaded successfully")
	else:
		print("DS_IF_PL_RPT_LOG41_3 file not uploaded")

	# DS_PLAN_RPT_LOG
	ds_if_pl_rpt_LOG41_4_ds = DS_41_PLAN_lkplan_nm_data.loc[
		(DS_41_PLAN_lkplan_nm_data['CHANGE_CODE'] == 3) | (DS_41_PLAN_lkplan_nm_data['CHANGE_CODE'] == 0) | (
				DS_41_PLAN_lkplan_nm_data['CHANGE_CODE'] == 1)]
	ds_if_pl_rpt_LOG41_4_ds.rename(columns={'IFTP_PLAN_NUM': 'NUMBER'})
	ds_if_pl_rpt_LOG41_4_ds.insert(3, 'DESCRIPTION', 5)
	print("ds_if_pl_rpt_LOG41_4_ds", ds_if_pl_rpt_LOG41_4_ds)
	ds_if_pl_rpt_LOG41_4_buffer = StringIO()

	ds_if_pl_rpt_LOG41_4_ds.to_csv(ds_if_pl_rpt_LOG41_4_buffer, index=False)

	# with open(DS_IF_PL_RPT_LOG41_4, 'r') as output:
	bucket_name, file_key, s3 = s3_Dataset_put_object()
	file_key = file_key + "DS_PLAN_RPT_LOG41_4.txt"

	result = s3.put_object(Bucket=bucket_name, Key=file_key, Body=ds_if_pl_rpt_LOG41_4_buffer.getvalue())
	res = result.get('ResponseMetadata')
	if res.get('HTTPStatusCode') == 200:

		print("ds_if_pl_rpt_LOG41_4 file uploaded successfully")
	else:
		print("ds_if_pl_rpt_LOG41_4 file not uploaded")


'''NEW_IND_Y File processing'''


def read_NEW_IND_Y(NEW_IND_Y):
	# with open(NEW_IND_Y,'r') as f:
	if NEW_IND_Y.empty:
		print('The  NEW_IND_Y dataframe is empty')
	else:
		print('The NEW_IND_Y dataframe is not empty')
	# key col list
	row_keys = []

	# Transformer_data

	df_Y_src = NEW_IND_Y.copy()  # pd.read_csv(NEW_IND_Y)
	df_Y_src.columns = df_Y_src.columns.str.replace("'", '')
	# df_Y_src.replace("'", '', regex=True, inplace=True)
	# Remove leading and trailing spaces from all column names
	df_Y_src.columns = df_Y_src.columns.str.strip(" ")
	print(df_Y_src.columns)

	# Set 'RCDKPER_CD', 'IFTP_PLAN_NUM' as the primary key
	df_Y_src.set_index(['RCDKPER_CD', 'IFTP_PLAN_NUM'], inplace=True)

	# Refrence table data getting from database
	UNIVL_PL_IND_Y = srccheck_UNIVL_PL_IND_Y_tgt()
	univl_pl_tgt = UNIVL_PL_IND_Y.copy()
	
	# Check if univl_pl_tgt is empty
	if univl_pl_tgt.empty:
		print("univl_pl_tgt is empty, skipping processing")
		return
	
	univl_pl_tgt['IFTP_PLAN_NUM'] = univl_pl_tgt['IFTP_PLAN_NUM'].apply(lambda x: str(x).strip())
	univl_pl_tgt.set_index(['RCDKPER_CD', 'IFTP_PLAN_NUM'], inplace=True)

	data_list_matched_rows = []

	for row in df_Y_src.index:

		if (row in univl_pl_tgt.index):  # (row in df_Y_src.index) and
			print("row  exist in both univl_pl_tgt place", row)

			# UNIVL_PLAN_ID, RCDKPER_CD IFTP_PLAN_NUM need to check on this logic
			data = univl_pl_tgt.loc[row].values

			# print(df_if_pl_src.loc[row].values)
			data1 = df_Y_src.loc[row].values

			data1 = [item for item in data1]

			# Below step to call  Stored_proc_call
			# To get  Parameter from the query
			# RCDKPER_CD,IFTP_PLAN_NUM
			for rekpr_cd, iftp_pl_nm in [row]:
				RCDKPER_CD = rekpr_cd
				IFTP_PLAN_NUM = iftp_pl_nm
			row_keys.append(RCDKPER_CD)
			row_keys.append(IFTP_PLAN_NUM)
			RCDKPER_CD = row_keys[0]
			IFTP_PLAN_NUM = row_keys[1]

			# calling stored proc
			# stored_proc_call_ind_y(RCDKPER_CD, IFTP_PLAN_NUM)

			# test_data=[ 'N', ' TOWN OF VAIL REC DISTRICT', ' TOWN OF VAIL', ' 75 SOUTH FRONTAGE ROAD', '  ', ' VAIL', ' CO', 816570000, 0, ' NH', 19011231, 1231, 999999999, 108670, ' ICMA RETIREMENT CORPORATION', 8006697400, ' WWW.ICMARC.ORG', ' ACCOUNT ACCESS', ' INVESTOR SERVICES', ' 8:30AM TO 9PM EASTERN TIME DAYS THE NY STOCK EXCHANGE IS OPEN  CLOSING AT 6PM DAYS NYSE CLOSES EARLY', 8006697471, 2029626999, ' ']
			# Appending matchec index rows to list

			data1 = [RCDKPER_CD] + data1[1:13] + [IFTP_PLAN_NUM] + data1[13:-1]
			data_list_matched_rows.append(data1)

			# clear the list before move to next next loop
			row_keys.clear()

	print(f"datalist matched rows:{data_list_matched_rows}")
	df_dummy = pd.DataFrame(columns=newplaninsert_ds_Col)

	if len(data_list_matched_rows) != 0:
		# PLANINSERT_Dataset generation from unique rows generted
		for row in data_list_matched_rows:
			Existedplaninsert_df = pd.concat(
				[df_dummy, pd.DataFrame([row], columns=df_dummy.columns)])  # ,axis=0,ignore_index=True)
	elif len(data_list_matched_rows) == 0:
		Existedplaninsert_df = pd.concat(
			[df_dummy, pd.DataFrame(data_list_matched_rows, columns=df_dummy.columns)])  # ,axis=0,ignore_index=True)

	# creating dataset from newplaninsert dataframe

	if Existedplaninsert_df.empty:
		print('The dataframe is empty no matched rows between src and target tables')
	else:
		print('The dataframe is not empty there are matched rows between src and trget tables')

	# DS_PLAN_RPT_LOG41_1 dataset generation

	Existed_univl_pl_ind_y_Dataset = Existedplaninsert_df[['RCDKPER_CD', 'TP_NAME', 'IFTP_PLAN_NUM', 'NAME']]
	Existed_univl_pl_ind_y_Dataset.rename(columns={'IFTP_PLAN_NUM': 'NUMBER'}, inplace=True)
	Existed_univl_pl_ind_y_Dataset['DESCRIPTION'] = '3'
	print(Existed_univl_pl_ind_y_Dataset)

	'''generated DS_PLAN_RPT_LOG41_1 dataset here'''
	# Existed_univl_pl_ind_y_Dataset.to_csv(DS_PLAN_RPT_LOG41_1,index=False)
	DS_PLAN_RPT_LOG41_1_BUFFER = StringIO()
	Existed_univl_pl_ind_y_Dataset.to_csv(DS_PLAN_RPT_LOG41_1_BUFFER, index=False)

	bucket_name, file_key, s3 = s3_Dataset_put_object()
	file_key = file_key + "DS_PLAN_RPT_LOG41_1.txt"

	result = s3.put_object(Bucket=bucket_name, Key=file_key, Body=DS_PLAN_RPT_LOG41_1_BUFFER.getvalue())
	res = result.get('ResponseMetadata')
	if res.get('HTTPStatusCode') == 200:

		print("DS_PLAN_RPT_LOG41_1 file uploaded successfully")
	else:
		print("DS_PLAN_RPT_LOG41_1 file not uploaded")

	data_list_unmatched_rows = []
	for row in df_Y_src.index:

		if (row not in univl_pl_tgt.index):  # (row in df_Y_src.index) and
			print("row not exist in univl_pl_tgt place", row)

			# UNIVL_PLAN_ID, RCDKPER_CD IFTP_PLAN_NUM need to check on this logic
			# data = univl_pl_tgt.loc[row].values

			# print(df_if_pl_src.loc[row].values)
			data1 = df_Y_src.loc[row].values
			print(data1)
			data1 = [item for item in data1]

			# Below step to call  Stored_proc_call
			# To get  Parameter from the query
			# RCDKPER_CD,IFTP_PLAN_NUM
			for rekpr_cd, iftp_pl_nm in [row]:
				RCDKPER_CD = rekpr_cd
				IFTP_PLAN_NUM = iftp_pl_nm
			row_keys.append(RCDKPER_CD)
			row_keys.append(IFTP_PLAN_NUM)
			RCDKPER_CD = row_keys[0]
			IFTP_PLAN_NUM = row_keys[1]
			data1 = [RCDKPER_CD] + data1[1:13] + [IFTP_PLAN_NUM] + data1[13:-1]
			data_list_unmatched_rows.append(data1)

			# clear the list before move to next next loop
			row_keys.clear()
	print("matched rows list", data_list_unmatched_rows)

	print("unmatched rows list", len(data_list_unmatched_rows))

	# sub_fuc_new_ind_y(data_list_unmatched_rows)
	df_dummy = pd.DataFrame(columns=newplaninsert_ds_Col)
	
	# Initialize newplaninsert_df with empty DataFrame
	newplaninsert_df = pd.DataFrame(columns=newplaninsert_ds_Col)

	if len(data_list_unmatched_rows) != 0:
		# PLANINSERT_Dataset generation from unique rows generted

		for row in data_list_unmatched_rows:
			newplaninsert_df = pd.concat([df_dummy, pd.DataFrame([row], columns=df_dummy.columns)], axis=0,
										 ignore_index=True)
	elif len(data_list_unmatched_rows) == 0:

		newplaninsert_df = pd.concat([df_dummy, pd.DataFrame(data_list_unmatched_rows, columns=df_dummy.columns)], axis=0,
									 ignore_index=True)

	if newplaninsert_df.empty:
		print('The dataframe is empty no unmatched rows between src and target tables')
	else:
		print('The dataframe is not empty there are unmatched rows between src and trget tables')
	print()
	NewPlaninsert_41_ds = newplaninsert_df.copy()
	print(NewPlaninsert_41_ds)
	NewPlaninsert_41_data_buffer = StringIO()
	NewPlaninsert_41_ds.to_csv(NewPlaninsert_41_data_buffer, index=False)
	# NewPlaninsert_41_ds.txt
	bucket_name, file_key, s3 = s3_Dataset_put_object()
	file_key = file_key + "NewPlaninsert_41.txt"

	result = s3.put_object(Bucket=bucket_name, Key=file_key, Body=NewPlaninsert_41_data_buffer.getvalue())
	res = result.get('ResponseMetadata')
	if res.get('HTTPStatusCode') == 200:

		print("NewPlaninsert_41_data file uploaded successfully")
	else:
		print("NewPlaninsert_41_data file not uploaded")

	New_univl_pl_ind_y_Dataset = NewPlaninsert_41_ds[['RCDKPER_CD', 'TP_NAME', 'IFTP_PLAN_NUM', 'NAME']]
	New_univl_pl_ind_y_Dataset.rename(columns={'IFTP_PLAN_NUM': 'NUMBER'}, inplace=True)
	New_univl_pl_ind_y_Dataset['DESCRIPTION'] = '1'
	print(New_univl_pl_ind_y_Dataset)
	DS_PLAN_RPT_LOG41_2_BUFFER = StringIO()
	New_univl_pl_ind_y_Dataset.to_csv(DS_PLAN_RPT_LOG41_2_BUFFER, index=False)

	bucket_name, file_key, s3 = s3_Dataset_put_object()
	file_key = file_key + "DS_PLAN_RPT_LOG41_2.txt"

	result = s3.put_object(Bucket=bucket_name, Key=file_key, Body=DS_PLAN_RPT_LOG41_2_BUFFER.getvalue())
	res = result.get('ResponseMetadata')
	if res.get('HTTPStatusCode') == 200:

		print("DS_PLAN_RPT_LOG41_2 file uploaded successfully")
	else:
		print("DS_PLAN_RPT_LOG41_2 file not uploaded")


# This below function step stored_proc_call_ind_y,sub_fuc_new_ind_y,read_NEW_IND_Y are realted to Y IND PART Logic
def stored_proc_call_ind_y(RCDKPER_CD, IFTP_PLAN_NUM):
	Conn = None
	cur = None
	try:
		# Calling db2 connection variable from partcipant_conn_to_DB2.py
		Conn = db2_conn_test()

		# Create a cursor object
		cur = Conn.cursor()

		# Call the stored procedure
		cur.execute("{call STGTPODS.P_INS_NEW_PLAN (RCDKPER_CD,  IFTP_PLAN_NUM)}")
	# cur.execute("{CALL TPODS.SP_TESTPARTHA()}")

	# Fetch the results this is only need while doing unit test
	# results = cur.fetchall()
	# print(results)
	except Exception as e:

		error_msg = traceback.format_exc()
		print("Exception while running plan stored proc error as :{}".format(e))
		print(f"Traceback:\n{error_msg}")
		sys.exit(4)

	finally:
		# Close the cursor and connection
		if cur is not None:
			cur.close()
		if Conn is not None:
			Conn.close()


def srccheck_UNIVL_PL_IND_Y_tgt():
	try:
		unvl_pl_y_ind = pd.read_sql(
			f"SELECT DISTINCT UNIVL_PLAN_ID,  RCDKPER_CD, IFTP_PLAN_NUM FROM TPODS.UNIVL_PLAN where RCDKPER_CD='41'",
			Conn)
		print(unvl_pl_y_ind)
	except Exception as e:

		error_msg = traceback.format_exc()
		print("error occured while excecuting the DB2 ststement for TPODS.STE_CD :{}".format(e))
		print(f"Traceback:\n{error_msg}")
		sys.exit(4)
	return unvl_pl_y_ind


def read_NEW_IND_NOT_EQL_Y_N(NEW_IND_NOT_EQL_Y_N):
	# with open(NEW_IND_NOT_EQL_Y_N,'r') as f:
	if NEW_IND_NOT_EQL_Y_N.empty:
		print('The NEW_IND_NOT_EQL_Y_N dataframe is empty')
		# Create empty dataset and upload
		df_noteql_N_Y_BUFFER = StringIO()
		# Write empty CSV with headers
		df_noteql_N_Y_BUFFER.write("RCDKPER_CD,TP_NAME,NUMBER,NAME,DESCRIPTION\n")
		bucket_name, file_key, s3 = s3_Dataset_put_object()
		file_key = file_key + "DS_PLAN_RPT_LOG41_7.txt"
		result = s3.put_object(Bucket=bucket_name, Key=file_key, Body=df_noteql_N_Y_BUFFER.getvalue())
		res = result.get('ResponseMetadata')
		if res.get('HTTPStatusCode') == 200:
			print("DS_PLAN_RPT_LOG41_7 empty file uploaded successfully")
		else:
			print("DS_PLAN_RPT_LOG41_7 empty file not uploaded")
		return
	else:
		print('The NEW_IND_NOT_EQL_Y_N dataframe is not empty so continue with furthur process')

	# df_noteql_N_Y = pd.read_csv(NEW_IND_NOT_EQL_Y_N)
	# print(df_noteql_N_Y.columns)
	df_noteql_N_Y = NEW_IND_NOT_EQL_Y_N.copy()
	df_noteql_N_Y.columns = df_noteql_N_Y.columns.str.replace("'", '')
	df_noteql_N_Y.replace("'", '', regex=True, inplace=True)
	# Remove leading and trailing spaces from all column names
	df_noteql_N_Y.columns = df_noteql_N_Y.columns.str.strip(" ")
	print(df_noteql_N_Y.columns)
	print(df_noteql_N_Y['RCDKPER_CD'])
	# DS_PLAN_RPT_LOG#jRcdkper_cd#_4.ds
	df_noteql_N_Y = df_noteql_N_Y[['RCDKPER_CD', 'TP_NAME', 'IFTP_PLAN_NUM', 'NAME']]
	df_noteql_N_Y.rename(columns={'IFTP_PLAN_NUM': 'NUMBER'}, inplace=True)
	# ADDING NEW COLUMN WITH DEFAULT VALUE TO THE  DATAFRAME
	df_noteql_N_Y['DESCRIPTION'] = '6'
	print(df_noteql_N_Y)
	df_noteql_N_Y_BUFFER = StringIO()
	df_noteql_N_Y.to_csv(df_noteql_N_Y_BUFFER, index=False)
	bucket_name, file_key, s3 = s3_Dataset_put_object()
	file_key = file_key + "DS_PLAN_RPT_LOG41_7.txt"

	result = s3.put_object(Bucket=bucket_name, Key=file_key, Body=df_noteql_N_Y_BUFFER.getvalue())
	res = result.get('ResponseMetadata')
	if res.get('HTTPStatusCode') == 200:

		print("DS_PLAN_RPT_LOG41_7 file uploaded successfully")
	else:
		print("DS_PLAN_RPT_LOG41_7 file not uploaded")


if __name__ == "__main__":
	event = {'Records': [{
		'body': '{"indId": "0", "gaId": null, "provCompany": null, "accuCode": "PscAfWR", "accuAccessTypeCode": null, "statusCode": "PENDING", "processCode": "CSES_SYNCH", "typeCode": "RKS", "batchNumber": "2422", "runOutput": null, "effdate": "24-Aug-2016", "runDpdateTime": "", "creationDpdateTime": "24-Aug-2016 16:07:01", "lastRunDpDateTime": "", "runEndDateTime": "", "seqnbr": "4", "evId": "0", "externalId": null, "externalSubId": "745638", "rowId": "AABsfQAADAAC4JzAAK"}'}]}
	context = []
	print("Loding the plan sequence two.............")
	try:

		lambda_handler(event, context)
		print("plan two completed successfully ")
	except Exception as e:

		error_msg = traceback.format_exc()
		print("Error occured while processing lambda plan second sequence:{}".format(e))
		print(f"Traceback:\n{error_msg}")
		sys.exit(5)
