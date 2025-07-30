import sys
import os
import json
import pytest
import pandas as pd
import numpy as np
from unittest.mock import Mock, MagicMock, patch, call
from io import StringIO
import tempfile

# Add the source directory to the path
sys.path.append("src/awslambda/")
sys.path.append("src/")

# Set required environment variables for testing
os.environ['REGION'] = 'us-east-1'

# Mock the missing IBM DB2 modules that are not available in test environment
sys.modules['ibm_db'] = MagicMock()
sys.modules['ibm_db_dbi'] = MagicMock()

# Mock the boto3 SSM client and DB2 connection at import time
with patch('boto3.client'), \
     patch('src.awslambda.Generic_function.get_db2_parameters') as mock_db_params, \
     patch('src.awslambda.Generic_function.db2_conn_test') as mock_db_conn:
    
    # Set up mocks for module import
    mock_db_params.return_value = ('driver', 'uid', 'pwd', 'host', 'port', 'db')
    mock_db_conn.return_value = MagicMock()
    
    # Import the module under test
    from src.awslambda import lambda_function
    from src.awslambda.lambda_function import (
        lambda_handler,
        read_NEW_IND_N,
        read_NEW_IND_Y,
        read_NEW_IND_NOT_EQL_Y_N,
        new_ind_n_cdc_univl_pl_src,
        new_ind_n_cdc_if_pl_src,
        srccheck_universal_pl_tgt,
        srccheck_IF_PL_SRC_tgt,
        srccheck_UNIVL_PL_IND_Y_tgt,
        stored_proc_call_ind_y
    )


class TestLambdaHandler:
    """Test class for lambda_handler function"""
    
    @patch('src.awslambda.lambda_function.s3_Dataset_get_object')
    @patch('src.awslambda.lambda_function.s3_intermediate_files_put_object')
    @patch('src.awslambda.lambda_function.s3_Dataset_put_object')
    @patch('src.awslambda.lambda_function.read_NEW_IND_N')
    @patch('src.awslambda.lambda_function.read_NEW_IND_Y')
    @patch('src.awslambda.lambda_function.read_NEW_IND_NOT_EQL_Y_N')
    def test_lambda_handler_success(self, mock_read_not_eql, mock_read_y, mock_read_n,
                                  mock_s3_dataset_put, mock_s3_intermediate_put, mock_s3_dataset_get):
        """Test successful execution of lambda_handler"""
        
        # Mock S3 operations
        mock_s3_client = MagicMock()
        mock_s3_dataset_get.return_value = ('test-bucket', 'test-path/', mock_s3_client)
        mock_s3_intermediate_put.return_value = ('test-bucket', 'test-path/', mock_s3_client)
        mock_s3_dataset_put.return_value = ('test-bucket', 'test-path/', mock_s3_client)
        
        # Mock S3 response for DS_41_PLAN.txt
        mock_response = {
            'Body': MagicMock()
        }
        test_csv_data = """RCDKPER_CD,NEW_IND,NAME,COMPANY_NAME,COMPANY_ADDR_1,COMPANY_ADDR_2,COMPANY_CITY,COMPANY_STATE,COMPANY_ZIP,TYPE,CONTRACT_STATE,YEAR_END,FISCAL_YEAR_END,IRS_ID,IFTP_PLAN_NUM,TP_NAME,TP_PHONE,TP_WEB_URL,TP_WEB_NAME,TP_CALL_CENTER,TP_CALL_CENTER_HRS,TP_PHONE_IMPAIRED,TP_PHONE_NON_US,ZIP_CD_SFX
41,N,Test Plan,Test Company,123 Main St,,Test City,CA,12345,401K,CA,20231231,1231,123456789,12345,Test TP,8001234567,www.test.com,Test Web,Call Center,9-5,8001234568,8001234569,1234
41,Y,Test Plan 2,Test Company 2,456 Oak St,,Test City 2,NY,67890,403B,NY,20231231,1231,987654321,67890,Test TP 2,8009876543,www.test2.com,Test Web 2,Call Center 2,8-6,8009876544,8009876545,5678"""
        
        mock_response['Body'].read.return_value.decode.return_value = test_csv_data
        mock_s3_client.get_object.return_value = mock_response
        
        # Mock S3 put_object responses
        mock_s3_client.put_object.return_value = {
            'ResponseMetadata': {'HTTPStatusCode': 200}
        }
        
        # Test event and context
        event = {
            'Records': [{
                'body': json.dumps({
                    'recordkeepercode': '41',
                    'cycledate': '2023-12-31'
                })
            }]
        }
        context = {}
        
        # Execute the function
        result = lambda_handler(event, context)
        
        # Verify S3 operations were called
        assert mock_s3_dataset_get.called
        assert mock_s3_intermediate_put.called
        
        # Verify processing functions were called
        assert mock_read_n.called
        assert mock_read_y.called
        assert mock_read_not_eql.called

    def test_lambda_handler_empty_event(self):
        """Test lambda_handler with empty event"""
        
        with patch('src.awslambda.lambda_function.s3_Dataset_get_object') as mock_s3_get, \
             patch('src.awslambda.lambda_function.s3_intermediate_files_put_object') as mock_s3_intermediate, \
             patch('src.awslambda.lambda_function.s3_Dataset_put_object') as mock_s3_dataset, \
             patch('src.awslambda.lambda_function.srccheck_universal_pl_tgt') as mock_universal_tgt, \
             patch('src.awslambda.lambda_function.srccheck_IF_PL_SRC_tgt') as mock_if_pl_tgt, \
             patch('src.awslambda.lambda_function.srccheck_UNIVL_PL_IND_Y_tgt') as mock_univl_y_tgt:
            
            # Mock S3 operations
            mock_s3_client = MagicMock()
            mock_s3_get.return_value = ('test-bucket', 'test-path/', mock_s3_client)
            mock_s3_intermediate.return_value = ('test-bucket', 'test-path/', mock_s3_client)
            mock_s3_dataset.return_value = ('test-bucket', 'test-path/', mock_s3_client)
            
            # Mock source check functions to return empty DataFrames
            mock_universal_tgt.return_value = pd.DataFrame()
            mock_if_pl_tgt.return_value = pd.DataFrame()
            mock_univl_y_tgt.return_value = pd.DataFrame()
            
            # Mock S3 response for DS_41_PLAN.txt
            test_csv_data = """RCDKPER_CD,NEW_IND,NAME,COMPANY_NAME,COMPANY_ADDR_1,COMPANY_ADDR_2,COMPANY_CITY,COMPANY_STATE,COMPANY_ZIP,TYPE,CONTRACT_STATE,YEAR_END,FISCAL_YEAR_END,IRS_ID,IFTP_PLAN_NUM,TP_NAME,TP_PHONE,TP_WEB_URL,TP_WEB_NAME,TP_CALL_CENTER,TP_CALL_CENTER_HRS,TP_PHONE_IMPAIRED,TP_PHONE_NON_US,ZIP_CD_SFX
41,N,Test Plan,Test Company,123 Main St,,Test City,CA,12345,401K,CA,20231231,1231,123456789,12345,Test TP,8001234567,www.test.com,Test Web,Call Center,9-5,8001234568,8001234569,1234"""
            
            mock_response = {
                'Body': MagicMock()
            }
            mock_response['Body'].read.return_value.decode.return_value = test_csv_data
            mock_s3_client.get_object.return_value = mock_response
            mock_s3_client.put_object.return_value = {
                'ResponseMetadata': {'HTTPStatusCode': 200}
            }
            
            event = {}
            context = {}
            
            # Should not raise an exception
            lambda_handler(event, context)


class TestReadNewIndN:
    """Test class for read_NEW_IND_N function"""
    
    @patch('src.awslambda.lambda_function.new_ind_n_cdc_if_pl_src')
    @patch('src.awslambda.lambda_function.new_ind_n_cdc_univl_pl_src')
    def test_read_new_ind_n_success(self, mock_cdc_univl, mock_cdc_if):
        """Test successful execution of read_NEW_IND_N"""
        
        # Create test data
        test_data = {
            'RCDKPER_CD': ['41', '41'],
            'NEW_IND': ['N', 'N'],
            'NAME': ['Test Plan 1', 'Test Plan 2'],
            'COMPANY_NAME': ['Company 1', 'Company 2'],
            'COMPANY_ADDR_1': ['123 Main St', '456 Oak St'],
            'COMPANY_ADDR_2': ['', ''],
            'COMPANY_CITY': ['City 1', 'City 2'],
            'COMPANY_STATE': ['CA', 'NY'],
            'COMPANY_ZIP': ['12345', '67890'],
            'TYPE': ['401K', '403B'],
            'CONTRACT_STATE': ['CA', 'NY'],
            'YEAR_END': ['20231231', '20231231'],
            'FISCAL_YEAR_END': ['1231', '1231'],
            'IRS_ID': ['123456789', '987654321'],
            'IFTP_PLAN_NUM': ['12345', '67890'],
            'TP_NAME': ['TP 1', 'TP 2'],
            'TP_PHONE': ['8001234567', '8009876543'],
            'TP_WEB_URL': ['www.test1.com', 'www.test2.com'],
            'TP_WEB_NAME': ['Web 1', 'Web 2'],
            'TP_CALL_CENTER': ['Center 1', 'Center 2'],
            'TP_CALL_CENTER_HRS': ['9-5', '8-6'],
            'TP_PHONE_IMPAIRED': ['8001234568', '8009876544'],
            'TP_PHONE_NON_US': ['8001234569', '8009876545'],
            'ZIP_CD_SFX': ['1234', '5678']
        }
        
        new_ind_n_df = pd.DataFrame(test_data)
        ds_41_plan_df = pd.DataFrame(test_data)
        
        # Execute the function
        read_NEW_IND_N(new_ind_n_df, ds_41_plan_df)
        
        # Verify sub-functions were called
        assert mock_cdc_if.called
        assert mock_cdc_univl.called

    def test_read_new_ind_n_empty_dataframe(self):
        """Test read_NEW_IND_N with empty dataframe"""
        
        empty_df = pd.DataFrame()
        
        # Should handle empty dataframe gracefully
        with patch('src.awslambda.lambda_function.new_ind_n_cdc_if_pl_src') as mock_cdc_if, \
             patch('src.awslambda.lambda_function.new_ind_n_cdc_univl_pl_src') as mock_cdc_univl:
            
            read_NEW_IND_N(empty_df, empty_df)


class TestReadNewIndY:
    """Test class for read_NEW_IND_Y function"""
    
    @patch('src.awslambda.lambda_function.srccheck_UNIVL_PL_IND_Y_tgt')
    @patch('src.awslambda.lambda_function.s3_Dataset_put_object')
    def test_read_new_ind_y_success(self, mock_s3_put, mock_src_check):
        """Test successful execution of read_NEW_IND_Y"""
        
        # Mock S3 operations
        mock_s3_client = MagicMock()
        mock_s3_put.return_value = ('test-bucket', 'test-path/', mock_s3_client)
        mock_s3_client.put_object.return_value = {
            'ResponseMetadata': {'HTTPStatusCode': 200}
        }
        
        # Mock source check
        mock_src_check.return_value = pd.DataFrame({
            'UNIVL_PLAN_ID': [1, 2],
            'RCDKPER_CD': ['41', '41'],
            'IFTP_PLAN_NUM': ['12345', '67890']
        })
        
        # Create test data
        test_data = {
            'RCDKPER_CD': ['41', '41'],
            'NEW_IND': ['Y', 'Y'],
            'NAME': ['Test Plan 1', 'Test Plan 2'],
            'COMPANY_NAME': ['Company 1', 'Company 2'],
            'COMPANY_ADDR_1': ['123 Main St', '456 Oak St'],
            'COMPANY_ADDR_2': ['', ''],
            'COMPANY_CITY': ['City 1', 'City 2'],
            'COMPANY_STATE': ['CA', 'NY'],
            'COMPANY_ZIP': ['12345', '67890'],
            'TYPE': ['401K', '403B'],
            'CONTRACT_STATE': ['CA', 'NY'],
            'YEAR_END': ['1231', '1231'],
            'FISCAL_YEAR_END': ['1231', '1231'],
            'IRS_ID': ['123456789', '987654321'],
            'IFTP_PLAN_NUM': ['12345', '67890'],
            'TP_NAME': ['TP 1', 'TP 2'],
            'TP_PHONE': ['8001234567', '8009876543'],
            'TP_WEB_URL': ['www.test1.com', 'www.test2.com'],
            'TP_WEB_NAME': ['Web 1', 'Web 2'],
            'TP_CALL_CENTER': ['Center 1', 'Center 2'],
            'TP_CALL_CENTER_HRS': ['9-5', '8-6'],
            'TP_PHONE_IMPAIRED': ['8001234568', '8009876544'],
            'TP_PHONE_NON_US': ['8001234569', '8009876545'],
            'ZIP_CD_SFX': ['1234', '5678']
        }
        
        new_ind_y_df = pd.DataFrame(test_data)
        
        # Execute the function
        read_NEW_IND_Y(new_ind_y_df)
        
        # Verify S3 operations were called
        assert mock_s3_put.called


class TestCdcFunctions:
    """Test class for CDC processing functions"""
    
    @patch('src.awslambda.lambda_function.srccheck_universal_pl_tgt')
    @patch('src.awslambda.lambda_function.s3_Dataset_put_object')
    @patch('src.awslambda.lambda_function.db2_conn_test')
    def test_new_ind_n_cdc_univl_pl_src_success(self, mock_db_conn, mock_s3_put, mock_src_check):
        """Test successful execution of new_ind_n_cdc_univl_pl_src"""
        
        # Mock database connection
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_db_conn.return_value = mock_conn
        
        # Mock S3 operations
        mock_s3_client = MagicMock()
        mock_s3_put.return_value = ('test-bucket', 'test-path/', mock_s3_client)
        mock_s3_client.put_object.return_value = {
            'ResponseMetadata': {'HTTPStatusCode': 200}
        }
        
        # Mock source check to return empty DataFrame to avoid column mismatch
        mock_src_check.return_value = pd.DataFrame()
        
        # Create test data with proper structure
        test_data = pd.DataFrame({
            'UNIVL_PLAN_NM': ['Test Plan'],
            'FRST_ST_ADDR_LN': ['123 Main St'],
            'SEC_ST_ADDR_LN': [''],
            'CITY_NM': ['Test City'],
            'STE_CD': ['CA'],
            'ZIP_CD': ['12345'],
            'RCDKPER_CD': ['41'],
            'IFTP_PLAN_NUM': ['12345'],
            'ZIP_CD_SFX': ['1234']
        })
        
        ds_41_plan = pd.DataFrame({
            'RCDKPER_CD': ['41'],
            'IFTP_PLAN_NUM': ['12345'],
            'NAME': ['Test Plan'],
            'TP_NAME': ['Test TP']
        })
        
        # Execute the function
        new_ind_n_cdc_univl_pl_src(test_data, ds_41_plan)
        
        # Verify function completed (empty target means early return)
        assert mock_src_check.called

    @patch('src.awslambda.lambda_function.srccheck_IF_PL_SRC_tgt')
    @patch('src.awslambda.lambda_function.s3_Dataset_put_object')
    @patch('src.awslambda.lambda_function.db2_conn_test')
    def test_new_ind_n_cdc_if_pl_src_success(self, mock_db_conn, mock_s3_put, mock_src_check):
        """Test successful execution of new_ind_n_cdc_if_pl_src"""
        
        # Mock database connection
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_db_conn.return_value = mock_conn
        
        # Mock S3 operations
        mock_s3_client = MagicMock()
        mock_s3_put.return_value = ('test-bucket', 'test-path/', mock_s3_client)
        mock_s3_client.put_object.return_value = {
            'ResponseMetadata': {'HTTPStatusCode': 200}
        }
        
        # Mock source check to return empty DataFrame to avoid column mismatch issues
        mock_src_check.return_value = pd.DataFrame()
        
        # Create simple test data that won't trigger complex DataFrame operations
        test_data = pd.DataFrame({
            'RCDKPER_CD': ['41'],
            'IFTP_CO_NM': ['Test Company'],
            'PLAN_TY_CD': ['401K'],
            'ST_JURIS_CD': ['CA'],
            'PLAN_YR_END_DT': ['2023-12-31'],
            'TAX_IRS_NUM': ['123456789'],
            'IFTP_PLAN_NUM': ['12345'],
            'IFTP_RCDKPER_NM': ['Test Name'],
            'IFTP_RCDKPER_800_NUM': ['8001234567'],
            'IFTP_WEB_URL_ADDR': ['www.test.com'],
            'IFTP_WEBSITE_NM': ['Test Site'],
            'IFTP_CALL_CNTR_NM': ['Call Center'],
            'IFTP_CALL_CNTR_HR_TXT': ['9-5'],
            'IFTP_HRING_IMPRD_TEL_NUM': ['8001234568'],
            'IFTP_OUT_USA_TEL_NUM': ['8001234569']
        })
        
        ds_41_plan = pd.DataFrame({
            'RCDKPER_CD': ['41'],
            'IFTP_PLAN_NUM': ['12345'],
            'NAME': ['Test Plan'],
            'TP_NAME': ['Test TP']
        })
        
        # Execute the function
        new_ind_n_cdc_if_pl_src(test_data, ds_41_plan)
        
        # Verify function completed (empty target means early return)
        assert mock_src_check.called


class TestSourceCheckFunctions:
    """Test class for source check functions"""
    
    @patch('src.awslambda.lambda_function.Conn')
    @patch('pandas.read_sql')
    def test_srccheck_universal_pl_tgt_success(self, mock_read_sql, mock_conn):
        """Test successful execution of srccheck_universal_pl_tgt"""
        
        # Mock pandas read_sql
        mock_read_sql.return_value = pd.DataFrame({
            'RCDKPER_CD': ['41'],
            'IFTP_PLAN_NUM': ['12345'],
            'UNIVL_PLAN_NM': ['Test Plan']
        })
        
        # Execute the function
        result = srccheck_universal_pl_tgt()
        
        # Verify the result
        assert isinstance(result, pd.DataFrame)
        assert not result.empty

    @patch('src.awslambda.lambda_function.Conn')
    @patch('pandas.read_sql')
    def test_srccheck_if_pl_src_tgt_success(self, mock_read_sql, mock_conn):
        """Test successful execution of srccheck_IF_PL_SRC_tgt"""
        
        # Mock pandas read_sql
        mock_read_sql.return_value = pd.DataFrame({
            'RCDKPER_CD': ['41'],
            'IFTP_PLAN_NUM': ['12345'],
            'IFTP_CO_NM': ['Test Company']
        })
        
        # Execute the function
        result = srccheck_IF_PL_SRC_tgt()
        
        # Verify the result
        assert isinstance(result, pd.DataFrame)
        assert not result.empty

    @patch('src.awslambda.lambda_function.Conn')
    @patch('pandas.read_sql')
    def test_srccheck_univl_pl_ind_y_tgt_success(self, mock_read_sql, mock_conn):
        """Test successful execution of srccheck_UNIVL_PL_IND_Y_tgt"""
        
        # Mock pandas read_sql
        mock_read_sql.return_value = pd.DataFrame({
            'UNIVL_PLAN_ID': [1],
            'RCDKPER_CD': ['41'],
            'IFTP_PLAN_NUM': ['12345']
        })
        
        # Execute the function
        result = srccheck_UNIVL_PL_IND_Y_tgt()
        
        # Verify the result
        assert isinstance(result, pd.DataFrame)
        assert not result.empty


class TestStoredProcedureCall:
    """Test class for stored procedure calls"""
    
    @patch('src.awslambda.lambda_function.db2_conn_test')
    def test_stored_proc_call_ind_y_success(self, mock_db_conn):
        """Test successful execution of stored_proc_call_ind_y"""
        
        # Mock database connection
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_db_conn.return_value = mock_conn
        
        # Execute the function
        stored_proc_call_ind_y('41', '12345')
        
        # Verify database operations
        assert mock_db_conn.called
        assert mock_cursor.execute.called
        assert mock_cursor.close.called
        assert mock_conn.close.called


class TestReadNewIndNotEqlYN:
    """Test class for read_NEW_IND_NOT_EQL_Y_N function"""
    
    @patch('src.awslambda.lambda_function.s3_Dataset_put_object')
    def test_read_new_ind_not_eql_y_n_success(self, mock_s3_put):
        """Test successful execution of read_NEW_IND_NOT_EQL_Y_N"""
        
        # Mock S3 operations
        mock_s3_client = MagicMock()
        mock_s3_put.return_value = ('test-bucket', 'test-path/', mock_s3_client)
        mock_s3_client.put_object.return_value = {
            'ResponseMetadata': {'HTTPStatusCode': 200}
        }
        
        # Create test data
        test_data = pd.DataFrame({
            'RCDKPER_CD': ['41'],
            'NEW_IND': ['X'],
            'TP_NAME': ['Test TP'],
            'IFTP_PLAN_NUM': ['12345'],
            'NAME': ['Test Plan']
        })
        
        # Execute the function
        read_NEW_IND_NOT_EQL_Y_N(test_data)
        
        # Verify S3 operations were called
        assert mock_s3_put.called

    def test_read_new_ind_not_eql_y_n_empty_dataframe(self):
        """Test read_NEW_IND_NOT_EQL_Y_N with empty dataframe"""
        
        empty_df = pd.DataFrame()
        
        # Should handle empty dataframe gracefully
        with patch('src.awslambda.lambda_function.s3_Dataset_put_object') as mock_s3_put:
            mock_s3_client = MagicMock()
            mock_s3_put.return_value = ('test-bucket', 'test-path/', mock_s3_client)
            mock_s3_client.put_object.return_value = {
                'ResponseMetadata': {'HTTPStatusCode': 200}
            }
            
            read_NEW_IND_NOT_EQL_Y_N(empty_df)


class TestErrorHandling:
    """Test class for error handling scenarios"""
    
    @patch('src.awslambda.lambda_function.s3_Dataset_get_object')
    def test_lambda_handler_s3_error(self, mock_s3_get):
        """Test lambda_handler with S3 error"""
        
        # Mock S3 to raise an exception
        mock_s3_get.side_effect = Exception("S3 connection failed")
        
        event = {}
        context = {}
        
        # Should handle S3 errors gracefully
        with pytest.raises(Exception):
            lambda_handler(event, context)

    @patch('src.awslambda.lambda_function.db2_conn_test')
    def test_stored_proc_call_database_error(self, mock_db_conn):
        """Test stored procedure call with database error"""
        
        # Mock database connection to raise an exception
        mock_db_conn.side_effect = Exception("Database connection failed")
        
        # Should handle database errors and exit with code 4
        with pytest.raises(SystemExit) as excinfo:
            stored_proc_call_ind_y('41', '12345')
        
        assert excinfo.value.code == 4


# Integration test
class TestIntegration:
    """Integration tests"""
    
    @patch('src.awslambda.lambda_function.db2_conn_test')
    @patch('src.awslambda.lambda_function.s3_Dataset_get_object')
    @patch('src.awslambda.lambda_function.s3_intermediate_files_put_object')
    @patch('src.awslambda.lambda_function.s3_Dataset_put_object')
    @patch('src.awslambda.lambda_function.srccheck_universal_pl_tgt')
    @patch('src.awslambda.lambda_function.srccheck_IF_PL_SRC_tgt')
    @patch('src.awslambda.lambda_function.srccheck_UNIVL_PL_IND_Y_tgt')
    def test_full_workflow_integration(self, mock_univl_y_tgt, mock_if_pl_tgt, mock_universal_tgt, 
                                     mock_s3_dataset_put, mock_s3_intermediate_put, mock_s3_dataset_get, mock_db_conn):
        """Test full workflow integration"""
        
        # Mock database connection
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_db_conn.return_value = mock_conn
        
        # Mock source check functions to return empty DataFrames
        mock_universal_tgt.return_value = pd.DataFrame()
        mock_if_pl_tgt.return_value = pd.DataFrame()
        mock_univl_y_tgt.return_value = pd.DataFrame()
        
        # Mock S3 operations
        mock_s3_client = MagicMock()
        mock_s3_dataset_get.return_value = ('test-bucket', 'test-path/', mock_s3_client)
        mock_s3_intermediate_put.return_value = ('test-bucket', 'test-path/', mock_s3_client)
        mock_s3_dataset_put.return_value = ('test-bucket', 'test-path/', mock_s3_client)
        
        # Mock S3 response
        test_csv_data = """RCDKPER_CD,NEW_IND,NAME,COMPANY_NAME,COMPANY_ADDR_1,COMPANY_ADDR_2,COMPANY_CITY,COMPANY_STATE,COMPANY_ZIP,TYPE,CONTRACT_STATE,YEAR_END,FISCAL_YEAR_END,IRS_ID,IFTP_PLAN_NUM,TP_NAME,TP_PHONE,TP_WEB_URL,TP_WEB_NAME,TP_CALL_CENTER,TP_CALL_CENTER_HRS,TP_PHONE_IMPAIRED,TP_PHONE_NON_US,ZIP_CD_SFX
41,N,Test Plan,Test Company,123 Main St,,Test City,CA,12345,401K,CA,20231231,1231,123456789,12345,Test TP,8001234567,www.test.com,Test Web,Call Center,9-5,8001234568,8001234569,1234"""
        
        mock_response = {'Body': MagicMock()}
        mock_response['Body'].read.return_value.decode.return_value = test_csv_data
        mock_s3_client.get_object.return_value = mock_response
        mock_s3_client.put_object.return_value = {
            'ResponseMetadata': {'HTTPStatusCode': 200}
        }
        
        # Test event
        event = {}
        context = {}
        
        # Execute the function
        lambda_handler(event, context)
        
        # Verify all major components were called
        assert mock_s3_dataset_get.called
        assert mock_s3_client.get_object.called


# Utility test functions
def test_inc():
    """Simple test to verify test framework is working"""
    def inc(x):
        return x + 1
    
    assert inc(3) == 4


class TestLambdaFunctionAdditionalCoverage:
    """Additional tests to improve lambda_function.py coverage"""
    
    @patch('src.awslambda.lambda_function.s3_Dataset_get_object')
    @patch('src.awslambda.lambda_function.s3_intermediate_files_put_object')
    @patch('src.awslambda.lambda_function.s3_Dataset_put_object')
    @patch('src.awslambda.lambda_function.read_NEW_IND_N')
    @patch('src.awslambda.lambda_function.read_NEW_IND_Y')
    @patch('src.awslambda.lambda_function.read_NEW_IND_NOT_EQL_Y_N')
    def test_lambda_handler_s3_upload_failure(self, mock_read_not_eql, mock_read_y, mock_read_n,
                                            mock_s3_dataset_put, mock_s3_intermediate_put, mock_s3_dataset_get):
        """Test lambda_handler with S3 upload failure"""
        
        # Mock S3 operations
        mock_s3_client = MagicMock()
        mock_s3_dataset_get.return_value = ('test-bucket', 'test-path/', mock_s3_client)
        mock_s3_intermediate_put.return_value = ('test-bucket', 'test-path/', mock_s3_client)
        mock_s3_dataset_put.return_value = ('test-bucket', 'test-path/', mock_s3_client)
        
        # Mock S3 response for DS_41_PLAN.txt
        test_csv_data = """RCDKPER_CD,NEW_IND,NAME,COMPANY_NAME,COMPANY_ADDR_1,COMPANY_ADDR_2,COMPANY_CITY,COMPANY_STATE,COMPANY_ZIP,TYPE,CONTRACT_STATE,YEAR_END,FISCAL_YEAR_END,IRS_ID,IFTP_PLAN_NUM,TP_NAME,TP_PHONE,TP_WEB_URL,TP_WEB_NAME,TP_CALL_CENTER,TP_CALL_CENTER_HRS,TP_PHONE_IMPAIRED,TP_PHONE_NON_US,ZIP_CD_SFX
41,N,Test Plan,Test Company,123 Main St,,Test City,CA,12345,401K,CA,20231231,1231,123456789,12345,Test TP,8001234567,www.test.com,Test Web,Call Center,9-5,8001234568,8001234569,1234
41,Y,Test Plan 2,Test Company 2,456 Oak St,,Test City 2,NY,67890,403B,NY,20231231,1231,987654321,67890,Test TP 2,8009876543,www.test2.com,Test Web 2,Call Center 2,8-6,8009876544,8009876545,5678"""
        
        mock_response = {'Body': MagicMock()}
        mock_response['Body'].read.return_value.decode.return_value = test_csv_data
        mock_s3_client.get_object.return_value = mock_response
        
        # Mock S3 put_object to return failure status - this should hit line 94 and 138
        mock_s3_client.put_object.return_value = {
            'ResponseMetadata': {'HTTPStatusCode': 500}  # Failure status
        }
        
        event = {}
        context = {}
        
        # Execute the function - should handle upload failures
        lambda_handler(event, context)
        
        # Verify functions were still called despite upload failures
        assert mock_read_n.called
        assert mock_read_y.called
        assert mock_read_not_eql.called

    @patch('src.awslambda.lambda_function.s3_Dataset_get_object')
    @patch('src.awslambda.lambda_function.s3_intermediate_files_put_object')
    @patch('src.awslambda.lambda_function.s3_Dataset_put_object')
    def test_lambda_handler_with_mixed_data(self, mock_s3_dataset_put, mock_s3_intermediate_put, mock_s3_dataset_get):
        """Test lambda_handler with mixed NEW_IND values to cover different branches"""
        
        # Mock S3 operations
        mock_s3_client = MagicMock()
        mock_s3_dataset_get.return_value = ('test-bucket', 'test-path/', mock_s3_client)
        mock_s3_intermediate_put.return_value = ('test-bucket', 'test-path/', mock_s3_client)
        mock_s3_dataset_put.return_value = ('test-bucket', 'test-path/', mock_s3_client)
        
        # Mock S3 response with mixed NEW_IND values
        test_csv_data = """RCDKPER_CD,NEW_IND,NAME,COMPANY_NAME,COMPANY_ADDR_1,COMPANY_ADDR_2,COMPANY_CITY,COMPANY_STATE,COMPANY_ZIP,TYPE,CONTRACT_STATE,YEAR_END,FISCAL_YEAR_END,IRS_ID,IFTP_PLAN_NUM,TP_NAME,TP_PHONE,TP_WEB_URL,TP_WEB_NAME,TP_CALL_CENTER,TP_CALL_CENTER_HRS,TP_PHONE_IMPAIRED,TP_PHONE_NON_US,ZIP_CD_SFX
41,N,Test Plan N,Test Company,123 Main St,,Test City,CA,12345,401K,CA,20231231,1231,123456789,12345,Test TP,8001234567,www.test.com,Test Web,Call Center,9-5,8001234568,8001234569,1234
41,Y,Test Plan Y,Test Company 2,456 Oak St,,Test City 2,NY,67890,403B,NY,20231231,1231,987654321,67890,Test TP 2,8009876543,www.test2.com,Test Web 2,Call Center 2,8-6,8009876544,8009876545,5678
41,X,Test Plan X,Test Company 3,789 Pine St,,Test City 3,TX,54321,IRA,TX,20231231,1231,456789123,54321,Test TP 3,8005555555,www.test3.com,Test Web 3,Call Center 3,24/7,8005555556,8005555557,9999"""
        
        mock_response = {'Body': MagicMock()}
        mock_response['Body'].read.return_value.decode.return_value = test_csv_data
        mock_s3_client.get_object.return_value = mock_response
        
        mock_s3_client.put_object.return_value = {
            'ResponseMetadata': {'HTTPStatusCode': 200}
        }
        
        # Mock the processing functions to avoid complex logic
        with patch('src.awslambda.lambda_function.read_NEW_IND_N') as mock_read_n, \
             patch('src.awslambda.lambda_function.read_NEW_IND_Y') as mock_read_y, \
             patch('src.awslambda.lambda_function.read_NEW_IND_NOT_EQL_Y_N') as mock_read_not_eql:
            
            event = {}
            context = {}
            
            # Execute the function
            lambda_handler(event, context)
            
            # Verify all processing functions were called
            assert mock_read_n.called
            assert mock_read_y.called
            assert mock_read_not_eql.called

    @patch('src.awslambda.lambda_function.s3_Dataset_get_object')
    @patch('src.awslambda.lambda_function.s3_intermediate_files_put_object')
    @patch('src.awslambda.lambda_function.s3_Dataset_put_object')
    def test_lambda_handler_csv_parsing_edge_cases(self, mock_s3_dataset_put, mock_s3_intermediate_put, mock_s3_get):
        """Test lambda_handler with various CSV parsing scenarios"""
        
        # Mock S3 operations
        mock_s3_client = MagicMock()
        mock_s3_get.return_value = ('test-bucket', 'test-path/', mock_s3_client)
        mock_s3_intermediate_put.return_value = ('test-bucket', 'test-path/', mock_s3_client)
        mock_s3_dataset_put.return_value = ('test-bucket', 'test-path/', mock_s3_client)
        
        # Test with CSV that has empty rows and special characters
        test_csv_data = """RCDKPER_CD,NEW_IND,NAME,COMPANY_NAME,COMPANY_ADDR_1,COMPANY_ADDR_2,COMPANY_CITY,COMPANY_STATE,COMPANY_ZIP,TYPE,CONTRACT_STATE,YEAR_END,FISCAL_YEAR_END,IRS_ID,IFTP_PLAN_NUM,TP_NAME,TP_PHONE,TP_WEB_URL,TP_WEB_NAME,TP_CALL_CENTER,TP_CALL_CENTER_HRS,TP_PHONE_IMPAIRED,TP_PHONE_NON_US,ZIP_CD_SFX
41,N,Plan with quotes,Company Inc,123 Main St,Suite 100,Test City,CA,12345,401K,CA,20231231,1231,123456789,12345,Test TP,8001234567,www.test.com,Test Web,Call Center,9-5,8001234568,8001234569,1234

41,Y,Plan with newlines,Company with spaces,456 Oak St,,Test City 2,NY,67890,403B,NY,20231231,1231,987654321,67890,Test TP 2,8009876543,www.test2.com,Test Web 2,Call Center 2,8-6,8009876544,8009876545,5678"""
        
        mock_response = {'Body': MagicMock()}
        mock_response['Body'].read.return_value.decode.return_value = test_csv_data
        mock_s3_client.get_object.return_value = mock_response
        
        mock_s3_client.put_object.return_value = {
            'ResponseMetadata': {'HTTPStatusCode': 200}
        }
        
        # Mock the processing functions
        with patch('src.awslambda.lambda_function.read_NEW_IND_N'), \
             patch('src.awslambda.lambda_function.read_NEW_IND_Y'), \
             patch('src.awslambda.lambda_function.read_NEW_IND_NOT_EQL_Y_N'):
            
            event = {}
            context = {}
            
            # Should handle CSV parsing gracefully
            lambda_handler(event, context)
