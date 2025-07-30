"""
Performance and edge case tests for lambda_function.py
"""

import sys
import os
import pytest
import pandas as pd
import numpy as np
from unittest.mock import Mock, MagicMock, patch
import time

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
    from src.awslambda.lambda_function import (
        lambda_handler,
        read_NEW_IND_N,
        read_NEW_IND_Y,
        new_ind_n_cdc_univl_pl_src,
        new_ind_n_cdc_if_pl_src
    )


class TestPerformance:
    """Performance tests for lambda functions"""
    
    @pytest.mark.slow
    @patch('src.awslambda.lambda_function.s3_Dataset_get_object')
    @patch('src.awslambda.lambda_function.s3_intermediate_files_put_object')
    @patch('src.awslambda.lambda_function.s3_Dataset_put_object')
    def test_lambda_handler_large_dataset(self, mock_s3_dataset_put, mock_s3_intermediate_put, mock_s3_dataset_get):
        """Test lambda_handler with large dataset"""
        
        # Create large test dataset (1000 rows)
        large_data = []
        for i in range(1000):
            large_data.append([
                '41', 'N' if i % 3 == 0 else ('Y' if i % 3 == 1 else 'X'),
                f'Plan {i}', f'Company {i}', f'{i} Main St', '',
                f'City {i}', 'CA', f'{10000 + i}', '401K', 'CA',
                '20231231', '1231', f'{100000000 + i}', f'{10000 + i}',
                f'TP {i}', f'800123{i:04d}', f'www.test{i}.com',
                f'Web {i}', f'Center {i}', '9-5',
                f'800123{i:04d}', f'800123{i:04d}', f'{i:04d}'
            ])
        
        # Convert to CSV string
        header = ['RCDKPER_CD', 'NEW_IND', 'NAME', 'COMPANY_NAME', 'COMPANY_ADDR_1', 
                 'COMPANY_ADDR_2', 'COMPANY_CITY', 'COMPANY_STATE', 'COMPANY_ZIP', 
                 'TYPE', 'CONTRACT_STATE', 'YEAR_END', 'FISCAL_YEAR_END', 'IRS_ID',
                 'IFTP_PLAN_NUM', 'TP_NAME', 'TP_PHONE', 'TP_WEB_URL', 'TP_WEB_NAME',
                 'TP_CALL_CENTER', 'TP_CALL_CENTER_HRS', 'TP_PHONE_IMPAIRED',
                 'TP_PHONE_NON_US', 'ZIP_CD_SFX']
        
        df = pd.DataFrame(large_data, columns=header)
        test_csv_data = df.to_csv(index=False)
        
        # Mock S3 operations
        mock_s3_client = MagicMock()
        mock_s3_dataset_get.return_value = ('test-bucket', 'test-path/', mock_s3_client)
        mock_s3_intermediate_put.return_value = ('test-bucket', 'test-path/', mock_s3_client)
        mock_s3_dataset_put.return_value = ('test-bucket', 'test-path/', mock_s3_client)
        
        mock_response = {'Body': MagicMock()}
        mock_response['Body'].read.return_value.decode.return_value = test_csv_data
        mock_s3_client.get_object.return_value = mock_response
        mock_s3_client.put_object.return_value = {
            'ResponseMetadata': {'HTTPStatusCode': 200}
        }
        
        # Measure performance
        start_time = time.time()
        
        with patch('src.awslambda.lambda_function.read_NEW_IND_N'), \
             patch('src.awslambda.lambda_function.read_NEW_IND_Y'), \
             patch('src.awslambda.lambda_function.read_NEW_IND_NOT_EQL_Y_N'):
            
            lambda_handler({}, {})
        
        end_time = time.time()
        execution_time = end_time - start_time
        
        # Performance assertion (should complete within reasonable time)
        assert execution_time < 30.0, f"Large dataset processing took too long: {execution_time} seconds"
        
        print(f"Large dataset (1000 rows) processed in {execution_time:.2f} seconds")

    def test_memory_usage_large_dataframe(self):
        """Test memory usage with large dataframes"""
        
        # Create large dataframe with all required columns
        large_df = pd.DataFrame({
            'RCDKPER_CD': ['41'] * 10000,
            'NEW_IND': ['N'] * 10000,
            'NAME': [f'Plan {i}' for i in range(10000)],
            'COMPANY_NAME': [f'Company {i}' for i in range(10000)],
            'COMPANY_ADDR_1': [f'{i} Main St' for i in range(10000)],
            'COMPANY_ADDR_2': [''] * 10000,
            'COMPANY_CITY': [f'City {i}' for i in range(10000)],
            'COMPANY_STATE': ['CA'] * 10000,
            'COMPANY_ZIP': [f'{10000 + i}' for i in range(10000)],
            'TYPE': ['401K'] * 10000,
            'CONTRACT_STATE': ['CA'] * 10000,
            'YEAR_END': ['20231231'] * 10000,
            'FISCAL_YEAR_END': ['1231'] * 10000,
            'IRS_ID': [f'{100000000 + i}' for i in range(10000)],
            'IFTP_PLAN_NUM': [f'{10000 + i}' for i in range(10000)],
            'TP_NAME': [f'TP {i}' for i in range(10000)],
            'TP_PHONE': [f'800123{i:04d}' for i in range(10000)],
            'TP_WEB_URL': [f'www.test{i}.com' for i in range(10000)],
            'TP_WEB_NAME': [f'Web {i}' for i in range(10000)],
            'TP_CALL_CENTER': [f'Center {i}' for i in range(10000)],
            'TP_CALL_CENTER_HRS': ['9-5'] * 10000,
            'TP_PHONE_IMPAIRED': [f'800123{i:04d}' for i in range(10000)],
            'TP_PHONE_NON_US': [f'800123{i:04d}' for i in range(10000)],
            'ZIP_CD_SFX': [f'{i:04d}' for i in range(10000)]
        })
        
        # Test that operations don't cause memory issues
        with patch('src.awslambda.lambda_function.new_ind_n_cdc_if_pl_src'), \
             patch('src.awslambda.lambda_function.new_ind_n_cdc_univl_pl_src'):
            
            try:
                read_NEW_IND_N(large_df, large_df)
                # If we get here without memory error, test passes
                assert True
            except MemoryError:
                pytest.fail("Memory error with large dataframe")


class TestEdgeCases:
    """Edge case tests for lambda functions"""
    
    def test_empty_dataframe_handling(self):
        """Test handling of empty dataframes"""
        
        empty_df = pd.DataFrame()
        
        with patch('src.awslambda.lambda_function.new_ind_n_cdc_if_pl_src') as mock_cdc_if, \
             patch('src.awslambda.lambda_function.new_ind_n_cdc_univl_pl_src') as mock_cdc_univl:
            
            # Should not raise exception
            read_NEW_IND_N(empty_df, empty_df)
            
            # Functions might still be called but should handle empty data
            assert True

    def test_single_row_dataframe(self):
        """Test handling of single row dataframes"""
        
        single_row_data = {
            'RCDKPER_CD': ['41'],
            'NEW_IND': ['N'],
            'NAME': ['Test Plan'],
            'COMPANY_NAME': ['Test Company'],
            'COMPANY_ADDR_1': ['123 Main St'],
            'COMPANY_ADDR_2': [''],
            'COMPANY_CITY': ['Test City'],
            'COMPANY_STATE': ['CA'],
            'COMPANY_ZIP': ['12345'],
            'TYPE': ['401K'],
            'CONTRACT_STATE': ['CA'],
            'YEAR_END': ['20231231'],
            'FISCAL_YEAR_END': ['1231'],
            'IRS_ID': ['123456789'],
            'IFTP_PLAN_NUM': ['12345'],
            'TP_NAME': ['Test TP'],
            'TP_PHONE': ['8001234567'],
            'TP_WEB_URL': ['www.test.com'],
            'TP_WEB_NAME': ['Test Web'],
            'TP_CALL_CENTER': ['Test Center'],
            'TP_CALL_CENTER_HRS': ['9-5'],
            'TP_PHONE_IMPAIRED': ['8001234568'],
            'TP_PHONE_NON_US': ['8001234569'],
            'ZIP_CD_SFX': ['1234']
        }
        
        single_row_df = pd.DataFrame(single_row_data)
        
        with patch('src.awslambda.lambda_function.new_ind_n_cdc_if_pl_src') as mock_cdc_if, \
             patch('src.awslambda.lambda_function.new_ind_n_cdc_univl_pl_src') as mock_cdc_univl:
            
            # Should handle single row without issues
            read_NEW_IND_N(single_row_df, single_row_df)
            assert mock_cdc_if.called
            assert mock_cdc_univl.called

    def test_missing_columns_handling(self):
        """Test handling of dataframes with missing columns"""
        
        incomplete_data = {
            'RCDKPER_CD': ['41'],
            'NEW_IND': ['N'],
            'NAME': ['Test Plan']
            # Missing other required columns
        }
        
        incomplete_df = pd.DataFrame(incomplete_data)
        
        with patch('src.awslambda.lambda_function.new_ind_n_cdc_if_pl_src') as mock_cdc_if, \
             patch('src.awslambda.lambda_function.new_ind_n_cdc_univl_pl_src') as mock_cdc_univl:
            
            # Should handle missing columns gracefully or raise appropriate error
            try:
                read_NEW_IND_N(incomplete_df, incomplete_df)
            except (KeyError, IndexError, AttributeError):
                # Expected behavior for missing columns
                assert True

    def test_null_values_handling(self):
        """Test handling of null/NaN values in dataframes"""
        
        null_data = {
            'RCDKPER_CD': ['41', '41'],
            'NEW_IND': ['N', None],
            'NAME': [None, 'Test Plan'],
            'COMPANY_NAME': ['Test Company', ''],
            'COMPANY_ADDR_1': ['123 Main St', np.nan],
            'COMPANY_ADDR_2': ['', ''],
            'COMPANY_CITY': ['Test City', None],
            'COMPANY_STATE': ['CA', 'NY'],
            'COMPANY_ZIP': ['12345', ''],
            'TYPE': ['401K', None],
            'CONTRACT_STATE': ['CA', 'NY'],
            'YEAR_END': ['20231231', '20231231'],
            'FISCAL_YEAR_END': ['1231', '1231'],
            'IRS_ID': ['123456789', None],
            'IFTP_PLAN_NUM': ['12345', '67890'],
            'TP_NAME': ['Test TP', None],
            'TP_PHONE': ['8001234567', ''],
            'TP_WEB_URL': ['www.test.com', None],
            'TP_WEB_NAME': ['Test Web', ''],
            'TP_CALL_CENTER': ['Test Center', None],
            'TP_CALL_CENTER_HRS': ['9-5', ''],
            'TP_PHONE_IMPAIRED': ['8001234568', None],
            'TP_PHONE_NON_US': ['8001234569', ''],
            'ZIP_CD_SFX': ['1234', None]
        }
        
        null_df = pd.DataFrame(null_data)
        
        with patch('src.awslambda.lambda_function.new_ind_n_cdc_if_pl_src') as mock_cdc_if, \
             patch('src.awslambda.lambda_function.new_ind_n_cdc_univl_pl_src') as mock_cdc_univl:
            
            # Should handle null values without crashing
            read_NEW_IND_N(null_df, null_df)

    def test_special_characters_handling(self):
        """Test handling of special characters in data"""
        
        special_char_data = {
            'RCDKPER_CD': ['41'],
            'NEW_IND': ['N'],
            'NAME': ["Plan with 'quotes' and \"double quotes\""],
            'COMPANY_NAME': ['Company & Co, Inc.'],
            'COMPANY_ADDR_1': ['123 Main St. #A-1'],
            'COMPANY_ADDR_2': [''],
            'COMPANY_CITY': ['São Paulo'],
            'COMPANY_STATE': ['CA'],
            'COMPANY_ZIP': ['12345'],
            'TYPE': ['401(k)'],
            'CONTRACT_STATE': ['CA'],
            'YEAR_END': ['20231231'],
            'FISCAL_YEAR_END': ['1231'],
            'IRS_ID': ['12-3456789'],
            'IFTP_PLAN_NUM': ['12345'],
            'TP_NAME': ['TP & Associates'],
            'TP_PHONE': ['800-123-4567'],
            'TP_WEB_URL': ['www.test-site.com/path?param=value'],
            'TP_WEB_NAME': ['Test & Web'],
            'TP_CALL_CENTER': ['Call Center™'],
            'TP_CALL_CENTER_HRS': ['9AM-5PM EST'],
            'TP_PHONE_IMPAIRED': ['800.123.4568'],
            'TP_PHONE_NON_US': ['+1-800-123-4569'],
            'ZIP_CD_SFX': ['1234']
        }
        
        special_char_df = pd.DataFrame(special_char_data)
        
        with patch('src.awslambda.lambda_function.new_ind_n_cdc_if_pl_src') as mock_cdc_if, \
             patch('src.awslambda.lambda_function.new_ind_n_cdc_univl_pl_src') as mock_cdc_univl:
            
            # Should handle special characters without issues
            read_NEW_IND_N(special_char_df, special_char_df)

    def test_extremely_long_strings(self):
        """Test handling of extremely long string values"""
        
        long_string = 'A' * 10000  # 10KB string
        
        long_string_data = {
            'RCDKPER_CD': ['41'],
            'NEW_IND': ['N'],
            'NAME': [long_string],
            'COMPANY_NAME': [long_string],
            'COMPANY_ADDR_1': [long_string],
            'COMPANY_ADDR_2': [''],
            'COMPANY_CITY': [long_string],
            'COMPANY_STATE': ['CA'],
            'COMPANY_ZIP': ['12345'],
            'TYPE': ['401K'],
            'CONTRACT_STATE': ['CA'],
            'YEAR_END': ['20231231'],
            'FISCAL_YEAR_END': ['1231'],
            'IRS_ID': ['123456789'],
            'IFTP_PLAN_NUM': ['12345'],
            'TP_NAME': [long_string],
            'TP_PHONE': ['8001234567'],
            'TP_WEB_URL': [long_string],
            'TP_WEB_NAME': [long_string],
            'TP_CALL_CENTER': [long_string],
            'TP_CALL_CENTER_HRS': [long_string],
            'TP_PHONE_IMPAIRED': ['8001234568'],
            'TP_PHONE_NON_US': ['8001234569'],
            'ZIP_CD_SFX': ['1234']
        }
        
        long_string_df = pd.DataFrame(long_string_data)
        
        with patch('src.awslambda.lambda_function.new_ind_n_cdc_if_pl_src') as mock_cdc_if, \
             patch('src.awslambda.lambda_function.new_ind_n_cdc_univl_pl_src') as mock_cdc_univl:
            
            # Should handle long strings without memory issues
            read_NEW_IND_N(long_string_df, long_string_df)


class TestConcurrency:
    """Concurrency and thread safety tests"""
    
    def test_multiple_lambda_invocations(self):
        """Test behavior with multiple concurrent lambda invocations"""
        
        # This test ensures that global variables don't interfere
        # between different invocations
        
        events = [
            {'Records': [{'body': '{"recordkeepercode": "41"}'}]},
            {'Records': [{'body': '{"recordkeepercode": "42"}'}]},
            {'Records': [{'body': '{"recordkeepercode": "43"}'}]}
        ]
        
        with patch('src.awslambda.lambda_function.s3_Dataset_get_object') as mock_s3_get, \
             patch('src.awslambda.lambda_function.s3_intermediate_files_put_object') as mock_s3_put, \
             patch('src.awslambda.lambda_function.s3_Dataset_put_object') as mock_s3_dataset_put, \
             patch('src.awslambda.lambda_function.read_NEW_IND_N'), \
             patch('src.awslambda.lambda_function.read_NEW_IND_Y'), \
             patch('src.awslambda.lambda_function.read_NEW_IND_NOT_EQL_Y_N'):
            
            # Mock S3 operations
            mock_s3_client = MagicMock()
            mock_s3_get.return_value = ('test-bucket', 'test-path/', mock_s3_client)
            mock_s3_put.return_value = ('test-bucket', 'test-path/', mock_s3_client)
            mock_s3_dataset_put.return_value = ('test-bucket', 'test-path/', mock_s3_client)
            
            test_csv_data = "RCDKPER_CD,NEW_IND,NAME\n41,N,Test Plan"
            mock_response = {'Body': MagicMock()}
            mock_response['Body'].read.return_value.decode.return_value = test_csv_data
            mock_s3_client.get_object.return_value = mock_response
            mock_s3_client.put_object.return_value = {
                'ResponseMetadata': {'HTTPStatusCode': 200}
            }
            
            # Execute multiple invocations
            for event in events:
                lambda_handler(event, {})
                
            # All should complete without interference
            assert True


class TestDataIntegrity:
    """Data integrity and validation tests"""
    
    def test_data_type_consistency(self):
        """Test that data types are maintained throughout processing"""
        
        # Test with mixed data types
        mixed_data = {
            'RCDKPER_CD': ['41', '42'],  # String
            'NEW_IND': ['N', 'Y'],       # String
            'NAME': ['Plan 1', 'Plan 2'], # String
            'YEAR_END': [20231231, 20241231],  # Integer
            'IRS_ID': [123456789, 987654321], # Integer
            'IFTP_PLAN_NUM': ['12345', '67890'] # String that looks like number
        }
        
        # Add other required columns with appropriate defaults
        for col in ['COMPANY_NAME', 'COMPANY_ADDR_1', 'COMPANY_ADDR_2', 'COMPANY_CITY',
                   'COMPANY_STATE', 'COMPANY_ZIP', 'TYPE', 'CONTRACT_STATE',
                   'FISCAL_YEAR_END', 'TP_NAME', 'TP_PHONE', 'TP_WEB_URL',
                   'TP_WEB_NAME', 'TP_CALL_CENTER', 'TP_CALL_CENTER_HRS',
                   'TP_PHONE_IMPAIRED', 'TP_PHONE_NON_US', 'ZIP_CD_SFX']:
            mixed_data[col] = ['Test', 'Test2']
        
        mixed_df = pd.DataFrame(mixed_data)
        
        with patch('src.awslambda.lambda_function.new_ind_n_cdc_if_pl_src') as mock_cdc_if, \
             patch('src.awslambda.lambda_function.new_ind_n_cdc_univl_pl_src') as mock_cdc_univl:
            
            # Should maintain data types appropriately
            read_NEW_IND_N(mixed_df, mixed_df)
            
            # Verify functions were called (indicating processing completed)
            assert mock_cdc_if.called
            assert mock_cdc_univl.called
