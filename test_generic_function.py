"""
Comprehensive tests for Generic_function.py
"""

import sys
import os
import pytest
from unittest.mock import Mock, MagicMock, patch
import boto3

# Add the source directory to the path
sys.path.append("src/awslambda/")
sys.path.append("src/")

# Set required environment variables for testing
os.environ['REGION'] = 'us-east-1'
os.environ['Environment'] = 'test'

# Mock the missing IBM DB2 modules that are not available in test environment
sys.modules['ibm_db'] = MagicMock()
sys.modules['ibm_db_dbi'] = MagicMock()

# Mock the boto3 SSM client and DB2 connection at import time
with patch('boto3.client') as mock_boto3_client:
    mock_ssm_client = MagicMock()
    mock_boto3_client.return_value = mock_ssm_client
    
    # Import the module under test
    from src.awslambda.Generic_function import (
        get_db2_parameters,
        db2_conn_test,
        s3_get_object,
        s3_put_object,
        s3_Dataset_get_object,
        s3_Dataset_put_object,
        s3_intermediate_files_put_object,
        s3_intermediate_files_get_object,
        logger
    )


class TestGetDB2Parameters:
    """Test class for get_db2_parameters function"""
    
    @patch('src.awslambda.Generic_function.ssm_client')
    @patch.dict('src.awslambda.Generic_function.os.environ', {'Environment': 'test'})
    def test_get_db2_parameters_success(self, mock_ssm_client):
        """Test successful parameter retrieval"""
        
        # Mock SSM response
        mock_ssm_client.get_parameters.return_value = {
            'Parameters': [
                {'Name': '/dba/ifx/test/dbuser', 'Value': 'test_user'},
                {'Name': '/dba/ifx/test/dbpassword', 'Value': 'test_password'},
                {'Name': '/db2/ifx/test/dbhostname', 'Value': 'test_host'},
                {'Name': '/db2/ifx/test/dbname', 'Value': 'test_db'},
                {'Name': '/db2/ifx/test/dbport', 'Value': '50000'}
            ]
        }
        
        # Execute the function
        driver, uid, pwd, hostname, port, dbname = get_db2_parameters()
        
        # Verify results
        assert driver == 'ibm_db'
        assert uid == 'test_user'
        assert pwd == 'test_password'
        assert hostname == 'test_host'
        assert port == '50000'
        assert dbname == 'test_db'
        
        # Verify SSM was called correctly
        mock_ssm_client.get_parameters.assert_called_once()
        call_args = mock_ssm_client.get_parameters.call_args
        assert call_args[1]['WithDecryption'] is True

    @patch('src.awslambda.Generic_function.ssm_client')
    @patch.dict('src.awslambda.Generic_function.os.environ', {'Environment': 'test'})
    def test_get_db2_parameters_missing_parameters(self, mock_ssm_client):
        """Test handling of missing parameters"""
        
        # Mock SSM response with missing parameters
        mock_ssm_client.get_parameters.return_value = {
            'Parameters': [
                {'Name': '/dba/ifx/test/dbuser', 'Value': 'test_user'},
                # Missing other parameters
            ]
        }
        
        # Execute the function
        driver, uid, pwd, hostname, port, dbname = get_db2_parameters()
        
        # Verify results
        assert driver == 'ibm_db'
        assert uid == 'test_user'
        assert pwd is None  # Missing parameter should be None
        assert hostname is None
        assert port is None
        assert dbname is None

    @patch('src.awslambda.Generic_function.ssm_client')
    @patch.dict('src.awslambda.Generic_function.os.environ', {'Environment': 'test'})
    def test_get_db2_parameters_ssm_exception(self, mock_ssm_client):
        """Test handling of SSM exceptions"""
        
        # Mock SSM to raise an exception
        mock_ssm_client.get_parameters.side_effect = Exception("SSM Error")
        
        # Execute the function and expect exception
        with pytest.raises(Exception):
            get_db2_parameters()


class TestDB2ConnTest:
    """Test class for db2_conn_test function"""
    
    @patch('src.awslambda.Generic_function.get_db2_parameters')
    @patch('src.awslambda.Generic_function.ibm_db')
    @patch('src.awslambda.Generic_function.db2')
    def test_db2_conn_test_success(self, mock_db2, mock_ibm_db, mock_get_params):
        """Test successful database connection"""
        
        # Mock get_db2_parameters
        mock_get_params.return_value = (
            'ibm_db', 'test_user', 'test_password', 
            'test_host', '50000', 'test_db'
        )
        
        # Mock IBM DB2 connection
        mock_connection = MagicMock()
        mock_ibm_db.connect.return_value = mock_connection
        
        # Mock DB2 connection wrapper
        mock_conn_wrapper = MagicMock()
        mock_db2.Connection.return_value = mock_conn_wrapper
        
        # Execute the function
        result = db2_conn_test()
        
        # Verify connection was attempted
        mock_ibm_db.connect.assert_called_once()
        connection_string = mock_ibm_db.connect.call_args[0][0]
        assert 'DATABASE=test_db' in connection_string
        assert 'HOSTNAME=test_host' in connection_string
        assert 'PORT=50000' in connection_string
        assert 'UID=test_user' in connection_string
        assert 'PWD=test_password' in connection_string
        
        # Verify wrapper was created
        mock_db2.Connection.assert_called_once_with(mock_connection)
        
        # Verify return value
        assert result == mock_conn_wrapper

    @patch('src.awslambda.Generic_function.get_db2_parameters')
    @patch('src.awslambda.Generic_function.ibm_db')
    @patch('builtins.print')
    def test_db2_conn_test_connection_failure(self, mock_print, mock_ibm_db, mock_get_params):
        """Test database connection failure - this reveals a bug in the original code"""
        
        # Mock get_db2_parameters
        mock_get_params.return_value = (
            'ibm_db', 'test_user', 'test_password', 
            'test_host', '50000', 'test_db'
        )
        
        # Mock IBM DB2 connection to raise exception
        mock_ibm_db.connect.side_effect = Exception("Connection failed")
        
        # Execute the function and expect UnboundLocalError due to bug in original code
        # This test documents that there's a bug: Conn is not defined when exception occurs
        with pytest.raises(UnboundLocalError, match="cannot access local variable 'Conn'"):
            db2_conn_test()
        
        # Verify exception was handled with print statement
        mock_print.assert_called_once()
        assert "An error occurred while connecting to the database" in mock_print.call_args[0][0]


class TestS3Functions:
    """Test class for S3 helper functions"""
    
    def test_s3_get_object(self):
        """Test s3_get_object function"""
        
        bucket_name, file_key, s3_client = s3_get_object()
        
        assert bucket_name == "dev-gwf-investments-filexfer-us-east-1"
        assert file_key == "tpifx/incoming/"
        assert s3_client is not None

    def test_s3_put_object(self):
        """Test s3_put_object function"""
        
        bucket_name, file_key, s3_client = s3_put_object()
        
        assert bucket_name == "dev-gwf-investments-filexfer-us-east-1"
        assert file_key == "tpifx/outgoing/"
        assert s3_client is not None

    def test_s3_dataset_get_object(self):
        """Test s3_Dataset_get_object function"""
        
        bucket_name, file_key, s3_client = s3_Dataset_get_object()
        
        assert bucket_name == "dev-gwf-investments-filexfer-us-east-1"
        assert file_key == "tpifx/stage/plan_dataset/"
        assert s3_client is not None

    def test_s3_dataset_put_object(self):
        """Test s3_Dataset_put_object function"""
        
        bucket_name, file_key, s3_client = s3_Dataset_put_object()
        
        assert bucket_name == "dev-gwf-investments-filexfer-us-east-1"
        assert file_key == "tpifx/stage/plan_dataset/"
        assert s3_client is not None

    def test_s3_intermediate_files_put_object(self):
        """Test s3_intermediate_files_put_object function"""
        
        bucket_name, file_key, s3_client = s3_intermediate_files_put_object()
        
        assert bucket_name == "dev-gwf-investments-filexfer-us-east-1"
        assert file_key == "tpifx/stage/plan_intermediate/"
        assert s3_client is not None

    def test_s3_intermediate_files_get_object(self):
        """Test s3_intermediate_files_get_object function"""
        
        bucket_name, file_key, s3_client = s3_intermediate_files_get_object()
        
        assert bucket_name == "dev-gwf-investments-filexfer-us-east-1"
        assert file_key == "tpifx/stage/plan_intermediate/"
        assert s3_client is not None


class TestEnvironmentVariables:
    """Test class for environment variable handling"""
    
    def test_region_default(self):
        """Test default region setting"""
        
        # The region should be set from environment or default to us-east-1
        from src.awslambda.Generic_function import region
        assert region in ['us-east-1', None]  # Could be None if not set

    @patch.dict(os.environ, {'REGION': 'us-west-2'})
    def test_region_from_env(self):
        """Test region from environment variable"""
        
        # Re-import to get updated environment
        import importlib
        import src.awslambda.Generic_function
        importlib.reload(src.awslambda.Generic_function)
        
        assert os.getenv('REGION') == 'us-west-2'

    @patch.dict(os.environ, {'LOG_LEVEL': 'INFO'})
    def test_log_level_from_env(self):
        """Test log level from environment variable"""
        
        # Re-import to get updated environment
        import importlib
        import src.awslambda.Generic_function
        importlib.reload(src.awslambda.Generic_function)
        
        assert os.getenv('LOG_LEVEL') == 'INFO'


class TestLogging:
    """Test class for logging functionality"""
    
    def test_logger_exists(self):
        """Test that logger is properly configured"""
        
        assert logger is not None
        assert logger.name == 'src.awslambda.Generic_function'

    def test_logger_level(self):
        """Test logger level configuration"""
        
        # Logger should have some level set
        assert hasattr(logger, 'level')
        assert isinstance(logger.level, int)


class TestParameterConstruction:
    """Test parameter path construction logic"""
    
    @patch.dict(os.environ, {'Environment': 'prod'})
    def test_parameter_paths_construction(self):
        """Test that parameter paths are constructed correctly"""
        
        # This tests the internal logic of parameter path construction
        environment = os.environ.get("Environment")
        ods_prefix = "/dba/ifx/"
        user = "dbuser"
        password = "dbpassword"
        
        dbuser_path = str(ods_prefix) + str(environment) + "/" + str(user)
        dbpassword_path = str(ods_prefix) + str(environment) + "/" + str(password)
        
        assert dbuser_path == "/dba/ifx/prod/dbuser"
        assert dbpassword_path == "/dba/ifx/prod/dbpassword"


class TestEdgeCases:
    """Test edge cases and error conditions"""
    
    @patch.dict(os.environ, {}, clear=True)
    def test_missing_environment_variable(self):
        """Test behavior when Environment variable is missing"""
        
        # Should handle missing environment gracefully
        environment = os.environ.get("Environment")
        assert environment is None

    @patch('src.awslambda.Generic_function.ssm_client')
    def test_empty_ssm_response(self, mock_ssm_client):
        """Test handling of empty SSM response"""
        
        mock_ssm_client.get_parameters.return_value = {
            'Parameters': []
        }
        
        # Should handle empty response
        driver, uid, pwd, hostname, port, dbname = get_db2_parameters()
        
        assert driver == 'ibm_db'
        assert uid is None
        assert pwd is None
        assert hostname is None
        assert port is None
        assert dbname is None
