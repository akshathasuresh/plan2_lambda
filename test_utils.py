"""
Test utilities and fixtures for lambda function tests
"""

import pandas as pd
import pytest
from unittest.mock import MagicMock


class TestDataFactory:
    """Factory class for creating test data"""
    
    @staticmethod
    def create_ds_41_plan_data():
        """Create sample DS_41_PLAN data for testing"""
        return pd.DataFrame({
            'RCDKPER_CD': ['41', '41', '41'],
            'NEW_IND': ['N', 'Y', 'X'],
            'NAME': ['Plan A', 'Plan B', 'Plan C'],
            'COMPANY_NAME': ['Company A', 'Company B', 'Company C'],
            'COMPANY_ADDR_1': ['123 Main St', '456 Oak Ave', '789 Pine Rd'],
            'COMPANY_ADDR_2': ['', 'Suite 100', ''],
            'COMPANY_CITY': ['City A', 'City B', 'City C'],
            'COMPANY_STATE': ['CA', 'NY', 'TX'],
            'COMPANY_ZIP': ['12345', '67890', '54321'],
            'TYPE': ['401K', '403B', '457'],
            'CONTRACT_STATE': ['CA', 'NY', 'TX'],
            'YEAR_END': ['20231231', '20231231', '20231231'],
            'FISCAL_YEAR_END': ['1231', '1231', '1231'],
            'IRS_ID': ['123456789', '987654321', '456789123'],
            'IFTP_PLAN_NUM': ['12345', '67890', '54321'],
            'TP_NAME': ['TP A', 'TP B', 'TP C'],
            'TP_PHONE': ['8001234567', '8009876543', '8005551234'],
            'TP_WEB_URL': ['www.a.com', 'www.b.com', 'www.c.com'],
            'TP_WEB_NAME': ['Web A', 'Web B', 'Web C'],
            'TP_CALL_CENTER': ['Center A', 'Center B', 'Center C'],
            'TP_CALL_CENTER_HRS': ['9-5', '8-6', '24/7'],
            'TP_PHONE_IMPAIRED': ['8001234568', '8009876544', '8005551235'],
            'TP_PHONE_NON_US': ['8001234569', '8009876545', '8005551236'],
            'ZIP_CD_SFX': ['1234', '5678', '9012']
        })
    
    @staticmethod
    def create_universal_pl_target_data():
        """Create sample universal plan target data for testing"""
        return pd.DataFrame({
            'RCDKPER_CD': ['41', '41'],
            'IFTP_PLAN_NUM': ['12345', '67890'],
            'UNIVL_PLAN_NM': ['Plan A', 'Plan B Updated'],
            'FRST_ST_ADDR_LN': ['123 Main St', '456 Oak Ave'],
            'SEC_ST_ADDR_LN': ['', 'Suite 100'],
            'CITY_NM': ['City A', 'City B'],
            'STE_CD': ['CA', 'NY'],
            'ZIP_CD': ['12345', '67890']
        })
    
    @staticmethod
    def create_if_pl_src_target_data():
        """Create sample IF_PL_SRC target data for testing"""
        return pd.DataFrame({
            'RCDKPER_CD': ['41', '41'],
            'IFTP_CO_NM': ['Company A', 'Company B Updated'],
            'PLAN_TY_CD': ['401K', '403B'],
            'ST_JURIS_CD': ['CA', 'NY'],
            'PLAN_YR_END_DT': ['2023-12-31', '2023-12-31'],
            'TAX_IRS_NUM': ['123456789', '987654321'],
            'IFTP_PLAN_NUM': ['12345', '67890'],
            'IFTP_RCDKPER_NM': ['TP A', 'TP B Updated'],
            'IFTP_RCDKPER_800_NUM': ['8001234567', '8009876543'],
            'IFTP_WEB_URL_ADDR': ['www.a.com', 'www.b.com'],
            'IFTP_WEBSITE_NM': ['Web A', 'Web B'],
            'IFTP_CALL_CNTR_NM': ['Center A', 'Center B'],
            'IFTP_CALL_CNTR_HR_TXT': ['9-5', '8-6'],
            'IFTP_HRING_IMPRD_TEL_NUM': ['8001234568', '8009876544'],
            'IFTP_OUT_USA_TEL_NUM': ['8001234569', '8009876545']
        })
    
    @staticmethod
    def create_univl_pl_ind_y_target_data():
        """Create sample UNIVL_PL_IND_Y target data for testing"""
        return pd.DataFrame({
            'UNIVL_PLAN_ID': [1, 2],
            'RCDKPER_CD': ['41', '41'],
            'IFTP_PLAN_NUM': ['12345', '67890']
        })


class MockS3Client:
    """Mock S3 client for testing"""
    
    def __init__(self, success=True):
        self.success = success
        self.uploaded_files = {}
        
    def get_object(self, Bucket, Key):
        """Mock get_object method"""
        if not self.success:
            raise Exception("S3 get_object failed")
            
        # Return test CSV data
        test_data = TestDataFactory.create_ds_41_plan_data()
        csv_string = test_data.to_csv(index=False)
        
        mock_response = {
            'Body': MagicMock()
        }
        mock_response['Body'].read.return_value.decode.return_value = csv_string
        return mock_response
    
    def put_object(self, Bucket, Key, Body):
        """Mock put_object method"""
        if not self.success:
            return {'ResponseMetadata': {'HTTPStatusCode': 500}}
            
        self.uploaded_files[Key] = Body
        return {'ResponseMetadata': {'HTTPStatusCode': 200}}


class MockDB2Connection:
    """Mock DB2 connection for testing"""
    
    def __init__(self, success=True):
        self.success = success
        self.executed_queries = []
        self.cursor_instance = MockDB2Cursor(success)
    
    def cursor(self):
        """Return mock cursor"""
        return self.cursor_instance
    
    def close(self):
        """Mock close method"""
        pass


class MockDB2Cursor:
    """Mock DB2 cursor for testing"""
    
    def __init__(self, success=True):
        self.success = success
        self.executed_queries = []
    
    def execute(self, query, params=None):
        """Mock execute method"""
        if not self.success:
            raise Exception("Database query failed")
        
        self.executed_queries.append({
            'query': query,
            'params': params
        })
    
    def fetchall(self):
        """Mock fetchall method"""
        return []
    
    def close(self):
        """Mock close method"""
        pass


@pytest.fixture
def mock_s3_success():
    """Fixture for successful S3 operations"""
    return MockS3Client(success=True)


@pytest.fixture
def mock_s3_failure():
    """Fixture for failed S3 operations"""
    return MockS3Client(success=False)


@pytest.fixture
def mock_db_success():
    """Fixture for successful database operations"""
    return MockDB2Connection(success=True)


@pytest.fixture
def mock_db_failure():
    """Fixture for failed database operations"""
    return MockDB2Connection(success=False)


@pytest.fixture
def sample_lambda_event():
    """Fixture for sample Lambda event"""
    return {
        'Records': [{
            'body': '{"recordkeepercode": "41", "cycledate": "2023-12-31"}'
        }]
    }


@pytest.fixture
def sample_ds_41_plan_data():
    """Fixture for sample DS_41_PLAN data"""
    return TestDataFactory.create_ds_41_plan_data()


@pytest.fixture
def sample_universal_pl_target_data():
    """Fixture for sample universal plan target data"""
    return TestDataFactory.create_universal_pl_target_data()


@pytest.fixture
def sample_if_pl_src_target_data():
    """Fixture for sample IF_PL_SRC target data"""
    return TestDataFactory.create_if_pl_src_target_data()
