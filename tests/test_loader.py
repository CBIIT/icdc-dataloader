import pytest
import os
from unittest.mock import MagicMock
from bento.common.utils import get_logger, removeTrailingSlash, UUID
from data_loader import DataLoader
from icdc_schema import ICDC_Schema
from props import Props


@pytest.fixture
def mock_driver():
    """Mock Neo4j driver to avoid database dependency"""
    return MagicMock()


@pytest.fixture
def props():
    """Load props configuration"""
    return Props('config/props-icdc.yml')


@pytest.fixture
def schema(props):
    """Create ICDC schema"""
    return ICDC_Schema(['tests/data/icdc-model.yml', 'tests/data/icdc-model-props.yml'], props)


@pytest.fixture
def loader(mock_driver, schema):
    """Create DataLoader with mocked driver"""
    return DataLoader(mock_driver, schema)


@pytest.fixture
def file_list():
    """Test file list"""
    return [
        "tests/data/Dataset/COP-program.txt",
        "tests/data/Dataset/NCATS-COP01-case.txt",
        "tests/data/Dataset/NCATS-COP01-diagnosis.txt",
        "tests/data/Dataset/NCATS-COP01_cohort_file.txt",
        "tests/data/Dataset/NCATS-COP01_study_file.txt"
    ]


# Tests for removeTrailingSlash utility
def test_remove_trailing_slash_with_slash():
    assert removeTrailingSlash('abc/') == 'abc'


def test_remove_trailing_slash_without_slash():
    assert removeTrailingSlash('abc') == 'abc'


def test_remove_trailing_slash_multiple_slashes():
    assert removeTrailingSlash('abc//') == 'abc'


def test_remove_trailing_slash_with_protocol():
    assert removeTrailingSlash('bolt://12.34.56.78') == 'bolt://12.34.56.78'


def test_remove_trailing_slash_with_protocol_and_slash():
    assert removeTrailingSlash('bolt://12.34.56.78/') == 'bolt://12.34.56.78'


def test_remove_trailing_slash_with_protocol_multiple_slashes():
    assert removeTrailingSlash('bolt://12.34.56.78//') == 'bolt://12.34.56.78'
    assert removeTrailingSlash('bolt://12.34.56.78////') == 'bolt://12.34.56.78'


# Tests for DataLoader construction
def test_loader_construction_with_none_values(mock_driver):
    """Test that DataLoader raises exceptions with None values"""
    with pytest.raises(Exception):
        DataLoader(None, None, None)
    
    with pytest.raises(Exception):
        DataLoader(mock_driver, None, None)


def test_loader_construction_success(loader):
    """Test successful DataLoader construction"""
    assert isinstance(loader, DataLoader)


# Tests for validate_parents_exist_in_file
def test_validate_parents_exist_in_file_failure(loader):
    """Test validation with invalid parent file"""
    # Mock session to return False for node_exists (parent doesn't exist)
    mock_session = MagicMock()
    mock_session_cm = MagicMock()
    mock_session_cm.__enter__ = MagicMock(return_value=mock_session)
    mock_session_cm.__exit__ = MagicMock(return_value=False)
    loader.driver.session.return_value = mock_session_cm
    
    # Mock node_exists to return False
    loader.node_exists = MagicMock(return_value=False)
    
    result = loader.validate_parents_exist_in_file('tests/data/pathology-reports-failure.txt', 100)
    assert result is False


def test_validate_parents_exist_in_file_success(loader):
    """Test validation with valid parent file"""
    # Mock session to return True for node_exists (parent exists)
    mock_session = MagicMock()
    mock_session_cm = MagicMock()
    mock_session_cm.__enter__ = MagicMock(return_value=mock_session)
    mock_session_cm.__exit__ = MagicMock(return_value=False)
    loader.driver.session.return_value = mock_session_cm
    
    # Mock node_exists to return True (all parent nodes exist in database)
    loader.node_exists = MagicMock(return_value=True)
    
    result = loader.validate_parents_exist_in_file('tests/data/pathology-reports-success.txt', 100)
    assert result is True


# Tests for validate_file
def test_validate_file_valid(loader):
    """Test file validation with valid file"""
    assert loader.validate_file('tests/data/Dataset/NCATS-COP01-case.txt', 10, None) is True


def test_validate_file_duplicated_ids(loader):
    """Test file validation with duplicated IDs"""
    assert loader.validate_file('tests/data/NCATS01-case-dup.txt', 10, None) is False


# Tests for get_signature
def test_get_signature_empty_dict(loader):
    """Test signature generation for empty dict"""
    assert loader.get_signature({}) == '{  }'


def test_get_signature_single_key(loader):
    """Test signature generation for single key"""
    assert loader.get_signature({'key1': 'value1'}) == '{ key1: value1 }'


def test_get_signature_multiple_keys(loader):
    """Test signature generation for multiple keys"""
    assert loader.get_signature({'key1': 'value1', 'key2': 'value2'}) == '{ key1: value1, key2: value2 }'


# Tests for prepare_node
def test_prepare_node_missing_type(loader):
    """Test that prepare_node raises exception when type is missing"""
    with pytest.raises(Exception):
        loader.prepare_node({})


def test_prepare_node_uuid_generation(loader):
    """Test UUID generation in prepare_node"""
    result = loader.prepare_node({'type': 'case', 'case_id': '123', ' key1 ': ' value1  '}, "test.tsv")
    assert result['key1'] == 'value1'
    assert result['type'] == 'case'
    assert result['case_id'] == '123'
    assert result['uuid'] == 'f0cf40a7-3cdb-51fe-a596-e29e40123f56'


def test_prepare_node_existing_uuid(loader):
    """Test prepare_node with existing UUID"""
    result = loader.prepare_node({'type': 'file', 'uuid': '123', ' key1 ': ' value1  '}, "test.tsv")
    assert result == {'key1': 'value1', 'type': 'file', 'uuid': '123'}


def test_prepare_node_parent_ids(loader):
    """Test parent ID extraction in prepare_node"""
    obj = loader.prepare_node({'type': 'case', 'cohort_id': 'abc132'}, "test.tsv")
    assert obj['cohort_id'] == 'abc132'
    
    obj = loader.prepare_node({'type': 'case', 'cohort.cohort_id': 'abc132', 'cohort_id': 'def333'}, "test.tsv")
    assert obj['cohort_id'] == 'def333'
    assert obj['cohort.cohort_id'] == 'abc132'
    assert len(obj[UUID]) == 36


def test_prepare_node_boolean_invalid(loader):
    """Test boolean conversion with invalid value"""
    obj = loader.prepare_node({'type': 'vital_signs', 'ecg': 'abc132'}, "test.tsv")
    assert obj['ecg'] is None


def test_prepare_node_boolean_yes(loader):
    """Test boolean conversion with 'yes' variants"""
    obj = loader.prepare_node({'type': 'vital_signs', 'ecg': 'yes'}, "test.tsv")
    assert obj['ecg'] is True

    obj = loader.prepare_node({'type': 'vital_signs', 'ecg': 'YeS'}, "test.tsv")
    assert obj['ecg'] is True
    
    obj = loader.prepare_node({'type': 'vital_signs', 'ecg': 'YeS13'}, "test.tsv")
    assert obj['ecg'] is True


def test_prepare_node_boolean_no(loader):
    """Test boolean conversion with 'no' variants"""
    obj = loader.prepare_node({'type': 'vital_signs', 'ecg': 'no'}, "test.tsv")
    assert obj['ecg'] is False
    
    obj = loader.prepare_node({'type': 'vital_signs', 'ecg': 'No'}, "test.tsv")
    assert obj['ecg'] is False

    obj = loader.prepare_node({'type': 'vital_signs', 'ecg': ' No33 '}, "test.tsv")
    assert obj['ecg'] is False

    obj = loader.prepare_node({'type': 'vital_signs', 'ecg': ' Normal '}, "test.tsv")
    assert obj['ecg'] is False


def test_prepare_node_integer_invalid(loader):
    """Test integer conversion with invalid value"""
    obj = loader.prepare_node({'type': 'physical_exam', 'day_in_cycle': ' Normal '}, "test.tsv")
    assert obj['day_in_cycle'] is None


def test_prepare_node_integer_valid(loader):
    """Test integer conversion with valid value"""
    obj = loader.prepare_node({'type': 'physical_exam', 'day_in_cycle': ' 13 '}, "test.tsv")
    assert obj['day_in_cycle'] == 13
    assert obj['day_in_cycle'] != '13'


def test_prepare_node_integer_mixed(loader):
    """Test integer conversion with mixed value"""
    obj = loader.prepare_node({'type': 'physical_exam', 'day_in_cycle': ' 12 Normal '}, "test.tsv")
    assert obj['day_in_cycle'] is None


def test_prepare_node_float_invalid(loader):
    """Test float conversion with invalid value"""
    obj = loader.prepare_node({'type': 'file', 'file_size': ' Normal '}, "test.tsv")
    assert obj['file_size'] is None
    
    obj = loader.prepare_node({'type': 'file', 'file_size': ' 1.5 Normal '}, "test.tsv")
    assert obj['file_size'] is None


def test_prepare_node_float_valid(loader):
    """Test float conversion with valid value"""
    obj = loader.prepare_node({'type': 'file', 'file_size': ' 1.5 '}, "test.tsv")
    assert obj['file_size'] == 1.5
    
    obj = loader.prepare_node({'type': 'file', 'file_size': ' 15 '}, "test.tsv")
    assert obj['file_size'] == 15
