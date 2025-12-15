"""
Tests for native_entrypoint.py

Tests cover:
- Environment variable parsing
- Configuration logic
- Virtuoso configuration generation
- VIRT_* environment variable setting
"""

import os

from virtuoso_utilities.native_entrypoint import (
    DEFAULT_ORIGINAL_ENTRYPOINT,
    ENV_DBA_PASSWORD,
    ENV_ENABLE_WRITE_PERMISSIONS,
    ENV_ESTIMATED_DB_SIZE_GB,
    ENV_EXTRA_DIRS_ALLOWED,
    ENV_MAX_DIRTY_BUFFERS,
    ENV_MEMORY,
    ENV_NUMBER_OF_BUFFERS,
    ENV_ORIGINAL_ENTRYPOINT,
    ENV_PARALLEL_THREADS,
    get_config_from_env,
    set_virt_env_vars,
)
from virtuoso_utilities.launch_virtuoso import DEFAULT_CONTAINER_DATA_DIR


class TestGetConfigFromEnv:
    """Tests for get_config_from_env function."""

    def test_default_values(self, monkeypatch):
        """Test default values when no env vars are set."""
        for var in [
            ENV_MEMORY,
            ENV_DBA_PASSWORD,
            "DBA_PASSWORD",
            ENV_ESTIMATED_DB_SIZE_GB,
            ENV_PARALLEL_THREADS,
            ENV_ENABLE_WRITE_PERMISSIONS,
            ENV_NUMBER_OF_BUFFERS,
            ENV_MAX_DIRTY_BUFFERS,
            ENV_EXTRA_DIRS_ALLOWED,
            ENV_ORIGINAL_ENTRYPOINT,
        ]:
            monkeypatch.delenv(var, raising=False)

        config = get_config_from_env()

        assert config["dba_password"] == "dba"
        assert config["estimated_db_size_gb"] == 0
        assert config["parallel_threads"] is None
        assert config["enable_write_permissions"] is False
        assert config["number_of_buffers"] is None
        assert config["max_dirty_buffers"] is None
        assert config["data_dir"] == DEFAULT_CONTAINER_DATA_DIR
        assert config["extra_dirs_allowed"] == ""
        assert config["original_entrypoint"] == DEFAULT_ORIGINAL_ENTRYPOINT

    def test_memory_from_env(self, monkeypatch):
        """Test memory configuration from environment variable."""
        monkeypatch.setenv(ENV_MEMORY, "16g")
        config = get_config_from_env()
        assert config["memory"] == "16g"

    def test_dba_password_from_virtuoso_env(self, monkeypatch):
        """Test VIRTUOSO_DBA_PASSWORD takes precedence."""
        monkeypatch.setenv(ENV_DBA_PASSWORD, "virtuoso_pass")
        monkeypatch.setenv("DBA_PASSWORD", "dba_pass")
        config = get_config_from_env()
        assert config["dba_password"] == "virtuoso_pass"

    def test_dba_password_fallback(self, monkeypatch):
        """Test fallback to DBA_PASSWORD for compatibility."""
        monkeypatch.delenv(ENV_DBA_PASSWORD, raising=False)
        monkeypatch.setenv("DBA_PASSWORD", "compat_pass")
        config = get_config_from_env()
        assert config["dba_password"] == "compat_pass"

    def test_estimated_db_size(self, monkeypatch):
        """Test estimated DB size parsing."""
        monkeypatch.setenv(ENV_ESTIMATED_DB_SIZE_GB, "100.5")
        config = get_config_from_env()
        assert config["estimated_db_size_gb"] == 100.5

    def test_parallel_threads(self, monkeypatch):
        """Test parallel threads parsing."""
        monkeypatch.setenv(ENV_PARALLEL_THREADS, "8")
        config = get_config_from_env()
        assert config["parallel_threads"] == 8

    def test_enable_write_permissions_true(self, monkeypatch):
        """Test write permissions enabled with 'true'."""
        monkeypatch.setenv(ENV_ENABLE_WRITE_PERMISSIONS, "true")
        config = get_config_from_env()
        assert config["enable_write_permissions"] is True

    def test_enable_write_permissions_one(self, monkeypatch):
        """Test write permissions enabled with '1'."""
        monkeypatch.setenv(ENV_ENABLE_WRITE_PERMISSIONS, "1")
        config = get_config_from_env()
        assert config["enable_write_permissions"] is True

    def test_enable_write_permissions_yes(self, monkeypatch):
        """Test write permissions enabled with 'yes'."""
        monkeypatch.setenv(ENV_ENABLE_WRITE_PERMISSIONS, "yes")
        config = get_config_from_env()
        assert config["enable_write_permissions"] is True

    def test_enable_write_permissions_false(self, monkeypatch):
        """Test write permissions disabled with other values."""
        monkeypatch.setenv(ENV_ENABLE_WRITE_PERMISSIONS, "false")
        config = get_config_from_env()
        assert config["enable_write_permissions"] is False

    def test_buffer_overrides(self, monkeypatch):
        """Test buffer value overrides."""
        monkeypatch.setenv(ENV_NUMBER_OF_BUFFERS, "500000")
        monkeypatch.setenv(ENV_MAX_DIRTY_BUFFERS, "375000")
        config = get_config_from_env()
        assert config["number_of_buffers"] == 500000
        assert config["max_dirty_buffers"] == 375000

    def test_extra_dirs_allowed(self, monkeypatch):
        """Test extra DirsAllowed paths."""
        monkeypatch.setenv(ENV_EXTRA_DIRS_ALLOWED, "/data,/imports,/exports")
        config = get_config_from_env()
        assert config["extra_dirs_allowed"] == "/data,/imports,/exports"

    def test_original_entrypoint_override(self, monkeypatch):
        """Test original entrypoint override."""
        monkeypatch.setenv(ENV_ORIGINAL_ENTRYPOINT, "/custom-entrypoint.sh")
        config = get_config_from_env()
        assert config["original_entrypoint"] == "/custom-entrypoint.sh"


class TestSetVirtEnvVars:
    """Tests for set_virt_env_vars function."""

    def test_threading_config_default(self, monkeypatch):
        """Test threading configuration with default CPU count."""
        for key in list(os.environ.keys()):
            if key.startswith("VIRT_"):
                monkeypatch.delenv(key, raising=False)

        config = {
            "memory": "8g",
            "parallel_threads": None,
            "number_of_buffers": 500000,
            "max_dirty_buffers": 375000,
            "estimated_db_size_gb": 0,
        }
        set_virt_env_vars(config)

        cpu_cores = os.cpu_count() or 1
        assert os.environ["VIRT_Parameters_AsyncQueueMaxThreads"] == str(
            int(cpu_cores * 1.5)
        )
        assert os.environ["VIRT_Parameters_ThreadsPerQuery"] == str(cpu_cores)
        assert os.environ["VIRT_Parameters_MaxClientConnections"] == str(cpu_cores * 2)
        assert os.environ["VIRT_HTTPServer_ServerThreads"] == str(cpu_cores * 2)

    def test_threading_config_explicit(self, monkeypatch):
        """Test threading configuration with explicit thread count."""
        for key in list(os.environ.keys()):
            if key.startswith("VIRT_"):
                monkeypatch.delenv(key, raising=False)

        config = {
            "memory": "8g",
            "parallel_threads": 4,
            "number_of_buffers": 500000,
            "max_dirty_buffers": 375000,
            "estimated_db_size_gb": 0,
        }
        set_virt_env_vars(config)

        assert os.environ["VIRT_Parameters_AsyncQueueMaxThreads"] == "6"
        assert os.environ["VIRT_Parameters_ThreadsPerQuery"] == "4"
        assert os.environ["VIRT_Parameters_MaxClientConnections"] == "8"
        assert os.environ["VIRT_HTTPServer_ServerThreads"] == "8"

    def test_vector_settings(self, monkeypatch):
        """Test vector size settings."""
        for key in list(os.environ.keys()):
            if key.startswith("VIRT_"):
                monkeypatch.delenv(key, raising=False)

        config = {
            "memory": "8g",
            "parallel_threads": 4,
            "number_of_buffers": 500000,
            "max_dirty_buffers": 375000,
            "estimated_db_size_gb": 0,
        }
        set_virt_env_vars(config)

        assert os.environ["VIRT_Parameters_AdjustVectorSize"] == "0"
        assert os.environ["VIRT_Parameters_VectorSize"] == "1000"
        assert os.environ["VIRT_Parameters_CheckpointInterval"] == "1"

    def test_client_timeouts(self, monkeypatch):
        """Test client timeout settings."""
        for key in list(os.environ.keys()):
            if key.startswith("VIRT_"):
                monkeypatch.delenv(key, raising=False)

        config = {
            "memory": "8g",
            "parallel_threads": 4,
            "number_of_buffers": 500000,
            "max_dirty_buffers": 375000,
            "estimated_db_size_gb": 0,
        }
        set_virt_env_vars(config)

        assert os.environ["VIRT_Client_SQL_QUERY_TIMEOUT"] == "0"
        assert os.environ["VIRT_Client_SQL_TXN_TIMEOUT"] == "0"

    def test_checkpoint_remap_large_db(self, monkeypatch):
        """Test MaxCheckpointRemap is set for large databases."""
        for key in list(os.environ.keys()):
            if key.startswith("VIRT_"):
                monkeypatch.delenv(key, raising=False)

        config = {
            "memory": "8g",
            "parallel_threads": 4,
            "number_of_buffers": 500000,
            "max_dirty_buffers": 375000,
            "estimated_db_size_gb": 50,
        }
        set_virt_env_vars(config)

        assert "VIRT_Database_MaxCheckpointRemap" in os.environ
        assert "VIRT_TempDatabase_MaxCheckpointRemap" in os.environ
        expected_remap = int(50 * 1024**3 / 8192 / 4)
        assert os.environ["VIRT_Database_MaxCheckpointRemap"] == str(expected_remap)

    def test_checkpoint_remap_small_db(self, monkeypatch):
        """Test MaxCheckpointRemap is not set for small databases."""
        for key in list(os.environ.keys()):
            if key.startswith("VIRT_"):
                monkeypatch.delenv(key, raising=False)

        config = {
            "memory": "8g",
            "parallel_threads": 4,
            "number_of_buffers": 500000,
            "max_dirty_buffers": 375000,
            "estimated_db_size_gb": 0.5,
        }
        set_virt_env_vars(config)

        assert "VIRT_Database_MaxCheckpointRemap" not in os.environ
        assert "VIRT_TempDatabase_MaxCheckpointRemap" not in os.environ

    def test_max_query_mem(self, monkeypatch):
        """Test MaxQueryMem is calculated correctly."""
        for key in list(os.environ.keys()):
            if key.startswith("VIRT_"):
                monkeypatch.delenv(key, raising=False)

        config = {
            "memory": "8g",
            "parallel_threads": 4,
            "number_of_buffers": 100000,
            "max_dirty_buffers": 75000,
            "estimated_db_size_gb": 0,
        }
        set_virt_env_vars(config)

        assert "VIRT_Parameters_MaxQueryMem" in os.environ
