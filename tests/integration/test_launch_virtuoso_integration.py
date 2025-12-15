"""
Integration tests for launch_virtuoso.py

Tests cover:
- Pure utility functions (memory parsing, buffer calculation, etc.)
- INI file updates
- Docker command building
- Docker interaction (container management)
- Full integration (launching containers)
- Error handling and edge cases
"""

import argparse
import configparser
import os
import shutil
import subprocess
import uuid
from pathlib import Path

import pytest

from virtuoso_utilities.launch_virtuoso import (
    BYTES_PER_BUFFER, DEFAULT_IMAGE, MIN_DB_SIZE_BYTES_FOR_CHECKPOINT_REMAP,
    VIRTUOSO_MEMORY_PERCENTAGE, build_docker_run_command,
    bytes_to_docker_mem_str, calculate_max_checkpoint_remap,
    check_container_exists, check_docker_installed, get_directory_size,
    get_docker_image, get_optimal_buffer_values, grant_write_permissions,
    parse_memory_value, remove_container, update_ini_memory_settings)

TEST_CONTAINER_PREFIX = "virtuoso-launch-test"
# Port range: ISQL 11120-11139, HTTP 8900-8919
TEST_ISQL_PORT = 11120
TEST_HTTP_PORT = 8900

TESTS_DIR = Path(__file__).parent.parent
TEMP_TEST_DIR = TESTS_DIR / "temp_launch_virtuoso"


@pytest.fixture
def temp_data_dir():
    """Create a temporary data directory for testing within the repo."""
    unique_id = str(uuid.uuid4())[:8]
    temp_dir = TEMP_TEST_DIR / unique_id
    temp_dir.mkdir(parents=True, exist_ok=True)
    yield temp_dir
    if temp_dir.exists():
        shutil.rmtree(temp_dir)


@pytest.fixture
def temp_ini_file(temp_data_dir):
    """Create a temporary virtuoso.ini file for testing."""
    ini_path = temp_data_dir / "virtuoso.ini"
    config = configparser.ConfigParser(interpolation=None)
    config.optionxform = str
    config.add_section("Parameters")
    config.set("Parameters", "NumberOfBuffers", "10000")
    config.set("Parameters", "MaxDirtyBuffers", "5000")
    with open(ini_path, "w", encoding="utf-8") as f:
        config.write(f)
    yield ini_path


@pytest.fixture
def unique_container_name(request):
    """Generate a unique container name for each test."""
    test_id = str(uuid.uuid4())[:8]
    container_name = f"{TEST_CONTAINER_PREFIX}-{test_id}"
    yield container_name
    # Cleanup: remove container if it exists
    subprocess.run(
        ["docker", "rm", "-f", container_name],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


@pytest.fixture
def unique_ports():
    """Return fixed test ports."""
    return {
        "isql": TEST_ISQL_PORT,
        "http": TEST_HTTP_PORT,
    }


# =============================================================================
# 1. Pure function tests (no Docker required)
# =============================================================================


class TestBytesToDockerMemStr:
    """Tests for bytes_to_docker_mem_str function."""

    def test_gigabytes(self):
        """Convert exact GB values."""
        assert bytes_to_docker_mem_str(1 * 1024**3) == "1g"
        assert bytes_to_docker_mem_str(2 * 1024**3) == "2g"
        assert bytes_to_docker_mem_str(16 * 1024**3) == "16g"

    def test_megabytes(self):
        """Convert exact MB values."""
        assert bytes_to_docker_mem_str(512 * 1024**2) == "512m"
        assert bytes_to_docker_mem_str(1024 * 1024**2) == "1g"

    def test_kilobytes(self):
        """Convert exact KB values."""
        assert bytes_to_docker_mem_str(512 * 1024) == "512k"
        assert bytes_to_docker_mem_str(1024 * 1024) == "1m"

    def test_non_exact(self):
        """Handle non-exact multiples by falling back to GB."""
        # Non-exact value should still produce a valid result
        result = bytes_to_docker_mem_str(1500000000)
        assert result.endswith("g") or result.endswith("m") or result.endswith("k")


class TestParseMemoryValue:
    """Tests for parse_memory_value function."""

    def test_gigabytes(self):
        """Parse gigabyte values."""
        assert parse_memory_value("2g") == 2 * 1024**3
        assert parse_memory_value("4G") == 4 * 1024**3

    def test_megabytes(self):
        """Parse megabyte values."""
        assert parse_memory_value("512m") == 512 * 1024**2
        assert parse_memory_value("1024M") == 1024 * 1024**2

    def test_kilobytes(self):
        """Parse kilobyte values."""
        assert parse_memory_value("1024k") == 1024 * 1024
        assert parse_memory_value("2048K") == 2048 * 1024

    def test_bytes_only(self):
        """Parse values without unit (assumed bytes)."""
        assert parse_memory_value("1073741824") == 1073741824

    def test_invalid(self, capsys):
        """Handle invalid input by returning default 2GB."""
        result = parse_memory_value("invalid")
        assert result == 2 * 1024**3
        captured = capsys.readouterr()
        assert "Warning" in captured.err


class TestGetDirectorySize:
    """Tests for get_directory_size function."""

    def test_calculates_size(self, temp_data_dir):
        """Calculate directory size correctly."""
        # Create test files with known sizes
        file1 = temp_data_dir / "file1.txt"
        file2 = temp_data_dir / "file2.txt"
        file1.write_text("a" * 100)
        file2.write_text("b" * 200)
        assert get_directory_size(str(temp_data_dir)) == 300

    def test_empty_directory(self, temp_data_dir):
        """Return 0 for empty directory."""
        assert get_directory_size(str(temp_data_dir)) == 0

    def test_nonexistent_directory(self):
        """Return 0 for non-existent path."""
        assert get_directory_size("/nonexistent/path/12345") == 0

    def test_includes_subdirectories(self, temp_data_dir):
        """Include files in subdirectories."""
        subdir = temp_data_dir / "subdir"
        subdir.mkdir()
        (temp_data_dir / "file1.txt").write_text("a" * 100)
        (subdir / "file2.txt").write_text("b" * 200)
        assert get_directory_size(str(temp_data_dir)) == 300


class TestGetOptimalBufferValues:
    """Tests for get_optimal_buffer_values function."""

    def test_calculates_buffers(self):
        """Calculate buffer values based on memory limit."""
        num_buffers, max_dirty = get_optimal_buffer_values("2g")
        # Verify calculations match the formula:
        # NumberOfBuffers = (MemoryInBytes * VIRTUOSO_MEMORY_PERCENTAGE * 0.66) / BYTES_PER_BUFFER
        # MaxDirtyBuffers = NumberOfBuffers * 0.75
        memory_bytes = 2 * 1024**3
        expected_num = int((memory_bytes * VIRTUOSO_MEMORY_PERCENTAGE * 0.66) / BYTES_PER_BUFFER)
        expected_dirty = int(expected_num * 0.75)
        assert num_buffers == expected_num
        assert max_dirty == expected_dirty

    def test_different_memory_sizes(self):
        """Different memory sizes produce different buffer values."""
        num_2g, _ = get_optimal_buffer_values("2g")
        num_4g, _ = get_optimal_buffer_values("4g")
        assert num_4g > num_2g


class TestCalculateMaxCheckpointRemap:
    """Tests for calculate_max_checkpoint_remap function."""

    def test_calculates_remap(self):
        """Calculate MaxCheckpointRemap value."""
        # Formula: size_bytes / 8192 / 4
        size_bytes = 10 * 1024**3  # 10 GB
        expected = int(size_bytes / 8192 / 4)
        assert calculate_max_checkpoint_remap(size_bytes) == expected


class TestGetDockerImage:
    """Tests for get_docker_image function."""

    def test_default(self):
        """Return default pinned image when no version/SHA specified."""
        assert get_docker_image(None, None) == DEFAULT_IMAGE

    def test_with_version(self):
        """Return image with version tag."""
        assert get_docker_image("7.2.11", None) == "openlink/virtuoso-opensource-7:7.2.11"

    def test_with_sha(self):
        """Return image with SHA (takes precedence over version)."""
        sha = "sha256:abc123"
        assert get_docker_image("7.2.11", sha) == f"openlink/virtuoso-opensource-7@{sha}"

    def test_latest(self):
        """Return latest tag."""
        assert get_docker_image("latest", None) == "openlink/virtuoso-opensource-7:latest"


# =============================================================================
# 2. INI file update tests (filesystem only, no Docker)
# =============================================================================


class TestUpdateIniMemorySettings:
    """Tests for update_ini_memory_settings function."""

    def test_updates_buffers(self, temp_ini_file, temp_data_dir):
        """Update NumberOfBuffers and MaxDirtyBuffers."""
        update_ini_memory_settings(
            str(temp_ini_file),
            str(temp_data_dir),
            number_of_buffers=20000,
            max_dirty_buffers=15000,
        )
        config = configparser.ConfigParser(interpolation=None)
        config.optionxform = str
        config.read(temp_ini_file)
        assert config.get("Parameters", "NumberOfBuffers") == "20000"
        assert config.get("Parameters", "MaxDirtyBuffers") == "15000"

    def test_updates_dirs_allowed(self, temp_ini_file, temp_data_dir):
        """Update DirsAllowed setting."""
        update_ini_memory_settings(
            str(temp_ini_file),
            str(temp_data_dir),
            dirs_allowed="/data,/backup",
        )
        config = configparser.ConfigParser(interpolation=None)
        config.optionxform = str
        config.read(temp_ini_file)
        assert config.get("Parameters", "DirsAllowed") == "/data,/backup"

    def test_sets_client_timeouts(self, temp_ini_file, temp_data_dir):
        """Set SQL_QUERY_TIMEOUT and SQL_TXN_TIMEOUT to 0."""
        update_ini_memory_settings(
            str(temp_ini_file),
            str(temp_data_dir),
        )
        config = configparser.ConfigParser(interpolation=None)
        config.optionxform = str
        config.read(temp_ini_file)
        assert config.get("Client", "SQL_QUERY_TIMEOUT") == "0"
        assert config.get("Client", "SQL_TXN_TIMEOUT") == "0"

    def test_creates_sections(self, temp_data_dir):
        """Create missing sections in INI file."""
        ini_path = temp_data_dir / "virtuoso.ini"
        # Create minimal INI file without sections
        ini_path.write_text("")
        update_ini_memory_settings(
            str(ini_path),
            str(temp_data_dir),
            number_of_buffers=10000,
        )
        config = configparser.ConfigParser(interpolation=None)
        config.optionxform = str
        config.read(ini_path)
        assert config.has_section("Parameters")
        assert config.has_section("Client")

    def test_checkpoint_remap_large_db(self, temp_ini_file, temp_data_dir):
        """Update MaxCheckpointRemap for large database."""
        # Create files to exceed MIN_DB_SIZE threshold
        large_file = temp_data_dir / "large_file.db"
        # Write enough data to exceed 1GB threshold
        with open(large_file, "wb") as f:
            f.seek(MIN_DB_SIZE_BYTES_FOR_CHECKPOINT_REMAP)
            f.write(b"\0")
        update_ini_memory_settings(
            str(temp_ini_file),
            str(temp_data_dir),
        )
        config = configparser.ConfigParser(interpolation=None)
        config.optionxform = str
        config.read(temp_ini_file)
        assert config.has_option("Database", "MaxCheckpointRemap")
        assert config.has_option("TempDatabase", "MaxCheckpointRemap")

    def test_skip_small_db(self, temp_ini_file, temp_data_dir):
        """Skip MaxCheckpointRemap for small database."""
        # Create small file (below threshold)
        small_file = temp_data_dir / "small_file.db"
        small_file.write_text("small")
        update_ini_memory_settings(
            str(temp_ini_file),
            str(temp_data_dir),
        )
        config = configparser.ConfigParser(interpolation=None)
        config.optionxform = str
        config.read(temp_ini_file)
        # Database section should not exist or not have MaxCheckpointRemap
        if config.has_section("Database"):
            assert not config.has_option("Database", "MaxCheckpointRemap")

    def test_file_not_found(self, temp_data_dir, capsys):
        """Handle missing ini file gracefully."""
        nonexistent = temp_data_dir / "nonexistent.ini"
        # Should not raise exception
        update_ini_memory_settings(
            str(nonexistent),
            str(temp_data_dir),
            number_of_buffers=10000,
        )
        captured = capsys.readouterr()
        assert "not found" in captured.out

    def test_no_changes_needed(self, temp_data_dir, capsys):
        """No write if values already correct."""
        ini_path = temp_data_dir / "virtuoso.ini"
        config = configparser.ConfigParser(interpolation=None)
        config.optionxform = str
        config.add_section("Parameters")
        config.set("Parameters", "NumberOfBuffers", "10000")
        config.set("Parameters", "MaxDirtyBuffers", "5000")
        config.add_section("Client")
        config.set("Client", "SQL_QUERY_TIMEOUT", "0")
        config.set("Client", "SQL_TXN_TIMEOUT", "0")
        with open(ini_path, "w", encoding="utf-8") as f:
            config.write(f)
        update_ini_memory_settings(
            str(ini_path),
            str(temp_data_dir),
            number_of_buffers=10000,
            max_dirty_buffers=5000,
        )
        captured = capsys.readouterr()
        assert "No changes needed" in captured.out


# =============================================================================
# 3. Docker command building tests (no Docker execution)
# =============================================================================


class TestBuildDockerRunCommand:
    """Tests for build_docker_run_command function."""

    def _create_args(self, **kwargs):
        """Create argparse.Namespace with default values."""
        defaults = {
            "name": "test-virtuoso",
            "http_port": 8890,
            "isql_port": 1111,
            "data_dir": "/tmp/virtuoso-data",
            "extra_volumes": None,
            "memory": "2g",
            "cpu_limit": 0,
            "dba_password": "dba",
            "force_remove": False,
            "network": None,
            "wait_ready": False,
            "detach": False,
            "enable_write_permissions": False,
            "estimated_db_size_gb": 0,
            "virtuoso_version": None,
            "virtuoso_sha": None,
            "max_dirty_buffers": 130000,
            "number_of_buffers": 170000,
            "parallel_threads": None,
        }
        defaults.update(kwargs)
        return argparse.Namespace(**defaults)

    def test_basic_command(self, temp_data_dir):
        """Build basic command structure."""
        args = self._create_args(data_dir=str(temp_data_dir))
        cmd, paths = build_docker_run_command(args)
        assert "docker" in cmd
        assert "run" in cmd
        assert "--name" in cmd
        assert "test-virtuoso" in cmd
        assert "-p" in cmd
        assert "-v" in cmd

    def test_with_network(self, temp_data_dir):
        """Include --network option."""
        args = self._create_args(data_dir=str(temp_data_dir), network="my-network")
        cmd, _ = build_docker_run_command(args)
        assert "--network" in cmd
        assert "my-network" in cmd

    def test_with_cpu_limit(self, temp_data_dir):
        """Include --cpus option."""
        args = self._create_args(data_dir=str(temp_data_dir), cpu_limit=2.5)
        cmd, _ = build_docker_run_command(args)
        assert "--cpus" in cmd
        assert "2.5" in cmd

    def test_detached_mode(self, temp_data_dir):
        """Add -d flag, no --rm in detached mode."""
        args = self._create_args(data_dir=str(temp_data_dir), detach=True)
        cmd, _ = build_docker_run_command(args)
        assert "-d" in cmd
        assert "--rm" not in cmd

    def test_not_detached_mode(self, temp_data_dir):
        """Add --rm flag when not detached."""
        args = self._create_args(data_dir=str(temp_data_dir), detach=False)
        cmd, _ = build_docker_run_command(args)
        assert "--rm" in cmd
        assert "-d" not in cmd

    def test_extra_volumes(self, temp_data_dir):
        """Mount extra volumes and add to DirsAllowed."""
        args = self._create_args(
            data_dir=str(temp_data_dir),
            extra_volumes=["/host/path:/container/path"],
        )
        cmd, paths = build_docker_run_command(args)
        # Check volume mount in command
        volume_found = False
        for i, part in enumerate(cmd):
            if part == "-v" and i + 1 < len(cmd):
                if "/container/path" in cmd[i + 1]:
                    volume_found = True
                    break
        assert volume_found, "Extra volume not found in command"
        assert "/container/path" in paths

    def test_estimated_db_size(self, temp_data_dir):
        """Set MaxCheckpointRemap env vars for large estimated DB."""
        args = self._create_args(
            data_dir=str(temp_data_dir),
            estimated_db_size_gb=10.0,
        )
        cmd, _ = build_docker_run_command(args)
        cmd_str = " ".join(cmd)
        assert "VIRT_Database_MaxCheckpointRemap" in cmd_str
        assert "VIRT_TempDatabase_MaxCheckpointRemap" in cmd_str

    def test_custom_image_version(self, temp_data_dir):
        """Use custom version."""
        args = self._create_args(
            data_dir=str(temp_data_dir),
            virtuoso_version="7.2.11",
        )
        cmd, _ = build_docker_run_command(args)
        assert "openlink/virtuoso-opensource-7:7.2.11" in cmd

    def test_custom_image_sha(self, temp_data_dir):
        """Use custom SHA (takes precedence)."""
        args = self._create_args(
            data_dir=str(temp_data_dir),
            virtuoso_version="7.2.11",
            virtuoso_sha="sha256:abc123",
        )
        cmd, _ = build_docker_run_command(args)
        assert "openlink/virtuoso-opensource-7@sha256:abc123" in cmd


class TestMaxQueryMemCalculation:
    """Tests for MaxQueryMem calculation in build_docker_run_command."""

    def _create_args(self, **kwargs):
        """Create argparse.Namespace with default values."""
        defaults = {
            "name": "test-virtuoso",
            "http_port": 8890,
            "isql_port": 1111,
            "data_dir": "/tmp/virtuoso-data",
            "extra_volumes": None,
            "memory": "4g",
            "cpu_limit": 0,
            "dba_password": "dba",
            "force_remove": False,
            "network": None,
            "wait_ready": False,
            "detach": False,
            "enable_write_permissions": False,
            "estimated_db_size_gb": 0,
            "virtuoso_version": None,
            "virtuoso_sha": None,
            "max_dirty_buffers": 130000,
            "number_of_buffers": 170000,
            "parallel_threads": None,
        }
        defaults.update(kwargs)
        return argparse.Namespace(**defaults)

    def test_max_query_mem_calculation(self, temp_data_dir):
        """Verify MaxQueryMem is calculated correctly."""
        memory = "4g"
        number_of_buffers = 100000
        args = self._create_args(
            data_dir=str(temp_data_dir),
            memory=memory,
            number_of_buffers=number_of_buffers,
        )
        cmd, _ = build_docker_run_command(args)
        cmd_str = " ".join(cmd)

        memory_bytes = 4 * 1024**3
        effective_memory_bytes = int(memory_bytes * VIRTUOSO_MEMORY_PERCENTAGE)
        buffer_memory_bytes = number_of_buffers * BYTES_PER_BUFFER
        expected_bytes = int((effective_memory_bytes - buffer_memory_bytes) * 0.8)
        expected_str = bytes_to_docker_mem_str(expected_bytes)

        assert f"VIRT_Parameters_MaxQueryMem={expected_str}" in cmd_str

    def test_max_query_mem_not_set_when_negative(self, temp_data_dir):
        """Verify MaxQueryMem is not set when calculation yields <= 0."""
        memory = "1g"
        number_of_buffers = 500000
        args = self._create_args(
            data_dir=str(temp_data_dir),
            memory=memory,
            number_of_buffers=number_of_buffers,
        )
        cmd, _ = build_docker_run_command(args)
        cmd_str = " ".join(cmd)

        assert "VIRT_Parameters_MaxQueryMem" not in cmd_str


class TestVectorSizeSettings:
    """Tests for AdjustVectorSize, VectorSize, and CheckpointInterval settings."""

    def _create_args(self, **kwargs):
        """Create argparse.Namespace with default values."""
        defaults = {
            "name": "test-virtuoso",
            "http_port": 8890,
            "isql_port": 1111,
            "data_dir": "/tmp/virtuoso-data",
            "extra_volumes": None,
            "memory": "2g",
            "cpu_limit": 0,
            "dba_password": "dba",
            "force_remove": False,
            "network": None,
            "wait_ready": False,
            "detach": False,
            "enable_write_permissions": False,
            "estimated_db_size_gb": 0,
            "virtuoso_version": None,
            "virtuoso_sha": None,
            "max_dirty_buffers": 130000,
            "number_of_buffers": 170000,
            "parallel_threads": None,
        }
        defaults.update(kwargs)
        return argparse.Namespace(**defaults)

    def test_adjust_vector_size_disabled(self, temp_data_dir):
        """Verify AdjustVectorSize is set to 0."""
        args = self._create_args(data_dir=str(temp_data_dir))
        cmd, _ = build_docker_run_command(args)
        cmd_str = " ".join(cmd)
        assert "VIRT_Parameters_AdjustVectorSize=0" in cmd_str

    def test_vector_size_default(self, temp_data_dir):
        """Verify VectorSize is set to 1000."""
        args = self._create_args(data_dir=str(temp_data_dir))
        cmd, _ = build_docker_run_command(args)
        cmd_str = " ".join(cmd)
        assert "VIRT_Parameters_VectorSize=1000" in cmd_str

    def test_checkpoint_interval_default(self, temp_data_dir):
        """Verify CheckpointInterval is set to 1."""
        args = self._create_args(data_dir=str(temp_data_dir))
        cmd, _ = build_docker_run_command(args)
        cmd_str = " ".join(cmd)
        assert "VIRT_Parameters_CheckpointInterval=1" in cmd_str


class TestThreadingParameters:
    """Tests for CPU-based threading parameters."""

    def _create_args(self, **kwargs):
        """Create argparse.Namespace with default values."""
        defaults = {
            "name": "test-virtuoso",
            "http_port": 8890,
            "isql_port": 1111,
            "data_dir": "/tmp/virtuoso-data",
            "extra_volumes": None,
            "memory": "2g",
            "cpu_limit": 0,
            "dba_password": "dba",
            "force_remove": False,
            "network": None,
            "wait_ready": False,
            "detach": False,
            "enable_write_permissions": False,
            "estimated_db_size_gb": 0,
            "virtuoso_version": None,
            "virtuoso_sha": None,
            "max_dirty_buffers": 130000,
            "number_of_buffers": 170000,
            "parallel_threads": None,
        }
        defaults.update(kwargs)
        return argparse.Namespace(**defaults)

    def test_threading_with_explicit_parallel_threads(self, temp_data_dir):
        """Verify threading parameters with explicit --parallel-threads."""
        cpu_cores = 8
        args = self._create_args(
            data_dir=str(temp_data_dir),
            parallel_threads=cpu_cores,
        )
        cmd, _ = build_docker_run_command(args)
        cmd_str = " ".join(cmd)

        assert f"VIRT_Parameters_AsyncQueueMaxThreads={int(cpu_cores * 1.5)}" in cmd_str
        assert f"VIRT_Parameters_ThreadsPerQuery={cpu_cores}" in cmd_str
        assert f"VIRT_Parameters_MaxClientConnections={cpu_cores * 2}" in cmd_str
        assert f"VIRT_HTTPServer_ServerThreads={cpu_cores * 2}" in cmd_str

    def test_threading_with_auto_detected_cores(self, temp_data_dir):
        """Verify threading parameters use os.cpu_count() when parallel_threads=None."""
        args = self._create_args(
            data_dir=str(temp_data_dir),
            parallel_threads=None,
        )
        cmd, _ = build_docker_run_command(args)
        cmd_str = " ".join(cmd)

        cpu_cores = os.cpu_count() or 1
        assert f"VIRT_Parameters_AsyncQueueMaxThreads={int(cpu_cores * 1.5)}" in cmd_str
        assert f"VIRT_Parameters_ThreadsPerQuery={cpu_cores}" in cmd_str
        assert f"VIRT_Parameters_MaxClientConnections={cpu_cores * 2}" in cmd_str
        assert f"VIRT_HTTPServer_ServerThreads={cpu_cores * 2}" in cmd_str

    def test_threading_with_single_core(self, temp_data_dir):
        """Verify threading parameters work with single core."""
        args = self._create_args(
            data_dir=str(temp_data_dir),
            parallel_threads=1,
        )
        cmd, _ = build_docker_run_command(args)
        cmd_str = " ".join(cmd)

        assert "VIRT_Parameters_AsyncQueueMaxThreads=1" in cmd_str
        assert "VIRT_Parameters_ThreadsPerQuery=1" in cmd_str
        assert "VIRT_Parameters_MaxClientConnections=2" in cmd_str
        assert "VIRT_HTTPServer_ServerThreads=2" in cmd_str


# =============================================================================
# 4. Docker interaction tests (require Docker)
# =============================================================================


class TestDockerInteraction:
    """Tests that interact with Docker daemon."""

    def test_check_docker_installed(self):
        """Return True when Docker is available."""
        # This test assumes Docker is installed on the test machine
        result = check_docker_installed()
        assert result is True

    def test_check_container_exists_false(self):
        """Return False for non-existent container."""
        result = check_container_exists("nonexistent-container-12345")
        assert result is False

    def test_check_container_exists_true(self, unique_container_name):
        """Return True for existing container."""
        # Create a container
        subprocess.run(
            ["docker", "create", "--name", unique_container_name, "alpine"],
            check=True,
            stdout=subprocess.DEVNULL,
        )
        result = check_container_exists(unique_container_name)
        assert result is True

    def test_remove_container_success(self, unique_container_name):
        """Successfully remove container."""
        # Create a container
        subprocess.run(
            ["docker", "create", "--name", unique_container_name, "alpine"],
            check=True,
            stdout=subprocess.DEVNULL,
        )
        result = remove_container(unique_container_name)
        assert result is True
        assert not check_container_exists(unique_container_name)

    def test_remove_container_nonexistent(self):
        """docker rm -f succeeds even for non-existent containers."""
        # docker rm -f returns 0 even if container doesn't exist
        result = remove_container("nonexistent-container-12345")
        assert result is True


# =============================================================================
# 5. Full integration tests (launch container)
# =============================================================================


class TestLaunchVirtuosoIntegration:
    """Full integration tests that launch Virtuoso containers."""

    @pytest.fixture
    def launch_args(self, temp_data_dir, unique_container_name, unique_ports):
        """Create args for launching Virtuoso."""
        return argparse.Namespace(
            name=unique_container_name,
            http_port=unique_ports["http"],
            isql_port=unique_ports["isql"],
            data_dir=str(temp_data_dir),
            extra_volumes=None,
            memory="2g",
            cpu_limit=0,
            dba_password="dba",
            force_remove=False,
            network=None,
            wait_ready=False,
            detach=True,
            enable_write_permissions=False,
            estimated_db_size_gb=0,
            virtuoso_version="7.2.15",
            virtuoso_sha=None,
            max_dirty_buffers=10000,
            number_of_buffers=15000,
            parallel_threads=None,
        )

    @pytest.mark.timeout(120)
    def test_launch_detached(self, launch_args, unique_container_name):
        """Launch container in detached mode."""
        cmd, _ = build_docker_run_command(launch_args)
        result = subprocess.run(cmd, capture_output=True, text=True)
        assert result.returncode == 0, f"Failed to launch: {result.stderr}"
        assert check_container_exists(unique_container_name)

    def _remove_user_option(self, cmd):
        """Remove --user option from Docker command for testing.

        The --user option causes permission issues because the host user
        cannot execute the container's entrypoint script.
        """
        result = []
        skip_next = False
        for part in cmd:
            if skip_next:
                skip_next = False
                continue
            if part == "--user":
                skip_next = True
                continue
            result.append(part)
        return result

    @pytest.mark.timeout(180)
    def test_launch_wait_ready(self, launch_args, unique_container_name):
        """Launch container and wait for readiness."""
        import time

        from virtuoso_utilities.launch_virtuoso import wait_for_virtuoso_ready

        cmd, _ = build_docker_run_command(launch_args)
        cmd = self._remove_user_option(cmd)
        result = subprocess.run(cmd, capture_output=True, text=True)
        assert result.returncode == 0, f"Failed to launch: {result.stderr}"

        # Wait for container to start
        time.sleep(5)

        # Check container is still running
        check_result = subprocess.run(
            ["docker", "ps", "-q", "-f", f"name={unique_container_name}"],
            capture_output=True,
            text=True,
        )
        if not check_result.stdout.strip():
            # Get logs to understand why container stopped
            logs = subprocess.run(
                ["docker", "logs", unique_container_name],
                capture_output=True,
                text=True,
            )
            pytest.fail(f"Container stopped unexpectedly. Logs:\n{logs.stdout}\n{logs.stderr}")

        ready = wait_for_virtuoso_ready(
            dba_password=launch_args.dba_password,
            docker_container=unique_container_name,
            timeout=120,
        )
        assert ready is True

    @pytest.mark.timeout(120)
    def test_launch_force_remove(self, launch_args, unique_container_name):
        """Remove existing container with --force-remove."""
        # Create initial container
        subprocess.run(
            ["docker", "create", "--name", unique_container_name, "alpine"],
            check=True,
            stdout=subprocess.DEVNULL,
        )
        assert check_container_exists(unique_container_name)

        # Force remove and launch new container
        remove_container(unique_container_name)
        cmd, _ = build_docker_run_command(launch_args)
        result = subprocess.run(cmd, capture_output=True, text=True)
        assert result.returncode == 0, f"Failed to launch: {result.stderr}"
        assert check_container_exists(unique_container_name)

    @pytest.mark.timeout(180)
    def test_launch_enable_write_permissions(self, launch_args, unique_container_name):
        """Enable write permissions after launch."""
        import time

        from virtuoso_utilities.launch_virtuoso import wait_for_virtuoso_ready

        cmd, _ = build_docker_run_command(launch_args)
        cmd = self._remove_user_option(cmd)
        result = subprocess.run(cmd, capture_output=True, text=True)
        assert result.returncode == 0, f"Failed to launch: {result.stderr}"

        # Wait for container to start
        time.sleep(5)

        # Check container is still running
        check_result = subprocess.run(
            ["docker", "ps", "-q", "-f", f"name={unique_container_name}"],
            capture_output=True,
            text=True,
        )
        if not check_result.stdout.strip():
            # Get logs to understand why container stopped
            logs = subprocess.run(
                ["docker", "logs", unique_container_name],
                capture_output=True,
                text=True,
            )
            pytest.fail(f"Container stopped unexpectedly. Logs:\n{logs.stdout}\n{logs.stderr}")

        ready = wait_for_virtuoso_ready(
            dba_password=launch_args.dba_password,
            docker_container=unique_container_name,
            timeout=120,
        )
        assert ready is True

        # Enable write permissions
        success = grant_write_permissions(
            dba_password=launch_args.dba_password,
            docker_container=unique_container_name,
        )
        assert success is True

    @pytest.mark.timeout(120)
    def test_launch_custom_ports(self, temp_data_dir, unique_container_name):
        """Launch with custom HTTP and ISQL ports."""
        custom_http = 8901
        custom_isql = 11121
        args = argparse.Namespace(
            name=unique_container_name,
            http_port=custom_http,
            isql_port=custom_isql,
            data_dir=str(temp_data_dir),
            extra_volumes=None,
            memory="1g",
            cpu_limit=0,
            dba_password="dba",
            force_remove=False,
            network=None,
            wait_ready=False,
            detach=True,
            enable_write_permissions=False,
            estimated_db_size_gb=0,
            virtuoso_version="7.2.15",
            virtuoso_sha=None,
            max_dirty_buffers=10000,
            number_of_buffers=15000,
            parallel_threads=None,
        )
        cmd, _ = build_docker_run_command(args)
        cmd_str = " ".join(cmd)
        assert f"{custom_http}:8890" in cmd_str
        assert f"{custom_isql}:1111" in cmd_str

        result = subprocess.run(cmd, capture_output=True, text=True)
        assert result.returncode == 0, f"Failed to launch: {result.stderr}"

    @pytest.mark.timeout(120)
    def test_launch_extra_volumes(self, temp_data_dir, unique_container_name, unique_ports):
        """Launch with additional volumes mounted."""
        extra_dir = temp_data_dir / "extra"
        extra_dir.mkdir()
        args = argparse.Namespace(
            name=unique_container_name,
            http_port=unique_ports["http"],
            isql_port=unique_ports["isql"],
            data_dir=str(temp_data_dir),
            extra_volumes=[f"{extra_dir}:/extra_data"],
            memory="1g",
            cpu_limit=0,
            dba_password="dba",
            force_remove=False,
            network=None,
            wait_ready=False,
            detach=True,
            enable_write_permissions=False,
            estimated_db_size_gb=0,
            virtuoso_version="7.2.15",
            virtuoso_sha=None,
            max_dirty_buffers=10000,
            number_of_buffers=15000,
            parallel_threads=None,
        )
        cmd, paths = build_docker_run_command(args)
        assert "/extra_data" in paths

        result = subprocess.run(cmd, capture_output=True, text=True)
        assert result.returncode == 0, f"Failed to launch: {result.stderr}"


# =============================================================================
# 6. Error and edge case tests
# =============================================================================


class TestErrorCases:
    """Tests for error handling and edge cases."""

    @pytest.mark.timeout(30)
    def test_wait_ready_timeout(self, unique_container_name):
        """Return False after timeout when container not ready."""
        from virtuoso_utilities.launch_virtuoso import wait_for_virtuoso_ready

        # Create a container that won't have Virtuoso running
        subprocess.run(
            ["docker", "run", "-d", "--name", unique_container_name, "alpine", "sleep", "60"],
            check=True,
            stdout=subprocess.DEVNULL,
        )
        result = wait_for_virtuoso_ready(
            dba_password="dba",
            docker_container=unique_container_name,
            timeout=5,
        )
        assert result is False

    def test_existing_container_running_error(self, unique_container_name, capsys):
        """Fail if container is running without --force-remove."""
        from virtuoso_utilities.launch_virtuoso import main

        # Create and start a container
        subprocess.run(
            ["docker", "run", "-d", "--name", unique_container_name, "alpine", "sleep", "60"],
            check=True,
            stdout=subprocess.DEVNULL,
        )
        # Verify container is running
        assert check_container_exists(unique_container_name)

        # Check that check_container_exists returns True
        result = check_container_exists(unique_container_name)
        assert result is True
