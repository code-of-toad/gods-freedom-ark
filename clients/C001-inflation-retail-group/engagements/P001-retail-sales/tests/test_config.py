"""
Tests for P001 runtime configuration.
"""

from pathlib import Path

import pytest

from p001_retail_sales.config import (
    ConfigError,
    is_uri_location,
    join_location,
    load_config,
    require_location,
    require_path,
)
from p001_retail_sales.__main__ import (
    _parse_args,
    _require_input_file,
)


def _write(
    path: Path,
    content: str,
) -> None:
    path.write_text(
        content,
        encoding='utf-8',
    )


def test_config_merges_base_and_environment(
    tmp_path,
):
    config_dir = (
        tmp_path
        / 'config'
    )

    config_dir.mkdir()

    _write(
        config_dir / 'base.yaml',
        """
client_id: C001
engagement_id: P001

logging:
  format: json

spark:
  app_name: p001-retail-sales
""",
    )

    _write(
        config_dir / 'dev.yaml',
        """
environment: dev

logging:
  level: DEBUG

spark:
  master: local[2]

paths:
  input: ./data/input
  output: ./data/output
  quarantine: ./data/quarantine
""",
    )

    config = load_config(
        'dev',
        config_dir=config_dir,
    )

    assert config['client_id'] == 'C001'
    assert config['engagement_id'] == 'P001'

    # Nested configuration must merge rather than replace.
    assert config['logging']['format'] == 'json'
    assert config['logging']['level'] == 'DEBUG'

    assert (
        config['spark']['app_name']
        == 'p001-retail-sales'
    )

    assert (
        config['spark']['master']
        == 'local[2]'
    )


def test_config_resolves_paths_relative_to_project_root(
    tmp_path,
):
    config_dir = (
        tmp_path
        / 'config'
    )

    config_dir.mkdir()

    _write(
        config_dir / 'base.yaml',
        """
client_id: C001
""",
    )

    _write(
        config_dir / 'dev.yaml',
        """
environment: dev

paths:
  input: ./data/input
  output: ./data/output
  quarantine: ./data/quarantine
""",
    )

    config = load_config(
        'dev',
        config_dir=config_dir,
    )

    assert require_path(
        config,
        'input',
    ) == (
        tmp_path
        / 'data'
        / 'input'
    ).resolve()

    assert require_path(
        config,
        'output',
    ) == (
        tmp_path
        / 'data'
        / 'output'
    ).resolve()


def test_config_rejects_environment_mismatch(
    tmp_path,
):
    config_dir = (
        tmp_path
        / 'config'
    )

    config_dir.mkdir()

    _write(
        config_dir / 'base.yaml',
        """
client_id: C001
""",
    )

    _write(
        config_dir / 'dev.yaml',
        """
environment: test
""",
    )

    with pytest.raises(
        ConfigError,
        match='Environment mismatch',
    ):
        load_config(
            'dev',
            config_dir=config_dir,
        )


def test_require_path_rejects_null_path():
    config = {
        'paths': {
            'output': None,
        },
    }

    with pytest.raises(
        ConfigError,
        match='paths.output',
    ):
        require_path(
            config,
            'output',
        )


def test_cli_parses_dev_batch_arguments():
    args = _parse_args([
        '--env',
        'dev',
        '--sales-file',
        'sales_2026-10-01.csv',
        '--run-id',
        'run-001',
    ])

    assert args.environment == 'dev'
    assert args.sales_file == 'sales_2026-10-01.csv'
    assert args.run_id == 'run-001'


def test_config_preserves_cloud_locations(
    tmp_path,
):
    config_dir = (
        tmp_path
        / 'config'
    )

    config_dir.mkdir()

    _write(
        config_dir / 'base.yaml',
        """
client_id: C001
""",
    )

    _write(
        config_dir / 'prod.yaml',
        """
environment: prod

paths:
  input: gs://example-bucket/C001/P001/input
  output: gs://example-bucket/C001/P001/output
  quarantine: gs://example-bucket/C001/P001/quarantine
""",
    )

    config = load_config(
        'prod',
        config_dir=config_dir,
    )

    assert require_location(
        config,
        'input',
    ) == 'gs://example-bucket/C001/P001/input'

    assert require_location(
        config,
        'output',
    ) == 'gs://example-bucket/C001/P001/output'

    assert require_location(
        config,
        'quarantine',
    ) == 'gs://example-bucket/C001/P001/quarantine'


def test_require_path_rejects_cloud_uri():
    config = {
        'paths': {
            'input': 'gs://example-bucket/input',
        },
    }

    with pytest.raises(
        ConfigError,
        match='not a local filesystem path',
    ):
        require_path(
            config,
            'input',
        )


def test_join_location_joins_local_path(
    tmp_path,
):
    root = tmp_path / 'input'

    result = join_location(
        root,
        'sales',
        'sales-day1.csv',
    )

    assert Path(result) == (
        root
        / 'sales'
        / 'sales-day1.csv'
    )


def test_join_location_joins_cloud_uri():
    result = join_location(
        'gs://example-bucket/C001/P001/input',
        'sales',
        'sales-day1.csv',
    )

    assert result == (
        'gs://example-bucket/'
        'C001/P001/input/'
        'sales/sales-day1.csv'
    )


def test_join_location_handles_trailing_slashes():
    result = join_location(
        'gs://example-bucket/input/',
        '/sales/',
        '/sales-day1.csv',
    )

    assert result == (
        'gs://example-bucket/input/'
        'sales/sales-day1.csv'
    )


def test_is_uri_location_detects_cloud_uri():
    assert is_uri_location(
        'gs://example-bucket/input'
    )

    assert not is_uri_location(
        './data/dev/input'
    )


def test_cli_input_check_allows_remote_uri():
    _require_input_file(
        'gs://example-bucket/input/products.csv'
    )
