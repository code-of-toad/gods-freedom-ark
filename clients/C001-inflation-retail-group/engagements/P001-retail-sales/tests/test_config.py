"""
Tests for P001 runtime configuration.
"""

from pathlib import Path

import pytest

from p001_retail_sales.config import (
    ConfigError,
    load_config,
    require_path,
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


from p001_retail_sales.__main__ import _parse_args


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
