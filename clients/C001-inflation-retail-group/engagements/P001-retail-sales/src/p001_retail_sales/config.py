"""
Configuration loading for P001.

Configuration precedence:

    base.yaml + <environment>.yaml
    = effective configuration
"""
from urllib.parse import urlparse
from copy import deepcopy
from pathlib import Path
from typing import Any

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_DIR = PROJECT_ROOT / 'config'


class ConfigError(RuntimeError):
    """
    Raised when P001 runtime configuration is missing or invalid.
    """


def _load_yaml(path: Path) -> dict[str, Any]:
    """
    Load one YAML configuration file.
    """
    if not path.exists():
        raise ConfigError(f'Configuration file does not exist: {path}')

    with path.open('r', encoding='utf-8') as file:
        data = yaml.safe_load(file) or {}

    if not isinstance(data, dict):
        raise ConfigError(f'Configuration root must be a mapping: {path}')

    return data


def _deep_merge(
    base: dict[str, Any],
    override: dict[str, Any],
) -> dict[str, Any]:
    """
    Recursively merge environment overrides into base configuration.

    Nested mappings are merged rather than replaced wholesale.

    Example:

        base:
            logging:
                format: json
        
        dev:
            logging:
                level: DEBUG
        
        result:
            logging:
                format: json
                level: DEBUG
    """
    result = deepcopy(base)

    for key, val in override.items():
        if isinstance(val, dict) and isinstance(result.get(key), dict):
            result[key] = _deep_merge(result[key], val)
        else:
            result[key] = deepcopy(val)

    return result


def _is_uri(value: str) -> bool:
    """
    Return True when a configured location uses a URI scheme.

    Examples:
        gs://bucket/path
        s3://bucket/path
        file:///tmp/path
    """
    parsed = urlparse(value)

    return bool(parsed.scheme and '://' in value)


def is_uri_location(
    value: str | Path,
) -> bool:
    """
    Return True when a runtime location uses a URI scheme.
    """
    return _is_uri(str(value))


def join_location(
    root: str | Path,
    *parts: str,
) -> str:
    """
    Join child components onto either a local filesystem location
    or a URI-based location.

    Examples:

        C:/data + sales + day1.csv
        -> C:/data/sales/day1.csv

        gs://bucket/input + sales + day1.csv
        -> gs://bucket/input/sales/day1.csv
    """
    root_value = str(root)

    if _is_uri(root_value):
        clean_root = root_value.rstrip('/')
        clean_parts = [
            part.strip('/')
            for part in parts
        ]

        return '/'.join(
            [clean_root, *clean_parts]
        )

    return str(
        Path(root_value).joinpath(*parts)
    )


def _resolve_paths(
    config: dict[str, Any],
    project_root: Path,
) -> dict[str, Any]:
    """
    Normalize configured runtime locations.

    Local relative paths are resolved relative to the P001 engagement root.

    URI-based locations such as `gs://...` are preserved unchanged because
    they are not local filesystem paths.
    """
    result = deepcopy(config)
    paths = result.get('paths')

    if paths is None:
        return result

    if not isinstance(paths, dict):
        raise ConfigError('Configuration key "paths" must be a mapping.')

    for key, val in paths.items():
        if val is None:
            continue
        if not isinstance(val, str):
            raise ConfigError(
                f'Configured path "{key}" must be a string or null.'
            )

        # Cloud/object-storage locations are already absolute logical
        # locations. pathlib must not reinterpret them as local paths.
        if _is_uri(val):
            result['paths'][key] = val
            continue

        path = Path(val)

        if not path.is_absolute():
            path = (project_root / path).resolve()

        result['paths'][key] = str(path)

    return result


def load_config(
    environment: str,
    config_dir: str | Path | None = None,
) -> dict[str, Any]:
    """
    Load the effective configuration for one environment.
    """
    if config_dir is None:
        config_dir = DEFAULT_CONFIG_DIR
    else:
        config_dir = Path(config_dir).resolve()

    config_dir = Path(config_dir)

    base_config = _load_yaml(config_dir / 'base.yaml')
    environment_config = _load_yaml(config_dir / f'{environment}.yaml')

    config = _deep_merge(base_config, environment_config)

    configured_environment = config.get('environment')

    if configured_environment != environment:
        raise ConfigError(
            'Environment mismatch: '
            f'requested "{environment}" but configuration declares '
            f'"{configured_environment}".'
        )

    # config/ is directly inside the engagement root.
    project_root = config_dir.parent

    return _resolve_paths(config, project_root)


def require_location(config: dict[str, Any], name: str) -> str:
    """
    Return one required configured runtime location.

    A location may be either:

        C:/...              local filesystem
        /...                local filesystem
        gs://bucket/...     cloud object storage
    """
    value = config.get('paths', {}).get(name)

    if not value:
        raise ConfigError(f'Required configured path is missing: paths.{name}')

    if not isinstance(value, str):
        raise ConfigError(f'Configured path "{name}" must be a string.')

    return value


def require_path(config: dict[str, Any], name: str) -> Path:
    """
    Return one required LOCAL filesystem path.

    URI-based locations must use require_location() instead.
    """
    value = require_location(config, name)

    if _is_uri(value):
        raise ConfigError(
            f'Configured location is not a local filesystem path: '
            f'paths.{name}'
        )

    return Path(value)
