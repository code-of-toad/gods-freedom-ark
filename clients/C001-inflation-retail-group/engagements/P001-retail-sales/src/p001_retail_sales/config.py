"""
Configuration loading for P001.

Configuration precedence:

    base.yaml + <environment>.yaml
    = effective configuration
"""
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


def _resolve_paths(
    config: dict[str, Any],
    project_root: Path,
) -> dict[str, Any]:
    """
    Convert configured relative runtime paths into absolute paths.

    Relative paths in the YAML files are interpreted relative to the
    P001 engagement root, not the caller's cwd.
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


def require_path(config: dict[str, Any], name: str) -> Path:
    """
    Return one required configured path.

    Null production placeholders are rejected at runtime rather than
    silently converted into invalid filesystem paths.
    """
    value = config.get('paths', {}).get(name)

    if not value:
        raise ConfigError(f'Required configured path is missing: paths.{name}')

    return Path(value)








