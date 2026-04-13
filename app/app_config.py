"""
Application config loader for YAML-based settings.
"""

import os
from typing import Any, Dict

import yaml


DEFAULT_CONFIG_PATH = os.path.join("config", "config.yaml")


def _as_dict(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    merged: Dict[str, Any] = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


DEFAULT_CONFIG: Dict[str, Any] = {
    "runtime": {
        "log_level": "INFO",
    },
    "mcp": {
        "endpoint": "",
    },
    "robot": {
        "ip": "",
        "port": 9000,
        "control_path": "/control",
        "timeout_seconds": 12.0,
    },
    "hcmut": {
        "enabled": True,
    },
}


def load_config() -> Dict[str, Any]:
    """
    Load app config from CONFIG_PATH or ./config/config.yaml with sane defaults.
    """
    path = os.getenv("CONFIG_PATH", DEFAULT_CONFIG_PATH)
    if not os.path.exists(path):
        return dict(DEFAULT_CONFIG)

    try:
        with open(path, "r", encoding="utf-8") as f:
            loaded = yaml.safe_load(f) or {}
    except Exception:
        return dict(DEFAULT_CONFIG)

    return _deep_merge(DEFAULT_CONFIG, _as_dict(loaded))
