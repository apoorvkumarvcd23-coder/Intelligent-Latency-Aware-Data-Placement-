"""Tiny shared helper to load the central YAML config."""
import yaml


def load_config(path: str = "/app/config/config.yaml") -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)
