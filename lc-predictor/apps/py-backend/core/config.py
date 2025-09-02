import yaml
import os

_yaml_config = None

def get_yaml_config():
    """
    Loads config.yaml safely and caches it.
    :return: dict
    """
    global _yaml_config
    if _yaml_config is None:
        config_path = os.path.join(os.path.dirname(__file__), "config.yaml")
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                _yaml_config = yaml.safe_load(f)
        except FileNotFoundError:
            raise FileNotFoundError(f"Config file not found: {config_path}")
        except yaml.YAMLError as e:
            raise ValueError(f"Error parsing YAML: {e}")
    return _yaml_config
