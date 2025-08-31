import os
import yaml

yaml_config = None

def get_yaml_config():
    global yaml_config
    if yaml_config is None:
        # Get absolute path of the current file
        base_dir = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(base_dir, "config.yaml")
        
        with open(config_path, "r") as yaml_file:
            yaml_config = yaml.safe_load(yaml_file)
    return yaml_config
