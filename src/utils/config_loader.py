import yaml
from pathlib import Path

def load_config(filename="config.yml") -> dict:
    config_path = Path(__file__).resolve().parents[1] / filename
    with open(config_path, "r") as f:
        return yaml.safe_load(f)

if __name__=='__main__':
    config = load_config()