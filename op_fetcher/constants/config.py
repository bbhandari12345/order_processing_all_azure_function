from op_fetcher.constants.constant import CONFIG_FILE_PATH
from typing import Dict, Union, Tuple
import logging
import tomlkit
import os


logger = logging


def open_config_file(file_path: str) -> str:
    try:
        with open(file_path, 'r') as f:
            config_content = f.read()
    except Exception as e:
        raise Exception(e)
    return config_content


def parse_config() -> Dict:
    content = open_config_file(CONFIG_FILE_PATH)
    try:
        loaded_config = tomlkit.loads(content)
    except Exception as e:
        raise Exception(e)
    return loaded_config


def load_db_config(data: Union[Dict, None]) -> Union[Dict, None]:
    if data:
        for k, v in data.items():
            if os.getenv(v):
                data.update({
                    k: os.getenv(v)
                })
    else:
        logger.warning("DB config is missing or is invalid")
    return data


def main() -> Tuple[Dict, Dict]:
    config = parse_config()
    db_config = load_db_config(config.get("DB_CONFIG"))

    if not config.get('CONFIG_DIRECTORY'):
        logger.error("Directory for config file is missing. Please check config.toml file for CONFIG_DIRECTORY", exc_info=True)
        raise Exception("Directory for config file is missing")

    vendor_config_path = config.get('CONFIG_DIRECTORY') + config.get('VENDOR_CONFIG_PATH')
    netsuite_config_path = config.get('CONFIG_DIRECTORY') + config.get('NETSUITE_CONFIG_PATH')
    fetcher_write_path = config.get('FETCHER_WRITE_PATH')
    extractor_write_path = config.get('EXTRACTOR_WRITE_PATH')

    return db_config, vendor_config_path, netsuite_config_path, fetcher_write_path, extractor_write_path, config


if __name__ == "__main__":
    main()
