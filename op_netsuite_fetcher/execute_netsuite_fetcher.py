from op_netsuite_fetcher.order_processing.fetcher import NetsuiteFetcher
from op_netsuite_fetcher.constants import NETSUITE_CONFIG_PATH
from op_netsuite_fetcher.conf import get_logger
import time
import math

logger = get_logger()


def main() -> None:
    start = time.time()
    logger.info(
        "Starting process of fetching purchase orders from Netsuite"
    )
    NetsuiteFetcher(
        vendor_name=None,
        config_file_path=NETSUITE_CONFIG_PATH
    ).execute()

    end = time.time()
    logger.info(
        "The time elapsed in fetching purchase orders from Netsuite: %s seconds"
        % math.ceil(end - start)
    )
    return


if __name__ == "__main__":
    main()
