import argparse
import asyncio
import logging
import os
from pprint import pprint

import dotenv
import goodwe

dotenv.load_dotenv()


async def get_runtime_data(ip_address: str):
    inverter = await goodwe.connect(ip_address, retries=60)
    print("---------------- sensors ----------------")
    pprint(inverter.sensors())
    print("---------------- settings ----------------")
    pprint(inverter.settings())
    print("---------------- read_settings_data ----------------")
    pprint(await inverter.read_settings_data())
    print("---------------- read_runtime_data ----------------")
    # pprint(await inverter.read_runtime_data())


def main():
    parser = argparse.ArgumentParser(description="Dump inverter sensors/settings for debugging")
    parser.add_argument('--ip', help="Inverter IP address (defaults to INVERTER_IP from .env)",
                        default=os.environ.get('INVERTER_IP'))
    args = parser.parse_args()
    assert args.ip, "No IP address given: pass --ip or set INVERTER_IP in .env"
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    asyncio.run(get_runtime_data(args.ip))


if __name__ == '__main__':
    main()
