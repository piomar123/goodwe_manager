import asyncio
import goodwe
import logging
from pprint import pprint


async def get_runtime_data():
    ip_address = '192.168.1.106'

    inverter = await goodwe.connect(ip_address, retries=60)
    print("---------------- sensors ----------------")
    pprint(inverter.sensors())
    print("---------------- settings ----------------")
    pprint(inverter.settings())
    print("---------------- read_settings_data ----------------")
    pprint(await inverter.read_settings_data())
    print("---------------- read_runtime_data ----------------")
    # pprint(await inverter.read_runtime_data())

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    asyncio.run(get_runtime_data())
