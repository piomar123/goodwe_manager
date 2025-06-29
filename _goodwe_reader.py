import asyncio
import goodwe
import logging
from pprint import pprint

from goodwe import ProtocolCommand, UdpInverterProtocol, InverterError


async def search_inverters() -> bytes:
    """Scan the network for inverters.
    Answer the inverter discovery response string (which includes it IP address)

    Raise InverterError if unable to contact any inverter
    """
    goodwe.logger.debug("Searching inverters by broadcast to port 48899")
    command = ProtocolCommand("WIFIKIT-214028-READ".encode("utf-8"), lambda r: True)
    try:
        result = await command.execute(UdpInverterProtocol("10.10.100.255", 48899, 1, 0))
        if result is not None:
            return result.response_data()
        else:
            raise InverterError("No response received to broadcast request.")
    except asyncio.CancelledError:
        raise InverterError("No valid response received to broadcast request.") from None

async def get_runtime_data():
    ip_address = '192.168.1.106'
    print(await search_inverters())

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