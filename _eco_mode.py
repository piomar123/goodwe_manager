"""
Experiments with Eco mode and available sensors
"""
import asyncio
from pprint import pprint

import goodwe
from goodwe.protocol import ProtocolResponse
from goodwe.sensor import EcoModeV2

from eco_encoder import encode_schedule

EVERY_DAY = 0b1111111


async def get_runtime_data():
    ip_address = '192.168.1.106'

    print("Connecting to the inverter...")
    inverter = await goodwe.connect(ip_address, retries=60)
    pprint(inverter.sensors())
    print("----------------")
    # pprint(inverter.settings())
    # print("---------------- read_device_info ----------------")
    # pprint(await inverter.read_device_info())
    # print("---------------- read_runtime_data ----------------")
    # pprint(await inverter.read_runtime_data())
    # print("---------------- read_settings_data ----------------")
    # pprint(await inverter.read_settings_data())

    ################################
    ################################
    # print("Writing setting")
    # await inverter.write_setting('battery_discharge_depth', 20)
    ################################
    ################################

    selected_settings = [
        'unbalanced_output',
        'grid_export',  # actually, should be grid_export_limit_switch
        'grid_export_limit',
        'battery_discharge_depth',
        'battery_discharge_depth_offline',
        'bms_bat_charge_i_max',
        'bms_status',
        'dod_holding',
        'fast_charging',
        'fast_charging_power',
        # 'work_mode',
    ]
    # await inverter.write_setting('grid_export', 0)
    # await inverter.write_setting('unbalanced_output', 1)
    # for setting in selected_settings:
    #     print(f"{setting} = {await inverter.read_setting(setting)}")
    runtime_data = await inverter.read_runtime_data()
    print(f"{runtime_data.get('battery_max_cell_temp_id')}")
    print(f"{runtime_data.get('battery_min_cell_temp_id')}")
    print(f"{runtime_data.get('battery_max_cell_voltage_id')}")
    print(f"{runtime_data.get('battery_min_cell_voltage_id')}")
    print(f"{runtime_data.get('battery_max_cell_temp')}")
    print(f"{runtime_data.get('battery_min_cell_temp')}")
    print(f"{runtime_data.get('battery_max_cell_voltage')}")
    print(f"{runtime_data.get('battery_min_cell_voltage')}")
    print(f"{runtime_data.get('battery_modules')}")
    print(f"{runtime_data.get('battery_charge_limit')}")
    print(f"{runtime_data.get('battery_discharge_limit')}")
    print(f"{runtime_data.get('battery_status')}")
    print(f"{runtime_data.get('battery_bms')}")
    # print(await inverter.get_operation_mode())

    # for i in range(1, 5):
    #     eco_mode_id = f'eco_mode_{i}'
    #     eco_mode_entry: EcoModeV2 = await inverter.read_setting(eco_mode_id)
    #     print(f"{eco_mode_id}: {str(eco_mode_entry)}")
    #
    # eco_mode_entries = await asyncio.gather(*[read_eco_entry(inverter, i) for i in range(1, 5)])
    # print("\n".join(eco_mode_entries))

    ################################
    # await write_eco_entry(inverter)
    ################################


async def read_eco_entry(inverter, i):
    eco_mode_id = f'eco_mode_{i}'
    eco_mode_entry: EcoModeV2 = await inverter.read_setting(eco_mode_id)
    return f"{eco_mode_id}: {str(eco_mode_entry)}"


async def write_eco_entry(inverter):
    # TODO ensure working mode is Eco before writing eco_mode_entry
    eco_mode_id = 'eco_mode_1'
    eco_mode_entry: EcoModeV2 = await inverter.read_setting(eco_mode_id)
    print(str(eco_mode_entry))
    # print(eco_mode_entry.day_bits)
    print(eco_mode_entry.on_off)
    eco_mode_entry.day_bits = EVERY_DAY
    eco_mode_entry.start_h = 13
    eco_mode_entry.start_m = 00
    eco_mode_entry.end_h = 14
    eco_mode_entry.end_m = 00
    eco_mode_entry.month_bits = 0
    eco_mode_entry.on_off = -1
    eco_mode_entry.power = 15
    eco_mode_entry.soc = 0
    encoded_bytes = encode_schedule(eco_mode_entry)
    print(encoded_bytes)
    # decoded = inverter._settings[eco_mode_id].read_value(ProtocolResponse(encoded_bytes, None))
    # print(str(decoded))
    await inverter.write_setting(eco_mode_id, encoded_bytes)
    eco_mode_entry: EcoModeV2 = await inverter.read_setting(eco_mode_id)
    print(str(eco_mode_entry))


if __name__ == '__main__':
    asyncio.run(get_runtime_data())
