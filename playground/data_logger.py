import asyncio
from datetime import datetime

import goodwe
import csv

selected_sensors = [
    'timestamp',
    'ppv',
    'pgrid',
    'pgrid2',
    'pgrid3',
    'vgrid',
    'igrid',
    'fgrid',
    'vgrid2',
    'igrid2',
    'fgrid2',
    'vgrid3',
    'igrid3',
    'fgrid3',
    'total_inverter_power',
    'active_power',
    'reactive_power',
    'apparent_power',
    'load_mode1',
    'load_mode2',
    'load_mode3',
    'load_p1',
    'load_p2',
    'load_p3',
    'load_ptotal',
    'active_power1',
    'active_power2',
    'active_power3',
    'active_power_total',
    'reactive_power_total',
    'meter_active_power1',
    'meter_active_power2',
    'meter_active_power3',
    'meter_active_power_total',
    'meter_reactive_power1',
    'meter_reactive_power2',
    'meter_reactive_power3',
    'meter_reactive_power_total',
    'meter_apparent_power1',
    'meter_apparent_power2',
    'meter_apparent_power3',
    'meter_apparent_power_total',
    'meter_type',
    'backup_p1',
    'backup_p2',
    'backup_p3',
    'backup_ptotal',
    'ups_load',
    'temperature_air',
    'temperature',
    'vbattery1',
    'pbattery1',
    'battery_mode_label',
    'battery_temperature',
    'battery_soc',
    'warning_code',
    'diagnose_result_label'
]


async def get_runtime_data():
    ip_address = '192.168.1.106'

    inverter = await goodwe.connect(ip_address, retries=60)
    print(inverter.sensors())
    # sensors = {s.id_: s for s in inverter.sensors()}
    # inverter.settings()
    log_file_with_current_timestamp = 'data-' + datetime.now().strftime('%Y-%m-%d_%H-%M-%S') + '.csv'
    with open(log_file_with_current_timestamp, mode='w', newline='') as data_file:
        csv_writer = csv.writer(data_file, dialect='excel')
        csv_writer.writerow(selected_sensors)
        while True:
            try:
                runtime_data = await inverter.read_runtime_data()
                sensors_data = [runtime_data[sensor_id] for sensor_id in selected_sensors]
                print(sensors_data)
                csv_writer.writerow(sensors_data)
                await asyncio.sleep(1)
            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"Error: {e}")
                await asyncio.sleep(1)

if __name__ == '__main__':
    try:
        asyncio.run(get_runtime_data())
    except KeyboardInterrupt:
        pass
