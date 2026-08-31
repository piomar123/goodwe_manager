"""
Sensor domain definitions shared by main.py, storage.py, and the CSV
migration script. Deliberately has no import-time side effects (no env var
requirements) so it can be imported safely from tests and utility scripts.
"""
from typing import Any, Mapping, Optional

SELECTED_SENSORS = [
    'timestamp',
    'ppv',
    'ppv1',
    'ppv2',
    'vpv1',
    'vpv2',
    'ipv1',
    'ipv2',
    'pv1_mode_label',
    'pv2_mode_label',
    'function_bit',
    'bus_voltage',
    'nbus_voltage',
    'operation_mode',
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
    'meter_freq',
    'grid_mode',
    'grid_mode_label',
    'grid_in_out',
    'grid_in_out_label',
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
    'house_consumption',
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
    'meter_power_factor1',
    'meter_power_factor2',
    'meter_power_factor3',
    'meter_power_factor',
    'meter_type',
    'backup_p1',
    'backup_p2',
    'backup_p3',
    'backup_ptotal',
    'backup_v1',
    'backup_v2',
    'backup_v3',
    'backup_i1',
    'backup_i2',
    'backup_i3',
    'backup_f1',
    'backup_f2',
    'backup_f3',
    'ups_load',
    'temperature_air',
    'temperature',
    'vbattery1',
    'ibattery1',
    'pbattery1',
    'battery_mode_label',
    'battery_temperature',
    'battery_soc',
    'battery_charge_limit',
    'battery_discharge_limit',
    'battery_error',
    'battery_warning',
    'warning_code',
    'diagnose_result_label',
    'error_codes',
    'errors',
    'e_total_exp',
    'e_total_imp',
    'e_day',
    'e_load_total',
    'meter_e_total_exp',
    'meter_e_total_imp',
    'e_bat_charge_total',
    'e_bat_discharge_total',
    'work_mode_label',
    'rssi',
]

# Columns whose values are text labels/codes, not continuous numeric
# measurements. Everything else in SELECTED_SENSORS is stored as REAL.
TEXT_SENSOR_COLUMNS = {
    'timestamp',
    'pv1_mode_label',
    'pv2_mode_label',
    'grid_mode_label',
    'grid_in_out_label',
    'battery_mode_label',
    'diagnose_result_label',
    'work_mode_label',
    'error_codes',
    'errors',
}

CALCULATED_VALUE_HEADERS = [
    '_hour_start_timestamp',
    '_hourly_meter_export',
    '_hourly_meter_import',
    '_hourly_load',
]
TEXT_CALCULATED_COLUMNS = {'_hour_start_timestamp'}


def sensor_columns() -> list:
    """Ordered (column_name, sql_type) pairs for the inverter_history table,
    in the same order as SELECTED_SENSORS + CalculatedValuesEvaluator.headers().
    """
    columns = []
    for name in SELECTED_SENSORS:
        columns.append((name, 'TEXT' if name in TEXT_SENSOR_COLUMNS else 'REAL'))
    for name in CALCULATED_VALUE_HEADERS:
        columns.append((name, 'TEXT' if name in TEXT_CALCULATED_COLUMNS else 'REAL'))
    return columns


class CalculatedValuesEvaluator:
    def __init__(self):
        self._hour_start_sensors = None

    def calculate_values(self, sensors_data: Mapping[str, Any]) -> dict:
        if self._hour_start_sensors is None or sensors_data['timestamp'][:13] != self._hour_start_sensors['timestamp'][
                                                                                 :13]:
            self._hour_start_sensors = sensors_data
        calculated_values = {
            '_hour_start_timestamp': self._hour_start_sensors['timestamp'],
            '_hourly_meter_export': f"{float(sensors_data['meter_e_total_exp']) - float(self._hour_start_sensors['meter_e_total_exp']):.2f}",
            '_hourly_meter_import': f"{float(sensors_data['meter_e_total_imp']) - float(self._hour_start_sensors['meter_e_total_imp']):.2f}",
            '_hourly_load': f"{float(sensors_data['e_load_total']) - float(self._hour_start_sensors['e_load_total']):.1f}",
        }
        self._verify_header(calculated_values)
        return calculated_values

    def seed_hour_start(self, sensors_data: Optional[Mapping[str, Any]]) -> None:
        """Restores the hour-start baseline from a previously stored sample
        (see storage.get_current_hour_start_sample*), typically called once
        at startup. Passing None leaves the evaluator in its cold-start
        state, where the next incoming sample becomes the new baseline -
        the correct behavior when no sample exists for the current hour
        yet (empty DB, or the last sample predates the current hour).
        """
        self._hour_start_sensors = dict(sensors_data) if sensors_data is not None else None

    @staticmethod
    def headers():
        return CALCULATED_VALUE_HEADERS

    def _verify_header(self, calculated_values):
        for header, key in zip(self.headers(), calculated_values.keys()):
            if header != key:
                raise AssertionError(f"Implementation error: headers do not correspond to set keys: {key} != {header}, "
                                     f"{self.headers()} != {calculated_values.keys()}")
