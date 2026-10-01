"""
Imported before any test module. main.py loads .env at import time, and in a
deployed checkout that's the production config - blank everything that would
reach real systems, so a test run there can't publish to the real MQTT
broker, start battery control, or poll the BMS. load_dotenv() never
overrides variables that are already set, so these win over .env.
"""
import os

os.environ['INVERTER_IP'] = '127.0.0.1'
for _key in ('MQTT_HOST', 'CONTROL_MODE', 'BMS_LOGGER_HOST', 'BMS_LOGGER_SERIAL'):
    os.environ[_key] = ''
