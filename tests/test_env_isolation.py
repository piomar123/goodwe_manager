"""
tests/test_env_isolation.py
main.py loads .env at import. In a deployed checkout that is the production
config, so a stray test run there must still not reach the real MQTT broker
or start real battery control (2026-10-01: a test run in the Pi's live
directory published a fake off_grid state and set off a grid-outage alert).
"""
import os
import subprocess
import sys
import unittest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class EnvIsolationTest(unittest.TestCase):
    def test_production_settings_are_ignored_under_tests(self):
        # Environment variables stand in for a production .env: load_dotenv()
        # never overrides variables that are already set, so these behave the
        # same as .env values would.
        env = dict(os.environ, INVERTER_IP='10.10.100.253', MQTT_HOST='localhost', CONTROL_MODE='on',
                   CONTROL_CHARGE_CURRENT_A='19', CONTROL_DISCHARGE_CURRENT_A='19', CONTROL_MIN_SOC='10',
                   OFF_GRID_SHADOW_SCAN_GUARD='on',
                   BMS_LOGGER_HOST='192.168.1.221', BMS_LOGGER_SERIAL='4060493924')
        code = ("import tests, main; "
                "print(main.mqtt.enabled, main.CONTROL_CONFIG, main.SHADOW_SCAN_GUARD_CONFIG, main.INVERTER_IP, "
                "repr(os.environ.get('BMS_LOGGER_HOST')))")
        result = subprocess.run([sys.executable, '-c', 'import os; ' + code], cwd=REPO_ROOT,
                                capture_output=True, text=True, env=env)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip().splitlines()[-1], "False None None 127.0.0.1 ''")


if __name__ == '__main__':
    unittest.main()
