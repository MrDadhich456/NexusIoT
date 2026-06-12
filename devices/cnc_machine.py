from .base_device import BaseDevice
import math, time

class CNCMachine(BaseDevice):
    def __init__(self, device_id="cnc-001"):
        super().__init__(device_id, "cnc_machine")
        self._tool_wear = 0.0         # accumulates over time
        self._cycle_count = 0

    def read(self) -> dict:
        self._tool_wear += 0.001      # gradual wear drift
        self._cycle_count += 1
        base_rpm = 3000

        return {
            "spindle_rpm": self.inject_anomaly(
                self.add_noise(base_rpm - self._tool_wear * 10)
            ),
            "vibration_g": self.inject_anomaly(
                self.add_noise(0.8 + self._tool_wear * 2), spike_factor=8
            ),
            "tool_wear_pct": min(self._tool_wear * 100, 100),
            "feed_rate_mmpm": self.add_noise(500),
            "cycle_count": self._cycle_count,
            "cutting_temp_c": self.inject_anomaly(
                self.add_noise(280 + self._tool_wear * 50)
            )
        }

if __name__ == "__main__":
    CNCMachine().run()
