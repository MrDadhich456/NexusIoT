from .base_device import BaseDevice
import math, random

class RoboticArm(BaseDevice):
    def __init__(self, device_id="arm-001"):
        super().__init__(device_id, "robotic_arm")
        self._cycle = 0

    def read(self) -> dict:
        self._cycle += 1
        t = self._cycle * 0.1
        return {
            "joint1_torque_nm": self.inject_anomaly(
                self.add_noise(45 + 5 * math.sin(t))
            ),
            "joint2_torque_nm": self.inject_anomaly(
                self.add_noise(32 + 3 * math.cos(t))
            ),
            "joint_temp_c": self.add_noise(68 + 2 * math.sin(t/2)),
            "position_error_mm": self.inject_anomaly(
                abs(self.add_noise(0.12)), probability=0.04, spike_factor=20
            ),
            "cycles_completed": self._cycle,
            "servo_current_a": self.add_noise(8.4 + 1.2 * abs(math.sin(t)))
        }

if __name__ == "__main__":
    RoboticArm().run()
