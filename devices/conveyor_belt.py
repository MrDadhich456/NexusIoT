from .base_device import BaseDevice


class ConveyorBelt(BaseDevice):
    def __init__(self, device_id="belt-001"):
        super().__init__(device_id, "conveyor_belt")
        self._runtime_hours = 0.0

    def read(self) -> dict:
        self._runtime_hours += 0.0005
        return {
            "belt_speed_mps": self.inject_anomaly(self.add_noise(1.5)),
            "motor_current_a": self.inject_anomaly(
                self.add_noise(12.0 + self._runtime_hours * 0.5)
            ),
            "belt_tension_n": self.inject_anomaly(
                self.add_noise(350 + self._runtime_hours * 5), spike_factor=4.0
            ),
            "roller_temp_c": self.add_noise(55 + self._runtime_hours * 2),
            "items_per_min": self.add_noise(42),
            "runtime_hours": round(self._runtime_hours, 4),
        }


if __name__ == "__main__":
    ConveyorBelt().run()
