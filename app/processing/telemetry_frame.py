"""
Processing Module — TelemetryFrame

Binary frame layout (10 bytes):
  [0]     SYNC byte  = 0xAB
  [1]     satellite_id (uint8, 1–5)
  [2:6]   temperature (float32 big-endian, °C)
  [6:8]   voltage_mv  (uint16 big-endian, millivolts)
  [8]     battery_pct (uint8, 0–100)
  [9]     checksum    = XOR of bytes[0:9]
"""
from __future__ import annotations

import struct

FRAME_LEN = 10
SYNC_BYTE = 0xAB


class InvalidChecksumError(Exception):
    """Raised when a TelemetryFrame's XOR checksum does not match."""

    def __init__(self, expected: int, actual: int) -> None:
        self.expected = expected
        self.actual = actual
        super().__init__(
            f"Checksum mismatch: computed 0x{expected:02X}, got 0x{actual:02X}"
        )


class TelemetryFrame:
    """
    Parses and validates a raw 10-byte telemetry frame from a CubeSat.

    Raises:
        ValueError: if the byte array is the wrong length or missing the SYNC byte.
        InvalidChecksumError: if the XOR checksum is incorrect.
    """

    def __init__(self, raw: bytes) -> None:
        if len(raw) != FRAME_LEN:
            raise ValueError(
                f"Expected {FRAME_LEN} bytes, got {len(raw)}"
            )
        if raw[0] != SYNC_BYTE:
            raise ValueError(
                f"Missing SYNC byte: expected 0x{SYNC_BYTE:02X}, got 0x{raw[0]:02X}"
            )
        self._raw = raw
        self._validate_checksum()

    # ------------------------------------------------------------------ #
    # Public properties                                                    #
    # ------------------------------------------------------------------ #

    @property
    def satellite_id(self) -> int:
        return self._raw[1]

    @property
    def temperature_c(self) -> float:
        """Temperature in degrees Celsius (float32 big-endian)."""
        (value,) = struct.unpack_from(">f", self._raw, offset=2)
        return round(float(value), 2)

    @property
    def voltage_v(self) -> float:
        """Bus voltage in Volts (uint16 big-endian millivolts → V)."""
        (value_mv,) = struct.unpack_from(">H", self._raw, offset=6)
        return round(value_mv / 1000.0, 3)

    @property
    def battery_pct(self) -> int:
        """Battery state-of-charge as a percentage (0–100)."""
        return self._raw[8]

    # ------------------------------------------------------------------ #
    # Checksum validation                                                  #
    # ------------------------------------------------------------------ #

    def _validate_checksum(self) -> None:
        """XOR of bytes[0:9] must equal bytes[9]."""
        computed = self._compute_checksum()
        stored = self._raw[9]
        if computed != stored:
            raise InvalidChecksumError(expected=computed, actual=stored)

    def _compute_checksum(self) -> int:
        result = 0
        for byte in self._raw[:9]:
            result ^= byte
        return result

    # ------------------------------------------------------------------ #
    # Helpers                                                              #
    # ------------------------------------------------------------------ #

    def __repr__(self) -> str:
        return (
            f"TelemetryFrame(sat={self.satellite_id}, "
            f"temp={self.temperature_c}°C, "
            f"volt={self.voltage_v}V, "
            f"batt={self.battery_pct}%)"
        )
