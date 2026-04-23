"""
USB external storage: detect removable sd*, mount at /mnt/usb, expose status.
Adapted from BlueOS_videorecorder dropcam branch.
"""

from __future__ import annotations

import glob
import logging
import os
import subprocess
import threading
import time

logger = logging.getLogger(__name__)

USB_MOUNT_POINT = "/mnt/usb"
KAUMAUI_DIR = "KaumauiCam"
PROBE_INTERVAL_S = 30

_lock = threading.Lock()
_mounted = False
_device: str | None = None
_probe_thread: threading.Thread | None = None
_stop_probe = threading.Event()


def _scan_usb_devices() -> list[str]:
    """Partitions on removable block devices (sd*)."""
    partitions: list[str] = []
    for block in glob.glob("/sys/block/sd*"):
        try:
            with open(os.path.join(block, "removable"), "r") as f:
                if f.read().strip() != "1":
                    continue
        except OSError:
            continue
        dev_name = os.path.basename(block)
        for part in sorted(glob.glob(os.path.join(block, dev_name + "*"))):
            part_name = os.path.basename(part)
            dev_path = f"/dev/{part_name}"
            if os.path.exists(dev_path):
                partitions.append(dev_path)
        if not partitions:
            dev_path = f"/dev/{dev_name}"
            if os.path.exists(dev_path):
                partitions.append(dev_path)
    return partitions


def is_mounted() -> bool:
    try:
        with open("/proc/mounts", "r") as f:
            for line in f:
                parts = line.split()
                if len(parts) >= 2 and parts[1] == USB_MOUNT_POINT:
                    return True
    except OSError:
        pass
    return False


def try_mount() -> bool:
    global _mounted, _device
    with _lock:
        if _mounted and is_mounted():
            return True
        partitions = _scan_usb_devices()
        if not partitions:
            _mounted = False
            _device = None
            return False
        os.makedirs(USB_MOUNT_POINT, exist_ok=True)
        if is_mounted():
            _mounted = True
            _device = _device or partitions[0]
            return True
        for dev in partitions:
            result = subprocess.run(
                ["mount", "-o", "rw", dev, USB_MOUNT_POINT],
                capture_output=True,
                timeout=10,
            )
            if result.returncode == 0:
                _mounted = True
                _device = dev
                logger.info("USB mounted: %s -> %s", dev, USB_MOUNT_POINT)
                return True
            logger.debug("mount %s failed: %s", dev, result.stderr.decode(errors="replace").strip())
        _mounted = False
        _device = None
        return False


def get_free_mb(path: str | None = None) -> float | None:
    base = path or (USB_MOUNT_POINT if is_mounted() else None)
    if not base:
        return None
    try:
        st = os.statvfs(base)
        return round((st.f_bavail * st.f_frsize) / (1024 * 1024), 1)
    except OSError:
        return None


def get_recording_dir_usb() -> str:
    base = os.path.join(USB_MOUNT_POINT, KAUMAUI_DIR, "recordings")
    os.makedirs(base, exist_ok=True)
    return base


def get_status() -> dict:
    mounted = is_mounted()
    free = get_free_mb(USB_MOUNT_POINT) if mounted else None
    return {
        "mounted": mounted,
        "device": _device,
        "free_mb": free,
        "mount_point": USB_MOUNT_POINT,
    }


def sd_card_free_gb(path: str = "/app/data") -> float | None:
    """Free space on the filesystem hosting extension data (SD on BlueOS)."""
    try:
        st = os.statvfs(path)
        return (st.f_bavail * st.f_frsize) / (1024**3)
    except OSError:
        return None


def _probe_loop() -> None:
    while not _stop_probe.is_set():
        if not is_mounted():
            try:
                try_mount()
            except Exception as e:
                logger.debug("USB probe error: %s", e)
        _stop_probe.wait(PROBE_INTERVAL_S)


def start_probe() -> None:
    global _probe_thread
    if _probe_thread and _probe_thread.is_alive():
        return
    _stop_probe.clear()
    _probe_thread = threading.Thread(target=_probe_loop, daemon=True, name="usb-probe")
    _probe_thread.start()
    logger.info("USB probe thread started")


def stop_probe() -> None:
    _stop_probe.set()
    if _probe_thread and _probe_thread.is_alive():
        _probe_thread.join(timeout=5)
