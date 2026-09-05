"""Module for concurrent, host-side DepthAI queue polling."""

import logging
import threading
import time
from typing import List, Optional

import depthai as dai
from sync import Synchronizer

logger = logging.getLogger(__name__)

# Offset to translate host steady clock (used by getTimestamp()) to real UTC time
_MONOTONIC_TO_UTC_OFFSET: float = time.time() - time.monotonic()


def queue_polling_worker(
    queue_name: str,
    dai_queue: dai.DataOutputQueue,
    synchronizer: Synchronizer,
    shutdown_event: threading.Event,
    last_frame_time_ref: List[float],
    watchdog_lock: threading.Lock,
) -> None:
    """Worker thread target that polls a DepthAI queue and pushes frames to the synchronizer.

    Args:
        queue_name: The name of the stream (e.g. "color", "left", "right").
        dai_queue: The DepthAI output queue to poll.
        synchronizer: The host-side frame synchronizer.
        shutdown_event: Signal to stop the worker loop.
        last_frame_time_ref: A mutable list holding the timestamp of the last received frame.
        watchdog_lock: Lock to protect updates to last_frame_time_ref.
    """
    while not shutdown_event.is_set():
        try:
            message: Optional[dai.ImgFrame] = dai_queue.tryGet()
            if message is not None:
                with watchdog_lock:
                    last_frame_time_ref[0] = time.time()
                synchronizer.push(
                    queue_name,
                    message.getRaw().data,
                    message.getTimestamp().total_seconds() + _MONOTONIC_TO_UTC_OFFSET,
                )
            else:
                time.sleep(0.001)
        except Exception:
            logger.exception(f"Error in queue polling worker for {queue_name}")
            time.sleep(0.01)
