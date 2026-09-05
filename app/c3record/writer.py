"""Module for background video writing segments using C3VideoWriterManager."""

import logging
import queue
import threading
from datetime import datetime
from typing import Optional, Tuple

from c3_video import C3VideoWriterManager

logger = logging.getLogger(__name__)


def writer_thread_worker(
    raw_queue: queue.Queue,
    manager: C3VideoWriterManager,
    shutdown_event: threading.Event,
) -> None:
    """Worker thread target that consumes raw MJPEG frame packets and writes to video segments.

    Args:
        raw_queue: The queue to pull raw packets from.
        manager: The C3VideoWriterManager instance.
        shutdown_event: Event signaling whether shutdown is requested.
    """
    logger.info("Background video writer thread started.")
    while True:
        try:
            # Short timeout to periodically inspect shutdown state if queue is empty
            item: Optional[Tuple[bytes, bytes, bytes, datetime]] = raw_queue.get(
                timeout=0.1
            )

            if item is None:
                logger.info(
                    "End-of-stream sentinel received. Finalizing writing queue."
                )
                break

            color_bytes, left_bytes, right_bytes, timestamp = item

            # Directly write raw MJPEG bytes to GStreamer (no host-side decoding!)
            manager.write_frame(color_bytes, left_bytes, right_bytes, timestamp)

        except queue.Empty:
            # If shutdown requested and queue fully drained, exit loop
            if shutdown_event.is_set():
                break
        except Exception:
            logger.exception("Unexpected error in background video writer thread")

    # Cleanup and close any open GStreamer writing pipeline gracefully
    try:
        manager.close()
    except Exception:
        logger.exception("Error during video writer manager closing")
    logger.info("Background video writer thread terminated cleanly.")
