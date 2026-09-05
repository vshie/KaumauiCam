"""Module for high-precision, host-side camera frame synchronization."""

import logging
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class Synchronizer:
    """Synchronizer for grouping camera frames on the host device.

    Attributes:
        tolerance (float): Synchronization tolerance in milliseconds.
        max_age (float): Maximum frame age in milliseconds.
    """

    def __init__(self, tolerance: float = 50.0, max_age: float = 1000.0) -> None:
        """Initializes the Synchronizer with tolerance and maximum age.

        Args:
            tolerance: Sync tolerance in milliseconds.
            max_age: Maximum age of a frame in milliseconds.
        """
        self.tolerance: float = tolerance
        self.max_age: float = max_age
        self._lock: threading.Lock = threading.Lock()
        self._condition: threading.Condition = threading.Condition(self._lock)
        
        self._queues: Dict[str, List[Tuple[Any, float, float]]] = {
            "color": [],
            "left": [],
            "right": [],
        }
        self._ready_queue: List[Dict[str, Any]] = []

    def push(self, stream_name: str, frame: Any, timestamp: float) -> None:
        """Pushes a frame from a specific stream with its timestamp.

        Args:
            stream_name: The name of the stream (e.g. "color", "left", "right").
            frame: The frame data.
            timestamp: The acquisition device timestamp in seconds.
        """
        with self._condition:
            if stream_name not in self._queues:
                self._queues[stream_name] = []
                
            self._queues[stream_name].append((frame, timestamp, time.time()))

            self._try_match()
            self._collect_garbage()

    def _try_match(self) -> None:
        """Attempts to match frames from the head of all queues."""
        while all(len(q) > 0 for q in self._queues.values()):
            # Get the front of all queues
            fronts = {name: q[0] for name, q in self._queues.items()}
            timestamps = {name: item[1] for name, item in fronts.items()}

            min_ts = min(timestamps.values())
            max_ts = max(timestamps.values())

            if max_ts - min_ts <= self.tolerance / 1000.0:
                # Match found! Pop them all.
                frames = {name: self._queues[name].pop(0)[0] for name in self._queues}
                avg_timestamp = sum(timestamps.values()) / len(timestamps)

                self._ready_queue.append(
                    {
                        "color": frames.get("color"),
                        "left": frames.get("left"),
                        "right": frames.get("right"),
                        "timestamp": avg_timestamp,
                    }
                )
                self._condition.notify_all()
            else:
                # Streams out of sync, drop the oldest frame across all streams to realign
                oldest_stream = min(timestamps, key=timestamps.get)
                logger.warning(
                    "Dropping frame from %s due to sync mismatch. Tolerance: %.3f ms, Diff: %.3f ms",
                    oldest_stream,
                    self.tolerance,
                    (max_ts - min_ts) * 1000.0
                )
                self._queues[oldest_stream].pop(0)

    def _collect_garbage(self) -> None:
        """Removes stale frames from queues.

        Assumes self._lock (or self._condition) is already acquired.
        """
        current_time = time.time()
        for stream_name, q in self._queues.items():
            # item is (frame, timestamp, host_timestamp)
            while len(q) > 0 and (current_time - q[0][2]) > self.max_age / 1000.0:
                logger.warning("Garbage collecting stale frame from %s queue", stream_name)
                q.pop(0)

    def poll(self, timeout: float = 10.0) -> Optional[Dict[str, Any]]:
        """Polls for a synchronized frame set.

        Args:
            timeout: The maximum time to wait in seconds.

        Returns:
            A dictionary containing the synchronized frames and their average timestamp,
            or None if the timeout expires.
        """
        end_time: float = time.time() + timeout
        with self._condition:
            while not self._ready_queue:
                remaining: float = end_time - time.time()
                if remaining <= 0:
                    return None
                if not self._condition.wait(timeout=remaining):
                    if not self._ready_queue:
                        return None
            return self._ready_queue.pop(0)
