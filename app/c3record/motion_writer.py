"""Module for asynchronous motion analysis and parquet file persistence."""

import logging
import os
import queue
import threading
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple

import polars as pl

from motion_detector import MotionDetector

logger: logging.Logger = logging.getLogger(__name__)


class MotionParquetWriter:
    """Collects frame-level motion sensitivity metrics and writes Parquet batches in a dedicated thread."""

    def __init__(
        self,
        output_dir: str,
        detector: Optional[MotionDetector] = None,
        flush_threshold: int = 100_000,
        queue_maxsize: int = 2000,
    ) -> None:
        """Initializes the MotionParquetWriter.

        Args:
            output_dir: Directory where Parquet files will be stored.
            detector: Configured MotionDetector instance. If None, default detector is created.
            flush_threshold: Number of buffered rows before triggering a Parquet write.
            queue_maxsize: Maximum size of the internal frame processing queue.
        """
        self.output_dir: str = os.path.abspath(output_dir)
        os.makedirs(self.output_dir, exist_ok=True)

        self.detector: MotionDetector = (
            detector if detector is not None else MotionDetector(fps=0.0)
        )
        self.flush_threshold: int = flush_threshold
        self._queue: queue.Queue[Optional[Tuple[bytes, str, int, datetime]]] = (
            queue.Queue(maxsize=queue_maxsize)
        )

        self._batch_file_names: List[str] = []
        self._batch_frame_ids: List[int] = []
        self._batch_timestamps: List[datetime] = []
        self._batch_motions: List[float] = []
        self._batch_smoothed_motions: List[float] = []

        self._thread: Optional[threading.Thread] = None
        self._shutdown_event: threading.Event = threading.Event()

    def start(self) -> None:
        """Starts the background worker thread."""
        if self._thread is not None and self._thread.is_alive():
            return
        self._shutdown_event.clear()
        self._thread = threading.Thread(
            target=self._worker_loop, name="MotionParquetWriterThread", daemon=True
        )
        self._thread.start()
        logger.info(
            f"MotionParquetWriter background thread started for output directory: {self.output_dir}"
        )

    def enqueue_frame(
        self, center_bytes: bytes, file_name: str, frame_id: int, timestamp: datetime
    ) -> None:
        """Enqueues a center camera frame for asynchronous motion processing.

        Args:
            center_bytes: Raw JPEG bytes of the center camera frame.
            file_name: Base name of the target video file.
            frame_id: 0-indexed frame number within the video segment.
            timestamp: Synchronized UTC datetime of the frame.
        """
        try:
            self._queue.put_nowait((center_bytes, file_name, frame_id, timestamp))
        except queue.Full:
            logger.warning(
                "Motion frame queue full. Discarding frame to prevent latency build-up."
            )

    def _worker_loop(self) -> None:
        """Background loop continuously consuming frames and writing Parquet batches."""
        logger.info("MotionParquetWriter worker loop running.")
        while True:
            try:
                item: Optional[Tuple[bytes, str, int, datetime]] = self._queue.get(
                    timeout=0.1
                )
            except queue.Empty:
                if self._shutdown_event.is_set():
                    break
                continue

            if item is None:
                logger.info("Sentinel received in MotionParquetWriter worker.")
                break

            center_bytes, file_name, frame_id, timestamp = item
            self._process_item(center_bytes, file_name, frame_id, timestamp)

            if len(self._batch_frame_ids) >= self.flush_threshold:
                self._flush_batch()

        # Drain remaining items upon shutdown signal
        while not self._queue.empty():
            try:
                item = self._queue.get_nowait()
                if item is not None:
                    center_bytes, file_name, frame_id, timestamp = item
                    self._process_item(center_bytes, file_name, frame_id, timestamp)
                    if len(self._batch_frame_ids) >= self.flush_threshold:
                        self._flush_batch()
            except queue.Empty:
                break

        if self._batch_frame_ids:
            self._flush_batch()

        logger.info("MotionParquetWriter worker loop exited cleanly.")

    def _process_item(
        self, center_bytes: bytes, file_name: str, frame_id: int, timestamp: datetime
    ) -> None:
        """Processes an individual frame item and appends row to the current in-memory batch.

        Args:
            center_bytes: Raw JPEG bytes.
            file_name: Video segment file name.
            frame_id: Frame ID within segment.
            timestamp: Frame acquisition timestamp.
        """
        _, raw_motion, smoothed_motion = self.detector.process_frame(
            center_bytes, timestamp
        )

        self._batch_file_names.append(file_name)
        self._batch_frame_ids.append(frame_id)
        self._batch_timestamps.append(timestamp)
        self._batch_motions.append(float(raw_motion))
        self._batch_smoothed_motions.append(float(smoothed_motion))

    def _flush_batch(self) -> None:
        """Writes current in-memory batch to a Parquet file and clears buffer."""
        if not self._batch_frame_ids:
            return

        first_ts: datetime = self._batch_timestamps[0]
        base_filename: str = f"motion_{first_ts.strftime('%Y-%m-%d_%H-%M-%S')}.parquet"
        target_path: Path = Path(self.output_dir) / base_filename

        counter: int = 1
        while target_path.exists():
            target_path = (
                Path(self.output_dir)
                / f"motion_{first_ts.strftime('%Y-%m-%d_%H-%M-%S')}_{counter}.parquet"
            )
            counter += 1

        try:
            df: pl.DataFrame = pl.DataFrame(
                {
                    "file_name": self._batch_file_names,
                    "frame_id": self._batch_frame_ids,
                    "timestamp": self._batch_timestamps,
                    "motion": self._batch_motions,
                    "smoothed_motion": self._batch_smoothed_motions,
                },
                schema={
                    "file_name": pl.String,
                    "frame_id": pl.Int64,
                    "timestamp": pl.Datetime("us", "UTC"),
                    "motion": pl.Float64,
                    "smoothed_motion": pl.Float64,
                },
            )
            df.write_parquet(target_path)
            logger.info(
                f"Successfully flushed {len(df)} motion records to Parquet: {target_path}"
            )
        except Exception as exc:
            logger.exception(f"Failed to write Parquet file to {target_path}: {exc}")
        finally:
            self._batch_file_names.clear()
            self._batch_frame_ids.clear()
            self._batch_timestamps.clear()
            self._batch_motions.clear()
            self._batch_smoothed_motions.clear()

    def stop(self, timeout: float = 15.0) -> None:
        """Signals shutdown, drains queue, and waits for background thread to exit.

        Args:
            timeout: Maximum seconds to wait for worker thread termination.
        """
        self._shutdown_event.set()
        try:
            self._queue.put(None, timeout=1.0)
        except queue.Full:
            pass

        if self._thread is not None and self._thread.is_alive():
            self._thread.join(timeout=timeout)
            logger.info("MotionParquetWriter background thread stopped.")
