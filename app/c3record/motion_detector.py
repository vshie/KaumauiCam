"""Module for fast motion detection using Numba JIT-accelerated motion cells and linear Kalman filtering."""

import io
import logging
from datetime import datetime
from typing import Optional, Tuple

from filterpy.common import Q_discrete_white_noise
from filterpy.kalman import KalmanFilter
from numba import njit
import numpy as np
from PIL import Image

logger: logging.Logger = logging.getLogger(__name__)


@njit(fastmath=True)
def _compute_motion_cells(
    curr_frame: np.ndarray,
    prev_frame: np.ndarray,
    grid_x: int,
    grid_y: int,
    pixel_threshold: int,
    cell_threshold: float,
) -> Tuple[int, int]:
    """Computes active motion cells by comparing current and previous grayscale frames.

    Args:
        curr_frame: Current 2D uint8 grayscale image array of shape (H, W).
        prev_frame: Previous 2D uint8 grayscale image array of shape (H, W).
        grid_x: Number of horizontal grid columns.
        grid_y: Number of vertical grid rows.
        pixel_threshold: Absolute pixel intensity difference threshold (0-255).
        cell_threshold: Minimum fraction of pixels in a cell that must change (0.0-1.0).

    Returns:
        A tuple of (active_cells, total_cells).
    """
    height: int = curr_frame.shape[0]
    width: int = curr_frame.shape[1]
    active_cells: int = 0
    total_cells: int = grid_x * grid_y

    for r in range(grid_y):
        y0: int = (r * height) // grid_y
        y1: int = ((r + 1) * height) // grid_y
        for c in range(grid_x):
            x0: int = (c * width) // grid_x
            x1: int = ((c + 1) * width) // grid_x
            changed_pixels: int = 0
            cell_pixel_count: int = (y1 - y0) * (x1 - x0)
            for y in range(y0, y1):
                for x in range(x0, x1):
                    if (
                        abs(int(curr_frame[y, x]) - int(prev_frame[y, x]))
                        > pixel_threshold
                    ):
                        changed_pixels += 1
            if (
                cell_pixel_count > 0
                and (changed_pixels / cell_pixel_count) >= cell_threshold
            ):
                active_cells += 1

    return active_cells, total_cells


@njit(fastmath=True)
def _rgb_to_gray(rgb: np.ndarray) -> np.ndarray:
    """Converts a 3D RGB uint8 array to a 2D grayscale uint8 array using Rec. 601 luma.

    Args:
        rgb: 3D uint8 array of shape (H, W, 3).

    Returns:
        2D uint8 array of shape (H, W).
    """
    height: int = rgb.shape[0]
    width: int = rgb.shape[1]
    gray: np.ndarray = np.empty((height, width), dtype=np.uint8)
    for y in range(height):
        for x in range(width):
            gray[y, x] = int(
                0.299 * float(rgb[y, x, 0])
                + 0.587 * float(rgb[y, x, 1])
                + 0.114 * float(rgb[y, x, 2])
            )
    return gray


class MotionKalmanFilter:
    """Linear Kalman filter modeling motion ratio and rate of change in 2D state space."""

    def __init__(self, q_var: float = 0.01, r_var: float = 0.05) -> None:
        """Initializes the 2D linear Kalman filter for motion smoothing.

        State: [motion, velocity]^T

        Args:
            q_var: Process noise variance for discrete white noise model.
            r_var: Measurement noise variance for sensor measurements.
        """
        self.q_var: float = q_var
        self.r_var: float = r_var
        self._kf: KalmanFilter = KalmanFilter(dim_x=2, dim_z=1)
        self._initialized: bool = False
        self._init_filter()

    def _init_filter(self) -> None:
        """Configures initial state and covariance matrices for the filter."""
        self._kf.x = np.array([[0.0], [0.0]], dtype=np.float64)
        self._kf.F = np.array([[1.0, 1.0], [0.0, 1.0]], dtype=np.float64)
        self._kf.H = np.array([[1.0, 0.0]], dtype=np.float64)
        self._kf.P = np.eye(2, dtype=np.float64) * 1.0
        self._kf.R = np.array([[self.r_var]], dtype=np.float64)
        self._kf.Q = Q_discrete_white_noise(dim=2, dt=1.0, var=self.q_var)
        self._initialized = False

    def reset(self) -> None:
        """Resets the Kalman filter state and covariance to initial values."""
        self._init_filter()

    def update(self, raw_motion: float, dt: float = 1.0) -> float:
        """Performs a predict and update step with the observed raw motion ratio.

        Args:
            raw_motion: Measured active cell motion ratio (0.0 to 1.0).
            dt: Elapsed time in seconds since the previous update.

        Returns:
            Smoothed motion ratio clamped to [0.0, 1.0].
        """
        effective_dt: float = max(1e-4, dt)

        if not self._initialized:
            self._kf.x = np.array([[raw_motion], [0.0]], dtype=np.float64)
            self._kf.P = np.eye(2, dtype=np.float64) * 1.0
            self._initialized = True
            return raw_motion

        self._kf.F[0, 1] = effective_dt
        self._kf.Q = Q_discrete_white_noise(dim=2, dt=effective_dt, var=self.q_var)

        self._kf.predict()
        self._kf.update(np.array([[raw_motion]], dtype=np.float64))

        return max(0.0, min(1.0, float(self._kf.x[0, 0])))


class MotionDetector:
    """Motion detector utilizing a grid-based motion cell algorithm with Kalman smoothing."""

    def __init__(
        self,
        fps: float = 5.0,
        grid_x: int = 16,
        grid_y: int = 12,
        sensitivity: float = 0.8,
        cell_threshold: float = 0.10,
        sensitivity_threshold: float = 0.05,
        pixel_threshold: Optional[int] = None,
        use_kalman: bool = True,
        kalman_q: float = 0.01,
        kalman_r: float = 0.05,
        warmup_s: float = 3.0,
    ) -> None:
        """Initializes the MotionDetector.

        Args:
            fps: Desired sampling rate in frames per second for motion evaluation.
            grid_x: Number of horizontal cells in the grid.
            grid_y: Number of vertical cells in the grid.
            sensitivity: Sensitivity of pixel change (0.0 to 1.0, where 1.0 is highest).
            cell_threshold: Fraction of pixels in a cell that must change to mark cell active.
            sensitivity_threshold: Threshold on smoothed motion ratio to declare frame in motion.
            pixel_threshold: Optional direct pixel intensity difference threshold (0 to 255).
            use_kalman: Whether to enable linear Kalman filter smoothing.
            kalman_q: Process noise variance for discrete white noise model.
            kalman_r: Measurement noise variance for the Kalman filter.
            warmup_s: Camera settling duration in seconds where motion is suppressed to 0.0.
        """
        self.fps: float = fps
        self.grid_x: int = grid_x
        self.grid_y: int = grid_y
        self.sensitivity: float = sensitivity
        self.cell_threshold: float = cell_threshold
        self.sensitivity_threshold: float = sensitivity_threshold
        self.pixel_threshold: int = (
            pixel_threshold
            if pixel_threshold is not None
            else max(1, int(round(128.0 * (1.0 - sensitivity))))
        )
        self.use_kalman: bool = use_kalman
        self.warmup_s: float = warmup_s

        self.kalman_filter: Optional[MotionKalmanFilter] = (
            MotionKalmanFilter(q_var=kalman_q, r_var=kalman_r) if use_kalman else None
        )

        self._min_interval_s: float = 1.0 / fps if fps > 0 else 0.0
        self._prev_gray: Optional[np.ndarray] = None
        self._last_sample_time: Optional[datetime] = None
        self._first_frame_time: Optional[datetime] = None
        self._elapsed_s: float = 0.0
        self._total_sampled_frames: int = 0
        self._motion_frames: int = 0
        self._motion_seconds: float = 0.0
        self._total_sampled_seconds: float = 0.0

    def _evaluate_gray(
        self, curr_gray: np.ndarray, dt: float, timestamp: Optional[datetime] = None
    ) -> Tuple[bool, float, float]:
        """Evaluates motion between the provided grayscale frame and the stored previous frame.

        Args:
            curr_gray: 2D uint8 grayscale image array.
            dt: Elapsed time in seconds since the previous frame.
            timestamp: Optional frame acquisition timestamp.

        Returns:
            A tuple of (is_motion, raw_motion_ratio, smoothed_motion_ratio).
        """
        self._total_sampled_frames += 1
        if self._prev_gray is None or self._prev_gray.shape != curr_gray.shape:
            self._prev_gray = curr_gray.copy()
            if timestamp is not None and self._first_frame_time is None:
                self._first_frame_time = timestamp
            if self.kalman_filter is not None:
                self.kalman_filter.reset()
            return False, 0.0, 0.0

        # Check warmup window for camera auto-white-balance / auto-exposure settling
        in_warmup: bool = False
        if self.warmup_s > 0.0:
            if timestamp is not None and self._first_frame_time is not None:
                in_warmup = (
                    timestamp - self._first_frame_time
                ).total_seconds() < self.warmup_s
            else:
                self._elapsed_s += dt
                in_warmup = self._elapsed_s < self.warmup_s

        if in_warmup:
            self._prev_gray = curr_gray.copy()
            if self.kalman_filter is not None:
                self.kalman_filter.reset()
            return False, 0.0, 0.0

        active_cells: int
        total_cells: int
        active_cells, total_cells = _compute_motion_cells(
            curr_gray,
            self._prev_gray,
            self.grid_x,
            self.grid_y,
            self.pixel_threshold,
            self.cell_threshold,
        )
        self._prev_gray = curr_gray.copy()

        raw_motion: float = (
            float(active_cells) / float(total_cells) if total_cells > 0 else 0.0
        )
        smoothed_motion: float = (
            self.kalman_filter.update(raw_motion, dt)
            if self.kalman_filter is not None
            else raw_motion
        )

        is_motion: bool = smoothed_motion >= self.sensitivity_threshold
        self._total_sampled_seconds += dt
        if is_motion:
            self._motion_frames += 1
            self._motion_seconds += dt

        return is_motion, raw_motion, smoothed_motion

    def process_frame(
        self, center_bytes: bytes, timestamp: Optional[datetime] = None
    ) -> Tuple[bool, float, float]:
        """Processes an encoded JPEG center camera frame if sufficient time has elapsed.

        Args:
            center_bytes: Raw JPEG bytes of the center camera frame.
            timestamp: Acquisition timestamp of the frame.

        Returns:
            A tuple of (is_motion, raw_motion_ratio, smoothed_motion_ratio).
        """
        dt: float = self._min_interval_s
        if timestamp is not None and self._last_sample_time is not None:
            elapsed: float = (timestamp - self._last_sample_time).total_seconds()
            if elapsed < self._min_interval_s:
                return False, 0.0, 0.0
            dt = elapsed

        try:
            im: Image.Image = Image.open(io.BytesIO(center_bytes))
            im.draft("L", (240, 135))
            curr_gray: np.ndarray = np.asarray(im, dtype=np.uint8)
        except Exception as exc:
            logger.warning(
                f"Failed to decode center frame for motion detection: {exc}",
                exc_info=True,
            )
            return False, 0.0, 0.0

        if timestamp is not None:
            self._last_sample_time = timestamp

        return self._evaluate_gray(curr_gray, dt, timestamp)

    def process_rgb_array(
        self, center_rgb: np.ndarray, timestamp: Optional[datetime] = None
    ) -> Tuple[bool, float, float]:
        """Processes a raw 3D RGB center camera numpy array.

        Args:
            center_rgb: 3D uint8 array of shape (H, W, 3).
            timestamp: Optional acquisition timestamp.

        Returns:
            A tuple of (is_motion, raw_motion_ratio, smoothed_motion_ratio).
        """
        dt: float = self._min_interval_s
        if timestamp is not None and self._last_sample_time is not None:
            elapsed: float = (timestamp - self._last_sample_time).total_seconds()
            if elapsed < self._min_interval_s:
                return False, 0.0, 0.0
            dt = elapsed

        step: int = max(1, center_rgb.shape[0] // 135)
        curr_gray: np.ndarray = _rgb_to_gray(
            np.ascontiguousarray(center_rgb[::step, ::step])
        )

        if timestamp is not None:
            self._last_sample_time = timestamp

        return self._evaluate_gray(curr_gray, dt, timestamp)

    def evaluate_rgb_frame(
        self,
        center_rgb: np.ndarray,
        dt: float = 1.0 / 30.0,
        timestamp: Optional[datetime] = None,
    ) -> Tuple[float, float, bool]:
        """Evaluates a raw 3D RGB center frame without frame rate throttling.

        Args:
            center_rgb: 3D uint8 array of shape (H, W, 3).
            dt: Elapsed time in seconds for the Kalman filter transition.
            timestamp: Optional frame acquisition timestamp.

        Returns:
            A tuple of (raw_motion_ratio, smoothed_motion_ratio, is_detected).
        """
        step: int = max(1, center_rgb.shape[0] // 135)
        curr_gray: np.ndarray = _rgb_to_gray(
            np.ascontiguousarray(center_rgb[::step, ::step])
        )
        is_detected, raw_motion, smoothed_motion = self._evaluate_gray(
            curr_gray, dt, timestamp
        )
        return raw_motion, smoothed_motion, is_detected

    def get_segment_stats(self) -> Tuple[float, float, int, int]:
        """Retrieves motion duration and frame statistics for the current segment.

        Returns:
            A tuple of (motion_seconds, total_sampled_seconds, motion_frames, total_sampled_frames).
        """
        return (
            self._motion_seconds,
            self._total_sampled_seconds,
            self._motion_frames,
            self._total_sampled_frames,
        )

    def reset_segment(self) -> None:
        """Resets counters and Kalman filter state for the next video segment."""
        self._motion_frames = 0
        self._total_sampled_frames = 0
        self._motion_seconds = 0.0
        self._total_sampled_seconds = 0.0
        self._prev_gray = None
        if self.kalman_filter is not None:
            self.kalman_filter.reset()

    def reset_for_new_video(self) -> None:
        """Resets counters, Kalman filter, and warmup state for an entirely new video file."""
        self.reset_segment()
        self._first_frame_time = None
        self._elapsed_s = 0.0

    def should_keep_segment(self, seconds_threshold: float) -> bool:
        """Determines whether the current segment should be retained based on motion duration.

        Args:
            seconds_threshold: Minimum cumulative seconds of motion required.

        Returns:
            True if cumulative motion duration meets or exceeds threshold or if no frames were sampled.
        """
        if self._total_sampled_frames == 0:
            return True
        return self._motion_seconds >= seconds_threshold
