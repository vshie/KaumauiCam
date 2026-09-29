"""Script to run motion detection on a directory of MKV video files and export results to Parquet."""

import argparse
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional

import polars as pl

from c3_video import ArchiveC3VideoReader
from motion_detector import MotionDetector

# Configure logging using the project's standard format
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger: logging.Logger = logging.getLogger(__name__)


def parse_filename_timestamp(file_stem: str) -> Optional[datetime]:
    """Parses base timestamp from a video filename stem.

    Tries "%Y-%m-%d-%H-%M-%S-%f" first, then falls back to "%Y-%m-%d_%H-%M-%S".

    Args:
        file_stem: Filename stem without extension.

    Returns:
        Timezone-aware UTC datetime if successfully parsed, or None.
    """
    for fmt in ("%Y-%m-%d-%H-%M-%S-%f", "%Y-%m-%d_%H-%M-%S"):
        try:
            return datetime.strptime(file_stem, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    logger.warning(f"Could not parse timestamp from filename stem: {file_stem}")
    return None


def extract_motion_from_video(
    video_path: Path,
    detector: MotionDetector,
    fps: float,
) -> List[dict[str, object]]:
    """Extracts raw and Kalman-smoothed motion scores from center camera frames of an MKV file.

    Args:
        video_path: Path to the input MKV file.
        detector: Configured MotionDetector instance.
        fps: Fallback framerate for timestamp offset calculation and Kalman dt.

    Returns:
        List of dictionaries with keys: motion, motion_smoothed, frame_id, file_path, timestamp.
    """
    logger.info(f"Processing video: {video_path}")
    base_ts: Optional[datetime] = parse_filename_timestamp(video_path.stem)
    rows: List[dict[str, object]] = []
    frame_id: int = 0
    detector.reset_for_new_video()
    dt: float = 1.0 / fps if fps > 0 else 1.0 / 30.0

    try:
        with ArchiveC3VideoReader(str(video_path)) as reader:
            for center_frame, _, _, sei_ts in reader:
                frame_ts: Optional[datetime] = (
                    sei_ts
                    if sei_ts is not None
                    else (
                        base_ts + timedelta(seconds=frame_id / fps)
                        if base_ts is not None
                        else None
                    )
                )
                raw_motion, smoothed_motion, is_detected = detector.evaluate_rgb_frame(
                    center_frame, dt=dt, timestamp=frame_ts
                )
                rows.append(
                    {
                        "motion": float(raw_motion),
                        "motion_smoothed": float(smoothed_motion),
                        "detected": bool(is_detected),
                        "frame_id": int(frame_id),
                        "file_path": str(video_path),
                        "timestamp": frame_ts,
                    }
                )
                frame_id += 1
    except Exception as exc:
        logger.error(f"Error reading video {video_path}: {exc}", exc_info=True)

    logger.info(f"Finished {video_path.name}: extracted {frame_id} frames.")
    return rows


def process_motion_directory(
    input_path: Path,
    output_path: Path,
    fps: float = 30.0,
    grid_x: int = 16,
    grid_y: int = 12,
    sensitivity: float = 0.8,
    cell_threshold: float = 0.10,
    sensitivity_threshold: float = 0.05,
    use_kalman: bool = True,
    kalman_q: float = 0.01,
    kalman_r: float = 0.05,
    warmup_seconds: float = 3.0,
) -> None:
    """Processes all video files in a directory and exports results to a Parquet file.

    Args:
        input_path: Path to input directory or individual video file.
        output_path: Path to output Parquet file.
        fps: Framerate for fallback timestamp offsets.
        grid_x: Number of horizontal motion grid cells.
        grid_y: Number of vertical motion grid cells.
        sensitivity: Sensitivity for pixel differences (0.0 to 1.0).
        cell_threshold: Fraction of pixels in a cell that must change to mark cell active.
        sensitivity_threshold: Threshold on smoothed active cell ratio to declare a frame detected.
        use_kalman: Whether to smooth motion over time using a linear Kalman filter.
        kalman_q: Process noise variance for the linear Kalman filter.
        kalman_r: Measurement noise variance for the linear Kalman filter.
        warmup_seconds: Initial video settling duration in seconds where motion is suppressed to 0.0.
    """
    video_files: List[Path] = (
        [input_path]
        if input_path.is_file()
        else sorted(
            [
                p
                for p in input_path.iterdir()
                if p.is_file() and p.suffix.lower() in (".mkv", ".mp4", ".avi")
            ]
        )
    )

    if not video_files:
        logger.warning(f"No video files found in {input_path}")
        return

    logger.info(f"Discovered {len(video_files)} video file(s) to process.")
    detector: MotionDetector = MotionDetector(
        fps=0.0,
        grid_x=grid_x,
        grid_y=grid_y,
        sensitivity=sensitivity,
        cell_threshold=cell_threshold,
        sensitivity_threshold=sensitivity_threshold,
        use_kalman=use_kalman,
        kalman_q=kalman_q,
        kalman_r=kalman_r,
        warmup_s=warmup_seconds,
    )

    all_rows: List[dict[str, object]] = []
    for vf in video_files:
        all_rows.extend(extract_motion_from_video(vf, detector, fps))

    if not all_rows:
        logger.warning(
            "No motion frames were extracted. Parquet file will not be created."
        )
        return

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df: pl.DataFrame = pl.DataFrame(
        all_rows,
        schema={
            "motion": pl.Float64,
            "motion_smoothed": pl.Float64,
            "detected": pl.Boolean,
            "frame_id": pl.Int64,
            "file_path": pl.String,
            "timestamp": pl.Datetime("us", "UTC"),
        },
    )
    df.write_parquet(output_path)
    logger.info(f"Successfully saved {len(df)} frame records to {output_path}")


def main() -> None:
    """Main CLI entrypoint for extracting motion data into Parquet."""
    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        description="Extract center camera motion detection metrics from video segments into Parquet."
    )
    parser.add_argument(
        "--input",
        type=str,
        required=True,
        help="Path to an MKV file or directory containing MKV files.",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="motion.parquet",
        help="Path to the output Parquet file (default: motion.parquet).",
    )
    parser.add_argument(
        "--fps",
        type=float,
        default=30.0,
        help="Framerate used for fallback timestamp offset calculation (default: 30.0).",
    )
    parser.add_argument(
        "--grid-x",
        type=int,
        default=16,
        help="Number of horizontal motion grid cells (default: 16).",
    )
    parser.add_argument(
        "--grid-y",
        type=int,
        default=12,
        help="Number of vertical motion grid cells (default: 12).",
    )
    parser.add_argument(
        "--sensitivity",
        type=float,
        default=0.8,
        help="Motion detection sensitivity from 0.0 to 1.0 (default: 0.8).",
    )
    parser.add_argument(
        "--cell-threshold",
        type=float,
        default=0.10,
        help="Fraction of pixels in a cell that must change to mark cell active (default: 0.10).",
    )
    parser.add_argument(
        "--sensitivity-threshold",
        dest="sensitivity_threshold",
        type=float,
        default=0.05,
        help="Threshold on smoothed active cell ratio (0.0 to 1.0) to declare a frame detected (default: 0.05).",
    )
    parser.add_argument(
        "--kalman-q",
        type=float,
        default=0.01,
        help="Process noise variance for the linear Kalman filter (default: 0.01).",
    )
    parser.add_argument(
        "--kalman-r",
        type=float,
        default=0.05,
        help="Measurement noise variance for the linear Kalman filter (default: 0.05).",
    )
    parser.add_argument(
        "--no-kalman",
        action="store_true",
        help="Disable linear Kalman filter motion smoothing.",
    )
    parser.add_argument(
        "--warmup-seconds",
        type=float,
        default=3.0,
        help="Initial video white balance settling duration in seconds where motion is suppressed to 0.0 (default: 3.0).",
    )

    args: argparse.Namespace = parser.parse_args()
    process_motion_directory(
        input_path=Path(args.input),
        output_path=Path(args.output),
        fps=args.fps,
        grid_x=args.grid_x,
        grid_y=args.grid_y,
        sensitivity=args.sensitivity,
        cell_threshold=args.cell_threshold,
        sensitivity_threshold=args.sensitivity_threshold,
        use_kalman=not args.no_kalman,
        kalman_q=args.kalman_q,
        kalman_r=args.kalman_r,
        warmup_seconds=args.warmup_seconds,
    )


if __name__ == "__main__":
    main()
