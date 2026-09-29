"""Script to extract synchronized camera frames from Matroska (MKV) video files as JPEG images."""

import argparse
import concurrent.futures
import logging
import os
import sys
import threading
from datetime import datetime
from typing import Dict, List, Optional, Set, Union

import numpy as np
from PIL import Image, ExifTags

from c3_video import ArchiveC3VideoReader

# Configure logging using the project's standard format
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger: logging.Logger = logging.getLogger(__name__)


def save_frame_set(
    center_frame: np.ndarray,
    left_frame: np.ndarray,
    right_frame: np.ndarray,
    center_path: str,
    left_path: str,
    right_path: str,
    quality: int,
    timestamp: Optional[datetime],
) -> None:
    """Saves a synchronized set of camera frames as JPEG files with EXIF metadata.

    Args:
        center_frame: Center camera frame as a numpy RGB array.
        left_frame: Left camera frame as a numpy RGB array.
        right_frame: Right camera frame as a numpy RGB array.
        center_path: Output filepath for the center frame.
        left_path: Output filepath for the left frame.
        right_path: Output filepath for the right frame.
        quality: The JPEG quality level (1-100).
        timestamp: Optional timestamp to encode in the EXIF metadata.
    """
    exif: Optional[Image.Exif] = None
    if timestamp is not None:
        exif = Image.Exif()
        datetime_str: str = timestamp.strftime("%Y:%m:%d %H:%M:%S")
        subsec_str: str = f"{timestamp.microsecond // 1000:03d}"
        exif[ExifTags.Base.DateTime] = datetime_str
        exif_ifd = exif.get_ifd(ExifTags.IFD.Exif)
        exif_ifd[ExifTags.Base.DateTimeOriginal] = datetime_str
        exif_ifd[ExifTags.Base.DateTimeDigitized] = datetime_str
        exif_ifd[ExifTags.Base.SubsecTime] = subsec_str
        exif_ifd[ExifTags.Base.SubsecTimeOriginal] = subsec_str
        exif_ifd[ExifTags.Base.SubsecTimeDigitized] = subsec_str

    try:
        save_kwargs: Dict[str, Union[int, Image.Exif]] = {"quality": quality}
        if exif is not None:
            save_kwargs["exif"] = exif

        Image.fromarray(center_frame).save(center_path, "JPEG", **save_kwargs)
        Image.fromarray(left_frame).save(left_path, "JPEG", **save_kwargs)
        Image.fromarray(right_frame).save(right_path, "JPEG", **save_kwargs)
    except Exception as err:
        logger.error(
            f"Failed to save frames to {center_path}: {err}",
            exc_info=True,
        )


def get_unique_filename(
    base_name: str, existing_files: Set[str], ext: str = ".jpg"
) -> str:
    """Generates a unique filename that is available across all target directories using a memory set.

    Args:
        base_name: The base filename without extension.
        existing_files: A set of already existing filenames in the target directories.
        ext: The file extension to append (including the dot).

    Returns:
        A unique filename string that does not exist in the set.
    """
    candidate: str = f"{base_name}{ext}"
    if candidate not in existing_files:
        existing_files.add(candidate)
        return candidate

    counter: int = 1
    while True:
        candidate = f"{base_name}_{counter}{ext}"
        if candidate not in existing_files:
            existing_files.add(candidate)
            return candidate
        counter += 1


def extract_mkv(
    filepath: str,
    output_dir: str,
    quality: int = 90,
    workers: int = 0,
) -> int:
    """Extracts synchronized RGB frames from an MKV file and saves them as JPEGs.

    Args:
        filepath: Absolute or relative path to the input MKV file.
        output_dir: Absolute or relative path to the root output directory.
        quality: JPEG quality level (1-100). Defaults to 90.
        workers: Number of worker threads for parallel saving. 0 scales automatically.

    Returns:
        The total number of frame sets successfully extracted from this file.
    """
    logger.info(f"Processing video segment: {filepath}")
    center_dir: str = os.path.join(output_dir, "center")
    left_dir: str = os.path.join(output_dir, "left")
    right_dir: str = os.path.join(output_dir, "right")

    os.makedirs(center_dir, exist_ok=True)
    os.makedirs(left_dir, exist_ok=True)
    os.makedirs(right_dir, exist_ok=True)

    existing_files: Set[str] = set()
    for d in [center_dir, left_dir, right_dir]:
        if os.path.exists(d):
            existing_files.update(os.listdir(d))

    semaphore: threading.Semaphore = threading.Semaphore(24)

    def task_done_callback(fut: concurrent.futures.Future) -> None:
        semaphore.release()

    count: int = 0
    futures: List[concurrent.futures.Future] = []
    try:
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=workers if workers > 0 else (os.cpu_count() or 4)
        ) as executor:
            with ArchiveC3VideoReader(filepath) as reader:
                for center_frame, left_frame, right_frame, timestamp in reader:
                    semaphore.acquire()
                    filename: str = get_unique_filename(
                        timestamp.strftime("%Y-%m-%d-%H-%M-%S-%f")
                        if timestamp is not None
                        else f"{os.path.splitext(os.path.basename(filepath))[0]}-frame-{count:06d}",
                        existing_files,
                    )

                    fut: concurrent.futures.Future = executor.submit(
                        save_frame_set,
                        center_frame,
                        left_frame,
                        right_frame,
                        os.path.join(center_dir, filename),
                        os.path.join(left_dir, filename),
                        os.path.join(right_dir, filename),
                        quality,
                        timestamp,
                    )
                    fut.add_done_callback(task_done_callback)
                    futures.append(fut)

                    count += 1
                    if count % 10 == 0:
                        logger.info(
                            f"Extracted {count} frames from {os.path.basename(filepath)}"
                        )

            concurrent.futures.wait(futures)

        logger.info(
            f"Successfully finished {os.path.basename(filepath)}: extracted {count} frame sets."
        )
        return count
    except Exception as err:
        logger.error(
            f"Failed to extract frames from {filepath}: {err}",
            exc_info=True,
        )
        return count


def main() -> None:
    """Parses command-line arguments and orchestrates the extraction process."""
    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        description="Extract camera frames from C3 Matroska (MKV) files as JPEG images with EXIF timestamp metadata."
    )
    parser.add_argument(
        "--input",
        type=str,
        required=True,
        help="Path to an MKV file or a folder containing MKV files.",
    )
    parser.add_argument(
        "--output",
        type=str,
        required=True,
        help="Path to the output folder where 'center', 'left', 'right' subdirectories will be created.",
    )
    parser.add_argument(
        "--quality",
        type=int,
        default=90,
        choices=range(1, 101),
        help="JPEG quality level (1-100). Default is 90.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=0,
        help="Number of background threads for parallel image saving. 0 scales automatically to CPU count.",
    )

    args: argparse.Namespace = parser.parse_args()

    input_path: str = os.path.abspath(args.input)
    output_path: str = os.path.abspath(args.output)

    if not os.path.exists(input_path):
        logger.error(f"Input path does not exist: {input_path}")
        sys.exit(1)

    # Resolve list of MKV files to process
    input_files: List[str] = []
    if os.path.isfile(input_path):
        input_files.append(input_path)
    elif os.path.isdir(input_path):
        input_files = sorted(
            [
                os.path.join(input_path, f)
                for f in os.listdir(input_path)
                if f.lower().endswith(".mkv")
            ]
        )
        if not input_files:
            logger.error(f"No MKV files found in directory: {input_path}")
            sys.exit(1)
    else:
        logger.error(f"Input path is neither a file nor a directory: {input_path}")
        sys.exit(1)

    # Ensure output directories are prepared
    os.makedirs(output_path, exist_ok=True)

    logger.info(
        f"Starting extraction of {len(input_files)} file(s) into: {output_path}"
    )

    total_extracted: int = 0
    start_time: float = datetime.now().timestamp()

    for mkv_file in input_files:
        total_extracted += extract_mkv(
            mkv_file,
            output_path,
            quality=args.quality,
            workers=args.workers,
        )

    elapsed_time: float = datetime.now().timestamp() - start_time
    logger.info(
        f"Extraction completed. Extracted {total_extracted} frame sets in {elapsed_time:.2f} seconds "
        f"(Avg: {total_extracted / elapsed_time if elapsed_time > 0 else 0:.2f} frame sets/sec)."
    )


if __name__ == "__main__":
    main()
