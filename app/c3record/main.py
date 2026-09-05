"""Main entry point for recording C3 video segments from OAK-D camera."""

import os
import sys
import time
import queue
import logging
import argparse
import threading
import signal
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional, Tuple, List

os.environ["DEPTHAI_BOOTUP_TIMEOUT"] = "30000"
os.environ["DEPTHAI_CONNECT_TIMEOUT"] = "30000"
os.environ["DEPTHAI_WATCHDOG_INITIAL_DELAY"] = "30000"

import depthai as dai

from c3_video import C3VideoWriterManager
from polling import queue_polling_worker
from sync import Synchronizer
from writer import writer_thread_worker

# Set up logging to standard output with timestamp, level, and message
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def get_color_resolution(resolution: str) -> dai.ColorCameraProperties.SensorResolution:
    """Converts color camera resolution argument to DepthAI enum.

    Args:
        resolution: A string representation of the color camera resolution (e.g. "1080").

    Returns:
        The matching DepthAI SensorResolution enum.

    Raises:
        ValueError: If resolution is not valid.
    """
    res_map: Dict[str, dai.ColorCameraProperties.SensorResolution] = {
        "720": dai.ColorCameraProperties.SensorResolution.THE_720_P,
        "720p": dai.ColorCameraProperties.SensorResolution.THE_720_P,
        "800": dai.ColorCameraProperties.SensorResolution.THE_800_P,
        "800p": dai.ColorCameraProperties.SensorResolution.THE_800_P,
        "1080": dai.ColorCameraProperties.SensorResolution.THE_1080_P,
        "1080p": dai.ColorCameraProperties.SensorResolution.THE_1080_P,
        "4k": dai.ColorCameraProperties.SensorResolution.THE_4_K,
        "4_k": dai.ColorCameraProperties.SensorResolution.THE_4_K,
        "2160": dai.ColorCameraProperties.SensorResolution.THE_4_K,
        "2160p": dai.ColorCameraProperties.SensorResolution.THE_4_K,
        "12mp": dai.ColorCameraProperties.SensorResolution.THE_12_MP,
        "12_mp": dai.ColorCameraProperties.SensorResolution.THE_12_MP,
    }
    key: str = str(resolution).lower().strip()
    if key in res_map:
        return res_map[key]
    raise ValueError(f"Invalid color camera resolution: {resolution}")


def get_mono_resolution(resolution: str) -> dai.MonoCameraProperties.SensorResolution:
    """Converts mono/stereo camera resolution argument to DepthAI enum.

    Args:
        resolution: A string representation of the mono camera resolution (e.g. "720").

    Returns:
        The matching DepthAI SensorResolution enum.

    Raises:
        ValueError: If resolution is not valid.
    """
    res_map: Dict[str, dai.MonoCameraProperties.SensorResolution] = {
        "400": dai.MonoCameraProperties.SensorResolution.THE_400_P,
        "400p": dai.MonoCameraProperties.SensorResolution.THE_400_P,
        "480": dai.MonoCameraProperties.SensorResolution.THE_480_P,
        "480p": dai.MonoCameraProperties.SensorResolution.THE_480_P,
        "720": dai.MonoCameraProperties.SensorResolution.THE_720_P,
        "720p": dai.MonoCameraProperties.SensorResolution.THE_720_P,
        "800": dai.MonoCameraProperties.SensorResolution.THE_800_P,
        "800p": dai.MonoCameraProperties.SensorResolution.THE_800_P,
    }
    key: str = str(resolution).lower().strip()
    if key in res_map:
        return res_map[key]
    raise ValueError(f"Invalid stereo camera resolution: {resolution}")


def get_resolution_dimensions(resolution: str, is_color: bool) -> Tuple[int, int]:
    """Converts a resolution string parameter to explicit width and height integers.

    Args:
        resolution: Resolution string (e.g. "1080" or "720").
        is_color: True if representing color camera, False if mono/stereo camera.

    Returns:
        A tuple of (width, height) integers.

    Raises:
        ValueError: If the resolution is not valid.
    """
    key: str = str(resolution).lower().strip()
    if is_color:
        res_dims = {
            "720": (1280, 720),
            "720p": (1280, 720),
            "800": (1280, 800),
            "800p": (1280, 800),
            "1080": (1920, 1080),
            "1080p": (1920, 1080),
            "4k": (3840, 2160),
            "4_k": (3840, 2160),
            "2160": (3840, 2160),
            "2160p": (3840, 2160),
            "12mp": (4056, 3040),
            "12_mp": (4056, 3040),
        }
    else:
        res_dims = {
            "400": (640, 400),
            "400p": (640, 400),
            "480": (640, 480),
            "480p": (640, 480),
            "720": (1280, 720),
            "720p": (1280, 720),
            "800": (1280, 800),
            "800p": (1280, 800),
        }
    if key in res_dims:
        return res_dims[key]
    camera_type = "color" if is_color else "stereo"
    raise ValueError(f"Unknown {camera_type} camera resolution: {resolution}")


def build_dai_pipeline(
    fps: int,
    sync_ms: int,
    mjpeg_quality: int,
    color_res: dai.ColorCameraProperties.SensorResolution,
    stereo_res: dai.MonoCameraProperties.SensorResolution,
) -> dai.Pipeline:
    """Builds the DepthAI camera pipeline containing Color + Left/Right Mono cameras and encoders.

    Args:
        fps: Recording frames per second.
        sync_ms: On-device frame synchronization interval in milliseconds.
        mjpeg_quality: Quality factor (1-100) for MJPEG encoders.
        color_res: Resolved ColorCamera sensor resolution.
        stereo_res: Resolved MonoCamera sensor resolution.

    Returns:
        A completely constructed dai.Pipeline ready to run.
    """
    pipeline: dai.Pipeline = dai.Pipeline()
    pipeline.setOpenVINOVersion(dai.OpenVINO.VERSION_2021_4)

    # Calculate maximum exposure time in microseconds based on FPS to prevent auto-exposure framerate drops
    max_exposure_us = int(1_000_000 / fps)

    # Color Camera (CAM_A)
    color: dai.node.ColorCamera = pipeline.create(dai.node.ColorCamera)
    color.setBoardSocket(dai.CameraBoardSocket.CAM_A)
    color.setResolution(color_res)
    color.setColorOrder(dai.ColorCameraProperties.ColorOrder.RGB)
    color.setFps(fps)
    color.initialControl.setAutoExposureLimit(max_exposure_us)

    color_encoder: dai.node.VideoEncoder = pipeline.create(dai.node.VideoEncoder)
    color_encoder.setDefaultProfilePreset(fps, dai.VideoEncoderProperties.Profile.MJPEG)
    color_encoder.setQuality(mjpeg_quality)
    color_encoder.setNumFramesPool(3)

    # Left Camera (CAM_B)
    left: dai.node.MonoCamera = pipeline.create(dai.node.MonoCamera)
    left.setBoardSocket(dai.CameraBoardSocket.CAM_B)
    left.setResolution(stereo_res)
    left.setFps(fps)
    left.initialControl.setAutoExposureLimit(max_exposure_us)

    left_encoder: dai.node.VideoEncoder = pipeline.create(dai.node.VideoEncoder)
    left_encoder.setDefaultProfilePreset(fps, dai.VideoEncoderProperties.Profile.MJPEG)
    left_encoder.setQuality(mjpeg_quality)
    left_encoder.setNumFramesPool(3)

    # Right Camera (CAM_C)
    right: dai.node.MonoCamera = pipeline.create(dai.node.MonoCamera)
    right.setBoardSocket(dai.CameraBoardSocket.CAM_C)
    right.setResolution(stereo_res)
    right.setFps(fps)
    right.initialControl.setAutoExposureLimit(max_exposure_us)

    right_encoder: dai.node.VideoEncoder = pipeline.create(dai.node.VideoEncoder)
    right_encoder.setDefaultProfilePreset(fps, dai.VideoEncoderProperties.Profile.MJPEG)
    right_encoder.setQuality(mjpeg_quality)
    right_encoder.setNumFramesPool(3)

    # Linking encoders to input cameras
    color.video.link(color_encoder.input)
    left.out.link(left_encoder.input)
    right.out.link(right_encoder.input)

    # Output color stream to host
    xout_color: dai.node.XLinkOut = pipeline.create(dai.node.XLinkOut)
    xout_color.setStreamName("color")
    color_encoder.bitstream.link(xout_color.input)

    # Output left stream to host
    xout_left: dai.node.XLinkOut = pipeline.create(dai.node.XLinkOut)
    xout_left.setStreamName("left")
    left_encoder.bitstream.link(xout_left.input)

    # Output right stream to host
    xout_right: dai.node.XLinkOut = pipeline.create(dai.node.XLinkOut)
    xout_right.setStreamName("right")
    right_encoder.bitstream.link(xout_right.input)

    pipeline.setXLinkChunkSize(0)

    return pipeline


def main() -> None:
    """Parses command-line arguments, connects to the OAK-D camera, and starts acquisition."""
    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        description="Record C3 video segments from connected OAK-D camera."
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=10.0,
        help="Duration of each video segment in seconds.",
    )
    parser.add_argument(
        "--output_dir",
        type=str,
        default="./recordings",
        help="Output directory where MKVs are written.",
    )
    parser.add_argument(
        "--output_format",
        type=str,
        default="%Y-%m-%d_%H-%M-%S.mkv",
        help="Naming format for MKV segment files.",
    )
    parser.add_argument(
        "--fps", type=int, default=30, help="Frames per second for video recording."
    )
    parser.add_argument(
        "--sync",
        type=int,
        default=50,
        help="On-device frame synchronization interval in milliseconds.",
    )
    parser.add_argument(
        "--mjpeg-quality",
        type=int,
        default=90,
        help="MJPEG stream compression quality (1-100).",
    )
    parser.add_argument(
        "--color-resolution",
        type=str,
        default="1080",
        help="Resolution for the color camera.",
    )
    parser.add_argument(
        "--stereo-resolution",
        type=str,
        default="720",
        help="Resolution for the stereo cameras.",
    )
    parser.add_argument(
        "--encoder",
        type=str,
        choices=["default", "nvidia", "vaapi"],
        default="default",
        help="H.264 encoder to use: 'default' (x264enc), 'nvidia' (nvh264enc), or 'vaapi' (vaapih264enc)."
    )

    parser.add_argument(
        "--bitrate",
        type=int,
        default=5000,
        help="H.264 video encoding bitrate in kbit/sec (for CBR).",
    )
    parser.add_argument(
        "--gop-size",
        type=int,
        default=30,
        help="Maximal distance between two keyframes.",
    )
    parser.add_argument(
        "--bframes",
        type=int,
        default=0,
        help="Number of B-frames between I and P frames.",
    )
    parser.add_argument(
        "--bitrate-control",
        type=str,
        default="cbr",
        choices=["cbr", "vbr"],
        help="Rate control mode (CBR or VBR).",
    )
    parser.add_argument(
        "--quantizer",
        type=int,
        default=21,
        help="Constant quantizer value (0 to 50) used for VBR.",
    )
    parser.add_argument(
        "--max-bitrate",
        type=int,
        default=None,
        help="Maximum H.264 video encoding bitrate in kbit/sec (for VBR).",
    )
    parser.add_argument(
        "--quality-min",
        type=int,
        default=None,
        help="Minimum quality boundary (mapped to qp-max, 0 to 63) for VBR.",
    )
    parser.add_argument(
        "--quality-max",
        type=int,
        default=None,
        help="Maximum quality boundary (mapped to qp-min, 0 to 63) for VBR.",
    )
    parser.add_argument(
        "--speed-preset",
        type=str,
        default="superfast",
        help="x264 encoder speed preset.",
    )
    parser.add_argument(
        "--tune",
        type=str,
        default="zerolatency",
        help="x264 encoder tuning option.",
    )
    parser.add_argument(
        "--video-tag",
        type=str,
        default=None,
        help="Custom metadata tag embedded into the output files.",
    )

    args: argparse.Namespace = parser.parse_args()

    # Graceful shutdown event signals
    shutdown_event: threading.Event = threading.Event()

    def sigint_handler(sig: int, frame: Any) -> None:
        logger.info("SIGINT/Ctrl+C received. Propagating shutdown...")
        shutdown_event.set()

    signal.signal(signal.SIGINT, sigint_handler)

    # Resolve camera resolution settings safely
    try:
        color_res: dai.ColorCameraProperties.SensorResolution = get_color_resolution(
            args.color_resolution
        )
        stereo_res: dai.MonoCameraProperties.SensorResolution = get_mono_resolution(
            args.stereo_resolution
        )
        w_color, h_color = get_resolution_dimensions(
            args.color_resolution, is_color=True
        )
        w_stereo, h_stereo = get_resolution_dimensions(
            args.stereo_resolution, is_color=False
        )
    except ValueError as val_err:
        logger.error("Configuration error: %s", val_err)
        sys.exit(1)

    # Configure device connection properties to maximize stability
    device_config: dai.Device.Config = dai.Device.Config()
    device_config.version = dai.OpenVINO.Version.VERSION_2021_4
    device_config.board.watchdogTimeoutMs = 5000
    device_config.board.network.mtu = 1500
    device_config.board.network.xlinkTcpNoDelay = False
    device_config.board.usb.maxSpeed = dai.UsbSpeed.SUPER_PLUS
    device_config.board.sysctl.append("net.inet.tcp.delayed_ack=1")

    # Connect to device and verify availability
    device_infos: List[dai.DeviceInfo] = dai.Device.getAllAvailableDevices()
    if not device_infos:
        logger.error("No OAK-D device discovered. Check connection.")
        sys.exit(1)

    logger.info("Discovered device: %s. Connecting...", device_infos[0].getMxId())
    device: dai.Device = dai.Device(device_config, device_infos[0])
    device.setXLinkChunkSize(0)

    def log_callback(message: dai.LogMessage) -> None:
        logger.info("Device Log: %s", message.payload)

    device.addLogCallback(log_callback)
    logger.info("Successfully connected to device.")

    # Build and start depthai pipeline
    dai_pipeline: dai.Pipeline = build_dai_pipeline(
        fps=args.fps,
        sync_ms=args.sync,
        mjpeg_quality=args.mjpeg_quality,
        color_res=color_res,
        stereo_res=stereo_res,
    )

    if not device.startPipeline(dai_pipeline):
        logger.error("Could not upload or initiate pipeline on OAK-D device.")
        sys.exit(1)

    # Active continuous timesync
    device.setTimesync(timedelta(seconds=5), 10, True)

    # Thread-safe queue to decouple acquisition from disk write / frame decoding
    # enough to hold 2x video segment worth of frames to prevent stutter during disk writes
    raw_queue: queue.Queue = queue.Queue(maxsize=args.fps * args.duration * 2)

    # Initialize video writer manager with explicit resolved dimensions
    manager: C3VideoWriterManager = C3VideoWriterManager(
        output_dir=args.output_dir,
        output_format=args.output_format,
        duration=args.duration,
        fps=args.fps,
        w_center=w_color,
        h_center=h_color,
        w_left=w_stereo,
        h_left=h_stereo,
        w_right=w_stereo,
        h_right=h_stereo,
        bitrate=args.bitrate,
        gop_size=args.gop_size,
        bframes=args.bframes,
        bitrate_control=args.bitrate_control,
        quantizer=args.quantizer,
        speed_preset=args.speed_preset,
        tune=args.tune,
        video_tag=args.video_tag,
        max_bitrate=args.max_bitrate,
        quality_min=args.quality_min,
        quality_max=args.quality_max,
        encoder=args.encoder,
    )

    # Start the worker writing thread
    writer_thread: threading.Thread = threading.Thread(
        target=writer_thread_worker,
        args=(raw_queue, manager, shutdown_event),
        daemon=True,
    )
    writer_thread.start()

    # Get output queues from OAK-D
    q_color: dai.DataOutputQueue = device.getOutputQueue(
        name="color", maxSize=int(args.fps), blocking=True
    )
    q_left: dai.DataOutputQueue = device.getOutputQueue(
        name="left", maxSize=int(args.fps), blocking=True
    )
    q_right: dai.DataOutputQueue = device.getOutputQueue(
        name="right", maxSize=int(args.fps), blocking=True
    )

    # Initialize the host Synchronizer
    synchronizer: Synchronizer = Synchronizer(
        tolerance=float(args.sync), max_age=1000.0
    )

    logger.info("Acquisition loop starting. Press Ctrl+C to terminate gracefully.")
    consecutive_dropped_frames: List[float] = [time.time()]
    watchdog_lock: threading.Lock = threading.Lock()

    # Start the 3 queue polling worker threads
    polling_threads: List[threading.Thread] = []
    for q_name, q_obj in [("color", q_color), ("left", q_left), ("right", q_right)]:
        t: threading.Thread = threading.Thread(
            target=queue_polling_worker,
            args=(
                q_name,
                q_obj,
                synchronizer,
                shutdown_event,
                consecutive_dropped_frames,
                watchdog_lock,
            ),
            daemon=True,
        )
        t.start()
        polling_threads.append(t)

    with device:
        while not shutdown_event.is_set():
            try:
                # Poll the synchronizer with a small timeout (e.g. 10ms)
                synchronized_set: Optional[Dict[str, Any]] = synchronizer.poll(
                    timeout=0.01
                )
                if synchronized_set is not None:
                    try:
                        raw_queue.put_nowait(
                            (
                                synchronized_set["color"],
                                synchronized_set["left"],
                                synchronized_set["right"],
                                datetime.fromtimestamp(
                                    synchronized_set["timestamp"], tz=timezone.utc
                                ),
                            )
                        )
                    except queue.Full:
                        logger.warning(
                            "Queue full. Frame package discarded to prevent stutter."
                        )

                # Connection watchdog (Timeout after 3 seconds of silent interface)
                with watchdog_lock:
                    is_heartbeat_lost: bool = (
                        time.time() - consecutive_dropped_frames[0] > 3.0
                    )

                if is_heartbeat_lost:
                    logger.error(
                        "No frames received from camera for over 3 seconds. Heartbeat lost."
                    )
                    break

            except Exception as loop_err:
                logger.exception(f"Error during camera frame acquisition: {loop_err}")
                break

    # Shutdown sequence
    logger.info("Terminating acquisition. Flushing remaining queued buffers...")
    shutdown_event.set()
    for t in polling_threads:
        t.join(timeout=2.0)
    raw_queue.put(None)  # Sentinel triggers safe termination
    writer_thread.join(timeout=15.0)

    logger.info("Recording process successfully finalized.")


if __name__ == "__main__":
    main()
