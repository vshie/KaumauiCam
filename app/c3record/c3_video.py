"""Module for writing synchronized video streams with custom SEI absolute timestamp units into Matroska (MKV) container."""

import os
import logging
import threading
from datetime import datetime
from typing import Dict, Optional, Any, Set, Tuple
from datetime import timezone
import queue

import gi
import numpy as np  # noqa: E402

# Ensure GStreamer is available and initialized
gi.require_version("Gst", "1.0")
from gi.repository import Gst  # noqa: E402

# Set up logging
logger = logging.getLogger(__name__)

if not Gst.is_initialized():
    Gst.init([])


def _find_insertion_index(data: bytes) -> int:
    """Finds the index of the first video slice NAL unit in the H.264 byte stream.

    This ensures that custom SEI NAL units are inserted after AUD, SPS, PPS,
    but before the actual frame slice data, making it a valid H.264 stream.

    Args:
        data: The raw H.264 byte stream buffer.

    Returns:
        The byte index where the SEI NAL unit should be inserted. Defaults to 0
        if no slice is found.
    """
    i: int = 0
    n: int = len(data)
    while i < n - 4:
        # Check for 4-byte start code \x00\x00\x00\x01
        if data[i] == 0 and data[i + 1] == 0 and data[i + 2] == 0 and data[i + 3] == 1:
            nal_type: int = data[i + 4] & 0x1F
            if 1 <= nal_type <= 5:
                return i
            i += 4
        # Check for 3-byte start code \x00\x00\x01
        elif data[i] == 0 and data[i + 1] == 0 and data[i + 2] == 1:
            nal_type: int = data[i + 3] & 0x1F
            if 1 <= nal_type <= 5:
                return i
            i += 3
        else:
            i += 1
    return 0


class ArchiveC3VideoWriter:
    """A context manager to stream three synchronized video streams to an MKV file with embedded SEI timestamps."""

    def __init__(
        self,
        filepath: str,
        w_center: int,
        h_center: int,
        w_left: int,
        h_left: int,
        w_right: int,
        h_right: int,
        fps: int = 30,
        uuid: bytes = b"ms-ts-1234567890",
        bitrate: int = 5000,
        gop_size: int = 30,
        bframes: int = 0,
        bitrate_control: str = "cbr",
        quantizer: int = 21,
        speed_preset: str = "medium",
        tune: Optional[str] = "zerolatency",
        video_tag: Optional[str] = None,
        max_bitrate: Optional[int] = None,
        quality_min: Optional[int] = None,
        quality_max: Optional[int] = None,
        encoder: str = "default",
    ) -> None:
        """Initializes the three-stream video writer.

        Args:
            filepath: Absolute or relative path to the output MKV file.
            w_center: Width of the center camera frame.
            h_center: Height of the center camera frame.
            w_left: Width of the left camera frame.
            h_left: Height of the left camera frame.
            w_right: Width of the right camera frame.
            h_right: Height of the right camera frame.
            fps: Desired framerate for the output video.
            uuid: Exactly 16-byte UUID bytes to identify the SEI payload.
            bitrate: Target bitrate in kbit/sec (used for CBR/ABR). Defaults to 5000.
            gop_size: Maximal distance between two key-frames. Defaults to 30.
            bframes: Number of B-frames between I and P frames (0 to 16). Defaults to 0.
            bitrate_control: Rate control mode. Either "cbr" or "vbr". Defaults to "cbr".
            quantizer: Constant quantizer or quality value (0 to 50) used for VBR. Defaults to 21.
            speed_preset: Speed/quality tradeoff preset (e.g., "medium", "ultrafast"). Defaults to "medium".
            tune: Non-psychovisual tuning preset (e.g., "zerolatency", "none"). Defaults to "zerolatency".
            video_tag: Custom tag to write to matroskamux comments.
            max_bitrate: Optional maximum H.264 video encoding bitrate in kbit/sec.
            quality_min: Optional minimum quality boundary (qp-max) for VBR.
            quality_max: Optional maximum quality boundary (qp-min) for VBR.

        Raises:
            ValueError: If UUID is not exactly 16 bytes.
        """
        if len(uuid) != 16:
            raise ValueError("SEI unregistered UUID must be exactly 16 bytes.")

        self.filepath: str = os.path.abspath(filepath)
        self.fps: int = fps
        self.uuid: bytes = uuid
        self.bitrate: int = bitrate
        self.gop_size: int = gop_size
        self.bframes: int = bframes
        self.bitrate_control: str = bitrate_control
        self.quantizer: int = quantizer
        self.speed_preset: str = speed_preset
        self.tune: Optional[str] = tune
        self.video_tag: Optional[str] = video_tag
        self.max_bitrate: Optional[int] = max_bitrate
        self.quality_min: Optional[int] = quality_min
        self.quality_max: Optional[int] = quality_max
        self.encoder: str = encoder

        # Pre-stored resolution dimensions
        self._width_center: int = w_center
        self._height_center: int = h_center
        self._width_left: int = w_left
        self._height_left: int = h_left
        self._width_right: int = w_right
        self._height_right: int = h_right

        # GStreamer pipeline and element handles
        self._pipeline: Optional[Gst.Pipeline] = None
        self._appsrc_center: Optional[Gst.Element] = None
        self._appsrc_left: Optional[Gst.Element] = None
        self._appsrc_right: Optional[Gst.Element] = None

        self._initialized: bool = False
        self._start_time: Optional[datetime] = None
        self._duration_ns: int = int((1 / fps) * 1e9)

        # Separate segments per stream
        self._segment_center: Gst.Segment = Gst.Segment()
        self._segment_center.init(Gst.Format.TIME)
        self._segment_left: Gst.Segment = Gst.Segment()
        self._segment_left.init(Gst.Format.TIME)
        self._segment_right: Gst.Segment = Gst.Segment()
        self._segment_right.init(Gst.Format.TIME)

        # Thread synchronization and mapping
        self._lock: threading.Lock = threading.Lock()
        self._timestamp_map_center: Dict[int, int] = {}
        self._timestamp_map_left: Dict[int, int] = {}
        self._timestamp_map_right: Dict[int, int] = {}

    def _add_timestamp(self, stream: str, pts: int, timestamp_ms: int) -> None:
        """Adds a PTS and its corresponding UTC millisecond timestamp to the map of the specified stream.

        Args:
            stream: The identifier of the stream ('center', 'left', or 'right').
            pts: Presentation timestamp (PTS) in nanoseconds.
            timestamp_ms: Absolute UTC timestamp in milliseconds.
        """
        with self._lock:
            if stream == "center":
                self._timestamp_map_center[pts] = timestamp_ms
            elif stream == "left":
                self._timestamp_map_left[pts] = timestamp_ms
            elif stream == "right":
                self._timestamp_map_right[pts] = timestamp_ms

    def _pop_timestamp(self, timestamp_map: Dict[int, int], pts: int) -> Optional[int]:
        """Gets and removes the millisecond timestamp for a given PTS in a specific map.

        Args:
            timestamp_map: The timestamp dictionary to pop from.
            pts: Presentation timestamp (PTS) in nanoseconds.

        Returns:
            The absolute UTC timestamp in milliseconds, or None if not found.
        """
        with self._lock:
            return timestamp_map.pop(pts, None)

    def _init_pipeline(
        self,
        w_center: int,
        h_center: int,
        w_left: int,
        h_left: int,
        w_right: int,
        h_right: int,
    ) -> None:
        """Initializes and starts the GStreamer pipeline dynamically based on frame dimensions.

        Args:
            w_center: Width of the center frame.
            h_center: Height of the center frame.
            w_left: Width of the left frame.
            h_left: Height of the left frame.
            w_right: Width of the right frame.
            h_right: Height of the right frame.

        Raises:
            RuntimeError: If pipeline initialization or state-change fails.
        """
        caps_center: str = (
            f"image/jpeg,width={w_center},height={h_center},framerate={self.fps}/1"
        )
        caps_left: str = (
            f"image/jpeg,width={w_left},height={h_left},framerate={self.fps}/1"
        )
        caps_right: str = (
            f"image/jpeg,width={w_right},height={h_right},framerate={self.fps}/1"
        )

        encoder_map = {
            "default": "x264enc",
            "nvidia": "nvh264enc",
            "vaapi": "vaapih264enc"
        }
        enc_element = encoder_map.get(self.encoder, "x264enc")

        pipeline_str: str = (
            "matroskamux name=mux ! queue max-size-buffers=100 max-size-bytes=0 max-size-time=0 ! filesink name=sink "
            f"appsrc name=source_center caps={caps_center} format=time max-bytes=0 do-timestamp=false is-live=false ! "
            "queue max-size-buffers=100 max-size-bytes=0 max-size-time=0 ! "
            "jpegdec ! "
            "videoconvert ! "
            f"{enc_element} name=enc_center ! "
            "video/x-h264,stream-format=byte-stream ! "
            "identity name=sei_center ! "
            "h264parse ! "
            "queue max-size-buffers=100 max-size-bytes=0 max-size-time=0 ! "
            "mux.video_0 "
            f"appsrc name=source_left caps={caps_left} format=time max-bytes=0 do-timestamp=false is-live=false ! "
            "queue max-size-buffers=100 max-size-bytes=0 max-size-time=0 ! "
            "jpegdec ! "
            "videoconvert ! "
            f"{enc_element} name=enc_left ! "
            "video/x-h264,stream-format=byte-stream ! "
            "identity name=sei_left ! "
            "h264parse ! "
            "queue max-size-buffers=100 max-size-bytes=0 max-size-time=0 ! "
            "mux.video_1 "
            f"appsrc name=source_right caps={caps_right} format=time max-bytes=0 do-timestamp=false is-live=false ! "
            "queue max-size-buffers=100 max-size-bytes=0 max-size-time=0 ! "
            "jpegdec ! "
            "videoconvert ! "
            f"{enc_element} name=enc_right ! "
            "video/x-h264,stream-format=byte-stream ! "
            "identity name=sei_right ! "
            "h264parse ! "
            "queue max-size-buffers=100 max-size-bytes=0 max-size-time=0 ! "
            "mux.video_2"
        )

        try:
            self._pipeline = Gst.parse_launch(pipeline_str)
            if not self._pipeline:
                raise RuntimeError("Failed to parse GStreamer writing pipeline.")

            # Set output file path programmatically
            sink: Optional[Gst.Element] = self._pipeline.get_by_name("sink")
            if not sink:
                raise RuntimeError("Failed to find 'sink' element in writer pipeline.")
            sink.set_property("location", self.filepath)

            # Retrieve source elements
            self._appsrc_center = self._pipeline.get_by_name("source_center")
            self._appsrc_left = self._pipeline.get_by_name("source_left")
            self._appsrc_right = self._pipeline.get_by_name("source_right")

            if (
                not self._appsrc_center
                or not self._appsrc_left
                or not self._appsrc_right
            ):
                raise RuntimeError(
                    "Failed to find all 'source' elements in writer pipeline."
                )

            preset_map: Dict[str, int] = {
                "none": 0,
                "ultrafast": 1,
                "superfast": 2,
                "veryfast": 3,
                "faster": 4,
                "fast": 5,
                "medium": 6,
                "slow": 7,
                "slower": 8,
                "veryslow": 9,
                "placebo": 10,
            }

            tune_map: Dict[str, int] = {
                "none": 0,
                "stillimage": 1,
                "fastdecode": 2,
                "zerolatency": 4,
            }

            # Configure encoder settings across all three streams
            for enc_name in ["enc_center", "enc_left", "enc_right"]:
                encoder: Optional[Gst.Element] = self._pipeline.get_by_name(enc_name)
                if not encoder:
                    raise RuntimeError(
                        f"Failed to find encoder element '{enc_name}' in writer pipeline."
                    )

                if self.encoder == "default":
                    # Default values configured first
                    preset_key: str = self.speed_preset.lower()
                    encoder.set_property("speed-preset", preset_map.get(preset_key, 6))

                    if self.tune:
                        tune_val: int = tune_map.get(self.tune.lower(), 0)
                        if tune_val > 0:
                            encoder.set_property("tune", tune_val)

                    if self.bitrate_control.lower() == "vbr":
                        encoder.set_property("pass", 5)  # Constant Quality (qual)
                        encoder.set_property("quantizer", self.quantizer)
                        if self.max_bitrate is not None:
                            encoder.set_property("bitrate", self.max_bitrate)
                        else:
                            encoder.set_property("vbv-buf-capacity", 0)
                    else:
                        encoder.set_property("pass", 0)  # CBR/ABR
                        encoder.set_property("bitrate", self.bitrate)

                    if self.quality_min is not None:
                        encoder.set_property("qp-max", self.quality_min)
                    if self.quality_max is not None:
                        encoder.set_property("qp-min", self.quality_max)

                    encoder.set_property("key-int-max", self.gop_size)
                    encoder.set_property("bframes", self.bframes)
                    encoder.set_property("sliced-threads", True)

                elif self.encoder == "nvidia":
                    if self.bitrate_control.lower() == "vbr":
                        encoder.set_property("rc-mode", 3) # VBR
                        if self.max_bitrate is not None:
                            encoder.set_property("max-bitrate", self.max_bitrate)
                        if self.bitrate > 0:
                            encoder.set_property("bitrate", self.bitrate)
                    else:
                        encoder.set_property("rc-mode", 2) # CBR
                        encoder.set_property("bitrate", self.bitrate)
                    
                    encoder.set_property("gop-size", self.gop_size)
                    encoder.set_property("bframes", self.bframes)

                    if self.tune and self.tune.lower() == "zerolatency":
                        encoder.set_property("zerolatency", True)
                    
                    # NVIDIA preset mapping (simplified)
                    preset_key: str = self.speed_preset.lower()
                    if preset_key in ["ultrafast", "superfast", "veryfast"]:
                        encoder.set_property("preset", 1) # hp - High Performance
                    elif preset_key in ["slow", "slower", "veryslow"]:
                        encoder.set_property("preset", 2) # hq - High Quality
                    else:
                        encoder.set_property("preset", 0) # default

                elif self.encoder == "vaapi":
                    encoder.set_property("bitrate", self.bitrate)
                    if self.bitrate_control.lower() == "vbr":
                        encoder.set_property("rate-control", 4) # VBR
                    else:
                        encoder.set_property("rate-control", 2) # CBR
                    
                    encoder.set_property("keyframe-period", self.gop_size)
                    # Note: VAAPI doesn't have a direct "zerolatency" property in the same way,
                    # but setting tune="zerolatency" can be ignored or handled if needed.
                    # B-frames can be set via max-bframes if needed, but omitted for simplicity
                    # as it depends heavily on the specific Intel driver version support.

            # Apply Custom metadata / video tags using TagSetter interface on matroskamux
            if self.video_tag:
                muxer: Optional[Gst.Element] = self._pipeline.get_by_name("mux")
                if muxer:
                    try:
                        tag_list: Gst.TagList = Gst.TagList.new_empty()
                        tag_list.add_value(
                            Gst.TagMergeMode.REPLACE, Gst.TAG_COMMENT, self.video_tag
                        )
                        muxer.merge_tags(tag_list, Gst.TagMergeMode.REPLACE)
                        logger.info(
                            "Successfully merged custom comment tag into matroskamux."
                        )
                    except Exception as tag_err:
                        logger.warning(
                            "Could not set custom metadata tags on matroskamux: %s",
                            tag_err,
                        )

            # Attach SEI metadata injection probes
            sei_center: Optional[Gst.Element] = self._pipeline.get_by_name("sei_center")
            sei_left: Optional[Gst.Element] = self._pipeline.get_by_name("sei_left")
            sei_right: Optional[Gst.Element] = self._pipeline.get_by_name("sei_right")

            if not sei_center or not sei_left or not sei_right:
                raise RuntimeError(
                    "Failed to find all 'sei_injector' elements in writer pipeline."
                )

            pad_center: Optional[Gst.Pad] = sei_center.get_static_pad("src")
            pad_left: Optional[Gst.Pad] = sei_left.get_static_pad("src")
            pad_right: Optional[Gst.Pad] = sei_right.get_static_pad("src")

            if not pad_center or not pad_left or not pad_right:
                raise RuntimeError("Failed to get src pads from identity injectors.")

            pad_center.add_probe(
                Gst.PadProbeType.DATA_DOWNSTREAM, self._inject_sei_probe_center
            )
            pad_left.add_probe(
                Gst.PadProbeType.DATA_DOWNSTREAM, self._inject_sei_probe_left
            )
            pad_right.add_probe(
                Gst.PadProbeType.DATA_DOWNSTREAM, self._inject_sei_probe_right
            )

            logger.info(
                "Initializing GStreamer writer pipeline at location: %s", self.filepath
            )
            self._pipeline.set_state(Gst.State.PLAYING)

            self._initialized = True
            logger.info("GStreamer writer pipeline is successfully PLAYING.")

        except Exception as e:
            logger.exception("Error during pipeline initialization")
            if self._pipeline:
                self._pipeline.set_state(Gst.State.NULL)
            raise RuntimeError(f"GStreamer initialization failed: {e}") from e

    def _inject_sei_probe_center(
        self, pad: Gst.Pad, info: Gst.PadProbeInfo
    ) -> Gst.PadProbeReturn:
        """Downstream data probe callback for the center video stream."""
        if info.type & Gst.PadProbeType.EVENT_DOWNSTREAM:
            event: Optional[Gst.Event] = info.get_event()
            if event is not None and event.type == Gst.EventType.SEGMENT:
                self._segment_center = event.parse_segment()
            return Gst.PadProbeReturn.OK
        return self._inject_sei_probe(
            pad, info, self._segment_center, self._timestamp_map_center
        )

    def _inject_sei_probe_left(
        self, pad: Gst.Pad, info: Gst.PadProbeInfo
    ) -> Gst.PadProbeReturn:
        """Downstream data probe callback for the left video stream."""
        if info.type & Gst.PadProbeType.EVENT_DOWNSTREAM:
            event: Optional[Gst.Event] = info.get_event()
            if event is not None and event.type == Gst.EventType.SEGMENT:
                self._segment_left = event.parse_segment()
            return Gst.PadProbeReturn.OK
        return self._inject_sei_probe(
            pad, info, self._segment_left, self._timestamp_map_left
        )

    def _inject_sei_probe_right(
        self, pad: Gst.Pad, info: Gst.PadProbeInfo
    ) -> Gst.PadProbeReturn:
        """Downstream data probe callback for the right video stream."""
        if info.type & Gst.PadProbeType.EVENT_DOWNSTREAM:
            event: Optional[Gst.Event] = info.get_event()
            if event is not None and event.type == Gst.EventType.SEGMENT:
                self._segment_right = event.parse_segment()
            return Gst.PadProbeReturn.OK
        return self._inject_sei_probe(
            pad, info, self._segment_right, self._timestamp_map_right
        )

    def _inject_sei_probe(
        self,
        pad: Gst.Pad,
        info: Gst.PadProbeInfo,
        segment: Gst.Segment,
        timestamp_map: Dict[int, int],
    ) -> Gst.PadProbeReturn:
        """Core logic to inject SEI timestamp metadata dynamically into H264 NAL units."""
        buffer: Optional[Gst.Buffer] = info.get_buffer()
        if buffer is None:
            return Gst.PadProbeReturn.OK

        pts: int = buffer.pts
        if pts == Gst.CLOCK_TIME_NONE:
            return Gst.PadProbeReturn.OK

        # Map the shifted PTS to original relative stream time using the active segment
        original_pts: int = segment.to_running_time(Gst.Format.TIME, pts)
        if original_pts == Gst.CLOCK_TIME_NONE:
            original_pts = pts

        exact_ts_ms: Optional[int] = self._pop_timestamp(timestamp_map, original_pts)

        if exact_ts_ms is not None:
            # Build unregistered user data SEI NAL unit
            start_code: bytes = b"\x00\x00\x00\x01"
            nal_type: bytes = b"\x06"
            payload_type: bytes = b"\x05"
            payload_size: bytes = (
                b"\x20"  # 32 in hex (16 bytes UUID + 16 bytes ASCII payload)
            )
            ts_ascii: bytes = str(exact_ts_ms).zfill(16).encode("ascii")
            trailing_bits: bytes = b"\x80"

            sei_nalu: bytes = (
                start_code
                + nal_type
                + payload_type
                + payload_size
                + self.uuid
                + ts_ascii
                + trailing_bits
            )

            # Prepend the SEI NAL unit to the current frame NAL unit
            old_data: bytes = buffer.extract_dup(0, buffer.get_size())
            insert_idx: int = _find_insertion_index(old_data)
            combined_data: bytes = (
                old_data[:insert_idx] + sei_nalu + old_data[insert_idx:]
            )

            new_buffer: Gst.Buffer = Gst.Buffer.new_allocate(
                None, len(combined_data), None
            )
            new_buffer.fill(0, combined_data)

            # Preserve buffer headers and timings
            new_buffer.pts = buffer.pts
            new_buffer.dts = buffer.dts
            new_buffer.duration = buffer.duration
            new_buffer.set_flags(buffer.get_flags())

            pad.push(new_buffer)
            return Gst.PadProbeReturn.DROP

        return Gst.PadProbeReturn.OK

    def write(
        self,
        center_bytes: bytes,
        left_bytes: bytes,
        right_bytes: bytes,
        timestamp: datetime,
    ) -> None:
        """Writes center, left, and right MJPEG frame bytes simultaneously with their matching synchronized timestamp.

        Args:
            center_bytes: MJPEG bytes representing the center frame.
            left_bytes: MJPEG bytes representing the left frame.
            right_bytes: MJPEG bytes representing the right frame.
            timestamp: A datetime object corresponding to the frames' synchronized acquisition time.

        Raises:
            ValueError: If any input frame is invalid.
            RuntimeError: If appsrc fails to push any buffers.
        """
        if center_bytes is None or left_bytes is None or right_bytes is None:
            raise ValueError("All three input frames must be provided.")

        if not self._initialized:
            self._init_pipeline(
                self._width_center,
                self._height_center,
                self._width_left,
                self._height_left,
                self._width_right,
                self._height_right,
            )

        if (
            self._pipeline is None
            or self._appsrc_center is None
            or self._appsrc_left is None
            or self._appsrc_right is None
        ):
            raise RuntimeError("Pipeline is not properly initialized.")

        if self._start_time is None:
            self._start_time = timestamp

        # Calculate actual presentation time relative to the first frame
        delta = timestamp - self._start_time
        current_pts: int = int(delta.total_seconds() * 1e9)

        # Compute UTC milliseconds timestamp
        exact_timestamp_ms: int = int(timestamp.timestamp() * 1000)
        self._add_timestamp("center", current_pts, exact_timestamp_ms)
        self._add_timestamp("left", current_pts, exact_timestamp_ms)
        self._add_timestamp("right", current_pts, exact_timestamp_ms)

        # Wrap frames into Gst.Buffers
        buf_center: Gst.Buffer = Gst.Buffer.new_wrapped(center_bytes)
        buf_center.pts = current_pts
        buf_center.duration = self._duration_ns

        buf_left: Gst.Buffer = Gst.Buffer.new_wrapped(left_bytes)
        buf_left.pts = current_pts
        buf_left.duration = self._duration_ns

        buf_right: Gst.Buffer = Gst.Buffer.new_wrapped(right_bytes)
        buf_right.pts = current_pts
        buf_right.duration = self._duration_ns

        # Push all three frames downstream
        res_center: Gst.FlowReturn = self._appsrc_center.emit("push-buffer", buf_center)
        res_left: Gst.FlowReturn = self._appsrc_left.emit("push-buffer", buf_left)
        res_right: Gst.FlowReturn = self._appsrc_right.emit("push-buffer", buf_right)

        if (
            res_center != Gst.FlowReturn.OK
            or res_left != Gst.FlowReturn.OK
            or res_right != Gst.FlowReturn.OK
        ):
            raise RuntimeError(
                f"GStreamer appsrc failed to push buffer: center={res_center}, left={res_left}, right={res_right}"
            )

    def close(self) -> None:
        """Closes the GStreamer pipeline and blocks until filesink fully finalizes the MKV index."""
        if not self._initialized or self._pipeline is None:
            return

        pipeline: Gst.Pipeline = self._pipeline
        appsrc_center: Optional[Gst.Element] = self._appsrc_center
        appsrc_left: Optional[Gst.Element] = self._appsrc_left
        appsrc_right: Optional[Gst.Element] = self._appsrc_right

        # Mark as uninitialized so subsequent calls do nothing
        self._initialized = False
        self._pipeline = None
        self._appsrc_center = None
        self._appsrc_left = None
        self._appsrc_right = None

        logger.info("Closing ArchiveC3VideoWriter, propagating EOS...")
        if appsrc_center:
            appsrc_center.emit("end-of-stream")
        if appsrc_left:
            appsrc_left.emit("end-of-stream")
        if appsrc_right:
            appsrc_right.emit("end-of-stream")

        # Synchronously block on the bus until EOS or ERROR is received to ensure file finalization
        bus: Gst.Bus = pipeline.get_bus()
        msg: Optional[Gst.Message] = bus.timed_pop_filtered(
            15 * Gst.SECOND, Gst.MessageType.EOS | Gst.MessageType.ERROR
        )

        if msg is None:
            logger.warning("GStreamer pipeline EOS finalization timed out.")
        elif msg.type == Gst.MessageType.ERROR:
            err, debug = msg.parse_error()
            logger.error(
                "GStreamer pipeline error on finalization: %s (%s)", err, debug
            )
        else:
            logger.info("GStreamer pipeline received EOS successfully.")

        pipeline.set_state(Gst.State.NULL)
        logger.info("ArchiveC3VideoWriter closed successfully.")

    def __enter__(self) -> "ArchiveC3VideoWriter":
        """Enters the context manager block."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exits the context manager and cleans up resources."""
        self.close()


class ArchiveC3VideoReader:
    """A context manager to read back raw video frames from 3 streams and extract their matching SEI timestamps."""

    def __init__(self, filepath: str, uuid: bytes = b"ms-ts-1234567890") -> None:
        """Initializes the three-stream video reader.

        Args:
            filepath: Path to the input MKV file.
            uuid: Exactly 16-byte UUID bytes to match the SEI payload signature.

        Raises:
            ValueError: If UUID is not exactly 16 bytes.
        """
        if len(uuid) != 16:
            raise ValueError("SEI unregistered UUID must be exactly 16 bytes.")

        self.filepath: str = os.path.abspath(filepath)
        self.uuid: bytes = uuid

        self._pipeline: Optional[Gst.Pipeline] = None
        self._appsink_center: Optional[Gst.Element] = None
        self._appsink_left: Optional[Gst.Element] = None
        self._appsink_right: Optional[Gst.Element] = None
        self._initialized: bool = False

        # Independent queues to align sequential decoded frames and absolute timestamps
        self._timestamp_queue_center: queue.Queue[datetime] = queue.Queue()
        self._timestamp_queue_left: queue.Queue[datetime] = queue.Queue()
        self._timestamp_queue_right: queue.Queue[datetime] = queue.Queue()

        # Dynamic linking lock and mapping
        self._link_lock: threading.Lock = threading.Lock()
        self._linked_bins: Set[str] = set()

    def _init_pipeline(self) -> None:
        """Initializes and runs the reading pipeline.

        Raises:
            FileNotFoundError: If the input file does not exist.
            RuntimeError: If GStreamer pipeline construction or state-change fails.
        """
        if not os.path.exists(self.filepath):
            raise FileNotFoundError(f"Video file not found: {self.filepath}")

        # Construct three separate, unlinked decoding pipelines starting with queues to prevent deadlocking
        pipeline_str: str = (
            "filesrc name=src ! matroskademux name=demux "
            "queue name=q_center ! h264parse ! video/x-h264,stream-format=byte-stream ! identity name=sei_center ! avdec_h264 ! videoconvert ! video/x-raw,format=RGB ! appsink name=sink_center emit-signals=false sync=false drop=false "
            "queue name=q_left ! h264parse ! video/x-h264,stream-format=byte-stream ! identity name=sei_left ! avdec_h264 ! videoconvert ! video/x-raw,format=RGB ! appsink name=sink_left emit-signals=false sync=false drop=false "
            "queue name=q_right ! h264parse ! video/x-h264,stream-format=byte-stream ! identity name=sei_right ! avdec_h264 ! videoconvert ! video/x-raw,format=RGB ! appsink name=sink_right emit-signals=false sync=false drop=false"
        )

        try:
            self._pipeline = Gst.parse_launch(pipeline_str)
            if not self._pipeline:
                raise RuntimeError("Failed to parse GStreamer reading pipeline.")

            # Set path programmatically
            src: Optional[Gst.Element] = self._pipeline.get_by_name("src")
            if not src:
                raise RuntimeError("Failed to find 'src' element in reader pipeline.")
            src.set_property("location", self.filepath)

            # Retrieve appsink elements
            self._appsink_center = self._pipeline.get_by_name("sink_center")
            self._appsink_left = self._pipeline.get_by_name("sink_left")
            self._appsink_right = self._pipeline.get_by_name("sink_right")

            if (
                not self._appsink_center
                or not self._appsink_left
                or not self._appsink_right
            ):
                raise RuntimeError(
                    "Failed to find all 'sink' elements in reader pipeline."
                )

            # Attach SEI extraction probes
            for name in ["sei_center", "sei_left", "sei_right"]:
                extractor: Optional[Gst.Element] = self._pipeline.get_by_name(name)
                if not extractor:
                    raise RuntimeError(
                        f"Failed to find extractor element '{name}' in reader pipeline."
                    )

                src_pad: Optional[Gst.Pad] = extractor.get_static_pad("src")
                if not src_pad:
                    raise RuntimeError(
                        f"Failed to get src pad from '{name}' extractor."
                    )

                if name == "sei_center":
                    src_pad.add_probe(
                        Gst.PadProbeType.BUFFER, self._extract_sei_probe_center
                    )
                elif name == "sei_left":
                    src_pad.add_probe(
                        Gst.PadProbeType.BUFFER, self._extract_sei_probe_left
                    )
                elif name == "sei_right":
                    src_pad.add_probe(
                        Gst.PadProbeType.BUFFER, self._extract_sei_probe_right
                    )

            # Register dynamic pad linker callback
            demux: Optional[Gst.Element] = self._pipeline.get_by_name("demux")
            if not demux:
                raise RuntimeError("Failed to find 'demux' element in reader pipeline.")
            demux.connect("pad-added", self._on_pad_added)

            # Start playing
            logger.info(
                "Starting GStreamer reader pipeline for location: %s", self.filepath
            )
            ret: Gst.StateChangeReturn = self._pipeline.set_state(Gst.State.PLAYING)
            if ret == Gst.StateChangeReturn.FAILURE:
                raise RuntimeError(
                    "Failed to set GStreamer reader pipeline to PLAYING state."
                )

            self._initialized = True

        except Exception as e:
            logger.exception("Error initializing reader pipeline")
            if self._pipeline:
                self._pipeline.set_state(Gst.State.NULL)
            raise RuntimeError(f"GStreamer reader initialization failed: {e}") from e

    def _on_pad_added(self, demux: Gst.Element, pad: Gst.Pad) -> None:
        """Handles matroskademux dynamic pad addition, linking tracks safely to corresponding queue bins."""
        pad_name: str = pad.get_name()

        if not pad_name.startswith("video"):
            return

        with self._link_lock:
            target_name: Optional[str] = None
            if pad_name == "video_0":
                target_name = "q_center"
            elif pad_name == "video_1":
                target_name = "q_left"
            elif pad_name == "video_2":
                target_name = "q_right"
            else:
                # Fallback sequentially based on order of discovery
                if "q_center" not in self._linked_bins:
                    target_name = "q_center"
                elif "q_left" not in self._linked_bins:
                    target_name = "q_left"
                elif "q_right" not in self._linked_bins:
                    target_name = "q_right"

            if target_name and target_name not in self._linked_bins:
                target_element: Optional[Gst.Element] = self._pipeline.get_by_name(
                    target_name
                )
                if target_element:
                    sink_pad: Optional[Gst.Pad] = target_element.get_static_pad("sink")
                    if sink_pad:
                        res: Gst.PadLinkReturn = pad.link(sink_pad)
                        if res == Gst.PadLinkReturn.OK:
                            self._linked_bins.add(target_name)
                        else:
                            logger.error(
                                "Failed to link dynamic pad %s to %s: %s",
                                pad_name,
                                target_name,
                                res,
                            )

    def _extract_sei_probe_center(
        self, pad: Gst.Pad, info: Gst.PadProbeInfo
    ) -> Gst.PadProbeReturn:
        """Extracts SEI timestamps from center stream."""
        return self._extract_sei_probe(pad, info, self._timestamp_queue_center)

    def _extract_sei_probe_left(
        self, pad: Gst.Pad, info: Gst.PadProbeInfo
    ) -> Gst.PadProbeReturn:
        """Extracts SEI timestamps from left stream."""
        return self._extract_sei_probe(pad, info, self._timestamp_queue_left)

    def _extract_sei_probe_right(
        self, pad: Gst.Pad, info: Gst.PadProbeInfo
    ) -> Gst.PadProbeReturn:
        """Extracts SEI timestamps from right stream."""
        return self._extract_sei_probe(pad, info, self._timestamp_queue_right)

    def _extract_sei_probe(
        self, pad: Gst.Pad, info: Gst.PadProbeInfo, q: "queue.Queue[datetime]"
    ) -> Gst.PadProbeReturn:
        """Probe callback to extract and queue raw SEI absolute timestamp metadata from a stream."""
        buffer: Optional[Gst.Buffer] = info.get_buffer()
        if buffer is None:
            return Gst.PadProbeReturn.OK

        success: bool
        map_info: Gst.MapInfo
        success, map_info = buffer.map(Gst.MapFlags.READ)
        if not success:
            return Gst.PadProbeReturn.OK

        try:
            data: bytes = map_info.data
            search_signature: bytes = b"\x06\x05\x20" + self.uuid
            idx: int = data.find(search_signature)

            if idx != -1:
                ts_start: int = idx + 19
                ts_end: int = ts_start + 16

                if ts_end <= len(data):
                    ts_bytes: bytes = data[ts_start:ts_end]
                    extracted_ts_ms: int = int(ts_bytes.decode("ascii"))
                    timestamp: datetime = datetime.fromtimestamp(
                        extracted_ts_ms / 1000.0, tz=timezone.utc
                    )
                    q.put(timestamp)
                    logger.debug("Extracted SEI Timestamp from stream: %s", timestamp)
        except Exception as e:
            logger.error("Failed to parse SEI metadata: %s", e)
        finally:
            buffer.unmap(map_info)

        return Gst.PadProbeReturn.OK

    def _extract_frame(self, sample: Gst.Sample) -> np.ndarray:
        """Helper to extract a frame as a numpy array from a Gst.Sample."""
        buffer: Optional[Gst.Buffer] = sample.get_buffer()
        caps: Optional[Gst.Caps] = sample.get_caps()
        if not buffer or not caps:
            raise RuntimeError("Pulled sample contains invalid buffer or caps.")

        # Extract native resolution from caps
        structure: Gst.Structure = caps.get_structure(0)
        success_w, width = structure.get_int("width")
        success_h, height = structure.get_int("height")

        if not success_w or not success_h:
            raise RuntimeError(
                "Failed to resolve frame dimensions from GStreamer caps."
            )

        # Map buffer memory into numpy array
        success: bool
        map_info: Gst.MapInfo
        success, map_info = buffer.map(Gst.MapFlags.READ)
        if not success:
            raise RuntimeError("Failed to map raw GStreamer buffer.")

        try:
            frame_data: bytes = map_info.data
            frame: np.ndarray = (
                np.frombuffer(frame_data, dtype=np.uint8)
                .reshape((height, width, 3))
                .copy()
            )
        finally:
            buffer.unmap(map_info)

        return frame

    def close(self) -> None:
        """Closes the reading pipeline and releases resources."""
        if self._pipeline:
            logger.info("Closing ArchiveC3VideoReader...")
            self._pipeline.set_state(Gst.State.NULL)
            self._pipeline = None
            self._appsink_center = None
            self._appsink_left = None
            self._appsink_right = None
            self._initialized = False

    def __enter__(self) -> "ArchiveC3VideoReader":
        """Enters the context manager block."""
        if not self._initialized:
            self._init_pipeline()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exits the context manager and cleans up resources."""
        self.close()

    def __iter__(self) -> "ArchiveC3VideoReader":
        """Iterates over the video frames."""
        return self

    def __next__(self) -> Tuple[np.ndarray, np.ndarray, np.ndarray, Optional[datetime]]:
        """Pulls the next synchronized frame from center, left, and right streams, with the SEI timestamp.

        Returns:
            A tuple of (center_frame, left_frame, right_frame, timestamp) where frame elements
            are numpy RGB arrays, and timestamp is a UTC datetime or None if missing.

        Raises:
            RuntimeError: If reader is closed or GStreamer error encountered.
            StopIteration: When end-of-stream is reached.
        """
        if (
            not self._initialized
            or self._pipeline is None
            or self._appsink_center is None
            or self._appsink_left is None
            or self._appsink_right is None
        ):
            raise RuntimeError("Reader is not open or has already been closed.")

        # Pull samples from all three appsinks sequentially
        sample_center: Optional[Gst.Sample] = self._appsink_center.emit("pull-sample")
        sample_left: Optional[Gst.Sample] = self._appsink_left.emit("pull-sample")
        sample_right: Optional[Gst.Sample] = self._appsink_right.emit("pull-sample")

        # If any are None, we reached the end of stream or hit an error
        if sample_center is None or sample_left is None or sample_right is None:
            # Check the bus for errors
            bus: Gst.Bus = self._pipeline.get_bus()
            msg: Optional[Gst.Message] = bus.pop_filtered(Gst.MessageType.ERROR)
            if msg:
                err, debug = msg.parse_error()
                raise RuntimeError(
                    f"GStreamer pipeline error during read: {err} ({debug})"
                )
            raise StopIteration

        center_frame: np.ndarray = self._extract_frame(sample_center)
        left_frame: np.ndarray = self._extract_frame(sample_left)
        right_frame: np.ndarray = self._extract_frame(sample_right)

        # Match popped decoded frames to corresponding queued timestamps
        ts_center: Optional[datetime] = None
        ts_left: Optional[datetime] = None
        ts_right: Optional[datetime] = None

        try:
            ts_center = self._timestamp_queue_center.get_nowait()
        except queue.Empty:
            pass

        try:
            ts_left = self._timestamp_queue_left.get_nowait()
        except queue.Empty:
            pass

        try:
            ts_right = self._timestamp_queue_right.get_nowait()
        except queue.Empty:
            pass

        # Select the timestamp (all should match perfectly as streams are synchronized)
        timestamp: Optional[datetime] = ts_center or ts_left or ts_right
        if timestamp is None:
            logger.warning(
                "No queued timestamp found for pulled synchronized frame set."
            )

        return center_frame, left_frame, right_frame, timestamp


class C3VideoWriterManager:
    """Manages writing segmented three-camera video files to disk."""

    def __init__(
        self,
        output_dir: str,
        output_format: str,
        duration: float,
        fps: int,
        w_center: int,
        h_center: int,
        w_left: int,
        h_left: int,
        w_right: int,
        h_right: int,
        bitrate: int = 5000,
        gop_size: int = 30,
        bframes: int = 0,
        bitrate_control: str = "cbr",
        quantizer: int = 21,
        speed_preset: str = "medium",
        tune: Optional[str] = "zerolatency",
        video_tag: Optional[str] = None,
        max_bitrate: Optional[int] = None,
        quality_min: Optional[int] = None,
        quality_max: Optional[int] = None,
        encoder: str = "default",
    ) -> None:
        """Initializes the segmented C3 video manager.

        Args:
            output_dir: Directory where the output MKV files will be saved.
            output_format: strftime-compatible format string for video naming.
            duration: Target duration in seconds of each segment.
            fps: Desired framerate.
            w_center: Width of center camera stream.
            h_center: Height of center camera stream.
            w_left: Width of left camera stream.
            h_left: Height of left camera stream.
            w_right: Width of right camera stream.
            h_right: Height of right camera stream.
            bitrate: H.264 video bitrate in kbit/sec.
            gop_size: Distance between keyframes.
            bframes: Number of B-frames.
            bitrate_control: "cbr" or "vbr".
            quantizer: Constant quantizer.
            speed_preset: Encoder speed preset.
            tune: Encoder tune option.
            video_tag: Optional custom string metadata tags.
            max_bitrate: Optional maximum H.264 video encoding bitrate in kbit/sec.
            quality_min: Optional minimum quality boundary (qp-max) for VBR.
            quality_max: Optional maximum quality boundary (qp-min) for VBR.
        """
        self.output_dir: str = os.path.abspath(output_dir)
        self.output_format: str = output_format
        self.duration: float = duration
        self.fps: int = fps
        self.bitrate: int = bitrate
        self.gop_size: int = gop_size
        self.bframes: int = bframes
        self.bitrate_control: str = bitrate_control
        self.quantizer: int = quantizer
        self.speed_preset: str = speed_preset
        self.tune: Optional[str] = tune
        self.video_tag: Optional[str] = video_tag
        self.max_bitrate: Optional[int] = max_bitrate
        self.quality_min: Optional[int] = quality_min
        self.quality_max: Optional[int] = quality_max
        self.encoder: str = encoder

        # Store resolutions
        self._width_center: int = w_center
        self._height_center: int = h_center
        self._width_left: int = w_left
        self._height_left: int = h_left
        self._width_right: int = w_right
        self._height_right: int = h_right

        self.current_writer: Optional[ArchiveC3VideoWriter] = None
        self.segment_start_time: Optional[datetime] = None

        os.makedirs(self.output_dir, exist_ok=True)

    def write_frame(
        self,
        center_bytes: bytes,
        left_bytes: bytes,
        right_bytes: bytes,
        timestamp: datetime,
    ) -> None:
        """Writes synchronized video frames, automatically rotating files at the segment boundaries.

        Args:
            center_bytes: MJPEG bytes representing the center frame.
            left_bytes: MJPEG bytes representing the left frame.
            right_bytes: MJPEG bytes representing the right frame.
            timestamp: Synchronized acquisition UTC timestamp.
        """
        # Automatically rotate if segment duration exceeded
        if self.current_writer is not None and self.segment_start_time is not None:
            elapsed: float = (timestamp - self.segment_start_time).total_seconds()
            if elapsed >= self.duration:
                logger.info(
                    "Segment duration %s exceeded (elapsed: %.2f s). Rotating to new video file...",
                    self.duration,
                    elapsed,
                )
                self.current_writer.close()
                self.current_writer = None
                self.segment_start_time = None

        # Initialize the current writer if not currently active
        if self.current_writer is None:
            filename: str = timestamp.strftime(self.output_format)
            # Guarantee file suffix is mkv
            if not filename.endswith(".mkv"):
                filename += ".mkv"

            filepath: str = os.path.join(self.output_dir, filename)
            logger.info("Opening new video segment: %s", filepath)

            self.current_writer = ArchiveC3VideoWriter(
                filepath=filepath,
                w_center=self._width_center,
                h_center=self._height_center,
                w_left=self._width_left,
                h_left=self._height_left,
                w_right=self._width_right,
                h_right=self._height_right,
                fps=self.fps,
                bitrate=self.bitrate,
                gop_size=self.gop_size,
                bframes=self.bframes,
                bitrate_control=self.bitrate_control,
                quantizer=self.quantizer,
                speed_preset=self.speed_preset,
                tune=self.tune,
                video_tag=self.video_tag,
                max_bitrate=self.max_bitrate,
                quality_min=self.quality_min,
                quality_max=self.quality_max,
                encoder=self.encoder,
            )
            self.segment_start_time = timestamp

        self.current_writer.write(center_bytes, left_bytes, right_bytes, timestamp)

    def close(self) -> None:
        """Closes and finalizes any active video writer segment."""
        if self.current_writer is not None:
            logger.info("Closing active video segment.")
            self.current_writer.close()
            self.current_writer = None
            self.segment_start_time = None
