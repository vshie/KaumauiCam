"""Hue-based encoding and decoding for 16-bit depth matrices with GPU (CUDA JIT) and CPU JIT acceleration.

Based on the 1,531 code-point hue colorization scheme described in:
https://github.com/jdtremaine/hue-codec
"""

import logging
import math
from typing import Any, Optional, Tuple

from numba import cuda, njit, prange
import numpy as np

logger: logging.Logger = logging.getLogger(__name__)

# Constants
HUE_ENCODER_MAX: int = 1530
CUDA_THREADS_PER_BLOCK_2D: Tuple[int, int] = (16, 16)


# -----------------------------------------------------------------------------
# CUDA JIT Kernels
# -----------------------------------------------------------------------------


@cuda.jit
def _hue_encode_cuda_kernel(
    depth: np.ndarray,
    out: np.ndarray,
    min_u: float,
    range_u: float,
    inverse: bool,
    is_bgr: bool,
    robust: bool,
) -> None:
    """2D CUDA kernel for encoding 16-bit depth pixels into 8-bit 3-channel hue values."""
    x: int
    y: int
    x, y = cuda.grid(2)
    height: int = depth.shape[0]
    width: int = depth.shape[1]

    if x < width and y < height:
        d: int = int(depth[y, x])
        if d == 0:
            out[y, x, 0] = 0
            out[y, x, 1] = 0
            out[y, x, 2] = 0
        else:
            d_val: float = float(d)
            if inverse:
                d_val = 1.0 / d_val

            scaled: float = (d_val - min_u) / range_u
            if scaled < 0.0:
                scaled = 0.0
            elif scaled > 1.0:
                scaled = 1.0

            v: int = (
                1 + int(round(1529.0 * scaled))
                if robust
                else int(round(1530.0 * scaled))
            )

            r: int
            g: int
            b: int
            if v == 0:
                r, g, b = 0, 0, 0
            elif v < 256:
                r, g, b = 255, v - 1, 0
            elif v < 511:
                r, g, b = 511 - v, 255, 0
            elif v < 766:
                r, g, b = 0, 255, v - 511
            elif v < 1021:
                r, g, b = 0, 1021 - v, 255
            elif v < 1276:
                r, g, b = v - 1021, 0, 255
            elif v < 1531:
                r, g, b = 255, 0, 1531 - v
            else:
                r, g, b = 255, 0, 0

            if is_bgr:
                out[y, x, 0] = b
                out[y, x, 1] = g
                out[y, x, 2] = r
            else:
                out[y, x, 0] = r
                out[y, x, 1] = g
                out[y, x, 2] = b


@cuda.jit
def _hue_decode_cuda_kernel(
    color: np.ndarray,
    out: np.ndarray,
    min_u: float,
    range_u: float,
    inverse: bool,
    is_bgr: bool,
    robust: bool,
) -> None:
    """2D CUDA kernel for decoding 8-bit 3-channel hue values back to 16-bit depth."""
    x: int
    y: int
    x, y = cuda.grid(2)
    height: int = color.shape[0]
    width: int = color.shape[1]

    if x < width and y < height:
        c0: int = int(color[y, x, 0])
        c1: int = int(color[y, x, 1])
        c2: int = int(color[y, x, 2])

        r: int = c2 if is_bgr else c0
        g: int = c1
        b: int = c0 if is_bgr else c2

        v: int = 0
        if r + g + b > 128:
            if r >= g and r >= b:
                v = g - b + 1 if g >= b else g - b + 1531
            elif g >= r and g >= b:
                v = b - r + 511
            elif b >= g and b >= r:
                v = r - g + 1021

        if v == 0 or v > 1530:
            out[y, x] = 0
        else:
            scaled: float = float(v - 1) / 1529.0 if robust else float(v) / 1530.0
            d_val: float = min_u + (range_u * scaled)
            if inverse:
                d_val = 1.0 / d_val
            out[y, x] = int(round(d_val))


# -----------------------------------------------------------------------------
# CPU Multi-Core JIT Kernels
# -----------------------------------------------------------------------------


@njit(parallel=True, fastmath=False)
def _hue_encode_cpu_kernel(
    depth: np.ndarray,
    out: np.ndarray,
    min_u: float,
    range_u: float,
    inverse: bool,
    is_bgr: bool,
    robust: bool,
) -> None:
    """Multi-threaded CPU JIT kernel for encoding 16-bit depth into 8-bit 3-channel hue."""
    height: int = depth.shape[0]
    width: int = depth.shape[1]

    for y in prange(height):
        for x in range(width):
            d: int = int(depth[y, x])
            if d == 0:
                out[y, x, 0] = 0
                out[y, x, 1] = 0
                out[y, x, 2] = 0
            else:
                d_val: float = float(d)
                if inverse:
                    d_val = 1.0 / d_val

                scaled: float = (d_val - min_u) / range_u
                if scaled < 0.0:
                    scaled = 0.0
                elif scaled > 1.0:
                    scaled = 1.0

                v: int = (
                    1 + int(round(1529.0 * scaled))
                    if robust
                    else int(round(1530.0 * scaled))
                )

                r: int
                g: int
                b: int
                if v == 0:
                    r, g, b = 0, 0, 0
                elif v < 256:
                    r, g, b = 255, v - 1, 0
                elif v < 511:
                    r, g, b = 511 - v, 255, 0
                elif v < 766:
                    r, g, b = 0, 255, v - 511
                elif v < 1021:
                    r, g, b = 0, 1021 - v, 255
                elif v < 1276:
                    r, g, b = v - 1021, 0, 255
                elif v < 1531:
                    r, g, b = 255, 0, 1531 - v
                else:
                    r, g, b = 255, 0, 0

                if is_bgr:
                    out[y, x, 0] = b
                    out[y, x, 1] = g
                    out[y, x, 2] = r
                else:
                    out[y, x, 0] = r
                    out[y, x, 1] = g
                    out[y, x, 2] = b


@njit(parallel=True, fastmath=False)
def _hue_decode_cpu_kernel(
    color: np.ndarray,
    out: np.ndarray,
    min_u: float,
    range_u: float,
    inverse: bool,
    is_bgr: bool,
    robust: bool,
) -> None:
    """Multi-threaded CPU JIT kernel for decoding 8-bit 3-channel hue into 16-bit depth."""
    height: int = color.shape[0]
    width: int = color.shape[1]

    for y in prange(height):
        for x in range(width):
            c0: int = int(color[y, x, 0])
            c1: int = int(color[y, x, 1])
            c2: int = int(color[y, x, 2])

            r: int = c2 if is_bgr else c0
            g: int = c1
            b: int = c0 if is_bgr else c2

            v: int = 0
            if r + g + b > 128:
                if r >= g and r >= b:
                    v = g - b + 1 if g >= b else g - b + 1531
                elif g >= r and g >= b:
                    v = b - r + 511
                elif b >= g and b >= r:
                    v = r - g + 1021

            if v == 0 or v > 1530:
                out[y, x] = 0
            else:
                scaled: float = float(v - 1) / 1529.0 if robust else float(v) / 1530.0
                d_val: float = min_u + (range_u * scaled)
                if inverse:
                    d_val = 1.0 / d_val
                out[y, x] = int(round(d_val))


# -----------------------------------------------------------------------------
# Parameter Calculation Helper
# -----------------------------------------------------------------------------


def compute_depth_range_units(
    depth_min_m: float,
    depth_max_m: float,
    depth_scale: float,
    inverse: bool,
) -> Tuple[float, float]:
    """Calculates min_u and range_u values for hue scaling.

    Args:
        depth_min_m: Minimum depth in meters.
        depth_max_m: Maximum depth in meters.
        depth_scale: Scale factor to convert raw uint16 integers to meters.
        inverse: Whether inverse colorization (disparity 1/d) is enabled.

    Returns:
        A tuple of (min_u, range_u).

    Raises:
        ValueError: If depth parameters are invalid.
    """
    if depth_min_m < 0:
        raise ValueError(f"depth_min_m must be non-negative, got {depth_min_m}")
    if depth_max_m <= depth_min_m:
        raise ValueError(
            f"depth_max_m ({depth_max_m}) must be greater than depth_min_m ({depth_min_m})"
        )
    if depth_scale <= 0:
        raise ValueError(f"depth_scale must be strictly positive, got {depth_scale}")

    min_u: float = depth_min_m / depth_scale
    max_u: float = depth_max_m / depth_scale

    if inverse:
        if min_u == 0.0:
            min_u = 1e-9
        min_u = 1.0 / min_u
        max_u = 1.0 / max_u

    return min_u, max_u - min_u


# -----------------------------------------------------------------------------
# High-Level Functional APIs
# -----------------------------------------------------------------------------


def hue_encode_cuda(
    depth_matrix: Any,
    depth_min_m: float,
    depth_max_m: float,
    depth_scale: float = 0.001,
    inverse_colorization: bool = False,
    bgr: bool = False,
    robust: bool = True,
    out: Optional[Any] = None,
    stream: Optional[Any] = None,
) -> Any:
    """Encodes a 16-bit depth matrix to an 8-bit 3-channel hue image using CUDA JIT.

    Args:
        depth_matrix: 2D uint16 numpy array or Numba DeviceNDArray of shape (H, W).
        depth_min_m: Minimum sensor depth in meters.
        depth_max_m: Maximum sensor depth in meters.
        depth_scale: Conversion factor from raw integers to meters (default: 0.001 for mm).
        inverse_colorization: When True, encodes inverse depth (1/d, uniform disparity resolution).
        bgr: When True, outputs BGR order instead of RGB.
        robust: When True, maps valid depths to [1, 1530], strictly reserving 0 for missing depth.
        out: Optional pre-allocated (H, W, 3) uint8 output array (host or device).
        stream: Optional CUDA stream for asynchronous execution.

    Returns:
        3D uint8 array of shape (H, W, 3) with encoded hue colors.

    Raises:
        RuntimeError: If CUDA is not available on this system.
        ValueError: If array dimensions or dtypes are invalid.
    """
    if not cuda.is_available():
        raise RuntimeError("CUDA is not available on this system.")

    is_device_input: bool = cuda.is_cuda_array(depth_matrix)
    height: int = depth_matrix.shape[0]
    width: int = depth_matrix.shape[1]

    if depth_matrix.ndim != 2:
        raise ValueError(f"depth_matrix must be 2D, got shape {depth_matrix.shape}")
    if depth_matrix.dtype != np.uint16:
        raise ValueError(f"depth_matrix dtype must be uint16, got {depth_matrix.dtype}")

    min_u: float
    range_u: float
    min_u, range_u = compute_depth_range_units(
        depth_min_m, depth_max_m, depth_scale, inverse_colorization
    )

    blocks_x: int = math.ceil(width / CUDA_THREADS_PER_BLOCK_2D[0])
    blocks_y: int = math.ceil(height / CUDA_THREADS_PER_BLOCK_2D[1])
    grid: Tuple[int, int] = (blocks_x, blocks_y)

    if is_device_input:
        d_out: Any = (
            out
            if out is not None
            else cuda.device_array((height, width, 3), dtype=np.uint8)
        )
        _hue_encode_cuda_kernel[grid, CUDA_THREADS_PER_BLOCK_2D, stream](
            depth_matrix, d_out, min_u, range_u, inverse_colorization, bgr, robust
        )
        return d_out

    # Host NumPy input
    d_depth: Any = cuda.to_device(depth_matrix, stream=stream)
    d_out: Any = cuda.device_array((height, width, 3), dtype=np.uint8, stream=stream)
    _hue_encode_cuda_kernel[grid, CUDA_THREADS_PER_BLOCK_2D, stream](
        d_depth, d_out, min_u, range_u, inverse_colorization, bgr, robust
    )

    if out is not None:
        if out.shape != (height, width, 3) or out.dtype != np.uint8:
            raise ValueError(f"out array must be shape ({height}, {width}, 3) uint8")
        d_out.copy_to_host(out, stream=stream)
        return out

    host_out: np.ndarray = np.empty((height, width, 3), dtype=np.uint8)
    d_out.copy_to_host(host_out, stream=stream)
    return host_out


def hue_decode_cuda(
    color_matrix: Any,
    depth_min_m: float,
    depth_max_m: float,
    depth_scale: float = 0.001,
    inverse_colorization: bool = False,
    bgr: bool = False,
    robust: bool = True,
    out: Optional[Any] = None,
    stream: Optional[Any] = None,
) -> Any:
    """Decodes an 8-bit 3-channel hue image back to a 16-bit depth matrix using CUDA JIT.

    Args:
        color_matrix: 3D uint8 numpy array or Numba DeviceNDArray of shape (H, W, 3).
        depth_min_m: Minimum sensor depth in meters used during encoding.
        depth_max_m: Maximum sensor depth in meters used during encoding.
        depth_scale: Scale factor used during encoding.
        inverse_colorization: True if encoded with inverse colorization.
        bgr: True if color_matrix is in BGR format instead of RGB.
        robust: True if encoded with robust mode.
        out: Optional pre-allocated (H, W) uint16 output array.
        stream: Optional CUDA stream for asynchronous execution.

    Returns:
        2D uint16 array of shape (H, W) containing recovered depth values.

    Raises:
        RuntimeError: If CUDA is not available.
        ValueError: If array dimensions or dtypes are invalid.
    """
    if not cuda.is_available():
        raise RuntimeError("CUDA is not available on this system.")

    is_device_input: bool = cuda.is_cuda_array(color_matrix)
    height: int = color_matrix.shape[0]
    width: int = color_matrix.shape[1]

    if color_matrix.ndim != 3 or color_matrix.shape[2] != 3:
        raise ValueError(
            f"color_matrix must have shape (H, W, 3), got {color_matrix.shape}"
        )
    if color_matrix.dtype != np.uint8:
        raise ValueError(f"color_matrix dtype must be uint8, got {color_matrix.dtype}")

    min_u: float
    range_u: float
    min_u, range_u = compute_depth_range_units(
        depth_min_m, depth_max_m, depth_scale, inverse_colorization
    )

    blocks_x: int = math.ceil(width / CUDA_THREADS_PER_BLOCK_2D[0])
    blocks_y: int = math.ceil(height / CUDA_THREADS_PER_BLOCK_2D[1])
    grid: Tuple[int, int] = (blocks_x, blocks_y)

    if is_device_input:
        d_out: Any = (
            out
            if out is not None
            else cuda.device_array((height, width), dtype=np.uint16)
        )
        _hue_decode_cuda_kernel[grid, CUDA_THREADS_PER_BLOCK_2D, stream](
            color_matrix, d_out, min_u, range_u, inverse_colorization, bgr, robust
        )
        return d_out

    d_color: Any = cuda.to_device(color_matrix, stream=stream)
    d_out: Any = cuda.device_array((height, width), dtype=np.uint16, stream=stream)
    _hue_decode_cuda_kernel[grid, CUDA_THREADS_PER_BLOCK_2D, stream](
        d_color, d_out, min_u, range_u, inverse_colorization, bgr, robust
    )

    if out is not None:
        if out.shape != (height, width) or out.dtype != np.uint16:
            raise ValueError(f"out array must be shape ({height}, {width}) uint16")
        d_out.copy_to_host(out, stream=stream)
        return out

    host_out: np.ndarray = np.empty((height, width), dtype=np.uint16)
    d_out.copy_to_host(host_out, stream=stream)
    return host_out


def hue_encode_cpu(
    depth_matrix: np.ndarray,
    depth_min_m: float,
    depth_max_m: float,
    depth_scale: float = 0.001,
    inverse_colorization: bool = False,
    bgr: bool = False,
    robust: bool = True,
    out: Optional[np.ndarray] = None,
) -> np.ndarray:
    """Encodes a 16-bit depth matrix to an 8-bit 3-channel hue image using CPU multi-core JIT.

    Args:
        depth_matrix: 2D uint16 numpy array of shape (H, W).
        depth_min_m: Minimum sensor depth in meters.
        depth_max_m: Maximum sensor depth in meters.
        depth_scale: Conversion factor from raw integers to meters (default: 0.001 for mm).
        inverse_colorization: When True, encodes inverse depth (1/d).
        bgr: When True, outputs BGR order instead of RGB.
        robust: When True, maps valid depths to [1, 1530], reserving 0 for missing depth.
        out: Optional pre-allocated (H, W, 3) uint8 output array.

    Returns:
        3D uint8 numpy array of shape (H, W, 3) with encoded hue colors.

    Raises:
        ValueError: If array dimensions or dtypes are invalid.
    """
    if depth_matrix.ndim != 2:
        raise ValueError(f"depth_matrix must be 2D, got shape {depth_matrix.shape}")
    if depth_matrix.dtype != np.uint16:
        raise ValueError(f"depth_matrix dtype must be uint16, got {depth_matrix.dtype}")

    height: int = depth_matrix.shape[0]
    width: int = depth_matrix.shape[1]

    min_u: float
    range_u: float
    min_u, range_u = compute_depth_range_units(
        depth_min_m, depth_max_m, depth_scale, inverse_colorization
    )

    if out is None:
        out = np.empty((height, width, 3), dtype=np.uint8)
    elif out.shape != (height, width, 3) or out.dtype != np.uint8:
        raise ValueError(f"out array must be shape ({height}, {width}, 3) uint8")

    _hue_encode_cpu_kernel(
        depth_matrix, out, min_u, range_u, inverse_colorization, bgr, robust
    )
    return out


def hue_decode_cpu(
    color_matrix: np.ndarray,
    depth_min_m: float,
    depth_max_m: float,
    depth_scale: float = 0.001,
    inverse_colorization: bool = False,
    bgr: bool = False,
    robust: bool = True,
    out: Optional[np.ndarray] = None,
) -> np.ndarray:
    """Decodes an 8-bit 3-channel hue image back to a 16-bit depth matrix using CPU multi-core JIT.

    Args:
        color_matrix: 3D uint8 numpy array of shape (H, W, 3).
        depth_min_m: Minimum sensor depth in meters used during encoding.
        depth_max_m: Maximum sensor depth in meters used during encoding.
        depth_scale: Scale factor used during encoding.
        inverse_colorization: True if encoded with inverse colorization.
        bgr: True if color_matrix is in BGR format instead of RGB.
        robust: True if encoded with robust mode.
        out: Optional pre-allocated (H, W) uint16 output array.

    Returns:
        2D uint16 numpy array of shape (H, W) containing recovered depth values.

    Raises:
        ValueError: If array dimensions or dtypes are invalid.
    """
    if color_matrix.ndim != 3 or color_matrix.shape[2] != 3:
        raise ValueError(
            f"color_matrix must have shape (H, W, 3), got {color_matrix.shape}"
        )
    if color_matrix.dtype != np.uint8:
        raise ValueError(f"color_matrix dtype must be uint8, got {color_matrix.dtype}")

    height: int = color_matrix.shape[0]
    width: int = color_matrix.shape[1]

    min_u: float
    range_u: float
    min_u, range_u = compute_depth_range_units(
        depth_min_m, depth_max_m, depth_scale, inverse_colorization
    )

    if out is None:
        out = np.empty((height, width), dtype=np.uint16)
    elif out.shape != (height, width) or out.dtype != np.uint16:
        raise ValueError(f"out array must be shape ({height}, {width}) uint16")

    _hue_decode_cpu_kernel(
        color_matrix, out, min_u, range_u, inverse_colorization, bgr, robust
    )
    return out


def hue_encode(
    depth_matrix: Any,
    depth_min_m: float,
    depth_max_m: float,
    depth_scale: float = 0.001,
    inverse_colorization: bool = False,
    bgr: bool = False,
    robust: bool = True,
    device: str = "auto",
    out: Optional[Any] = None,
    stream: Optional[Any] = None,
) -> Any:
    """Unified entry point to encode 16-bit depth matrix to 8-bit hue color image.

    Args:
        depth_matrix: 2D uint16 array (host or device).
        depth_min_m: Minimum sensor depth in meters.
        depth_max_m: Maximum sensor depth in meters.
        depth_scale: Conversion factor to meters (default: 0.001 for mm).
        inverse_colorization: True to encode inverse depth (1/d).
        bgr: True for BGR format, False for RGB.
        robust: True to reserve 0 for missing depth.
        device: Execution target ("auto", "cuda", or "cpu").
        out: Optional pre-allocated output array.
        stream: Optional CUDA stream (CUDA mode only).

    Returns:
        3D uint8 array with encoded hue colors.
    """
    selected_device: str = device.lower().strip()
    if selected_device == "auto":
        selected_device = "cuda" if cuda.is_available() else "cpu"

    if selected_device == "cuda":
        return hue_encode_cuda(
            depth_matrix,
            depth_min_m,
            depth_max_m,
            depth_scale=depth_scale,
            inverse_colorization=inverse_colorization,
            bgr=bgr,
            robust=robust,
            out=out,
            stream=stream,
        )
    elif selected_device == "cpu":
        return hue_encode_cpu(
            depth_matrix,
            depth_min_m,
            depth_max_m,
            depth_scale=depth_scale,
            inverse_colorization=inverse_colorization,
            bgr=bgr,
            robust=robust,
            out=out,
        )
    else:
        raise ValueError(
            f"Unsupported device: {device}. Expected 'auto', 'cuda', or 'cpu'."
        )


def hue_decode(
    color_matrix: Any,
    depth_min_m: float,
    depth_max_m: float,
    depth_scale: float = 0.001,
    inverse_colorization: bool = False,
    bgr: bool = False,
    robust: bool = True,
    device: str = "auto",
    out: Optional[Any] = None,
    stream: Optional[Any] = None,
) -> Any:
    """Unified entry point to decode 8-bit hue color image back to 16-bit depth matrix.

    Args:
        color_matrix: 3D uint8 array (host or device).
        depth_min_m: Minimum sensor depth in meters used during encoding.
        depth_max_m: Maximum sensor depth in meters used during encoding.
        depth_scale: Scale factor used during encoding.
        inverse_colorization: True if encoded with inverse colorization.
        bgr: True if color_matrix is in BGR format instead of RGB.
        robust: True if encoded with robust mode.
        device: Execution target ("auto", "cuda", or "cpu").
        out: Optional pre-allocated output array.
        stream: Optional CUDA stream (CUDA mode only).

    Returns:
        2D uint16 array containing recovered depth values.
    """
    selected_device: str = device.lower().strip()
    if selected_device == "auto":
        selected_device = "cuda" if cuda.is_available() else "cpu"

    if selected_device == "cuda":
        return hue_decode_cuda(
            color_matrix,
            depth_min_m,
            depth_max_m,
            depth_scale=depth_scale,
            inverse_colorization=inverse_colorization,
            bgr=bgr,
            robust=robust,
            out=out,
            stream=stream,
        )
    elif selected_device == "cpu":
        return hue_decode_cpu(
            color_matrix,
            depth_min_m,
            depth_max_m,
            depth_scale=depth_scale,
            inverse_colorization=inverse_colorization,
            bgr=bgr,
            robust=robust,
            out=out,
        )
    else:
        raise ValueError(
            f"Unsupported device: {device}. Expected 'auto', 'cuda', or 'cpu'."
        )


# -----------------------------------------------------------------------------
# Stateful Stream Codec Class
# -----------------------------------------------------------------------------


class HueCodec:
    """Stateful Hue Codec managing configuration and reusable GPU buffers for video pipelines."""

    def __init__(
        self,
        depth_min_m: float,
        depth_max_m: float,
        depth_scale: float = 0.001,
        inverse_colorization: bool = False,
        bgr: bool = False,
        robust: bool = True,
        device: str = "auto",
        shape: Optional[Tuple[int, int]] = None,
    ) -> None:
        """Initializes the HueCodec.

        Args:
            depth_min_m: Minimum sensor depth in meters.
            depth_max_m: Maximum sensor depth in meters.
            depth_scale: Scaling factor from integer units to meters (default: 0.001 for mm).
            inverse_colorization: Whether to use inverse depth (1/d) colorization.
            bgr: True to output BGR channel order, False for RGB.
            robust: True to reserve zero value for missing depth data.
            device: Target device ("auto", "cuda", or "cpu").
            shape: Optional (H, W) shape to pre-allocate persistent GPU buffers.
        """
        self.depth_min_m: float = depth_min_m
        self.depth_max_m: float = depth_max_m
        self.depth_scale: float = depth_scale
        self.inverse_colorization: bool = inverse_colorization
        self.bgr: bool = bgr
        self.robust: bool = robust

        self.device: str = device.lower().strip()
        if self.device == "auto":
            self.device = "cuda" if cuda.is_available() else "cpu"

        self.min_u: float
        self.range_u: float
        self.min_u, self.range_u = compute_depth_range_units(
            self.depth_min_m,
            self.depth_max_m,
            self.depth_scale,
            self.inverse_colorization,
        )

        self._shape: Optional[Tuple[int, int]] = shape
        self._d_depth_buf: Optional[Any] = None
        self._d_color_buf: Optional[Any] = None
        self._host_out_color: Optional[np.ndarray] = None
        self._host_out_depth: Optional[np.ndarray] = None

        if shape is not None and self.device == "cuda":
            self._allocate_buffers(shape[0], shape[1])

        logger.info(
            f"HueCodec initialized: device={self.device}, range=[{depth_min_m}, {depth_max_m}]m, "
            f"scale={depth_scale}, inv={inverse_colorization}, bgr={bgr}, robust={robust}"
        )

    def _allocate_buffers(self, height: int, width: int) -> None:
        """Allocates reusable CUDA device and host buffers."""
        self._shape = (height, width)
        if self.device == "cuda" and cuda.is_available():
            self._d_depth_buf = cuda.device_array((height, width), dtype=np.uint16)
            self._d_color_buf = cuda.device_array((height, width, 3), dtype=np.uint8)
        self._host_out_color = np.empty((height, width, 3), dtype=np.uint8)
        self._host_out_depth = np.empty((height, width), dtype=np.uint16)

    def encode(
        self,
        depth: Any,
        out: Optional[Any] = None,
        stream: Optional[Any] = None,
    ) -> Any:
        """Encodes a depth map into a hue-colored image.

        Args:
            depth: 2D uint16 numpy array or CUDA DeviceNDArray.
            out: Optional pre-allocated output array.
            stream: Optional CUDA stream.

        Returns:
            3D uint8 array with encoded hue colors.
        """
        height: int = depth.shape[0]
        width: int = depth.shape[1]

        if self._shape != (height, width):
            self._allocate_buffers(height, width)

        if self.device == "cuda":
            is_device_input: bool = cuda.is_cuda_array(depth)
            blocks_x: int = math.ceil(width / CUDA_THREADS_PER_BLOCK_2D[0])
            blocks_y: int = math.ceil(height / CUDA_THREADS_PER_BLOCK_2D[1])
            grid: Tuple[int, int] = (blocks_x, blocks_y)

            if is_device_input:
                d_out: Any = out if out is not None else self._d_color_buf
                _hue_encode_cuda_kernel[grid, CUDA_THREADS_PER_BLOCK_2D, stream](
                    depth,
                    d_out,
                    self.min_u,
                    self.range_u,
                    self.inverse_colorization,
                    self.bgr,
                    self.robust,
                )
                return d_out

            self._d_depth_buf.copy_to_device(depth, stream=stream)
            _hue_encode_cuda_kernel[grid, CUDA_THREADS_PER_BLOCK_2D, stream](
                self._d_depth_buf,
                self._d_color_buf,
                self.min_u,
                self.range_u,
                self.inverse_colorization,
                self.bgr,
                self.robust,
            )
            target_out: np.ndarray = out if out is not None else self._host_out_color
            self._d_color_buf.copy_to_host(target_out, stream=stream)
            return target_out

        # CPU mode
        target_out: np.ndarray = out if out is not None else self._host_out_color
        _hue_encode_cpu_kernel(
            depth,
            target_out,
            self.min_u,
            self.range_u,
            self.inverse_colorization,
            self.bgr,
            self.robust,
        )
        return target_out

    def decode(
        self,
        color: Any,
        out: Optional[Any] = None,
        stream: Optional[Any] = None,
    ) -> Any:
        """Decodes a hue-colored image back to a 16-bit depth map.

        Args:
            color: 3D uint8 numpy array or CUDA DeviceNDArray.
            out: Optional pre-allocated output array.
            stream: Optional CUDA stream.

        Returns:
            2D uint16 array with recovered depth values.
        """
        height: int = color.shape[0]
        width: int = color.shape[1]

        if self._shape != (height, width):
            self._allocate_buffers(height, width)

        if self.device == "cuda":
            is_device_input: bool = cuda.is_cuda_array(color)
            blocks_x: int = math.ceil(width / CUDA_THREADS_PER_BLOCK_2D[0])
            blocks_y: int = math.ceil(height / CUDA_THREADS_PER_BLOCK_2D[1])
            grid: Tuple[int, int] = (blocks_x, blocks_y)

            if is_device_input:
                d_out: Any = out if out is not None else self._d_depth_buf
                _hue_decode_cuda_kernel[grid, CUDA_THREADS_PER_BLOCK_2D, stream](
                    color,
                    d_out,
                    self.min_u,
                    self.range_u,
                    self.inverse_colorization,
                    self.bgr,
                    self.robust,
                )
                return d_out

            self._d_color_buf.copy_to_device(color, stream=stream)
            _hue_decode_cuda_kernel[grid, CUDA_THREADS_PER_BLOCK_2D, stream](
                self._d_color_buf,
                self._d_depth_buf,
                self.min_u,
                self.range_u,
                self.inverse_colorization,
                self.bgr,
                self.robust,
            )
            target_out: np.ndarray = out if out is not None else self._host_out_depth
            self._d_depth_buf.copy_to_host(target_out, stream=stream)
            return target_out

        # CPU mode
        target_out: np.ndarray = out if out is not None else self._host_out_depth
        _hue_decode_cpu_kernel(
            color,
            target_out,
            self.min_u,
            self.range_u,
            self.inverse_colorization,
            self.bgr,
            self.robust,
        )
        return target_out
