# Depth Inputs

`depth` is a file path under a sample in `inputs.data` or `inputs.references`. Its metadata is inherited by the runner through `job.primary_sample_metadata.depth`.

```yaml
depth:
  format: png
  encoding: float32_le_bgra
  units: meters
```

- `format` identifies the file container.
- `encoding` defines the exact operation that produces a two-dimensional depth array. Runners use this field to select the decoder.

## Depth Value Convention

A decoded value is the straight-line distance from the camera center to the visible surface point, expressed in `units`. For a panorama, it is distance along each panorama ray.

Camera-axis Z-depth, disparity, and inverse depth do not follow this convention. Convert them during dataset preparation before publishing a `depth` input.

## Encodings

Each encoding name defines one exact decoding operation. Do not use ambiguous names such as `uint16`, `rgb24`, or `packed_float`, which omit scale, channel order, or byte order. A runner implements the encodings it accepts and reports a clear error for all others.

### `float32` / `float16`

A native two-dimensional float array, normally stored as `.npy`, TIFF, or OpenEXR.

### `uint16_mm`

A single-channel uint16 image whose values are millimetres. This encoding is common for RGB-D cameras and ROS datasets.

### `uint16_256`

A single-channel uint16 image whose values are depth in metres multiplied by 256. Decode to metres by dividing by `256`. This encoding is common in KITTI-derived depth data.

### `float32_le_bgra`

Each depth value is a little-endian float32 packed losslessly into the four 8-bit BGRA channels of a PNG. Values are already in `units` after unpacking.

OpenCV exposes PNG channels as BGRA, so read the image unchanged and view its contiguous channel bytes as little-endian float32:

```python
encoded = cv2.imread(path, cv2.IMREAD_UNCHANGED)
depth = np.squeeze(np.ascontiguousarray(encoded).view("<f4"), axis=-1)
```

PIL exposes the same PNG channels as RGBA. Restore BGRA order before viewing the bytes as float32:

```python
rgba = np.asarray(Image.open(path).convert("RGBA"))
bgra = np.ascontiguousarray(rgba[..., [2, 1, 0, 3]])
depth = np.squeeze(bgra.view("<f4"), axis=-1)
```
