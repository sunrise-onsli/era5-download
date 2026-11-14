# -*- mode: python ; coding: utf-8 -*-
"""One windowed executable for the local ERA5 picker.

Only the libraries the picker imports are collected. Torch, numba and the
Dask distributed scheduler are not used and are excluded.
"""

from PyInstaller.utils.hooks import collect_dynamic_libs

hidden = [
    "aiohttp",
    "cftime",
    "dask",
    "dask.array",
    "dask.array.core",
    "dask.base",
    "dask.threaded",
    "dask.optimization",
    "fsspec",
    "fsspec.implementations.http",
    "netCDF4",
    "netCDF4._netCDF4",
    "numcodecs",
    "numcodecs.blosc",
    "numcodecs.compat",
    "numpy",
    "packaging",
    "packaging.version",
    "pandas",
    "xarray",
    "xarray.backends.netCDF4_",
    "xarray.backends.zarr",
    "zarr",
]

a = Analysis(
    ["app.py"],
    pathex=[],
    binaries=collect_dynamic_libs("netCDF4"),
    datas=[("static", "static")],
    hiddenimports=hidden,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        "torch",
        "torchvision",
        "torchaudio",
        "numba",
        "llvmlite",
        "distributed",
        "dask.distributed",
        "matplotlib",
        "tkinter",
        "pytest",
        "IPython",
        "sphinx",
        "notebook",
        "PyQt5",
        "PyQt6",
        "tensorboard",
        "tensorflow",
        "sklearn",
        "h5py",
        "pyarrow",
        "botocore",
        "boto3",
        "zmq",
    ],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name="ERA5选取",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
