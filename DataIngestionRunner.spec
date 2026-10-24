# -*- mode: python ; coding: utf-8 -*-


a = Analysis(
    ['DataIngestionRunner.py'],
    pathex=[],
    binaries=[],
    datas=[('version.json', '.'), ('man-page.txt', '.'), ('api', 'api'), ('model', 'model'), ('controller', 'controller'), ('service', 'service')],
    hiddenimports=['mysql.connector', 'uvicorn', 'fastapi', 'requests', 'main'],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
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
    name='DataIngestionRunner',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
