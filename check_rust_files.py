"""
Run this in your Warp directory to check if your Rust source files 
match what they should be.

Usage: python check_rust_files.py
"""
import hashlib, os, sys

# Expected MD5 hashes of the correct files
EXPECTED = {
    'src/styles.rs':   '9401abbb',  # dynamic find_bytes, 650-byte STYLES_BASE
    'src/workbook.rs': 'e1f3dfc6',  # correct WB_PREFIX/WB_SUFFIX
    'src/sheet.rs':    '96b438db',  # correct 160-byte footer
    'src/writer.rs':   '1f693d8e',  # bold_xf_idx after resolve_col_xf
    'src/sst.rs':      None,        # just check write_vi is removed
    'src/workbook.rs': None,
}

print('Checking source files in current directory...\n')
ok = True
for path in ['src/styles.rs', 'src/workbook.rs', 'src/sheet.rs', 
             'src/writer.rs', 'src/sst.rs', 'src/tests.rs']:
    if not os.path.exists(path):
        print(f'MISSING: {path}')
        ok = False
        continue
    with open(path, 'rb') as f:
        data = f.read()
    h = hashlib.md5(data).hexdigest()[:8]
    size = len(data)
    
    # Quick content checks
    issues = []
    src = data.decode('utf-8', errors='replace')
    
    if path == 'src/styles.rs':
        if 'find_bytes' not in src:
            issues.append('MISSING dynamic find_bytes — OLD VERSION')
        if '331' in src and 'CXF_COUNT_OFFSET' in src:
            issues.append('STILL HAS hardcoded offset 331 — OLD VERSION')
        import re
        m = re.search(r'const STYLES_BASE: &\[u8\] = &\[(.*?)\]', src, re.DOTALL)
        if m:
            count = len(re.findall(r'0x[0-9a-f]{2}', m.group(1)))
            if count != 650:
                issues.append(f'STYLES_BASE has {count} bytes, needs 650 — OLD VERSION')
    
    if path == 'src/workbook.rs':
        if 'write_r0,' in src:
            issues.append('HAS unused write_r0 import — OLD VERSION')
    
    if path == 'src/sst.rs':
        if 'write_vi' in src:
            issues.append('HAS unused write_vi import — OLD VERSION')

    if path == 'src/writer.rs':
        if 'FileOptions::default()' in src.replace('SimpleFileOptions::default()', ''):
            issues.append('HAS FileOptions instead of SimpleFileOptions — OLD VERSION')
        if 'bold_xf_idx' not in src:
            issues.append('MISSING bold_xf_idx variable — OLD VERSION')
    
    if path == 'src/tests.rs':
        if 'write_oracle_file' not in src:
            issues.append('MISSING write_oracle_file test — OLD VERSION')

    status = '✓' if not issues else '✗'
    print(f'{status} {path} ({size} bytes, md5={h})')
    for issue in issues:
        print(f'    ⚠ {issue}')
    if issues:
        ok = False

print()
if ok:
    print('All files look correct. Run: cargo test write_oracle_file -- --nocapture')
else:
    print('Some files are OLD. Download the latest versions from Claude and replace them.')
