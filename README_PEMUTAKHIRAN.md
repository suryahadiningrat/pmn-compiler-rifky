# Pemutakhiran Baseline - Documentation

## Overview
Script pemutakhiran baseline untuk 'mengawetkan' data QC yang sudah lolos validasi ke table baseline di masing-masing BPDAS dan postgres.pmn.

## Scripts

### 1. pemutakhiran_baseline.py
Script untuk proses pemutakhiran baseline.

**Features:**
- ✅ Membaca tahun dari `pmn.state` table berdasarkan `state_id`
- ✅ Membuat table `existing_{year}_baseline` dan `potensi_{year}_baseline` di masing-masing BPDAS (hanya field `geometry`)
- ✅ Membuat table `existing_{year}_baseline` dan `potensi_{year}_baseline` di `postgres.pmn` (field `geometry` dan `bpdas`)
- ✅ Filter data QC yang sudah lolos semua validasi (qcstatus = true untuk semua item kecuali Remarks)
- ✅ Handle duplicate geometries dengan DISTINCT ON dan order by ogc_fid DESC (ambil data terbaru)
- ✅ Avoid duplicate insert dengan check ST_Equals sebelum insert
- ✅ Update progress ke `pmn.state` (percentage, qc_completed, bpdas_complete, completed)
- ✅ Handle geometry Z dimension dengan ST_Force2D
- ✅ Skip BPDAS yang tidak memiliki table QC
- ✅ Test mode untuk testing dengan bpdastesting saja

**Usage:**
```bash
# Run for all BPDAS
python pemutakhiran_baseline.py <state_id>

# Run for bpdastesting only (TEST MODE)
python pemutakhiran_baseline.py <state_id> --test

# Example
python pemutakhiran_baseline.py 4 --test
```

**Process Flow:**
1. **Step 1 (5%)**: Get year from pmn.state and BPDAS list
2. **Step 2 (10%)**: Create baseline tables in pmn schema
3. **Step 3 (10-95%)**: Process each BPDAS database
   - Create baseline tables in BPDAS database
   - Get QC data with all validations passed
   - Filter latest record for duplicate geometries
   - Insert into BPDAS baseline table
   - Insert into PMN baseline table
4. **Step 4 (100%)**: Finalize and mark as completed

**QC Validation Logic:**
Data dianggap lolos QC jika:
- Semua item dalam qcstatus memiliki `value: "true"`
- Kecuali item dengan title yang mengandung "Remark" (boleh value apapun)

**Example QC Status:**
```json
[
  {"title": "Kesesuaian Format dan Data Struktur Data", "value": "true"},
  {"title": "Kelengkapan Pengisian Atribut Perubahan", "value": "true"},
  {"title": "Kesesuaian Interpetasi Kelas Tutupan Tajuk", "value": "true"},
  {"title": "Kesesuaian Interpetasi Kelas Struktur Vegetasi", "value": "true"},
  {"title": "Kesesuaian Interpetasi Objek Mangrove", "value": "true"},
  {"title": "Tidak Terdapat Gap/Overlap Polygon", "value": "true"},
  {"title": "Remarks", "value": "asdsadasd"}  // Remarks bisa value apapun
]
```

### 2. cleanup_baseline.py
Script untuk cleanup/delete semua table baseline.

**Features:**
- ✅ Delete table `existing_{year}_baseline` dan `potensi_{year}_baseline` dari semua BPDAS
- ✅ Delete table `existing_{year}_baseline` dan `potensi_{year}_baseline` dari `postgres.pmn`
- ✅ Test mode untuk testing dengan bpdastesting saja
- ✅ Confirmation prompt sebelum delete (tidak di test mode)

**Usage:**
```bash
# Cleanup all BPDAS (dengan confirmation)
python cleanup_baseline.py <year>

# Cleanup bpdastesting only (TEST MODE, no confirmation)
python cleanup_baseline.py <year> --test

# Example
python cleanup_baseline.py 2025 --test
```

**⚠️ WARNING:**
Script ini akan menghapus semua data baseline. Pastikan backup data sebelum menjalankan di production.

## Database Structure

### BPDAS Tables
```sql
-- Table di masing-masing BPDAS database
CREATE TABLE public.existing_{year}_baseline (
    ogc_fid SERIAL PRIMARY KEY,
    geometry GEOMETRY(MultiPolygon, 4326),
    bpdas VARCHAR(255)
);

CREATE TABLE public.potensi_{year}_baseline (
    ogc_fid SERIAL PRIMARY KEY,
    geometry GEOMETRY(MultiPolygon, 4326),
    bpdas VARCHAR(255)
);
```

### PMN Tables
```sql
-- Table di postgres.pmn schema
CREATE TABLE pmn.existing_{year}_baseline (
    ogc_fid SERIAL PRIMARY KEY,
    geometry GEOMETRY(MultiPolygon, 4326),
    bpdas VARCHAR(255)
);

CREATE TABLE pmn.potensi_{year}_baseline (
    ogc_fid SERIAL PRIMARY KEY,
    geometry GEOMETRY(MultiPolygon, 4326),
    bpdas VARCHAR(255)
);
```

## Testing Results

### Test Environment
- Database: bpdastesting
- Year: 2025
- State ID: 4

### Test Results
✅ **pemutakhiran_baseline.py --test**
- Table creation: SUCCESS
- Data filtering (QC validation): SUCCESS (9/30 geometries passed)
- Duplicate handling: SUCCESS
- Insert to BPDAS: SUCCESS (9 records)
- Insert to PMN: SUCCESS (9 records)
- Progress update: SUCCESS (100%, qc_completed=9, bpdas_complete=1)

✅ **cleanup_baseline.py --test**
- PMN tables cleanup: SUCCESS
- BPDAS tables cleanup: SUCCESS
- No errors

✅ **Re-run after cleanup**
- Table re-creation: SUCCESS
- Data re-insert: SUCCESS
- No duplicate data: VERIFIED

## Notes

1. **Geometry Handling:**
   - Script menggunakan `ST_AsBinary()` dan `ST_GeomFromWKB()` untuk handle geometry dengan benar
   - `ST_Force2D()` digunakan untuk remove Z dimension dari geometry source

2. **Duplicate Prevention:**
   - Query QC data menggunakan `DISTINCT ON (ST_AsText(geometry))` dengan `ORDER BY ogc_fid DESC` untuk ambil record terbaru
   - Sebelum insert, check dengan `ST_Equals()` untuk avoid duplicate

3. **Performance:**
   - Index GIST pada geometry column untuk performance
   - Batch commit per BPDAS untuk avoid long transaction

4. **Error Handling:**
   - Skip BPDAS yang tidak bisa diakses
   - Skip BPDAS yang tidak memiliki table QC
   - Continue processing meskipun ada error di 1 BPDAS

## Log Files
- `pemutakhiran_baseline.log` - Log file untuk pemutakhiran process

## Integration dengan API
Script dapat dipanggil dari API dengan parameter:
```python
import subprocess

# Run pemutakhiran
result = subprocess.run([
    'python3', 'pemutakhiran_baseline.py',
    str(state_id)  # ID dari pmn.state table
], capture_output=True, text=True)

# Check result
if result.returncode == 0:
    print("Success")
else:
    print(f"Failed: {result.stderr}")
```

## Support
Untuk bug report atau feature request, silakan hubungi maintainer.
