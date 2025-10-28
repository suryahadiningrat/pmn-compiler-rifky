# Script Python: PMN Compiler (compile_pmn.py)

## Tujuan
Membuat script Python yang mengkompilasi data mangrove dari berbagai database BPDAS ke dalam format final (GeoJSON, Shapefile, GDB, PMTiles) dengan progress tracking.

## Input Parameters
- `id`: ID untuk update logs processing
- `year`: Tahun data (contoh: 2025)

## Proses Workflow

### 1. Validasi Tabel (10%)
Cek keberadaan tabel:
- `postgres.pmn.existing_{year}`
- `postgres.pmn.potensi_{year}`

### 2. Pembersihan Data (20%)
Hapus semua records di:
- `postgres.pmn.existing_{year}`
- `postgres.pmn.potensi_{year}`

### 3. Ambil List BPDAS (30%)
**Endpoint:** 
```
GET https://api.ptnaghayasha.com/api/master/cursor/wilayah
?include_bpdas=true
&include_provinsi=false
&include_kabupaten=false
&include_kecamatan=false
&include_desa=false
```

**Response Structure:**
```json
{
  "data": [
    {
      "id": "11",
      "value": "CITARUMCILIWUNG",
      "type": "bpdas"
    }
  ]
}
```

**Aksi:** Ekstrak `data[].value`, konversi ke lowercase → ini adalah nama database BPDAS

### 4. Agregasi Data dari Semua BPDAS (40%)
Untuk setiap BPDAS database:
- Copy `{bpdas_db}.public.potensi_{year}` → `postgres.pmn.potensi_{year}`
- Copy `{bpdas_db}.public.existing_{year}` → `postgres.pmn.existing_{year}`

### 5. Bersihkan File Lama di MinIO (50%)
Hapus semua file di bucket `idpm`:
- Path: `/pmn-result/{year}/*`

### 6. Hapus PMTiles Lama (60%)
Hapus file di bucket `idpm`, path `/layers`:
- `EXISTING{year}.pmtiles`
- `POTENSI{year}.pmtiles`

### 7. Export ke GeoJSON (70%)
Query semua data dan export ke 2 file GeoJSON:
- `postgres.pmn.existing_{year}` → `existing_{year}.geojson`
- `postgres.pmn.potensi_{year}` → `potensi_{year}.geojson`

### 8. Konversi Format (80%)
Untuk setiap theme (existing & potensi):
- GeoJSON → Shapefile (.shp + .shx + .dbf + .prj)
- Shapefile → File Geodatabase (.gdb)
- Zip file SHP dan GDB

### 9. Generate PMTiles (90%)
Konversi GeoJSON ke PMTiles dan upload ke MinIO:
- Upload ke: `idpm/layers/EXISTING{year}.pmtiles`
- Upload ke: `idpm/layers/POTENSI{year}.pmtiles`

### 10. Update Metadata (95%)
Update tabel `postgres.pmn.compiler_datasets` untuk 2 records:

**Filter:** 
- `title = "Peta Eksisting Mangrove {year}"`
- `title = "Peta Potensi Mangrove {year}"`

**Update Fields:**
- `rows`: COUNT geometri dari tabel source
- `md5`: Hash MD5 dari file GDB
- `date_created`: Timestamp saat ini
- `size`: Ukuran file GDB (bytes)
- `gdb_url`: `http://52.76.171.132:9008/idpm/pmn-result/{year}/AR_25K_PETAMANGROVE_{THEME}_{year}.gdb.zip`
- `shp_url`: `http://52.76.171.132:9008/idpm/pmn-result/{year}/AR_25K_PETAMANGROVE_{THEME}_{year}.zip`
- `map_url`: MinIO URL untuk PMTiles
- `process`: `false`

### 11. Finalisasi (100%)
Update `postgres.pmn.compiler_status`:
- Set progress = 100% untuk `id` yang diberikan
- Set status = 'COMPLETED'

## Credentials

**PostgreSQL:**
```python
DB_HOST = "52.74.112.75"
DB_PORT = 5432
DB_USER = "pg"
DB_PASSWORD = "~nagha2025yasha@~"
```

**MinIO:**
```python
MINIO_HOST = "http://52.76.171.132:9005"
MINIO_PUBLIC_HOST = "https://api-minio.ptnaghayasha.com"
MINIO_BUCKET = "idpm"
MINIO_ACCESS_KEY = "eY7VQA55gjPQu1CGv540"
MINIO_SECRET_KEY = "u6feeKC1s8ttqU1PLLILrfyqdv79UOvBkzpWhIIn"
```

## Requirements
- Implementasi logging untuk setiap step
- Update progress ke `postgres.pmn.compiler_status` setelah setiap step selesai
- Error handling dengan rollback jika ada kegagalan
- Gunakan library: psycopg2, geopandas, minio, tippecanoe (untuk PMTiles)