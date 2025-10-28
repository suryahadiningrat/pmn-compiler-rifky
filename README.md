# PMN Compiler

Script Python untuk mengkompilasi data mangrove dari berbagai database BPDAS ke dalam format final (GeoJSON, Shapefile, GDB, PMTiles) dengan progress tracking.

## Fitur

- ✅ Validasi dan pembersihan tabel database
- ✅ Agregasi data dari multiple database BPDAS
- ✅ Export ke berbagai format: GeoJSON, Shapefile, File Geodatabase
- ✅ Generate PMTiles untuk web mapping
- ✅ Upload otomatis ke MinIO object storage
- ✅ Progress tracking real-time ke database
- ✅ Comprehensive logging dan error handling
- ✅ Cleanup otomatis temporary files

## Workflow Proses

1. **Validasi Tabel (10%)** - Cek dan buat tabel jika diperlukan
2. **Pembersihan Data (20%)** - Hapus data lama dari tabel target
3. **Ambil List BPDAS (30%)** - Fetch daftar database BPDAS dari API
4. **Agregasi Data (40%)** - Copy data dari semua database BPDAS
5. **Bersihkan File Lama (50%)** - Hapus file lama di MinIO
6. **Hapus PMTiles Lama (60%)** - Hapus PMTiles versi sebelumnya
7. **Export GeoJSON (70%)** - Export data ke format GeoJSON
8. **Konversi Format (80%)** - Convert ke Shapefile dan GDB
9. **Generate PMTiles (90%)** - Buat PMTiles untuk web mapping
10. **Update Metadata (95%)** - Update informasi dataset
11. **Finalisasi (100%)** - Selesaikan proses

## Instalasi

### 1. Install System Dependencies

**macOS:**
```bash
# Install Homebrew jika belum ada
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# Install GDAL dan Tippecanoe
brew install gdal tippecanoe
```

**Ubuntu/Debian:**
```bash
# Install GDAL
sudo apt-get update
sudo apt-get install gdal-bin

# Install Tippecanoe (build from source)
git clone https://github.com/felt/tippecanoe.git
cd tippecanoe
make -j
sudo make install
```

### 2. Install Python Dependencies

```bash
# Clone atau download project
cd "pmn compiler"

# Install Python packages
pip install -r requirements.txt
```

## Konfigurasi

Script sudah dikonfigurasi dengan credentials yang diperlukan:

- **PostgreSQL**: `52.74.112.75:5432`
- **MinIO**: `52.76.171.132:9005`
- **API Endpoint**: `https://api.ptnaghayasha.com`

## Penggunaan

### Command Line

```bash
python compile_pmn.py <process_id> <year>
```

**Parameter:**
- `process_id`: ID unik untuk tracking progress (string)
- `year`: Tahun data yang akan diproses (integer)

**Contoh:**
```bash
# Compile data mangrove tahun 2025
python compile_pmn.py "proc_001" 2025

# Compile data tahun 2024
python compile_pmn.py "batch_2024_01" 2024
```

### Programmatic Usage

```python
from compile_pmn import PMNCompiler

# Inisialisasi compiler
compiler = PMNCompiler(process_id="proc_001", year=2025)

# Jalankan proses kompilasi
try:
    compiler.run()
    print("Kompilasi berhasil!")
except Exception as e:
    print(f"Kompilasi gagal: {e}")
```

## Output Files

Setelah proses selesai, file-file berikut akan tersedia di MinIO:

### Data Files
- `pmn-result/{year}/AR_25K_PETAMANGROVE_EXISTING_{year}.zip` (Shapefile)
- `pmn-result/{year}/AR_25K_PETAMANGROVE_EXISTING_{year}.gdb.zip` (File Geodatabase)
- `pmn-result/{year}/AR_25K_PETAMANGROVE_POTENSI_{year}.zip` (Shapefile)
- `pmn-result/{year}/AR_25K_PETAMANGROVE_POTENSI_{year}.gdb.zip` (File Geodatabase)

### Web Mapping Files
- `layers/EXISTING{year}.pmtiles`
- `layers/POTENSI{year}.pmtiles`

## Monitoring Progress

Progress dapat dimonitor melalui tabel database `postgres.pmn.compiler_status`:

```sql
SELECT id, progress, status, updated_at 
FROM pmn.compiler_status 
WHERE id = 'your_process_id';
```

**Status Values:**
- `PROCESSING`: Sedang berjalan
- `COMPLETED`: Selesai berhasil
- `FAILED`: Gagal

## Logging

Log file akan dibuat dengan nama `pmn_compiler.log` di direktori yang sama dengan script. Log juga ditampilkan di console.

**Log Level:**
- `INFO`: Informasi proses normal
- `WARNING`: Peringatan (tidak menghentikan proses)
- `ERROR`: Error yang menghentikan proses

## Troubleshooting

### Error: "tippecanoe command not found"
```bash
# macOS
brew install tippecanoe

# Ubuntu (build from source)
git clone https://github.com/felt/tippecanoe.git
cd tippecanoe && make -j && sudo make install
```

### Error: "ogr2ogr command not found"
```bash
# macOS
brew install gdal

# Ubuntu
sudo apt-get install gdal-bin
```

### Error: Database connection failed
- Pastikan koneksi internet stabil
- Cek firewall/VPN settings
- Verifikasi credentials database

### Error: MinIO upload failed
- Cek koneksi ke MinIO server
- Verifikasi access key dan secret key
- Pastikan bucket `idpm` exists

### Error: API request failed
- Cek koneksi internet
- Verifikasi endpoint API masih aktif
- Cek response format API

## Database Schema

### Required Tables

**pmn.compiler_status**
```sql
CREATE TABLE pmn.compiler_status (
    id VARCHAR PRIMARY KEY,
    progress INTEGER DEFAULT 0,
    status VARCHAR DEFAULT 'PROCESSING',
    updated_at TIMESTAMP DEFAULT NOW()
);
```

**pmn.compiler_datasets**
```sql
CREATE TABLE pmn.compiler_datasets (
    id SERIAL PRIMARY KEY,
    title VARCHAR,
    rows INTEGER,
    md5 VARCHAR,
    date_created TIMESTAMP,
    size BIGINT,
    gdb_url VARCHAR,
    shp_url VARCHAR,
    map_url VARCHAR,
    process BOOLEAN DEFAULT FALSE
);
```

**Data Tables (auto-created)**
- `pmn.existing_{year}`
- `pmn.potensi_{year}`

## Kontribusi

1. Fork repository
2. Buat feature branch
3. Commit changes
4. Push ke branch
5. Buat Pull Request

## License

Internal use only - PT Nagha Yasha Indonesia