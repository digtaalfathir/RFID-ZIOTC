# HttpKeyout — RFID Gate Integration (Zebra IoT Connector)

> Repositori ini merupakan kumpulan contoh aplikasi **Zebra IoT Connector (ZIOTC)** bawaan Zebra untuk RFID reader **FX9600 / FX7500 / FXR90**. Seluruh folder lain (`KeyOut`, `Directionality`, `GPI-Message`, dll.) adalah contoh resmi dari Zebra dan tidak dimodifikasi.
>
> **Folder yang dibuat dan digunakan secara aktif hanyalah [`HttpKeyout/`](HttpKeyout/).** Dokumentasi di bawah ini fokus pada folder tersebut: apa fungsinya, cara build, dan cara upload hasilnya ke reader.

---

## 1. Gambaran Umum

`HttpKeyout` adalah aplikasi ZIOTC yang berjalan **langsung di dalam RFID reader Zebra**. Aplikasi ini membaca tag RFID yang terdeteksi antena, mengumpulkannya menjadi satu batch, lalu mengirimkannya via **HTTP POST** ke dua tujuan:

| Tujuan | Alamat | Fungsi |
| --- | --- | --- |
| **API Produksi** | `product.suite.stechoq-j.com` → `api/v1/warehouse-management/jmp/log-rfids` | Endpoint utama pencatatan tag RFID ke sistem Warehouse Management (JMP). |
| **Server Lokal (Flask)** | `192.168.0.88:5000/rfid` | Server debug/monitoring untuk melihat payload dan hasil respons API secara realtime. |

### Alur kerja singkat

```
Tag RFID  ──►  Reader (inventory)  ──►  HttpKeyout
                                          │
                                          ├─ kumpulkan tag ke batch (dedup, antena 1–8)
                                          ├─ idle 3 detik  ──►  flush batch
                                          │
                                          ├─►  POST ke API Produksi (stechoq-j)
                                          └─►  POST ke Server Lokal (monitoring)
```

**Perilaku utama:**
- Hanya tag dari **antena 1–8** yang diproses (di-*mapping* menjadi `antenna = "1"`).
- Tag duplikat dalam satu batch **diabaikan** (dedup via `set`).
- Batch otomatis dikirim (*flush*) jika tidak ada tag baru selama **3 detik** (`BATCH_TIMEOUT`).
- *Timestamp* memakai zona **WIB (UTC+7)** dengan format `YYYY-MM-DDThh:mm:ss.000+0700`.

---

## 2. Struktur Folder

```
HttpKeyout/
├── build.bat            # Perintah build (memanggil FX-Package)
├── src/                 # Kode sumber aplikasi (WAJIB ada file [Name].py utama)
│   ├── Httpkeyout.py    # Aplikasi utama: baca tag, batching, kirim HTTP POST
│   ├── RestAPI.py       # Wrapper REST API lokal reader (start/stop inventory, GPO, dll.)
│   └── Logger.py        # Logging ke syslog + file (rotating log)
├── pkg/                 # File tambahan (config, dll.) — saat ini kosong (.keep)
└── out/                 # Hasil build .deb ditaruh di sini
    └── Httpkeyout_1.1.x.deb
```

| File | Peran |
| --- | --- |
| [`src/Httpkeyout.py`](HttpKeyout/src/Httpkeyout.py) | Logika inti: menerima callback tag dari reader, membuat batch, dan mengirim POST ke API + server lokal. |
| [`src/RestAPI.py`](HttpKeyout/src/RestAPI.py) | Antarmuka ke REST API lokal IoT Connector (`https://127.0.0.1`) untuk memulai/menghentikan inventory, set GPO, ambil versi/serial reader. |
| [`src/Logger.py`](HttpKeyout/src/Logger.py) | Kelas Logger: mengirim log ke syslog server dan menyimpan `IOT_Connector.log` (rotating, maks 2 MB × 10 file). |

### Konfigurasi (di `src/Httpkeyout.py`)

Parameter dapat diubah di bagian atas file:

```python
DEBUG_SERVER = "192.168.0.88"   # IP server lokal (Flask + syslog)
DEBUG_PORT   = 514              # Port syslog
BATCH_TIMEOUT = 3               # Detik idle sebelum batch dikirim
KEYBOARD_COOLDOWN = 3           # Proteksi duplikat
```

---

## 3. Build

Aplikasi ZIOTC dikemas menjadi paket **Debian (`.deb`)** menggunakan tool **FX-Package** yang tersedia di folder [`Build-Utils/`](Build-Utils/).

### Prasyarat
- Struktur folder sudah benar: `src/` berisi file utama `Httpkeyout.py`, `pkg/` untuk file tambahan, `out/` sebagai tujuan output.
- Gunakan binary FX-Package sesuai OS Anda:

  | OS | Binary |
  | --- | --- |
  | Windows (64-bit) | `Build-Utils/FX-Package.exe` |
  | Linux (64-bit) | `Build-Utils/Fx-Package.elf` |
  | macOS (Intel) | `Build-Utils/Fx-Package-amd64` |
  | macOS (Apple Silicon) | `Build-Utils/Fx-Package-arm64` |

### Perintah build

Isi dari [`HttpKeyout/build.bat`](HttpKeyout/build.bat):

```bat
..\Build-Utils\FX-Package -name Httpkeyout -maintainer G.Crean -version 1.1.2
```

Jalankan **dari dalam folder `HttpKeyout/`**:

```bash
# Windows
build.bat

# Linux / macOS (jalankan binary yang sesuai dari folder HttpKeyout)
../Build-Utils/Fx-Package.elf -name Httpkeyout -maintainer G.Crean -version 1.1.2
```

**Parameter:**
- `-name` → nama aplikasi/paket (`Httpkeyout`).
- `-maintainer` → nama maintainer paket.
- `-version` → **naikkan versi setiap kali build baru** (mis. `1.1.2` → `1.1.3`). Nomor versi menjadi nama file output.

### Hasil build

File paket akan muncul di:

```
HttpKeyout/out/Httpkeyout_<versi>.deb
```

Contoh: `HttpKeyout/out/Httpkeyout_1.1.3.deb`. **File `.deb` inilah yang di-upload ke reader.**

> Tipe file yang boleh ditulis ulang oleh aplikasi saat runtime: `ini`, `yml`, `yaml`, `xml`, `txt`, `json` (letakkan di `pkg/`).

---

## 4. Upload ke Reader

Hasil build (`.deb`) di-upload melalui antarmuka web Zebra pada masing-masing gate.

### Langkah-langkah

1. **Pastikan VPN aktif** (koneksi ke jaringan lokasi sudah tersambung).
2. **VNC** ke workstation di lokasi:
   ```
   10.10.1.223
   ```
3. Dari dalam sesi VNC tersebut, **buka browser** dan akses antarmuka web reader sesuai gate:

   | Gate | Alamat Web Reader | Password |
   | --- | --- | --- |
   | **Inbound** | `192.168.0.241` | `Stc12345.` |
   | **Outbound** | `192.168.0.242` | `Stc12345.` |

4. Setelah login, pada menu Zebra pilih **Application**.
5. **Upload** file hasil build (`HttpKeyout/out/Httpkeyout_<versi>.deb`) di halaman tersebut, lalu install/jalankan aplikasinya.

> ⚠️ **Catatan keamanan:** Alamat IP internal dan password di atas bersifat rahasia dan hanya untuk keperluan operasional internal. Jangan sebarkan dokumen ini ke pihak luar. Jika repositori bersifat publik, sebaiknya kredensial ini dipindahkan ke catatan internal yang tidak ikut ter-*commit*.

---

## 5. Referensi

- Dokumentasi resmi Zebra IoT Connector: <https://zebradevs.github.io/rfid-ziotc-docs/>
- Tool build: [`Build-Utils/Readme.md`](Build-Utils/Readme.md)

## Prasyarat Firmware
Reader harus menjalankan versi firmware terbaru yang mendukung IoT Connector.
