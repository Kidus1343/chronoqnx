# ChronoQNX Build & Target Environment

- **Host OS:** Windows 11 (`cmd.exe` / PowerShell)
- **QNX SDP 8.0 Path:** `C:\qnx800` (always initialize via `call C:\qnx800\qnxsdp-env.bat` before running `qcc`)
- **Cross-Compiler:** `qcc -Vgcc_ntox86_64 -O2 <src.c> -o <out>`
- **Target VM:** QNX 8.0.0 x86_64 running in QEMU, accessible via SSH alias `qnx-qemu`
- **Target Deploy Directory:** Always deploy binaries to `/data/home/root/` (never `/tmp`, which is mounted `noexec`) and run `chmod +x /data/home/root/<binary>` before execution.
- **Telemetry Port:** Forwarded on `127.0.0.1:9000` (host) -> `:9000` (QNX target).
