"""GUI application for batch MF4 to Parquet conversion."""

from __future__ import annotations

import threading
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from pathlib import Path

from mf4_to_databricks.converter import mf4_to_dataframe, dataframe_to_parquet


class App(tk.Tk):
    """Tkinter application for batch MF4 to Parquet conversion."""

    def __init__(self) -> None:
        super().__init__()
        self.title("MF4 -> Parquet Converter")
        self.geometry("900x600")
        self.minsize(700, 450)

        self._files: list[Path] = []
        self._build_ui()

    # ------------------------------------------------------------------ UI
    def _build_ui(self) -> None:
        # --- Top: file list ---
        frm_files = ttk.LabelFrame(self, text="MF4 Dateien", padding=8)
        frm_files.pack(fill="both", expand=True, padx=10, pady=(10, 4))

        # Buttons
        frm_btns = ttk.Frame(frm_files)
        frm_btns.pack(fill="x", pady=(0, 4))
        ttk.Button(frm_btns, text="Dateien hinzufuegen ...", command=self._on_add_files).pack(side="left")
        ttk.Button(frm_btns, text="Ordner hinzufuegen ...", command=self._on_add_folder).pack(side="left", padx=(4, 0))
        ttk.Button(frm_btns, text="Ausgewaehlte entfernen", command=self._on_remove).pack(side="left", padx=(4, 0))
        ttk.Button(frm_btns, text="Alle entfernen", command=self._on_clear).pack(side="left", padx=(4, 0))

        # File list
        self._tree = ttk.Treeview(
            frm_files,
            columns=("file", "size", "status"),
            show="headings",
            selectmode="extended",
        )
        self._tree.heading("file", text="Datei")
        self._tree.heading("size", text="Groesse (MB)")
        self._tree.heading("status", text="Status")
        self._tree.column("file", width=450)
        self._tree.column("size", width=100, anchor="e")
        self._tree.column("status", width=250)

        sb = ttk.Scrollbar(frm_files, command=self._tree.yview)
        self._tree.configure(yscrollcommand=sb.set)
        sb.pack(side="right", fill="y")
        self._tree.pack(fill="both", expand=True)

        # --- Middle: export options ---
        frm_opts = ttk.LabelFrame(self, text="Export-Optionen", padding=8)
        frm_opts.pack(fill="x", padx=10, pady=4)

        ttk.Label(frm_opts, text="Ausgabeordner:").grid(row=0, column=0, sticky="w")
        self._var_outdir = tk.StringVar()
        ttk.Entry(frm_opts, textvariable=self._var_outdir, width=60).grid(row=0, column=1, sticky="ew", padx=4)
        ttk.Button(frm_opts, text="Waehlen ...", command=self._on_browse_outdir).grid(row=0, column=2)

        self._var_same_dir = tk.BooleanVar(value=True)
        ttk.Checkbutton(
            frm_opts,
            text="Parquet neben Quelldatei speichern (Ausgabeordner ignorieren)",
            variable=self._var_same_dir,
        ).grid(row=1, column=0, columnspan=3, sticky="w", pady=(4, 0))

        ttk.Label(frm_opts, text="Raster (s):").grid(row=2, column=0, sticky="w", pady=(4, 0))
        self._var_raster = tk.StringVar()
        ttk.Entry(frm_opts, textvariable=self._var_raster, width=12).grid(
            row=2, column=1, sticky="w", padx=4, pady=(4, 0)
        )
        ttk.Label(frm_opts, text="(leer = Original-Abtastrate)").grid(row=2, column=2, sticky="w", pady=(4, 0))

        frm_opts.columnconfigure(1, weight=1)

        # --- Bottom: convert button + status ---
        frm_bottom = ttk.Frame(self, padding=(10, 4, 10, 8))
        frm_bottom.pack(fill="x")

        ttk.Button(frm_bottom, text="Alle konvertieren", command=self._on_convert).pack(side="left")

        self._progress = ttk.Progressbar(frm_bottom, mode="determinate")
        self._progress.pack(side="left", fill="x", expand=True, padx=(8, 8))

        self._var_status = tk.StringVar(value="Bereit.")
        ttk.Label(frm_bottom, textvariable=self._var_status, width=40, anchor="e").pack(side="right")

    # --------------------------------------------------------- File mgmt
    def _add_paths(self, paths: list[Path]) -> None:
        for p in paths:
            if p in self._files:
                continue
            self._files.append(p)
            size_mb = p.stat().st_size / 1024 / 1024
            self._tree.insert("", "end", iid=str(p), values=(str(p), f"{size_mb:.2f}", "Ausstehend"))

    def _on_add_files(self) -> None:
        paths = filedialog.askopenfilenames(
            title="MF4 Dateien waehlen",
            filetypes=[("MF4 Dateien", "*.mf4 *.MF4"), ("Alle Dateien", "*.*")],
        )
        if paths:
            self._add_paths([Path(p) for p in paths])
            self._set_status(f"{len(self._files)} Datei(en) in der Liste.")

    def _on_add_folder(self) -> None:
        folder = filedialog.askdirectory(title="Ordner mit MF4 Dateien waehlen")
        if folder:
            mf4s = sorted(Path(folder).glob("*.mf4")) + sorted(Path(folder).glob("*.MF4"))
            if not mf4s:
                messagebox.showinfo("Hinweis", "Keine .mf4 Dateien im gewaehlten Ordner gefunden.")
                return
            self._add_paths(mf4s)
            self._set_status(f"{len(self._files)} Datei(en) in der Liste.")

    def _on_remove(self) -> None:
        selected = self._tree.selection()
        for iid in selected:
            p = Path(iid)
            if p in self._files:
                self._files.remove(p)
            self._tree.delete(iid)
        self._set_status(f"{len(self._files)} Datei(en) in der Liste.")

    def _on_clear(self) -> None:
        self._tree.delete(*self._tree.get_children())
        self._files.clear()
        self._set_status("Liste geleert.")

    def _on_browse_outdir(self) -> None:
        folder = filedialog.askdirectory(title="Ausgabeordner waehlen")
        if folder:
            self._var_outdir.set(folder)

    # --------------------------------------------------------- Conversion
    def _on_convert(self) -> None:
        if not self._files:
            messagebox.showwarning("Fehler", "Bitte mindestens eine MF4-Datei hinzufuegen.")
            return

        raster = None
        raster_str = self._var_raster.get().strip()
        if raster_str:
            try:
                raster = float(raster_str)
            except ValueError:
                messagebox.showwarning("Fehler", "Raster muss eine Zahl sein (z.B. 0.01).")
                return

        same_dir = self._var_same_dir.get()
        outdir = self._var_outdir.get().strip()
        if not same_dir and not outdir:
            messagebox.showwarning("Fehler", "Bitte einen Ausgabeordner waehlen oder 'neben Quelldatei' aktivieren.")
            return

        self._progress["maximum"] = len(self._files)
        self._progress["value"] = 0

        for iid in self._tree.get_children():
            self._tree.set(iid, "status", "Warte ...")

        threading.Thread(
            target=self._run_convert_all,
            args=(list(self._files), raster, same_dir, outdir),
            daemon=True,
        ).start()

    def _run_convert_all(
        self, files: list[Path], raster: float | None, same_dir: bool, outdir: str
    ) -> None:
        ok_count = 0
        fail_count = 0

        for i, mf4_path in enumerate(files):
            iid = str(mf4_path)
            self.after(0, self._tree.set, iid, "status", "Konvertiere ...")
            self.after(0, self._set_status, f"Konvertiere {i + 1}/{len(files)}: {mf4_path.name}")

            try:
                if same_dir:
                    out_path = mf4_path.with_suffix(".parquet")
                else:
                    out_path = Path(outdir) / (mf4_path.stem + ".parquet")

                df = mf4_to_dataframe(mf4_path, raster=raster)
                result = dataframe_to_parquet(df, out_path)
                size_mb = result.stat().st_size / 1024 / 1024
                status = f"OK  {size_mb:.1f} MB, {len(df)} Zeilen, {len(df.columns)} Spalten"
                self.after(0, self._tree.set, iid, "status", status)
                ok_count += 1
            except Exception as exc:
                self.after(0, self._tree.set, iid, "status", f"FEHLER: {exc}")
                fail_count += 1

            self.after(0, self._update_progress, i + 1)

        summary = f"Fertig: {ok_count} erfolgreich"
        if fail_count:
            summary += f", {fail_count} fehlgeschlagen"
        self.after(0, self._set_status, summary)
        self.after(0, messagebox.showinfo, "Konvertierung abgeschlossen", summary)

    def _update_progress(self, value: int) -> None:
        self._progress["value"] = value

    # ------------------------------------------------------------ Helpers
    def _set_status(self, msg: str) -> None:
        self._var_status.set(msg)


def run() -> None:
    """Launch the GUI."""
    app = App()
    app.mainloop()
