"""Main GUI application for MF4 analysis and Parquet export."""

from __future__ import annotations

import threading
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from pathlib import Path

from mf4_to_databricks.analyze_mf4 import analyze_mf4
from mf4_to_databricks.converter import mf4_to_dataframe, dataframe_to_parquet


class App(tk.Tk):
    """Tkinter application for MF4 → Parquet workflow."""

    def __init__(self) -> None:
        super().__init__()
        self.title("MF4 → Databricks Converter")
        self.geometry("1000x720")
        self.minsize(800, 600)

        # State
        self._mf4_path: Path | None = None
        self._analysis: dict | None = None

        self._build_ui()

    # ------------------------------------------------------------------ UI
    def _build_ui(self) -> None:
        # --- Top: file selection ---
        frm_file = ttk.LabelFrame(self, text="MF4 Datei", padding=8)
        frm_file.pack(fill="x", padx=10, pady=(10, 4))

        self._var_path = tk.StringVar()
        ttk.Entry(frm_file, textvariable=self._var_path, state="readonly").pack(side="left", fill="x", expand=True)
        ttk.Button(frm_file, text="Datei wählen …", command=self._on_browse).pack(side="left", padx=(8, 0))
        ttk.Button(frm_file, text="Analysieren", command=self._on_analyze).pack(side="left", padx=(4, 0))

        # --- Middle: notebook with tabs ---
        self._notebook = ttk.Notebook(self)
        self._notebook.pack(fill="both", expand=True, padx=10, pady=4)

        # Tab 1: Metadata
        self._frm_meta = ttk.Frame(self._notebook)
        self._notebook.add(self._frm_meta, text="Metadaten")
        self._txt_meta = tk.Text(self._frm_meta, wrap="word", state="disabled")
        sb_meta = ttk.Scrollbar(self._frm_meta, command=self._txt_meta.yview)
        self._txt_meta.configure(yscrollcommand=sb_meta.set)
        sb_meta.pack(side="right", fill="y")
        self._txt_meta.pack(fill="both", expand=True)

        # Tab 2: Groups / Channels
        self._frm_groups = ttk.Frame(self._notebook)
        self._notebook.add(self._frm_groups, text="Gruppen & Kanäle")
        self._tree = ttk.Treeview(
            self._frm_groups,
            columns=("samples", "channels", "bus", "source"),
            show="tree headings",
            selectmode="extended",
        )
        self._tree.heading("#0", text="Gruppe / Kanal")
        self._tree.heading("samples", text="Samples")
        self._tree.heading("channels", text="Kanäle")
        self._tree.heading("bus", text="Bus")
        self._tree.heading("source", text="Source")
        self._tree.column("#0", width=300)
        self._tree.column("samples", width=100, anchor="e")
        self._tree.column("channels", width=80, anchor="e")
        self._tree.column("bus", width=100)
        self._tree.column("source", width=200)
        sb_tree = ttk.Scrollbar(self._frm_groups, command=self._tree.yview)
        self._tree.configure(yscrollcommand=sb_tree.set)
        sb_tree.pack(side="right", fill="y")
        self._tree.pack(fill="both", expand=True)

        # Tab 3: Export
        self._frm_export = ttk.Frame(self._notebook, padding=10)
        self._notebook.add(self._frm_export, text="Parquet Export")
        self._build_export_tab()

        # --- Bottom: status bar ---
        self._var_status = tk.StringVar(value="Bereit.")
        ttk.Label(self, textvariable=self._var_status, relief="sunken", anchor="w").pack(
            fill="x", padx=10, pady=(0, 8)
        )

    def _build_export_tab(self) -> None:
        frm = self._frm_export

        ttk.Label(frm, text="Ausgabepfad:").grid(row=0, column=0, sticky="w")
        self._var_out = tk.StringVar()
        ttk.Entry(frm, textvariable=self._var_out, width=60).grid(row=0, column=1, sticky="ew", padx=4)
        ttk.Button(frm, text="Speichern unter …", command=self._on_browse_out).grid(row=0, column=2)

        ttk.Label(frm, text="Raster (s):").grid(row=1, column=0, sticky="w", pady=(8, 0))
        self._var_raster = tk.StringVar()
        ttk.Entry(frm, textvariable=self._var_raster, width=12).grid(row=1, column=1, sticky="w", padx=4, pady=(8, 0))
        ttk.Label(frm, text="(leer = Original-Abtastrate)").grid(row=1, column=2, sticky="w", pady=(8, 0))

        self._var_selected_only = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            frm, text="Nur in Tab 'Gruppen' ausgewählte Gruppen exportieren", variable=self._var_selected_only
        ).grid(row=2, column=0, columnspan=3, sticky="w", pady=(8, 0))

        ttk.Button(frm, text="▶ Als Parquet exportieren", command=self._on_export).grid(
            row=3, column=0, columnspan=3, pady=(16, 0)
        )

        frm.columnconfigure(1, weight=1)

    # ------------------------------------------------------------ Actions
    def _on_browse(self) -> None:
        path = filedialog.askopenfilename(
            title="MF4 Datei öffnen",
            filetypes=[("MF4 Dateien", "*.mf4 *.MF4"), ("Alle Dateien", "*.*")],
        )
        if path:
            self._var_path.set(path)
            self._mf4_path = Path(path)
            self._var_out.set(str(self._mf4_path.with_suffix(".parquet")))

    def _on_browse_out(self) -> None:
        path = filedialog.asksaveasfilename(
            title="Parquet speichern unter",
            defaultextension=".parquet",
            filetypes=[("Parquet", "*.parquet"), ("Alle Dateien", "*.*")],
        )
        if path:
            self._var_out.set(path)

    def _on_analyze(self) -> None:
        if not self._mf4_path or not self._mf4_path.exists():
            messagebox.showwarning("Fehler", "Bitte zuerst eine gültige MF4-Datei wählen.")
            return
        self._set_status("Analyse läuft …")
        threading.Thread(target=self._run_analyze, daemon=True).start()

    def _run_analyze(self) -> None:
        assert self._mf4_path is not None
        try:
            info = analyze_mf4(self._mf4_path)
            self._analysis = info
            self.after(0, self._populate_meta, info)
            self.after(0, self._populate_tree, info)
            self.after(0, self._set_status, f"Analyse abgeschlossen – {info['channel_count']} Kanäle, {info['group_count']} Gruppen")
        except Exception as exc:
            self.after(0, messagebox.showerror, "Fehler", str(exc))
            self.after(0, self._set_status, "Fehler bei der Analyse.")

    def _populate_meta(self, info: dict) -> None:
        self._txt_meta.configure(state="normal")
        self._txt_meta.delete("1.0", "end")
        lines = [
            f"Datei:       {info['file']}",
            f"Größe:       {info['file_size_mb']:.2f} MB",
            f"Version:     {info['version']}",
            f"Startzeit:   {info['start_time']}",
            f"Kanäle:      {info['channel_count']}",
            f"Gruppen:     {info['group_count']}",
            "",
            "CAN-Gruppen: " + (", ".join(map(str, info["can_groups"])) or "keine"),
            f"CAN-Kanäle:  {len(info['can_channels'])}",
        ]
        self._txt_meta.insert("end", "\n".join(lines))
        self._txt_meta.configure(state="disabled")

    def _populate_tree(self, info: dict) -> None:
        self._tree.delete(*self._tree.get_children())
        for g in info["groups"]:
            gid = self._tree.insert(
                "",
                "end",
                iid=f"g{g['index']}",
                text=f"Gruppe {g['index']}",
                values=(g["samples"], g["channel_count"], g["bus_type"], g["source_name"]),
            )
            for ch in g["channel_names"]:
                self._tree.insert(gid, "end", text=ch, values=("", "", "", ""))

    def _on_export(self) -> None:
        if not self._mf4_path or not self._mf4_path.exists():
            messagebox.showwarning("Fehler", "Bitte zuerst eine MF4-Datei wählen und analysieren.")
            return
        out = self._var_out.get().strip()
        if not out:
            messagebox.showwarning("Fehler", "Bitte einen Ausgabepfad angeben.")
            return

        raster = None
        raster_str = self._var_raster.get().strip()
        if raster_str:
            try:
                raster = float(raster_str)
            except ValueError:
                messagebox.showwarning("Fehler", "Raster muss eine Zahl sein (z.B. 0.01).")
                return

        group_indices = None
        if self._var_selected_only.get() and self._analysis:
            selected = self._tree.selection()
            group_indices = [int(s[1:]) for s in selected if s.startswith("g")]
            if not group_indices:
                messagebox.showwarning("Hinweis", "Keine Gruppen ausgewählt – es werden alle exportiert.")
                group_indices = None

        self._set_status("Export läuft …")
        threading.Thread(
            target=self._run_export, args=(out, raster, group_indices), daemon=True
        ).start()

    def _run_export(self, out: str, raster: float | None, group_indices: list[int] | None) -> None:
        try:
            assert self._mf4_path is not None
            df = mf4_to_dataframe(self._mf4_path, raster=raster, group_indices=group_indices)
            result = dataframe_to_parquet(df, out)
            size_mb = result.stat().st_size / 1024 / 1024
            self.after(
                0,
                messagebox.showinfo,
                "Fertig",
                f"Parquet gespeichert:\n{result}\n({size_mb:.2f} MB, {len(df)} Zeilen, {len(df.columns)} Spalten)",
            )
            self.after(0, self._set_status, f"Export fertig → {result}")
        except Exception as exc:
            self.after(0, messagebox.showerror, "Fehler beim Export", str(exc))
            self.after(0, self._set_status, "Export fehlgeschlagen.")

    # ------------------------------------------------------------ Helpers
    def _set_status(self, msg: str) -> None:
        self._var_status.set(msg)


def run() -> None:
    """Launch the GUI."""
    app = App()
    app.mainloop()
