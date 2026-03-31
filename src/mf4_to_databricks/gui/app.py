"""Main GUI application for MF4 analysis, plotting, and Parquet export."""

from __future__ import annotations

import threading
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from pathlib import Path

import matplotlib
matplotlib.use("TkAgg")
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg, NavigationToolbar2Tk
from matplotlib.figure import Figure

from asammdf import MDF

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
        self._mdf: MDF | None = None

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

        # Tab 3: Plot
        self._frm_plot = ttk.Frame(self._notebook)
        self._notebook.add(self._frm_plot, text="Plot")
        self._build_plot_tab()

        # Tab 4: Export
        self._frm_export = ttk.Frame(self._notebook, padding=10)
        self._notebook.add(self._frm_export, text="Parquet Export")
        self._build_export_tab()

        # --- Bottom: status bar ---
        self._var_status = tk.StringVar(value="Bereit.")
        ttk.Label(self, textvariable=self._var_status, relief="sunken", anchor="w").pack(
            fill="x", padx=10, pady=(0, 8)
        )

    def _build_plot_tab(self) -> None:
        frm = self._frm_plot

        # Left: channel selector
        frm_left = ttk.Frame(frm, width=260)
        frm_left.pack(side="left", fill="y", padx=(4, 0), pady=4)
        frm_left.pack_propagate(False)

        ttk.Label(frm_left, text="Kanäle filtern:").pack(anchor="w", padx=4, pady=(4, 0))
        self._var_ch_filter = tk.StringVar()
        self._var_ch_filter.trace_add("write", self._on_channel_filter)
        ttk.Entry(frm_left, textvariable=self._var_ch_filter).pack(fill="x", padx=4, pady=2)

        ttk.Label(frm_left, text="Verfügbare Kanäle:").pack(anchor="w", padx=4, pady=(4, 0))
        frm_list = ttk.Frame(frm_left)
        frm_list.pack(fill="both", expand=True, padx=4)
        sb_ch = ttk.Scrollbar(frm_list)
        sb_ch.pack(side="right", fill="y")
        self._lst_channels = tk.Listbox(frm_list, selectmode="extended", yscrollcommand=sb_ch.set)
        self._lst_channels.pack(fill="both", expand=True)
        sb_ch.config(command=self._lst_channels.yview)
        self._all_channel_names: list[str] = []

        frm_btns = ttk.Frame(frm_left)
        frm_btns.pack(fill="x", padx=4, pady=4)
        ttk.Button(frm_btns, text="▶ Plotten", command=self._on_plot).pack(side="left")
        ttk.Button(frm_btns, text="Alle abwählen", command=self._on_clear_selection).pack(side="right")

        ttk.Label(frm_left, text="Aktive Kanäle:").pack(anchor="w", padx=4, pady=(4, 0))
        frm_active = ttk.Frame(frm_left)
        frm_active.pack(fill="both", expand=True, padx=4, pady=(0, 4))
        sb_active = ttk.Scrollbar(frm_active)
        sb_active.pack(side="right", fill="y")
        self._lst_active = tk.Listbox(frm_active, yscrollcommand=sb_active.set)
        self._lst_active.pack(fill="both", expand=True)
        sb_active.config(command=self._lst_active.yview)

        ttk.Button(frm_left, text="Ausgewählte entfernen", command=self._on_remove_plotted).pack(fill="x", padx=4, pady=(0, 4))

        # Right: matplotlib canvas
        frm_right = ttk.Frame(frm)
        frm_right.pack(side="left", fill="both", expand=True, padx=4, pady=4)

        self._fig = Figure(figsize=(7, 4), dpi=100)
        self._ax = self._fig.add_subplot(111)
        self._ax.set_xlabel("Zeit (s)")
        self._ax.set_ylabel("Wert")
        self._ax.grid(True, alpha=0.3)
        self._fig.tight_layout()

        self._canvas = FigureCanvasTkAgg(self._fig, master=frm_right)
        self._canvas.draw()

        toolbar = NavigationToolbar2Tk(self._canvas, frm_right)
        toolbar.update()
        self._canvas.get_tk_widget().pack(fill="both", expand=True)

        self._plotted_channels: list[str] = []

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
            self._mdf = MDF(str(self._mf4_path))
            info = analyze_mf4(self._mf4_path)
            self._analysis = info
            self.after(0, self._populate_meta, info)
            self.after(0, self._populate_tree, info)
            self.after(0, self._populate_channel_list, info)
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

    def _populate_channel_list(self, info: dict) -> None:
        self._all_channel_names = info["all_channel_names"]
        self._lst_channels.delete(0, "end")
        for name in self._all_channel_names:
            self._lst_channels.insert("end", name)
        # Reset plot state
        self._plotted_channels.clear()
        self._lst_active.delete(0, "end")
        self._ax.clear()
        self._ax.set_xlabel("Zeit (s)")
        self._ax.set_ylabel("Wert")
        self._ax.grid(True, alpha=0.3)
        self._canvas.draw()

    def _on_channel_filter(self, *_args: object) -> None:
        pattern = self._var_ch_filter.get().strip().upper()
        self._lst_channels.delete(0, "end")
        for name in self._all_channel_names:
            if not pattern or pattern in name.upper():
                self._lst_channels.insert("end", name)

    def _on_clear_selection(self) -> None:
        self._lst_channels.selection_clear(0, "end")

    def _on_plot(self) -> None:
        sel_indices = self._lst_channels.curselection()
        if not sel_indices:
            messagebox.showinfo("Hinweis", "Bitte mindestens einen Kanal auswählen.")
            return
        if not self._mdf:
            messagebox.showwarning("Fehler", "Bitte zuerst eine MF4-Datei analysieren.")
            return
        new_channels = [self._lst_channels.get(i) for i in sel_indices]
        # Add only channels not already plotted
        to_add = [ch for ch in new_channels if ch not in self._plotted_channels]
        if not to_add:
            messagebox.showinfo("Hinweis", "Alle ausgewählten Kanäle sind bereits geplottet.")
            return
        self._set_status(f"Lade {len(to_add)} Kanal/Kanäle …")
        threading.Thread(target=self._run_plot, args=(to_add,), daemon=True).start()

    def _run_plot(self, channels: list[str]) -> None:
        assert self._mdf is not None
        try:
            signals: list[tuple[str, object, object]] = []
            for ch_name in channels:
                try:
                    sig = self._mdf.get(ch_name)
                    signals.append((ch_name, sig.timestamps, sig.samples))
                except Exception:
                    pass  # skip channels that can't be read
            if signals:
                self.after(0, self._update_plot, signals)
            else:
                self.after(0, messagebox.showwarning, "Fehler", "Keine der ausgewählten Kanäle konnte gelesen werden.")
            self.after(0, self._set_status, f"{len(signals)} Kanal/Kanäle geladen.")
        except Exception as exc:
            self.after(0, messagebox.showerror, "Fehler", str(exc))
            self.after(0, self._set_status, "Plot fehlgeschlagen.")

    def _update_plot(self, signals: list[tuple[str, object, object]]) -> None:
        for ch_name, timestamps, samples in signals:
            self._ax.plot(timestamps, samples, label=ch_name, linewidth=0.8)
            self._plotted_channels.append(ch_name)
            self._lst_active.insert("end", ch_name)
        self._ax.set_xlabel("Zeit (s)")
        self._ax.set_ylabel("Wert")
        self._ax.legend(fontsize=7, loc="upper right")
        self._ax.grid(True, alpha=0.3)
        self._fig.tight_layout()
        self._canvas.draw()

    def _on_remove_plotted(self) -> None:
        sel_indices = list(self._lst_active.curselection())
        if not sel_indices:
            messagebox.showinfo("Hinweis", "Bitte Kanäle in der 'Aktive Kanäle'-Liste auswählen.")
            return
        to_remove = {self._lst_active.get(i) for i in sel_indices}
        self._plotted_channels = [ch for ch in self._plotted_channels if ch not in to_remove]
        # Refresh active list
        self._lst_active.delete(0, "end")
        for ch in self._plotted_channels:
            self._lst_active.insert("end", ch)
        # Redraw
        self._redraw_plot()

    def _redraw_plot(self) -> None:
        if not self._mdf:
            return
        self._ax.clear()
        self._ax.set_xlabel("Zeit (s)")
        self._ax.set_ylabel("Wert")
        self._ax.grid(True, alpha=0.3)
        for ch_name in self._plotted_channels:
            try:
                sig = self._mdf.get(ch_name)
                self._ax.plot(sig.timestamps, sig.samples, label=ch_name, linewidth=0.8)
            except Exception:
                pass
        if self._plotted_channels:
            self._ax.legend(fontsize=7, loc="upper right")
        self._fig.tight_layout()
        self._canvas.draw()
        self._var_status.set(msg)


def run() -> None:
    """Launch the GUI."""
    app = App()
    app.mainloop()
