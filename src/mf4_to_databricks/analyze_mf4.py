"""Analyze MF4 files: metadata, groups, bus types, channels."""

from __future__ import annotations

import argparse
import os
from pathlib import Path

from asammdf import MDF

BUS_TYPE_NAMES = {
    0: "NONE", 1: "OTHER", 2: "CAN", 3: "LIN", 4: "MOST",
    5: "FlexRay", 6: "K_LINE", 7: "ETHERNET", 8: "USB",
}

SOURCE_TYPE_NAMES = {
    0: "OTHER", 1: "ECU", 2: "BUS", 3: "I/O", 4: "TOOL", 5: "USER",
}


def analyze_mf4(path: str | Path) -> dict:
    """Analyze an MF4 file and return structured metadata.

    Returns a dict with keys: version, start_time, channel_count, group_count,
    groups (list of dicts), sources (set), can_channels, can_groups.
    """
    path = Path(path)
    mdf = MDF(str(path))

    result = {
        "file": str(path),
        "file_size_mb": os.path.getsize(path) / 1024 / 1024,
        "version": mdf.version,
        "start_time": str(mdf.start_time),
        "channel_count": len(mdf.channels_db),
        "group_count": len(mdf.groups),
        "groups": [],
        "sources": set(),
        "can_channels": [],
        "can_groups": [],
        "all_channel_names": sorted(mdf.channels_db.keys()),
    }

    for i, group in enumerate(mdf.groups):
        cg = group.channel_group
        src = cg.acq_source
        src_name = src.name if src else ""
        src_path = src.path if src else ""
        src_type = src.source_type if src else -1
        src_bus = src.bus_type if src else -1
        bus_str = BUS_TYPE_NAMES.get(src_bus, str(src_bus))
        type_str = SOURCE_TYPE_NAMES.get(src_type, str(src_type))
        ch_names = [ch.name for ch in group.channels]

        group_info = {
            "index": i,
            "samples": cg.cycles_nr,
            "channel_count": len(group.channels),
            "source_name": src_name,
            "source_path": src_path,
            "source_type": type_str,
            "bus_type": bus_str,
            "channel_names": ch_names,
        }
        result["groups"].append(group_info)

        if src:
            result["sources"].add((src_name, src_path, bus_str, type_str))

        if src_bus == 2:
            result["can_groups"].append(i)
        elif src_name and "CAN" in src_name.upper():
            result["can_groups"].append(i)

    # Deduplicate CAN groups
    result["can_groups"] = sorted(set(result["can_groups"]))

    # Find CAN-related channel names
    can_keywords = ("CAN_", "CAN.", "CANID", "CAN_MSG", "CAN_FRAME", "CAN_DATA", "CAN_BUS", "CAN_ERROR")
    for name in mdf.channels_db:
        if any(kw in name.upper() for kw in can_keywords):
            result["can_channels"].append(name)

    return result


def print_analysis(info: dict) -> None:
    """Pretty-print the analysis result."""
    sep = "=" * 80

    print(sep)
    print("MF4 DATEI-ANALYSE")
    print(sep)
    print(f"Datei:    {info['file']}")
    print(f"Größe:    {info['file_size_mb']:.2f} MB")
    print(f"Version:  {info['version']}")
    print(f"Start:    {info['start_time']}")
    print(f"Kanäle:   {info['channel_count']}")
    print(f"Gruppen:  {info['group_count']}")

    print(f"\n{sep}")
    print("GRUPPEN-DETAILS")
    print(sep)
    for g in info["groups"]:
        print(f"\n--- Gruppe {g['index']} ---")
        print(f"  Samples:     {g['samples']}")
        print(f"  Kanäle:      {g['channel_count']}")
        print(f"  Source:      {g['source_name']}")
        print(f"  Source Path: {g['source_path']}")
        print(f"  Type:        {g['source_type']}")
        print(f"  Bus:         {g['bus_type']}")
        display = g["channel_names"][:15]
        print(f"  Kanäle:      {display}")
        if g["channel_count"] > 15:
            print(f"  ... und {g['channel_count'] - 15} weitere")

    print(f"\n{sep}")
    print("CAN-DATEN")
    print(sep)
    if info["can_groups"]:
        print(f"CAN-Gruppen: {info['can_groups']}")
    else:
        print("Keine Gruppen mit CAN-Bus-Typ gefunden.")
    if info["can_channels"]:
        print(f"\nKanäle mit 'CAN' im Namen ({len(info['can_channels'])}):")
        for c in info["can_channels"][:30]:
            print(f"  - {c}")
        if len(info["can_channels"]) > 30:
            print(f"  ... und {len(info['can_channels']) - 30} weitere")

    print(f"\n{sep}")
    print("QUELLEN (Sources)")
    print(sep)
    for s in sorted(info["sources"]):
        print(f"  Name: {s[0]} | Path: {s[1]} | Bus: {s[2]} | Type: {s[3]}")


def main():
    parser = argparse.ArgumentParser(description="Analyze MF4 measurement files")
    parser.add_argument("file", help="Path to the .mf4 file")
    parser.add_argument("--channels", action="store_true", help="List all channel names")
    args = parser.parse_args()

    info = analyze_mf4(args.file)
    print_analysis(info)

    if args.channels:
        print(f"\n{'=' * 80}")
        print(f"ALLE KANALNAMEN ({len(info['all_channel_names'])})")
        print("=" * 80)
        for n in info["all_channel_names"]:
            print(f"  {n}")


if __name__ == "__main__":
    main()
