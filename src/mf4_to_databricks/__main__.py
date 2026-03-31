"""Entry point: python -m mf4_to_databricks

Launches the GUI by default.  Use --cli for the original command-line analysis.
"""

import sys


def main() -> None:
    if "--cli" in sys.argv:
        sys.argv.remove("--cli")
        from mf4_to_databricks.analyze_mf4 import main as cli_main
        cli_main()
    else:
        from mf4_to_databricks.gui.app import run
        run()


main()
