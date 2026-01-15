from src.dmanager.core import DownloadManager
from src.dmanager.asyncio_thread import AsyncioEventLoopThread
from src.dmanager.gui import DownloadManagerGUI

import logging
import argparse

def main():
    parser = argparse.ArgumentParser(prog="Download Manager")
    parser.add_argument("--debug", action="store_true")

    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    else:
        logging.getLogger().setLevel(logging.INFO)
    runner = AsyncioEventLoopThread()
    dmanager = DownloadManager()

    gui = DownloadManagerGUI(dmanager, runner)

    gui.run_gui_loop()

    runner.submit(dmanager.shutdown())

if __name__ == "__main__":
    main()
