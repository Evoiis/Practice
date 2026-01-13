from src.dmanager.core import DownloadManager
from src.dmanager.asyncio_thread import AsyncioEventLoopThread
from src.dmanager.gui import DownloadManagerGUI

import logging

def main():

    logging.getLogger().setLevel(logging.INFO)
    # logging.getLogger().setLevel(logging.DEBUG)
    runner = AsyncioEventLoopThread()
    dmanager = DownloadManager()

    gui = DownloadManagerGUI(dmanager, runner)

    gui.run_gui_loop()

    runner.submit(dmanager.shutdown())

if __name__ == "__main__":
    main()
