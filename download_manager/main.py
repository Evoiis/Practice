from src.dmanager.core import DownloadManager
from src.dmanager.asyncio_thread import AsyncioEventLoopThread
from src.dmanager.gui import DownloadManagerGUI

import logging

def main():

    logging.getLogger().setLevel(logging.INFO)
    runner = AsyncioEventLoopThread()
    dmanager = DownloadManager(chunk_write_size_mb=5)

    gui = DownloadManagerGUI(dmanager, runner)

    gui.run_gui_loop()

    dmanager.shutdown()

if __name__ == "__main__":
    main()
