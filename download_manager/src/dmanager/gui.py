from typing import Dict

from src.dmanager.asyncio_thread import AsyncioEventLoopThread
from src.dmanager.core import DownloadManager, DownloadEvent, DownloadState, DownloadMetadata

import tkinter as tk
from tkinter import ttk


class DownloadManagerGUI:

    def __init__(self, dmanager: DownloadManager, runner: AsyncioEventLoopThread, event_poll_rate: int=50):
        self.runner = runner
        self.dmanager = dmanager
        self.event_poll_rate = event_poll_rate
        
        self.url_input_element = None
        self.file_name_input_element = None
        self.table = None
        self.task_id_to_table_row = dict()
        self.root = tk.Tk()
        self.download_data: Dict[int: DownloadMetadata] = dict()

        self._generate_gui_base_elements()
        self.root.protocol("WM_DELETE_WINDOW", self._shutdown)

        self.root.after(self.event_poll_rate, self._add_read_event_loop_to_async_thread)
    
    def run_gui_loop(self):
        self.root.mainloop()

    def _shutdown(self):
        self.root.destroy()

    def _add_read_event_loop_to_async_thread(self):
        self.runner.submit(self._read_event_loop())

    async def _read_event_loop(self):

        event = await self.dmanager.get_oldest_event()

        if event is not None:                        
            values = list(self.table.item(self.task_id_to_table_row[event.task_id], "values"))
            values[1] = event.state.name
            values[3] = event.output_file
            if event.percent_completed is not None:
                values[4] = round(event.percent_completed, 2)
            if event.download_speed is not None:
                values[5] = round((event.download_speed / (1048576)), 4)
            values[6] = event.error_string
            if event.active_time is not None:
                seconds = event.active_time.total_seconds()
                hours = 0
                minutes = 0

                if seconds > 60:
                    minutes = seconds // 60
                    if minutes > 60:
                        hours = minutes // 60
                        minutes %= 60
                    seconds %= 60
            
                values[7] = f"{hours}:{minutes}:{round(seconds, 2)}"

            self.table.item(
                self.task_id_to_table_row[event.task_id],
                values=values
            )

        self.root.after(self.event_poll_rate, self._add_read_event_loop_to_async_thread)

    def _add_new_download(self):
        url = self.url_input_element.get()
        file_name = self.file_name_input_element.get()

        if not url:
            return
        
        self.url_input_element.delete(0, tk.END)
        self.file_name_input_element.delete(0, tk.END)

        task_id = self.dmanager.add_download(
            url,
            file_name            
        )

        self.task_id_to_table_row[task_id] = self.table.insert(
            "",
            tk.END,
            values=(task_id, "PENDING", url, "", "", "", "", "")
        )

        self.runner.submit(
            self.dmanager.start_download(
                task_id
            )
        )

    def _generate_gui_base_elements(self):
        tk.Label(self.root, text="Download URL:").grid(row=0, column=0, padx=5, pady=5)
        self.url_input_element = tk.Entry(self.root)
        self.url_input_element.grid(row=0, column=1, padx=5, pady=5)

        tk.Label(self.root, text="Output File Name(Optional):").grid(row=0, column=2, padx=5, pady=5)
        self.file_name_input_element = tk.Entry(self.root)
        self.file_name_input_element.grid(row=0, column=3, padx=5, pady=5)

        tk.Button(
            self.root, 
            text="Add Download", 
            command=self._add_new_download
        ).grid(row=0, column=4)

        table_columns = ("Task ID", "Download State", "URL", "Output File", "Percent Complete", "Current Download Speed(MB/s)", "Error", "Active Time (H:M:S)")
        self.table = ttk.Treeview(
            self.root,
            columns=table_columns,
            show="headings"
        )
        self.table.grid(row=2, column=0, columnspan=10)

        for column in table_columns:
            self.table.heading(column, text=column)
            if column == "Task ID":
                self.table.column(column, width=50)
            elif column == "Current Download Speed(MB/s)":
                self.table.column(column, width=200)
            elif column == "Percent Complete":
                self.table.column(column, width=50)
            else:
                self.table.column(column, width=150)

