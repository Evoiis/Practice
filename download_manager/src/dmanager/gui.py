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
        self.root.title("Download Manager")
        self.root.resizable(False, False)

        # self.root.iconbitmap("<icon_file>") # TODO

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
            # print(event)
            if event.state == DownloadState.DELETED:
                self.table.delete(self.task_id_to_table_row[event.task_id])
            else:                
                values = list(self.table.item(self.task_id_to_table_row[event.task_id], "values"))
                values[1] = event.state.name
                values[3] = event.output_file
                if event.downloaded_bytes is not None and event.download_size_bytes is not None:
                    values[4] = f"{round(event.downloaded_bytes/1048576, 4)} MB / {round(event.download_size_bytes/1048576, 4)} MB ({round(100*event.downloaded_bytes/event.download_size_bytes, 2)})"
                if event.download_speed is not None:
                    values[5] = f"{round((event.download_speed / (1048576)), 4)} MB/s"
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
                
                    values[7] = f"{int(hours)}:{int(minutes)}:{int(seconds)}"

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
            values=(task_id, "PENDING", url, "", "", "", "", "", "▶️ RESUME ▶️", "⏸️ PAUSE ⏸️", "❌ DELETE ❌")
        )

        self.runner.submit(
            self.dmanager.start_download(
                task_id,
                use_parallel_download=True
            )
        )

    def _delete_pop_up(self, task_id):
        popup = tk.Toplevel(self.root)
        popup.title("Delete Check")
        popup.transient(self.root)
        popup.grab_set()
        del_file_check = tk.BooleanVar(value=False)

        def submit_delete_command():
            self.runner.submit(self.dmanager.delete_download(int(task_id), remove_file=del_file_check.get()))
            popup.destroy()

        tk.Label(popup, text=f"Are you sure you want to delete task {task_id}?", anchor="center").grid(row=0, column=0)
        
        tk.Checkbutton(popup, text="Delete file from disk?", variable=del_file_check, anchor="center").grid(row=1, column=0)

        tk.Button(popup, text="Yes", command=submit_delete_command).grid(row=2,column=0, sticky="e", pady=10, padx=5)
        tk.Button(popup, text="No", command=popup.destroy).grid(row=2,column=1, sticky="w", pady=10, padx=5)

        self._center_over_parent(popup, self.root)

    
    def _on_table_cell_click(self, event):
        row = self.table.identify_row(event.y)
        column = self.table.identify_column(event.x)

        if not row:
            return

        if column == "#9":
            task_id = self.table.item(row, "values")[0]
            self.runner.submit(self.dmanager.resume_download(int(task_id)))
        elif column == "#10":
            task_id = self.table.item(row, "values")[0]
            self.runner.submit(self.dmanager.pause_download(int(task_id)))
        elif column == "#11":
            task_id = self.table.item(row, "values")[0]
            self._delete_pop_up(task_id)


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

        table_columns = ("Task ID", "Download State", "URL", "Output File", "Downloaded / Total Size (%)", "Current Download Speed", "Error", "Active Time (H:M:S)", "", "", "")
        self.table = ttk.Treeview(
            self.root,
            columns=table_columns,
            show="headings"
        )
        self.table.grid(row=2, column=0, columnspan=10)

        self.table.bind("<Button-1>", self._on_table_cell_click)

        for column in table_columns:
            self.table.heading(column, text=column)
            if column == "Task ID":
                self.table.column(column, width=50)
            elif column == "Current Download Speed(MB/s)":
                self.table.column(column, width=200)
            elif column == "Downloaded / Total Size (%)":
                self.table.column(column, width=200)
            # elif column == "":
            #     self.table.column(column, width=50, anchor="center")
            else:
                self.table.column(column, width=150)
    
    @staticmethod
    def _center_over_parent(window, parent):
        window.update_idletasks()

        w = window.winfo_width()
        h = window.winfo_height()

        px = parent.winfo_x()
        py = parent.winfo_y()
        pw = parent.winfo_width()
        ph = parent.winfo_height()

        x = px + (pw // 2) - (w // 2)
        y = py + (ph // 2) - (h // 2)

        window.geometry(f"+{x}+{y}")


