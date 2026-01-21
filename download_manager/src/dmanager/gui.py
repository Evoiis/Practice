from typing import Dict

from src.dmanager.asyncio_thread import AsyncioEventLoopThread
from src.dmanager.core import DownloadManager, DownloadEvent, DownloadState, DownloadMetadata

import tkinter as tk
from tkinter import ttk

import traceback
import logging

ONE_MEBIBYTE = 1048576
ERROR_COL_WIDTH = 200

class DownloadManagerGUI:

    def __init__(self, dmanager: DownloadManager, runner: AsyncioEventLoopThread, event_poll_rate: int=50):
        self.runner = runner
        self.dmanager = dmanager
        self.event_poll_rate = event_poll_rate
        
        self.parallel_options = ["AUTO", "ON", "OFF"]
        self.use_parallel_choice = None
        self.url_input_element = None
        self.file_name_input_element = None
        self.table = None

        self.task_id_to_table_row = dict()
        self.task_id_and_worker_id_to_table_row = dict()
        self.root = tk.Tk()
        self.download_data: Dict[int: DownloadMetadata] = dict()
        self.last_event = None

        self._generate_gui_base_elements()
        self.root.protocol("WM_DELETE_WINDOW", self._shutdown)
        self.root.title("Download Manager")

        # self.root.iconbitmap("<icon_file>") # TODO

        self.root.after(self.event_poll_rate, self._add_read_event_loop_to_async_thread)
    
    def run_gui_loop(self):
        self.root.mainloop()

    def _shutdown(self):
        self.root.destroy()

    def _add_read_event_loop_to_async_thread(self):
        self.runner.submit(self._read_event_loop())

    @staticmethod
    def _translate_active_time_to_string(active_time):
        if active_time is not None:
            seconds = active_time.total_seconds()
            hours = 0
            minutes = 0

            if seconds > 60:
                minutes = seconds // 60
                if minutes > 60:
                    hours = minutes // 60
                    minutes %= 60
                seconds %= 60
        
            return f"{int(hours)}:{int(minutes)}:{int(seconds)}"
        return ""
    
    @staticmethod
    def _wrap_text(text, width):
        n_characters = int(width * 0.2)
        return "\n".join([text[i:i+(n_characters)] for i in range(0, len(text), int(n_characters))])

    async def _read_event_loop(self):
        try:
            event = await self.dmanager.get_oldest_event()
            if event is not None:
                logging.debug(event)
                # Schedule GUI update in main thread
                self.root.after(0, lambda: self._update_gui_with_event(event))
        except Exception as err:
            tb = traceback.format_exc()
            logging.error(f"Traceback: {tb}")
            logging.error(f"{repr(err)}, {err}")
        self.root.after(self.event_poll_rate, self._add_read_event_loop_to_async_thread)

    def _update_gui_with_event(self, event):
        """Update GUI widgets with event data."""
        if event.error_string:
            event.error_string = self._wrap_text(event.error_string, ERROR_COL_WIDTH)
        if event.state == DownloadState.DELETED:
            self.table.delete(self.task_id_to_table_row[event.task_id])
        elif event.worker_id is not None:
            if (event.task_id, event.worker_id) in self.task_id_and_worker_id_to_table_row:
                values = list(self.table.item(self.task_id_and_worker_id_to_table_row[(event.task_id, event.worker_id)], "values"))

                values[1] = event.state.name
                if event.download_speed is not None:
                    values[5] = f"{round((event.download_speed / ONE_MEBIBYTE), 4)} MiB/s"
                values[6] = event.error_string
                values[7] = self._translate_active_time_to_string(event.active_time)

                self.table.item(
                    self.task_id_and_worker_id_to_table_row[(event.task_id, event.worker_id)],
                    values=values
                )
            elif event.download_speed is not None:
                self.task_id_and_worker_id_to_table_row[(event.task_id, event.worker_id)] = self.table.insert(
                    self.task_id_to_table_row[event.task_id],
                    "end",
                    values=(f"🔼 Worker: {event.worker_id}", event.state.name, "", "", "", f"{round((event.download_speed / ONE_MEBIBYTE), 4)} MiB/s" , event.error_string, event.active_time, "", "", "")
                )

            values = list(self.table.item(self.task_id_to_table_row[event.task_id], "values"))
            
            if event.state not in [DownloadState.PAUSED, DownloadState.COMPLETED]:
                values[1] = event.state.name
            values[3] = event.output_file
            if event.downloaded_bytes is not None and event.download_size_bytes is not None and event.download_size_bytes > 0:
                values[4] = f"{round(event.downloaded_bytes/ONE_MEBIBYTE, 4)} MiB / {round(event.download_size_bytes/ONE_MEBIBYTE, 4)} MiB ({round(100*event.downloaded_bytes/event.download_size_bytes, 2)}%)"

            self.table.item(
                self.task_id_to_table_row[event.task_id],
                values=values
            )

        else:
            values = list(self.table.item(self.task_id_to_table_row[event.task_id], "values"))
            values[1] = event.state.name
            values[3] = event.output_file
            if event.downloaded_bytes is not None and event.download_size_bytes is not None and event.download_size_bytes > 0:
                values[4] = f"{round(event.downloaded_bytes/ONE_MEBIBYTE, 4)} MiB / {round(event.download_size_bytes/ONE_MEBIBYTE, 4)} MiB ({round(100*event.downloaded_bytes/event.download_size_bytes, 2)}%)"
            if event.download_speed is not None:
                values[5] = f"{round((event.download_speed / (ONE_MEBIBYTE)), 4)} MiB/s"
            values[6] = event.error_string
            values[7] = self._translate_active_time_to_string(event.active_time)

            self.table.item(
                self.task_id_to_table_row[event.task_id],
                values=values
            )

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
            open=True,
            values=(task_id, "PENDING", url, "", "", "", "", "", "▶️ RESUME ▶️", "⏸️ PAUSE ⏸️", "❌ DELETE ❌")
        )

        pc_choice = self.use_parallel_choice.get()
        if pc_choice == "ON":
            use_parallel_download = True
        elif pc_choice == "OFF":
            use_parallel_download = False
        else:
            use_parallel_download = None

        self.runner.submit(
            self.dmanager.start_download(
                task_id,
                use_parallel_download=use_parallel_download
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

    
    def _on_table_cell_left_click(self, event):
        row = self.table.identify_row(event.y)
        column = self.table.identify_column(event.x)

        if not row:
            return

        if "Worker" in self.table.item(row, "values")[0]:
            return

        if column == "#9":
            task_id = self.table.item(row, "values")[0]
            self.runner.submit(self.dmanager.start_download(int(task_id)))
        elif column == "#10":
            task_id = self.table.item(row, "values")[0]
            self.runner.submit(self.dmanager.pause_download(int(task_id)))
        elif column == "#11":
            task_id = self.table.item(row, "values")[0]
            self._delete_pop_up(task_id)

    def _copy_cell(self):
        item_id = self.table.focus()
        column = self.table.identify_column(self.last_event.x)
        if not item_id or not column:
            return
        col_index = int(column.replace("#","")) - 1
        value = self.table.item(item_id, "values")[col_index]
        self.root.clipboard_clear()
        self.root.clipboard_append(value)

    def _generate_gui_base_elements(self):
        # First Row, User Input
        first_row_frame = ttk.Frame(self.root)
        first_row_frame.grid(row=0, column=0, columnspan=10, sticky="e", padx=10)

        tk.Label(first_row_frame, text="Download URL:").grid(row=0, column=0)
        self.url_input_element = tk.Entry(first_row_frame, width=50)
        self.url_input_element.grid(row=0, column=1, padx=5)

        tk.Label(first_row_frame, text="Output File Name(Optional):").grid(row=0, column=2)
        self.file_name_input_element = tk.Entry(first_row_frame, width=50)
        self.file_name_input_element.grid(row=0, column=3, padx=5)

        tk.Label(first_row_frame, text="Parallel Download:").grid(row=0, column=4)
        self.use_parallel_choice = tk.StringVar(value=self.parallel_options[0])
        ttk.Combobox(
            first_row_frame,
            textvariable=self.use_parallel_choice,
            values=self.parallel_options,
            state="readonly"
        ).grid(row=0, column=5, padx=5)

        tk.Button(
            first_row_frame, 
            text="Add Download", 
            command=self._add_new_download
        ).grid(row=0, column=6, padx=5)

        # Table
        style = ttk.Style()
        style.configure("Treeview", rowheight=50)

        table_columns = ("Task ID", "Download State", "URL", "Output File", "Downloaded / Total Size (%)", "Current Download Speed", "Error", "Active Time (H:M:S)", "", "", "")
        self.table = ttk.Treeview(
            self.root,
            columns=table_columns,
            show="headings"
        )
        self.table.grid(row=1, column=0, columnspan=10, sticky="nsew")

        self.table.bind("<Button-1>", self._on_table_cell_left_click)

        right_click_menu = tk.Menu(self.root, tearoff=0)
        right_click_menu.add_command(label="Copy", command=self._copy_cell)
        def on_right_click(event):
            self.last_event = event
            right_click_menu.tk_popup(event.x_root, event.y_root)
        self.table.bind("<Button-3>", on_right_click)


        for column in table_columns:
            self.table.heading(column, text=column)
            if column == "Task ID":
                self.table.column(column, width=100)
            elif column == "Download State":
                self.table.column(column, width=100)
            elif column == "Current Download Speed(MiB/s)":
                self.table.column(column, width=200)
            elif column == "Downloaded / Total Size (%)":
                self.table.column(column, width=200)
            elif column == "Error":
                self.table.column(column, width=ERROR_COL_WIDTH)
            else:
                self.table.column(column, width=150)
        
        self.root.rowconfigure(0, weight=0)
        self.root.rowconfigure(1, weight=1)
        self.root.columnconfigure(0, weight=1)
    
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

