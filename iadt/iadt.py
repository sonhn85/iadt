version = '1.0.2'

import sys

import tkinter as tk
from tkinter import ttk
from tkinter import filedialog, StringVar, messagebox

from master import *

class StdoutRedirector:
    def __init__(self, text_area):
        self.text_area = text_area
    def write(self, str):
        self.text_area.insert("end", str)
        self.flush()
    def flush(self):
        self.text_area.update()
        self.text_area.see('end')

class FilterInfo(ttk.LabelFrame):
    def __init__(self, parent, labels):
        super().__init__(parent, text=labels[0])
        self.create_widgets(labels[1:])

    def create_widgets(self, labels):
        vals = []
        for i, label in enumerate(labels):
            val = StringVar()
            ttk.Label(self, text=label).grid(column=0, row=i, sticky='W')
            entry = ttk.Entry(self, textvariable=val)
            entry.grid(column=1, row=i)
            entry.grid_configure(padx=3, pady=3)
            vals.append(val)
        self.vals = vals

    def set_vals(self, vals):
        for i, val in enumerate(vals):
            self.vals[i].set(val)
        
    def get_vals(self):
        return [val.get() for val in self.vals]

class FileInfo(ttk.LabelFrame):
    def __init__(self, parent, labels):
        super().__init__(parent, text=labels[0])
        self.create_widgets(labels[1:])

    def create_widgets(self, labels):
        ttk.Label(self, text=labels[0]).grid(column=0, row=0, sticky='W')
        path = StringVar()
        entry = ttk.Entry(self, textvariable=path)
        entry.grid(column=1, row=0, columnspan=2)
        entry.grid_configure(padx=3, pady=3, sticky='WNES')

        def browse():
            filename = filedialog.askopenfilename(filetypes=[("Excel files", "*.xlsx"), ("Excel files (old)", "*.xls")])
            path.set(filename)

        ttk.Button(self, text='...', command=browse).grid(column=3, row=0, sticky='W')

        vals = [path]
        for i, label in enumerate(labels[1:]):
            row = 1 + i
            val = StringVar()
            ttk.Label(self, text=label).grid(column=0, row=row, sticky='W')
            entry = ttk.Entry(self, textvariable=val)
            entry.grid(column=1, row=row)
            entry.grid_configure(padx=3, pady=3)
            vals.append(val)

            val = StringVar()
            ttk.Label(self, text='Dòng').grid(column=2, row=row, sticky='W')
            entry = ttk.Entry(self, textvariable=val)
            entry.grid(column=3, row=row)
            vals.append(val)
        self.vals = vals

    def set_vals(self, vals):
        for i, val in enumerate(vals):
            self.vals[i].set(val)

    def get_vals(self):
        return [val.get() for val in self.vals]

class IADT(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title('IADT {}'.format(version))
        mainframe = tk.Frame(self)
        self.create_widgets(mainframe)
        mainframe.grid(column=0, row=0, sticky='WNES')
        self.mainframe = mainframe

    def create_widgets(self, frame):
        source_file = FileInfo(frame, ['File nguồn', 'Đường dẫn', 'LN sheet', 'MD,TF sheet'])
        #source_file.set_vals(['/home/sonhn/ocb/data/File_nguon_20210731.xlsx', 'LN001 (7)', '1', 'TF,MD (7)', '0'])
        source_file.set_vals(['', 'LN001', '2', 'TF,MD', '1'])
        source_file.grid(column=0, row=0, sticky='WNE')

        collateral_file = FileInfo(frame, ['File TSBĐ', 'Đường dẫn', 'TSBĐ sheet'])
        #collateral_file.set_vals(['/home/sonhn/ocb/data/BCN068_TAISAN_20210802.xls', 'Sheet1', '0'])
        collateral_file.set_vals(['', 'Sheet1', '1'])
        collateral_file.grid(column=1, row=0, sticky='WNE')

        cic_file = FileInfo(frame, ['File CIC', 'Đường dẫn', 'CIC sheet'])
        #cic_file.set_vals(['/home/sonhn/ocb/data/cic.xlsx', 'Sheet1', '0'])
        cic_file.set_vals(['', 'Sheet1', '1'])
        cic_file.grid(column=0, row=1, sticky='WNE')

        jica_rdf_vnsat_file = FileInfo(frame, ['File Jica/RDF/VNSAT', 'Đường dẫn', 'Sheet'])
        #jica_rdf_vnsat_file.set_vals(['/home/sonhn/ocb/data/jica_rdf_vnsat.xlsx', 'Sheet1', '0'])
        jica_rdf_vnsat_file.set_vals(['', 'Sheet1', '1'])
        jica_rdf_vnsat_file.grid(column=1, row=1, sticky='WNE')

        branch = FilterInfo(frame, ['Lọc', 'Chi nhánh', 'Bỏ PGD'])
        #branch.set_vals(['VN0010101,VN0010125', ''])
        branch.set_vals(['', ''])
        branch.grid(column=0, row=2, sticky='WNE')

        text = tk.Text(frame, width=150, height=20)
        scroll = ttk.Scrollbar(frame, orient=tk.VERTICAL, command=text.yview)
        text.configure(yscrollcommand=scroll.set)
        text.grid(column=0, row=3, columnspan=3)
        scroll.grid(column=3, row=3, sticky='NS')
        sys.stdout = StdoutRedirector(text)
        sys.stderr = StdoutRedirector(text)
        frame.columnconfigure(2, weight=1)
        frame.rowconfigure(3, weight=1)

        def cb():
            self.button['state'] = 'disable'
            self.button['text'] = 'RUNNING ...'
            text.delete(1.0, tk.END)
            try:
                print('IADT version {}'.format(version))
                run(
                    source_file_info=source_file.get_vals(),
                    collateral_file_info=collateral_file.get_vals(),
                    cic_file_info=cic_file.get_vals(),
                    branch_info=branch.get_vals(),
                    jica_rdf_vnsat_file_info=jica_rdf_vnsat_file.get_vals(),
                )
            except Exception as e:
                messagebox.showerror(message='An error occured!')
                raise e
            else:
                messagebox.showinfo(message='Success')
            finally:
                self.button['state'] = 'enable'
                self.button['text'] = 'RUN!'

        button = ttk.Button(frame, text='RUN!', command=cb)
        button.grid(column=1, row=2)
        self.button = button

        for child in frame.winfo_children(): 
            child.grid_configure(padx=5, pady=5)

root = IADT()

messagebox.showinfo('Advice', 'IADT version is {}, consider checking for update before using for new features and bug fixes.'.format(version))

root.mainloop()
sys.stdout = sys.__stdout__
sys.stderr = sys.__stderr__