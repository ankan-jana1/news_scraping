#!/usr/bin/env python3
"""
Simple Tkinter GUI to run a news analysis script with parsed arguments and a Run button.

This version:
 - Removes the visible "Script" selection (the GUI will use the default script
   news_search.py located in the same folder as this GUI).
 - Adds keyword search UI: choose between using default keywords or entering
   custom keywords with Category and Subcategory. Custom keywords are passed
   to the analysis script via --keywords in the format:
     keyword||category||subcategory;;keyword2||cat2||sub2
   (fields use '||' and entries are separated by ';;')
 - Keeps Playwright (--resolve-js) checkbox and "Verify Playwright" button.
 - Adds signature shown in the GUI footer: :ankan.j@sustaintel.com

Place this file next to analysis script and double-click/run.
Developed by Ankan.j@sustaintel.com
Signature: :ankan.j@sustaintel.com
"""
import os
import sys
import threading
import subprocess
import shlex
import queue
import time
import tkinter as tk
from tkinter import ttk, filedialog, messagebox

# ---------------------------
# Helper: Threaded subprocess
# ---------------------------
class ProcessRunner:
    def __init__(self, on_output_line, on_finished):
        self.proc = None
        self._thread = None
        self._stop_event = threading.Event()
        self.on_output_line = on_output_line
        self.on_finished = on_finished

    def start(self, cmd_args, cwd=None, env=None):
        if self.proc:
            raise RuntimeError("Process already running")

        def target():
            try:
                # Start subprocess, merge stderr into stdout
                self.proc = subprocess.Popen(
                    cmd_args,
                    cwd=cwd,
                    env=env,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    universal_newlines=True,
                    bufsize=1
                )

                # Read output line-by-line
                for line in self.proc.stdout:
                    if line is None:
                        break
                    if self._stop_event.is_set():
                        break
                    self.on_output_line(line.rstrip())
                # Wait for process to exit (if not killed)
                self.proc.wait()
                rc = self.proc.returncode
            except Exception as e:
                self.on_output_line(f"[PROCESS ERROR] {e}")
                rc = -1
            finally:
                self.proc = None
                self.on_finished(rc)

        self._stop_event.clear()
        self._thread = threading.Thread(target=target, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop_event.set()
        if self.proc and self.proc.poll() is None:
            try:
                self.proc.terminate()
                # give it a short grace period then kill if needed
                try:
                    self.proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    self.proc.kill()
            except Exception:
                pass

# ---------------------------
# GUI App
# ---------------------------
class NewsRunnerGUI(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("News Scraper")
        self.geometry("950x700")
        self.minsize(800, 520)
        self.create_widgets()
        self.process_runner = ProcessRunner(self.append_output, self.on_process_finished)
        self.output_queue = queue.Queue()

        # Periodic flush of queue to Text widget
        self.after(100, self._flush_output_queue)

    def create_widgets(self):
        pad = 6
        frm = ttk.Frame(self)
        frm.pack(fill="both", expand=True, padx=pad, pady=pad)

        row = 0

        # Note: Script selection removed. We use default script: news_search.py in cwd
        default_script = os.path.join(os.getcwd(), "news_search.py")
        ttk.Label(frm, text=f"Using script: {default_script}").grid(row=row, column=0, columnspan=3, sticky="w")
        row += 1

        ttk.Label(frm, text="Urban CSV:").grid(row=row, column=0, sticky="w")
        self.urban_var = tk.StringVar(value=os.path.join(os.getcwd(), "Urban_list.csv"))
        urban_entry = ttk.Entry(frm, textvariable=self.urban_var, width=70)
        urban_entry.grid(row=row, column=1, sticky="we", padx=(4, 4))
        ttk.Button(frm, text="Browse...", command=self.browse_urban_csv).grid(row=row, column=2, sticky="e")
        row += 1

        # Numeric / string options
        ttk.Label(frm, text="Days back:").grid(row=row, column=0, sticky="w")
        self.days_var = tk.StringVar(value="30")
        ttk.Entry(frm, textvariable=self.days_var, width=12).grid(row=row, column=1, sticky="w")
        ttk.Label(frm, text="Min relevance:").grid(row=row, column=1, sticky="e", padx=(180,0))
        self.minrel_var = tk.StringVar(value="30")
        ttk.Entry(frm, textvariable=self.minrel_var, width=8).grid(row=row, column=1, sticky="e", padx=(0,110))
        row += 1

        ttk.Label(frm, text="Max locations (optional):").grid(row=row, column=0, sticky="w")
        self.maxloc_var = tk.StringVar(value="")
        ttk.Entry(frm, textvariable=self.maxloc_var, width=12).grid(row=row, column=1, sticky="w")
        row += 1

        ttk.Label(frm, text="Sources (comma-separated):").grid(row=row, column=0, sticky="w")
        self.sources_var = tk.StringVar(value="google_news,rss_feeds,web_scraping")
        ttk.Entry(frm, textvariable=self.sources_var, width=70).grid(row=row, column=1, sticky="we", padx=(4,4))
        row += 1

        ttk.Label(frm, text="Output prefix:").grid(row=row, column=0, sticky="w")
        self.outpref_var = tk.StringVar(value="contextual_news")
        ttk.Entry(frm, textvariable=self.outpref_var, width=30).grid(row=row, column=1, sticky="w")
        row += 1

        # Checkboxes
        self.disable_scrapers_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(frm, text="Disable scrapers (only RSS & Google)", variable=self.disable_scrapers_var).grid(row=row, column=1, sticky="w")

        # Resolve-JS checkbox + verify button
        self.resolve_js_var = tk.BooleanVar(value=False)
        resolve_frame = ttk.Frame(frm)
        resolve_frame.grid(row=row, column=2, sticky="e")
        ttk.Checkbutton(resolve_frame, text="Enable JS resolution (--resolve-js)", variable=self.resolve_js_var).pack(side="left")
        ttk.Button(resolve_frame, text="Verify Playwright", command=self.verify_playwright).pack(side="left", padx=(6,0))
        row += 1

        # --- Keyword controls (new) ---
        kwframe = ttk.LabelFrame(frm, text="Keywords for search")
        kwframe.grid(row=row, column=0, columnspan=3, sticky="we", pady=(8,8))
        kwframe.columnconfigure(1, weight=1)

        self.keyword_mode_var = tk.StringVar(value="default")
        ttk.Radiobutton(kwframe, text="Use default keywords (script internally)", variable=self.keyword_mode_var, value="default", command=self._on_keyword_mode_change).grid(row=0, column=0, sticky="w", padx=4, pady=2)
        ttk.Radiobutton(kwframe, text="Use custom keywords", variable=self.keyword_mode_var, value="custom", command=self._on_keyword_mode_change).grid(row=0, column=1, sticky="w", padx=4, pady=2)

        # Custom keyword entry row
        ttk.Label(kwframe, text="Keyword:").grid(row=1, column=0, sticky="w", padx=4)
        self.kw_entry = ttk.Entry(kwframe, width=28)
        self.kw_entry.grid(row=1, column=1, sticky="w", padx=4)
        ttk.Label(kwframe, text="Category:").grid(row=1, column=2, sticky="w", padx=4)
        self.cat_entry = ttk.Entry(kwframe, width=20)
        self.cat_entry.grid(row=1, column=3, sticky="w", padx=4)
        ttk.Label(kwframe, text="Subcategory:").grid(row=1, column=4, sticky="w", padx=4)
        self.subcat_entry = ttk.Entry(kwframe, width=20)
        self.subcat_entry.grid(row=1, column=5, sticky="w", padx=4)
        ttk.Button(kwframe, text="Add", command=self._add_keyword).grid(row=1, column=6, sticky="w", padx=4)

        # Listbox to show custom keywords
        self.kw_listbox = tk.Listbox(kwframe, height=6)
        self.kw_listbox.grid(row=2, column=0, columnspan=6, sticky="we", padx=4, pady=(6,4))
        kw_buttons = ttk.Frame(kwframe)
        kw_buttons.grid(row=2, column=6, sticky="n")
        ttk.Button(kw_buttons, text="Remove Selected", command=self._remove_selected_keyword).pack(fill="x", pady=(0,4))
        ttk.Button(kw_buttons, text="Clear All", command=self._clear_keywords).pack(fill="x")

        ttk.Label(kwframe, text="Note: Custom keywords will be passed as --keywords using format\n"
                                 "keyword||category||subcategory  (fields use '||', entries separated by ';;')",
                  foreground="gray").grid(row=3, column=0, columnspan=7, sticky="w", padx=4, pady=(6,4))

        row += 1

        # Run / Stop buttons
        btn_frame = ttk.Frame(frm)
        btn_frame.grid(row=row, column=0, columnspan=3, pady=(8,12))
        self.run_btn = ttk.Button(btn_frame, text="Run", command=self.on_run)
        self.run_btn.pack(side="left", padx=4)
        self.stop_btn = ttk.Button(btn_frame, text="Stop", command=self.on_stop, state="disabled")
        self.stop_btn.pack(side="left", padx=4)
        ttk.Button(btn_frame, text="Clear Output", command=self.clear_output).pack(side="left", padx=4)
        row += 1

        # Output Text area
        text_frame = ttk.Frame(frm)
        text_frame.grid(row=row, column=0, columnspan=3, sticky="nsew")
        frm.rowconfigure(row, weight=1)
        frm.columnconfigure(1, weight=1)

        self.output_text = tk.Text(text_frame, wrap="none", state="disabled")
        self.output_text.pack(side="left", fill="both", expand=True)

        # Scrollbars
        yscroll = ttk.Scrollbar(text_frame, orient="vertical", command=self.output_text.yview)
        yscroll.pack(side="right", fill="y")
        self.output_text['yscrollcommand'] = yscroll.set
        xscroll = ttk.Scrollbar(self, orient="horizontal", command=self.output_text.xview)
        xscroll.pack(side="bottom", fill="x")
        self.output_text['xscrollcommand'] = xscroll.set

        # Footer signature
        footer = ttk.Frame(frm)
        footer.grid(row=row+1, column=0, columnspan=3, sticky="we", pady=(8,0))
        ttk.Label(footer, text="Developed by Ankan.j@sustaintel.com").pack(side="left")
        ttk.Label(footer, text="Sustainability Intelligence Pvt. Ltd.", foreground="green").pack(side="right")

        # initialize keyword mode UI state
        self._on_keyword_mode_change()

    def browse_urban_csv(self):
        path = filedialog.askopenfilename(
            title="Select Urban CSV",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")]
        )
        if path:
            self.urban_var.set(path)

    def verify_playwright(self):
        """
        Run a quick check using the same Python interpreter to see if Playwright is importable.
        If import works and the sync API exists, report OK. This does not install browsers.
        """
        cmd = [sys.executable, "-c", "import sys\ntry:\n from playwright.sync_api import sync_playwright\n print('OK')\nexcept Exception as e:\n print('ERR:', type(e).__name__, e)\n sys.exit(0)"]
        try:
            p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)
            out, _ = p.communicate(timeout=15)
            out = (out or "").strip()
            if out.startswith("OK"):
                self.append_output("[INFO] Playwright import OK in this Python environment.\n")
                messagebox.showinfo("Playwright check", "Playwright import OK.\nIf you haven't installed browsers, run:\n\npython -m playwright install chromium")
            else:
                self.append_output(f"[WARN] Playwright check output: {out}\n")
                messagebox.showwarning("Playwright check", f"Playwright not available or import failed:\n\n{out}")
        except subprocess.TimeoutExpired:
            self.append_output("[ERROR] Playwright check timed out.\n")
            messagebox.showerror("Playwright check", "Playwright check timed out.")
        except Exception as e:
            self.append_output(f"[ERROR] Playwright check failed: {e}\n")
            messagebox.showerror("Playwright check", f"Playwright check failed: {e}")

    # Keyword helper methods
    def _on_keyword_mode_change(self):
        mode = self.keyword_mode_var.get()
        state = "normal" if mode == "custom" else "disabled"
        for w in (self.kw_entry, self.cat_entry, self.subcat_entry, self.kw_listbox):
            w.configure(state=state)
        # Buttons in the keyword area:
        # find add/remove/clear buttons by walking children (simple and robust)
        # (they are still accessible even when disabled)
        for child in self.children.values():
            pass  # no-op, keep for future extensibility

    def _add_keyword(self):
        kw = self.kw_entry.get().strip()
        cat = self.cat_entry.get().strip()
        sub = self.subcat_entry.get().strip()
        if not kw:
            messagebox.showerror("Error", "Keyword cannot be empty.")
            return
        # Represent entry as keyword||category||subcategory (empty fields allowed)
        entry = f"{kw}||{cat}||{sub}"
        # Avoid duplicates
        existing = list(self.kw_listbox.get(0, "end"))
        if entry in existing:
            messagebox.showinfo("Info", "This keyword entry already exists.")
            return
        self.kw_listbox.insert("end", entry)
        # clear small inputs
        self.kw_entry.delete(0, "end")
        self.cat_entry.delete(0, "end")
        self.subcat_entry.delete(0, "end")

    def _remove_selected_keyword(self):
        sel = self.kw_listbox.curselection()
        if not sel:
            return
        for i in reversed(sel):
            self.kw_listbox.delete(i)

    def _clear_keywords(self):
        self.kw_listbox.delete(0, "end")

    def on_run(self):
        # Script path is fixed to default
        script_path = os.path.join(os.getcwd(), "news_search.py")
        if not os.path.exists(script_path):
            messagebox.showerror("Error", f"Script file not found at {script_path}. Please place the analysis script next to this GUI.")
            return

        urban_csv = self.urban_var.get().strip()
        if not urban_csv or not os.path.exists(urban_csv):
            if not messagebox.askyesno("Warning", "Urban CSV not found. Continue anyway?"):
                return

        # Validate numeric fields
        try:
            days_back = int(self.days_var.get().strip())
            if days_back < 0:
                raise ValueError()
        except Exception:
            messagebox.showerror("Error", "Days back must be a non-negative integer.")
            return

        try:
            min_rel = int(self.minrel_var.get().strip())
            if min_rel < 0 or min_rel > 100:
                raise ValueError()
        except Exception:
            messagebox.showerror("Error", "Min relevance must be an integer between 0 and 100.")
            return

        max_loc_raw = self.maxloc_var.get().strip()
        max_loc_arg = []
        if max_loc_raw:
            try:
                max_l = int(max_loc_raw)
                if max_l <= 0:
                    raise ValueError()
                max_loc_arg = ["--max-locations", str(max_l)]
            except Exception:
                messagebox.showerror("Error", "Max locations must be a positive integer or left blank.")
                return

        # Build args
        args = [
            sys.executable, script_path,
            "--urban-csv", urban_csv,
            "--days-back", str(days_back),
            "--min-relevance", str(min_rel),
            "--sources", self.sources_var.get().strip(),
            "--output-prefix", self.outpref_var.get().strip()
        ]
        args += max_loc_arg
        if self.disable_scrapers_var.get():
            args.append("--disable-scrapers")

        # Add --resolve-js if checkbox is selected
        if self.resolve_js_var.get():
            args.append("--resolve-js")

        # Keywords handling
        if self.keyword_mode_var.get() == "default":
            args.append("--use-default-keywords")
        else:
            # collect custom entries from listbox
            entries = list(self.kw_listbox.get(0, "end"))
            if not entries:
                if not messagebox.askyesno("No keywords", "No custom keywords have been added. Continue using default keywords?"):
                    return
                args.append("--use-default-keywords")
            else:
                # join entries with ';;' (each entry is already keyword||cat||sub)
                kw_arg = ";;".join(entries)
                args += ["--keywords", kw_arg]

        # Disable controls during run
        self.set_controls_state(running=True)
        self.append_output(f"[INFO] Running: {' '.join(shlex.quote(a) for a in args)}\n")
        self.process_runner.start(args, cwd=os.path.dirname(script_path) or None)

    def on_stop(self):
        if messagebox.askyesno("Stop", "Terminate the running script?"):
            self.append_output("[INFO] Stopping process...\n")
            self.process_runner.stop()
            # Controls will be re-enabled in on_process_finished

    def on_process_finished(self, return_code):
        self.append_output(f"\n[INFO] Process finished with return code: {return_code}\n")
        self.set_controls_state(running=False)

    def set_controls_state(self, running: bool):
        state = "disabled" if running else "normal"
        # disable inputs while running
        self.run_btn['state'] = "disabled" if running else "normal"
        self.stop_btn['state'] = "normal" if running else "disabled"

    def append_output(self, text):
        # From ProcessRunner threads, append into a queue to be flushed in the main thread
        self.output_queue.put(text)

    def _flush_output_queue(self):
        try:
            while True:
                line = self.output_queue.get_nowait()
                self.output_text.configure(state="normal")
                self.output_text.insert("end", line + ("\n" if not line.endswith("\n") else ""))
                self.output_text.see("end")
                self.output_text.configure(state="disabled")
        except queue.Empty:
            pass
        self.after(100, self._flush_output_queue)

    def clear_output(self):
        self.output_text.configure(state="normal")
        self.output_text.delete("1.0", "end")
        self.output_text.configure(state="disabled")


if __name__ == "__main__":
    app = NewsRunnerGUI()
    app.mainloop()