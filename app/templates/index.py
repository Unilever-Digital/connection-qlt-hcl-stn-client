import tkinter as tk
from tkinter import messagebox
from PIL import Image, ImageTk

class HomeApp(tk.Tk):
    def __init__(self):
        super().__init__()

        # Set window title and size
        self.title("Quality server")
        self.geometry("600x400+200+200")  # Adjust window size as needed

        # Initialize UI elements
        self.init_ui()

    def init_ui(self):
        # Load and display the PNG logo
        self.load_png_logo("app/static/images/logo.png")

        # Create the main title label
        self.label_background = tk.Label(self, text="Quality server")
        self.label_background.grid(
            row=1, column=0, columnspan=2, sticky="nsew")

        # Create the privacy label
        self.label_privacy = tk.Label(self, text="Privacy @2023", fg="black")
        self.label_privacy.grid(row=2, column=1, sticky="se", padx=10, pady=10)

        # Configure grid to ensure proper resizing behavior
        self.grid_rowconfigure(0, weight=1)
        self.grid_rowconfigure(1, weight=1)
        self.grid_rowconfigure(2, weight=0)
        self.grid_columnconfigure(0, weight=1)
        self.grid_columnconfigure(1, weight=1)

    def load_png_logo(self, png_path):
        # Open the PNG image with PIL
        image = Image.open(png_path)

        # Resize the image as needed (optional)
        logo_width, logo_height = 150, 150  # Adjust dimensions as needed
        image = image.resize((logo_width, logo_height),
                             resample = Image.ADAPTIVE)

        # Convert to Tkinter image
        self.logo_image = ImageTk.PhotoImage(image)

        # Create a label to display the logo
        self.label_logo = tk.Label(self, image=self.logo_image)
        self.label_logo.grid(row=0, column=0, columnspan=2, pady=20)

        # Center the label
        self.label_logo.grid(row=0, column=0, sticky="nsew")
        self.grid_rowconfigure(0, weight=1)
        self.grid_columnconfigure(0, weight=1)

    def close(self, event=None):
        # Confirmation dialog before closing
        if messagebox.askquestion("Quit?", "Are you sure you want to quit?") == "yes":
            super().destroy()  # Close the window
