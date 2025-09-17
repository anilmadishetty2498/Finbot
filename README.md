# FinBot

## 📖 Overview
FinBot is a chatbot application designed to assist users with financial queries. This application is built using Python and leverages Streamlit for the user interface.

## 🚀 Getting Started
Follow the steps below to set up and run the application locally.

### 1. Clone the Repository

```bash
# Clone the repository
git clone https://github.com/Maersk-Global/FinBot.git
cd FinBot
```

### 2. Create a Virtual Environment (Optional but Recommended)

Creating a virtual environment helps to isolate dependencies and avoid conflicts.

```bash
# Create a virtual environment
python -m venv .venv

# Activate the virtual environment
# On Windows:
.venv\Scripts\activate
# On macOS/Linux:
source ./venv/bin/activate
```

### 3. Install Dependencies

Install the required Python packages listed in the `requirements.txt` file.

```bash
pip install -r requirements.txt
```

### 4. Run the Application

Start the application using Streamlit.

```bash
streamlit run app.py
```

### 5. Access the Application

Once the application starts, it will provide a local URL (e.g., `http://localhost:8501`) that you can open in your web browser to interact with the app.

### 6. Access via Terminal

If you prefer to interact with the application through the terminal, you can run the following command:

```bash
python cli.py
```

This will display the output directly in the terminal.

## 📂 MockupAPI

The `mockupapi` folder is a separate module dedicated to API creation and testing. It contains its own  [Readme.md](./mockupapi/README.md) file with detailed instructions specific to this module.

