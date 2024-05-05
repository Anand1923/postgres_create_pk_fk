from fastapi import FastAPI
from routes import router
import subprocess

app = FastAPI()

app.include_router(router)


def install_dependencies():
    try:
        subprocess.check_call(['pip3', 'install', '-r', 'requirements.txt'])
        print("Dependencies installed successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error installing dependencies: {e}")

if __name__ == "__main__":

    import uvicorn
    install_dependencies()

    uvicorn.run(app, host="0.0.0.0", port=8000)
