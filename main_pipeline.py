import subprocess
import sys
import os

def run_module(module_name):
    result = subprocess.run([sys.executable, "-m", f"src.{module_name}"])
    if result.returncode != 0:
        print(f"Error running {module_name}")
        sys.exit(result.returncode)

if __name__ == "__main__":
    run_module("run_scraper")
    run_module("reviews_normalization")
    run_module("reviews_insight_pipeline")
