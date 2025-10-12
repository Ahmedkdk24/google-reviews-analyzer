import subprocess
import sys

def run_script(script_name):
    result = subprocess.run([sys.executable, script_name], cwd="src")
    if result.returncode != 0:
        print(f"Error running {script_name}")
        sys.exit(result.returncode)

if __name__ == "__main__":
    run_script("run_scraper.py")
    run_script("reviews_normalization.py")
    run_script("reviews_insight_pipeline.py")