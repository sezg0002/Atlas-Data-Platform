
import subprocess
import os

def run_dbt_models() -> None:
    project_dir = os.path.dirname(os.path.abspath(__file__))
    subprocess.run(["dbt", "run", "--project-dir", project_dir], check=True)

if __name__ == "__main__":
    run_dbt_models()
