from prefect import flow, task
import subprocess, sys

def run_py(path):
    r = subprocess.run([sys.executable, path], check=True)
    return r.returncode

@task
def ingest(): return run_py("pipelines/ingest.py")

@task
def clean(): return run_py("pipelines/clean.py")

@task
def sentiment(): return run_py("pipelines/sentiment.py")

@task
def aggregate(): return run_py("pipelines/aggregate.py")

@flow(name="ev-pipeline")
def ev_pipeline():
    ingest()
    clean()
    sentiment()
    aggregate()

if __name__ == "__main__":
    ev_pipeline()
