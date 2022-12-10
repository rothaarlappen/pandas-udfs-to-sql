import sys
import time 
from os import path,system
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
from converter import convert
from contextlib import redirect_stdout
from io import StringIO
import subprocess

pipeline_directory = path.join(path.dirname(path.dirname(path.abspath(__file__))), "testqueries" )

pipelines = ["simple_pipeline.py"]

def time_pipeline_execution(converted_pipeline : str):
    start = time.time()
    f = StringIO()
    with redirect_stdout(f):
        for i in range(5):
            subprocess.run("python " + converted_pipeline, stdout=subprocess.DEVNULL)
    end = time.time() 
    time_lapsed = end - start

    print(path.basename(converted_pipeline)," : " , time_lapsed) 


def main():
    for pipeline in pipelines: 
        pipeline = path.join(pipeline_directory, pipeline)
        converted_pipeline = convert.convert_pipeline(pipeline)
        
        time_pipeline_execution(pipeline)
        time_pipeline_execution(converted_pipeline)
        
if __name__ == '__main__':
    main()
    
    






