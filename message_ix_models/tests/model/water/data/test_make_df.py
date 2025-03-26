
from rust_eval_field import make_df as rust_make_df
from message_ix import make_df as python_make_df

import time

#Speed test
common_args = {
    "technology": "seawater_cooling", 
    "value": 1, 
    "unit": "GWa", 
    "level": "level", 
    "commodity": "electricity", 

}

#compare the output of the two functions
rust_start_time = time.time()
for i in range(1000):
    rust_output = rust_make_df("output", **common_args)
rust_time = time.time() - rust_start_time
rust_time = rust_time / 1000
print(f"Rust time: {rust_time}")

python_start_time = time.time()    
for i in range(1000):
    python_output = python_make_df("output", **common_args)
python_time = time.time() - python_start_time
python_time = python_time / 1000
print(f"Python time: {python_time}")





