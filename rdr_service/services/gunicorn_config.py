import multiprocessing

bind = "0.0.0.0:8081"
workers = 1
threads = 1

# workers = multiprocessing.cpu_count() * 2 + 1
# threads = multiprocessing.cpu_count() * 2 + 1