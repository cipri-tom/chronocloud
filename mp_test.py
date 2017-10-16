import multiprocessing as mp
import time
import os

def wc(word, count=1):
    time.sleep(len(word))
    if word.startswith('p'):
        return 1
    return word

# this belongs to global thread
s = 0
def logger(word):
    global s
    if type(word) is str:
        print(word)
    else:
        s += word
    # logger.q.put(word)

def logger_init(q):
    # print('Initting...', id(logger), os.getpid(), os.getppid())
    logger.q = q

# this goes on a separate process
def log_listener(q):
    start_time = time.time()
    s = 0
    while True:
        try:
            time.sleep(3)      # our messages are not generated faster than this
            record = q.get()
            if record is None: # use None as sentinel
                print(s)
                break
            if type(record) is str:
                print(record, int(time.time() - start_time), flush=True)
            else:
                s += record

        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            import sys, traceback
            print >> sys.stderr, 'Whoops! Problem:'
            traceback.print_exc(file=sys.stderr)


def aggregate(lang, n):
    # setup logger
    # Q = mp.Queue(-1)
    # listener = mp.Process(target=log_listener, args=[Q])
    # listener.start()

    pool = mp.Pool(2)
    results = []
    for k in "ala bala porto cala iesi mariuca la port ita ca o fetița".split(' '):
        if k.startswith('c'):
            continue

        res = pool.apply_async(wc, (k,), callback=logger)
        results.append(res)

    pool.close()
    # s = 0
    # for res in results:
    #     word = res.get()
    #     if type(word) is str:
    #         print(word, flush=True)
    #     else:
    #         s += word
    pool.join()

    global s
    print(s)


if __name__ == "__main__":
    aggregate('fre', 2)
