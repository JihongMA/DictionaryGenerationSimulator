import numpy
import random

# Produce the input files with only on column
def make_file(words, worker_num, num_words_per_file, zipf_arg, mode):
    if mode == "zipf":
        a = zipf_arg # zipf argument
        files = []
        offset = random.randint(0,len(words))
        for w in range(worker_num):
            file_name = "words" + str(w)
            files.append(file_name)
            zipf = numpy.random.zipf(a,num_words_per_file)
            with open(file_name, "w+") as f:
                for i in zipf:
                    f.write(words[(i+offset) % len(words)] + "\n")
        return files

    # the list of words to select for files
    use_words = words[:int(len(words))]

    # add skewed distribution to use_words
    if mode == "weighted":
        tmp = use_words[-1]
        use_words += (len(use_words)-2) * [tmp]
    elif mode == "uniform":
        pass
    else:
        raise Exception("Unknown mode %s" % mode)

    # for weighted and uniform
    files = []
    for w in range(worker_num):
        file_name = "words" + str(w)
        files.append(file_name)
        with open(file_name, "w+") as f:
            for i in range(num_words_per_file):
                f.write(random.choice(use_words) + "\n")
    return files


# Produce the input table files and pre produce part of dictionary
def make_sample_table(words, worker_num, num_words_per_file):
    a = 2.0 # zipf argument
    files = []
    offset = random.randint(0,len(words))
    use_words = words[:int(len(words))]
    for w in range(worker_num):
        file_name = "words" + str(w)
        files.append(file_name)
        zipf = numpy.random.zipf(a,num_words_per_file)
        with open(file_name, "w+") as f:
            ct = 0
            for i in zipf:
                ct += 1
                f.write(str(ct) + ',' +str(i) + ',' + random_str_fix_length() + ',' + format(random.uniform(-1000000, 1000000), 'f') + ',' + random_str() + ',' + words[(i+offset) % len(words)] + ',' + random.choice(use_words) + "\n")
    return files

# Generate fix width random string
def random_str_fix_length(randomlength=10):
    str = ''
    chars = 'AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz0123456789'
    for i in range(randomlength):
        str+=random.choice(chars)
    return str

# Generate variable width random string
def random_str():
    str = ''
    chars = 'AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz0123456789'
    for i in range(random.randint(1,20)):
        str+=random.choice(chars)
    return str

if __name__ == "__main__":
    with open('/usr/share/dict/words', "r") as f:
        words = f.read().split("\n")
    make_sample_table(words, 10, 100)
