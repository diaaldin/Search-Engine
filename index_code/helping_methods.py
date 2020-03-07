import re
import multiprocessing as mp, os
import csv
from collections import defaultdict, Counter
import datetime
import json
import psutil

DEBUG_MODE = 1

cores = mp.cpu_count()
size_of_chunk_in_MB = int(psutil.virtual_memory().free / 1024 / 1024 / (cores * 6))

# size_of_chunk_in_MB = 128

input_file = "1000.txt"
f_name = "huge_index_not_sorted.txt"
dir_name = "index"

splllit = "product/productId"


def start_and_end_with_review(data, specifier, at_end):
    """
    this function makes sure that the chuck data starts and ends with a review
    :param data: the data was read
    :param specifier: delimiter that separates data
    :param at_end: if it was 1 then we have reached the end of the file and no need to splice the data
    :return: the data ready to be process, and the index that we need to return to in the file
    """

    # print(data)
    starting_ending_list = list()
    for match in re.finditer(specifier, data):
        starting_ending_list.append((match.start(), match.end()))
        # print(match.start(), match.end())
    # if len(starting_ending_list) == 0:
    #     raise ValueError({"msg": "there is no review to read"})
    last = starting_ending_list[-1]
    # print(data[:last[0]])

    if at_end == 1:
        data1 = data[starting_ending_list[0][0]:]
    else:
        data1 = data[starting_ending_list[0][0]:last[0]]

    if at_end == 0 and len(data) <= 10:
        print(data)
        print(data1)
        msg = f"chunk size is not enough to load a certain data"
        raise ValueError({"msg": msg})
    return data1, last[0]


def chunkify(f_name, chunk=1024 * 1024 * size_of_chunk_in_MB):
    """ this function split the data into chunks and return the separated data
    :param f_name: the file name to read from
    :param chunk: the chunk size
    :return: return a generator to the data
    """
    file_end = os.path.getsize(f_name)
    m_end = 0
    back = 0

    with open(f_name, 'r') as f:
        while True:
            if m_end:
                break
            data = f.read(chunk)
            m_len = len(data)
            if back + m_len + 1 >= file_end:
                m_end = 1

            data, get_back = start_and_end_with_review(data, splllit, m_end)

            back += get_back

            f.seek(back)

            yield data


def test_chunkify():
    # create jobs
    counter = 0
    for x in chunkify(input_file):
        counter += 1
        print(x, counter)


numerator = "numerator"
denominator = "denominator"
rev_id = 'review_id'


def convert_reviews_text_to_reviews_list(string_data, base_split):
    """
    this function takes a text data
    and returns a list of strings, each string has a text between
    two numbers which has two connected words that starts with capital letter
    the function split them with coma

    :return: list of strings separated with comas and add a coma to the begin
    """
    text_to_list = list()
    min_len = 0
    for item in string_data.split(base_split):
        item = item.strip()
        if len(item) > min_len:
            text_to_list.append(base_split + item)

    return text_to_list


def test_from_text_to_list():
    counter = 0
    for data in chunkify(input_file):
        counter += 1
        # print(convert_reviews_text_to_reviews_list(data, splllit))
        print(len(convert_reviews_text_to_reviews_list(data, splllit)))
        print("\n\n")


# test_from_text_to_list()


def findStringBetween(value, a, b):
    # Find and validate before-part.
    pos_a = value.find(a)

    if pos_a == -1:
        return ""
    # Find and validate after part.
    sub_value = value[pos_a + len(a):]
    pos_b = sub_value.find(b)

    if pos_b == -1:
        if value.find(b) == 1:
            return ""
        pos_b = len(sub_value)
    # Return middle part.
    adjusted_pos_a = pos_a + len(a)
    return value[adjusted_pos_a:pos_a + pos_b + len(a)].strip()


def helpfulnessAndScoreValidator(review_obj, attr_filter):
    for filter in attr_filter:
        if "helpfulness" in filter:
            helpfulness_list = list()
            try:
                helpfulness_list.append(review_obj[denominator])
                helpfulness_list.append(review_obj[numerator])
                for number in helpfulness_list:
                    int(number)
            except (ValueError, KeyError):
                return False

        if "score" in filter:
            try:
                score = int(float(review_obj["score"]))
                if score < 1 or score > 5:
                    return False
            except ValueError:
                return False

    return True


att_filter = ["product/productId:", "review/helpfulness:", "review/score:", "review/text:"]
delimiter = '/'


def process_data(data_array, attr_filter, delimiter):
    """
    :param data_array:
    :param attr_filter:
    :param delimiter: the char that separate the words such as '/' in product/productId
    :return: list of dictionaries, in each one there is a review
    """
    global counter_id
    # += operation is not atomic, so we need to get a lock:

    # global counter_id
    filtered_data = dict()
    all_filtered_list = list()
    counter_rev = 0
    reversed_index_file = defaultdict(lambda: Counter())
    for data in data_array:
        # print(data, "\n\n\n\n\n")
        with counter_id.get_lock():
            # print(counter_id.value)
            counter_rev = counter_id.value
            counter_id.value += 1

        filtered_data[rev_id] = counter_rev
        # Write to stdout or logfile, etc.

        for filter in attr_filter:
            if "helpfulness" in filter:

                helpfulness_data = findStringBetween(data, filter, "\n").split(delimiter)
                if helpfulness_data[0] == '':
                    # print(helpfulness_data, "\n\nxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", data, "\n",
                    #       counter_rev)
                    break
                    # break
                filtered_data[numerator] = int(helpfulness_data[0])
                filtered_data[denominator] = int(helpfulness_data[1])
                # except (ValueError, IndexError):
                #     break
            else:
                m_data = findStringBetween(data, filter, "\n")
                filter = filter.split(delimiter)[1][0:-1]
                filtered_data[filter] = m_data
            if "text" in filter:
                filtered_data[filter] = filtered_data[filter].lower()
                # TODO add number
                text_list = [word for word in re.split(r"[^a-zA-Z0-9]", filtered_data[filter]) if
                             len(word) > 0 and not word[0].isdigit()]
                for word in text_list:
                    reversed_index_file[word][counter_rev] += 1
                filtered_data[filter] = len(filtered_data[filter].split())

        valid = helpfulnessAndScoreValidator(filtered_data, attr_filter)

        if valid:
            filtered_data["score"] = int(float(filtered_data["score"]))
            all_filtered_list.append(filtered_data)
        # else:
        #     print(data, "XXXXXXXXXXXXXXXXXXXXXXXx\n\n")
        filtered_data = {}

    return all_filtered_list, reversed_index_file


def writeReversedIndexFile(reversed_index):
    global counter_id
    global f_name

    list_of_words = list()

    mmm = []
    for word, m_dict in reversed_index.items():
        data_to_add = list()
        data_to_add.append(word)
        for review_id, count in m_dict.items():
            data_to_add.append(review_id)
            data_to_add.append(count)
        mmm.append(data_to_add)
    with counter_id.get_lock():
        with open(f_name, 'a+', newline='') as file_handle:
            w = csv.writer(file_handle)

            w.writerows(mmm)
    return list_of_words


def write_data(dic_list, dest_file):
    global counter_id
    if not os.path.exists(f"./{dir_name}"):
        os.makedirs(f"./{dir_name}")

    with open(dest_file, 'a+', encoding='utf-8', newline='') as output_file:
        with counter_id.get_lock():
            if len(dic_list) > 0:
                w = csv.DictWriter(output_file, dic_list[0].keys())
                if os.path.getsize(dest_file) == 0:
                    w.writeheader()
                for m_dict in dic_list:
                    w.writerow(m_dict)
            # for dic in dic_list:
            #     # print(dic[rev_id])
            #
            #     json.dump(dic, output_file)
            #     output_file.write("\n")


def start_processing_data(data):
    global DEBUG_MODE
    global m_count
    global dir_name

    with m_count.get_lock():
        m_count.value += 1
    x0 = datetime.datetime.now()
    listed_data = convert_reviews_text_to_reviews_list(data, splllit)
    x1 = datetime.datetime.now()
    x_len = 0
    if DEBUG_MODE:
        x_len = len(listed_data)
        print(f"time to split {x_len} reviews to list,  process {mp.current_process().name}", x1 - x0)

    # pprint(process_data(listed_data, att_filter, delimiter))
    all_data, reversed_index = process_data(listed_data, att_filter, delimiter)
    x2 = datetime.datetime.now()
    if DEBUG_MODE:
        print(f"time to process {x_len} reviews", x2 - x1)

    x0 = datetime.datetime.now()
    writeReversedIndexFile(reversed_index)
    x1 = datetime.datetime.now()
    if DEBUG_MODE:
        print(f"time to write to index {x1 - x0}")
    # customReversWriter(reversed_index)
    write_data(all_data, f"./{dir_name}/analyzed.txt")
    if DEBUG_MODE:
        x3 = datetime.datetime.now()
        print(f"time to write {x_len} reviews", x3 - x1)
    return


counter_id = None
m_count = None


def init(counter, count):
    global counter_id
    global m_count
    m_count = count
    counter_id = counter


g_alpha = None


def init0(first_alpha):
    global g_alpha
    g_alpha = first_alpha


def readWordsStartsWithAlpha(m_alpha, f_name):
    global dir_name
    if DEBUG_MODE:
        print(f"process {mp.current_process().name} is sorting all words that starts with {m_alpha}")

    # print(chr(m_alpha))
    m_all = []
    with open(f_name, "r") as f:
        for line in f:
            if line[0] == m_alpha:
                x = line.strip().split(',')
                m_all.append(x)
    if not os.path.exists(f"./{dir_name}/{m_alpha}"):
        os.makedirs(f"./{dir_name}/{m_alpha}")
    with open(f"./{dir_name}/{m_alpha}/sub_index.txt", "w+", newline='') as f_index:
        w = csv.writer(f_index)
        w.writerows(sorted(m_all))


def readWordsStartsWithNumber(f_name):
    global dir_name
    if DEBUG_MODE:
        print(f"process {mp.current_process().name} is sorting all words that starts with numbers")
    m_alpha = "numbers"
    m_all = []
    with open(f_name, "r") as f:
        for line in f:
            if line[0].isdigit():
                x = line.strip().split(',')
                m_all.append(x)
    if not os.path.exists(f"{dir_name}/{m_alpha}"):
        os.makedirs(f"{dir_name}/{m_alpha}")
    with open(f"{dir_name}\\{m_alpha}\\sub_index.txt", "w+", newline='') as f_index:
        w = csv.writer(f_index)
        w.writerows(sorted(m_all))


USING_MP_LIB = 1


def get_alpha():
    global g_alpha
    with g_alpha.get_lock():
        while g_alpha.value <= ord('z'):
            m_alpha = chr(g_alpha.value)
            g_alpha.value += 1
            yield m_alpha


def create_processes_to_process_data():
    global counter_id
    global m_count
    counter_id = mp.Value('i', 1)
    m_count = mp.Value('i', 0)

    x0 = datetime.datetime.now()
    chunk_count = 0
    if USING_MP_LIB:
        pool = mp.Pool(cores, initializer=init, initargs=(counter_id, m_count))
        jobs = []
        for data in chunkify(input_file):
            chunk_count += 1
            while chunk_count - m_count.value > int(cores / 2):
                x = 1
            jobs.append(pool.apply_async(start_processing_data, (data,)))

        x2 = datetime.datetime.now()
        if DEBUG_MODE:
            print("time to create taskes", x2 - x0)
        # wait for all jobs to finish
        for job in jobs:
            job.get()

        # clean up
        pool.close()

        x3 = datetime.datetime.now()
        if DEBUG_MODE:
            print("time to finish the job", x3 - x2)


def create_processes_to_sort_data():
    global g_alpha
    global f_name
    g_alpha = mp.Value('i', ord('a'))

    pool = mp.Pool(cores, initializer=init0, initargs=(g_alpha,))
    jobs = []
    x1 = datetime.datetime.now()

    for m_alpha in get_alpha():
        jobs.append(pool.apply_async(readWordsStartsWithAlpha, (m_alpha, f_name)))
    readWordsStartsWithNumber(f_name)
    x2 = datetime.datetime.now()
    if DEBUG_MODE:
        print("time to create taskes to sort", x2 - x1)
    # wait for all jobs to finish
    for job in jobs:
        job.get()
    x6 = datetime.datetime.now()
    if DEBUG_MODE:
        print("time to finish the sort", x6 - x2)

    os.remove(f_name)
    # clean up
    pool.close()


def init_input_and_dist_files(m_input_file, m_dir):
    global dir_name
    global input_file
    input_file = m_input_file
    dir_name = m_dir
