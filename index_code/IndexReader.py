from collections import defaultdict, Counter, OrderedDict
import os

reversed_format = OrderedDict({'Q': 8, 'I': 4, 'H': 2, 'B': 1})


def binary_search_on_string(what_to_find, path_of_file_to_search, placed):
    left = 0
    right = os.path.getsize(path_of_file_to_search)
    with open(path_of_file_to_search, "r") as m_file:
        while left <= right:
            mid = left + (right - left) / 2
            m_file.seek(mid)
            # seek wont be at the start of the line, so we read a line first to make
            # sure we are in the begin of the line
            m_file.readline()
            line = m_file.readline().strip().split(",")

            if line[0] == '':
                break

            if line[placed] == str(what_to_find):
                return line
            # Check if what_to_find is present at mid

            # If what_to_find is greater, ignore left half
            elif line[placed] < str(what_to_find):
                left = mid + 1
            # If what_to_find is smaller, ignore right half
            else:
                right = mid - 1

        return None


def binary_search_on_int(what_to_find, path_of_file_to_search, placed):
    left = 0
    right = os.path.getsize(path_of_file_to_search)
    with open(path_of_file_to_search, "r") as m_file:
        while left <= right:
            mid = left + (right - left) / 2
            m_file.seek(mid)
            # seek wont be at the start of the line, so we read a line first to make
            # sure we are in the begin of the line
            m_file.readline()
            line = m_file.readline().strip().split(",")
            if line[0] == '':
                break

            if int(line[placed]) == what_to_find:
                return line
            # Check if what_to_find is present at mid

            # If what_to_find is greater, ignore left half
            elif int(line[placed]) < what_to_find:
                left = mid + 1
            # If what_to_find is smaller, ignore right half
            else:
                right = mid - 1

        return None


class IndexReader:
    def __init__(self, directory):
        self.index_dir = ""
        self.analyzer_path = ""
        self.IndexReader(directory)

    def IndexReader(self, directory):
        """Creates an IndexReader which will read from the given directory"""
        self.index_dir = directory
        self.analyzer_path = f"{directory}analyzed.txt"

    @staticmethod
    def get_index_of_product_id():
        return 1

    def getProductId(self, review_id):
        """
            Returns the product identifier for the given review Returns null
            if there is no review with the given identifier
        """

        answer = binary_search_on_int(review_id, self.analyzer_path, 0)
        if not answer:
            return answer
        return answer[self.get_index_of_product_id()]

    @staticmethod
    def get_index_of_score():
        return 4

    def getReviewScore(self, review_id):
        """
            Returns the score for a given review Returns -1 if there
            is no review with the given identifier
        """
        answer = binary_search_on_int(review_id, self.analyzer_path, 0)
        if not answer:
            return -1
        return answer[self.get_index_of_score()]

    @staticmethod
    def get_index_helpfulness_numerator():
        return 2

    def getReviewHelpfulnessNumerator(self, review_id):
        """
            Returns the numerator for the helpfulness of a given
            review Returns -1 if there is no review with the given identifier
        """
        answer = binary_search_on_int(review_id, self.analyzer_path, 0)
        if not answer:
            return -1
        return answer[self.get_index_helpfulness_numerator()]

    @staticmethod
    def get_index_helpfulness_denominator():
        return 3

    def getReviewHelpfulnessDenominator(self, review_id):
        """
            Returns the denominator for the helpfulness of a given review Returns -1 if there is no
            review with the given identifier
        """

        answer = binary_search_on_int(review_id, self.analyzer_path, 0)
        if not answer:
            return -1
        return answer[self.get_index_helpfulness_denominator()]

    @staticmethod
    def get_index_token_length():
        return 5

    def getReviewLength(self, review_id):
        """
          Returns the number of tokens in a given review Returns -1 if there is
          no review with the given identifier
        """

        answer = binary_search_on_int(review_id, self.analyzer_path, 0)

        if not answer:
            return -1
        return answer[self.get_index_token_length()]

    def getTokenFrequency(self, token):
        """
            Return the number of reviews containing a given token (i.e., word) Returns 0
            if there are no reviews containing this token
        """
        path = f"{self.index_dir}\\{token[0]}\\sub_index.txt"
        answer = binary_search_on_string(token, path, 0)
        if not answer:
            return 0

        return int((len(answer) - 1) / 2)

    def getTokenCollectionFrequency(self, token):
        """
        Return the number of times that a given token (i.e., word) appears in the reviews indexed
        Returns 0 if there are no reviews containing this token
        """
        path = f"{self.index_dir}\\{token[0]}\\sub_index.txt"
        answer = binary_search_on_string(token, path, 0)
        if not answer:
            return 0
        counter = 0
        for i in range(2, len(answer), 2):
            counter += int(answer[i])
        return counter

    def getReviewsWithToken(self, token):
        """
        Returns a series of integers of the form id1, freq-1, id-2, freq-2, ...
        such that id-n is the n-th review containing the given token and freq-n is the number of times
        that the token appears in review id-n Note that the integers should be sorted by id Returns an empty
        Tuple if there are no reviews containing this token
        """
        path = f"{self.index_dir}\\{token[0]}\\sub_index.txt"
        answer = binary_search_on_string(token, path, 0)
        if not answer:
            return ()
        sub_answer = list()
        for i in range(1, len(answer), 2):
            sub_answer.append((int(answer[i]), int(answer[i + 1])))
        return tuple(sorted(sub_answer))

    @staticmethod
    def getLastLine(f_name, max_line_length=80):

        with open(f_name, "rb") as fp:
            fp.seek(-80, 2)  # 2 means "from the end of the file"
            last_line = fp.readlines()[-1]
        return last_line.decode('ascii')

    def getNumberOfReviews(self):
        """
            Return the number of product reviews available in the system
        """

        last_review = self.getLastLine(self.analyzer_path).split(",")
        return last_review[0]

    def getTokenSizeOfReviews(self):
        """
            Return the number of tokens in the system
            (Tokens should be counted as many times as they appear)
        """
        counter = 0
        min_len = 20
        with open(self.analyzer_path, "r") as fp:
            fp.readline()
            for line in fp:
                if len(line) < min_len:
                    break
                counter += int(line.strip().split(",")[self.get_index_token_length()])

        return counter

    @staticmethod
    def get_index_of_review_id():
        return 0

    def getProductReviews(self, product_id):
        """
            Return the ids of the reviews for a given product
            identifier Note that the integers returned should be sorted
            by id Returns an empty Tuple if there are no reviews for this product
        """
        answer = list()
        min_len = 20
        with open(self.analyzer_path, "r") as fp:
            fp.readline()
            for review in fp:
                if len(review) < min_len:
                    break
                review_as_list = review.strip().split(",")
                if review_as_list[self.get_index_of_product_id()] == product_id:
                    answer.append(int(review_as_list[self.get_index_of_review_id()]))
        return tuple(sorted(answer))

