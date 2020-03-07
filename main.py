import os
import datetime

from index_code import IndexWriter, IndexReader

if __name__ == '__main__':

    # here you have to fill the location of the raw data file (only path, no file name)
    # e.g. RAW_DATA_DIR = 'c:\raw_data\\'
    RAW_DATA_DIR = ''

    iw = IndexWriter.IndexWriter()

    # to work with big file, you can download zipped 1M review from
    # https://drive.google.com/file/d/1fDpwPpMOzaikDln1espcynDvgBjb3s6t/view?usp=sharing
    raw_files = ['data_files/Books100.txt']
    # raw_files = ['data_files/Books100.txt', 'data_files/Books1000000.txt']

    for raw_file in raw_files:
        print('\n***** ' + raw_file + ' *****\n')
        print('build duration: ')

        # create the index
        INDEX_DIR = os.getcwd() + '\index\\'
        time_stamp = datetime.datetime.now()
        iw.write(RAW_DATA_DIR + raw_file, INDEX_DIR)
        print(str(datetime.datetime.now() - time_stamp) + '\n')

        # calculate index size
        total_size = 0
        for dirpath, dirnames, filenames in os.walk(INDEX_DIR):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                # skip if it is symbolic link
                if not os.path.islink(fp):
                    total_size += os.path.getsize(fp)
        print('index size: ')
        print(str(total_size) + '\n')

        # query the index via IndexReader
        ir = IndexReader.IndexReader(INDEX_DIR)
        print("getProductId of 1: " + str(ir.getProductId(1)))
        print("getReviewScore of 1: " + str(ir.getReviewScore(1)))
        print("getReviewHelpfulnessNumerator of 1: " + str(ir.getReviewHelpfulnessNumerator(1)))
        print("getReviewHelpfulnessDenominator of 1: " + str(ir.getReviewHelpfulnessDenominator(1)))
        print("getReviewLength of 1: " + str(ir.getReviewLength(1)))
        print("getTokenFrequency of 'better': " + str(ir.getTokenFrequency("better")))
        print("getTokenCollectionFrequency of 'better': " + str(ir.getTokenCollectionFrequency("better")))
        print("getReviewsWithTokenof 'food': " + str(ir.getReviewsWithToken("food")))
        print("getNumberOfReviews: " + str(ir.getNumberOfReviews()))
        print("getTokenSizeOfReviews: " + str(ir.getTokenSizeOfReviews()))
        print("getProductReviews(\"B006K2ZZ7K\"): " + str(ir.getProductReviews("B006K2ZZ7K")))
        print("getReviewScore of 1000 (no review with the given identifier): " + str(ir.getReviewScore(1000)))

    # delete index files and directory
#    iw.removeIndex(INDEX_DIR)
