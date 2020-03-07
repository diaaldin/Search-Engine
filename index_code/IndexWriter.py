import shutil

from index_code.helping_methods import create_processes_to_process_data, create_processes_to_sort_data, \
    init_input_and_dist_files, DEBUG_MODE
import datetime


class IndexWriter:
    def write(self, inputFile, dir):
        """
            Given product review data, creates an on disk index inputFile is the path to the file containing the
            review data dir is the directory in which all index files will be created if the directory does not exist,
            it should be created
        """
        init_input_and_dist_files(inputFile, dir)
        x0 = datetime.datetime.now()
        create_processes_to_process_data()
        create_processes_to_sort_data()
        x1 = datetime.datetime.now()
        if DEBUG_MODE:
            print("total time consuming", x1 - x0)
            print("\nIf you wish to remove these debugging messages please go to helping_method model and change "
                  "DEBUG_MODE at line 8 to 0\n")

    def removeIndex(self, m_dir):
        """
            Delete all index files by removing the given directory
        """
        shutil.rmtree(m_dir)
