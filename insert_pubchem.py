from zipfile import ZipFile 
import gzip 
import os 
 
class directory_crawler: 
    def __init__(self, init_path: str): 
        self._root_directory = init_path 
         
    def set_file_paths(self, return_flag:bool=False) -> list: 
        self._file_list = [] 
        # crawling through directory and subdirectories 
        for root, directories, files in os.walk(self._root_directory): 
            for filename in files: 
                filepath = os.path.join(root, filename)
                self._file_list.append(filepath) 
        
        if return_flag:
            return self._file_list

    def generate_file_paths(self) -> object: 
        # crawling through directory and subdirectories 
        for root, directories, files in os.walk(self._root_directory): 
            for filename in files: 
                filepath = os.path.join(root, filename)
                yield filepath

class file_loader(directory_crawler):
    file_name_list = []
    current_file_name = None
    file_cursor = -1
    current_contents = None
    contents_cursor = -1

    def __init__(self, init_path, file_format=".sdf"):
        #Parameter
        self._root_directory = init_path
        self._file_format = file_format
        #Init Function
        self.filter_file_format()

    def filter_file_format(self) -> None:
        full_file_list = self.set_file_paths(return_flag = True)
        # Generate Decorator for each file extension
        for file_name in full_file_list:
            if (self.is_file_format(file_name, self._file_format)):
                self.file_name_list.append(file_name)
            # Compression file
            if (self.is_file_format(file_name, self._file_format + ".gz")):
                self.file_name_list.append(file_name)
            # Archive file
            if (self.is_file_format(file_name, self._file_format + ".zip")):
                self.file_name_list.append(file_name)

    def is_file_format(self, file_name: str, file_format: str) -> bool:
        # TODO: Change to uppder case automatically
        # TODO: Create decoratro class for further wide implementation
        return True if file_name.upper().endswith(file_format.upper()) else False
    
    def get_next_file_cursor(self) -> object:
        self.file_cursor += 1
        self.load_sdf(self.file_name_list[self.file_cursor])
    
    def load_sdf(self, file_name: str) -> None:
        if (self.is_file_format(file_name, ".gz")):
            self.current_file_name = file_name
            self.current_contents = gzip.open(file_name, 'rb')
        else:
            self.current_file_name = file_name
            self.current_contents = open(file_name, "r")

    def get_sdf_item(self, num_of_mol:int = 1, decode_type:str = 'utf-8') -> str:
        contents_buffer = ""
        temp_read_line = None
        if self.current_contents is None:
            raise TypeError("SDF file is not loaded")
        
        while(self.current_file_name != None):
            byte_read_line = self.current_contents.readline()
            str_read_line = byte_read_line.decode(decode_type)
            self.contents_cursor += 1

            if not str_read_line:
                pass
            else:
                contents_buffer += str_read_line
            
            # After Last Line, Check Return Value ""
            # TODO: change this logic to check the number of line compare to full length of file.
            # if not str_read_line:
            #     break

            # For each molecular, mol file format return "$$$$"
            if str_read_line.startswith("$"):
                num_of_mol -= 1
            if num_of_mol == 0:
                break

        return contents_buffer


# class insert_sdf_db:
#     def __init__(self, directory, target_file_format):
#         self._root_directory = directory
#         # initializing empty file paths list 
#         self._file_paths = []
#         self._target_file_format = target_file_format
#     def list_files(self):
#         for file_name in self._file_paths: 
#             file_contents = get_compressed_contents(file_name)
#             break
#
#     def get_compressed_contents(self, file_path):
#         file_content = []
#         # If gzip file .gz
#         if file_path.endswith('.gz'):
#             with gzip.open(file_path, 'rb') as f:
#                 file_content = f.read()
#         print(file_content)
#         return file_content
    
def test_code(): 
    sdf_full_path = '/mnt/d/DATABASES/PubChem/Compound/FULL/' 
    # calling function to get all file paths in the directory
    iidb_obj = file_loader(sdf_full_path, '.sdf')
    # printing the list of all files to be zipped 
    iidb_obj.get_next_file_cursor()
    one_mol_content = iidb_obj.get_sdf_item()
    # print(iidb_obj.get_all_file_paths())
    print("MOL 1")
    print(one_mol_content)


if __name__ == "__main__": 
    test_code() 
