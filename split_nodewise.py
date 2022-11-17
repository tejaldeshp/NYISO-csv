import os
def split_prices(input_path, output_path):
    """
    Method to split the prices by node
    using the awk command
    """
    files = os.listdir(input_path)
    files.sort()
    for file in files:
        if file.endswith(".csv"):
            _input_path = os.path.join(input_path,file)
            command = (
                "awk -F, "
                + "'{fname="
                + f'"{output_path}'
                + '"$2".csv";'
                + " print >> fname;}' "
                + f"{_input_path}"
            )
            print("Running command: ", command)
            os.system(command)
            print("Done\n")

split_prices("/home/tejaldeshpande/Desktop/DjangoProjects/NYISOfolder/sample","/home/tejaldeshpande/Desktop/DjangoProjects/NYISOfolder/nodewise_sample/")