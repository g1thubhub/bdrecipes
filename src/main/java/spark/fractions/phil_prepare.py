file_1_gb_bytes = 1000000000


def generate_file(file_size, file_name):
    mb_in_file = 100
    bytes_in_file = 1000 * 1000 * mb_in_file
    with open(file_name, 'w') as generated_file:
        file_rounds = (file_size / bytes_in_file) * 4
        index = 0
        for number in range(0, int(file_rounds)):
            generated_file.write(str(index) * int(bytes_in_file / 2))
            generated_file.write('\n')
            if index == 9:
                index = 0
            else:
                index += 1


print('Generating 2GB file')
generate_file(file_1_gb_bytes, 'generated_phil_file_2_gb.txt')


def generate_file(file_size, file_name):
    mb_in_file = 100
    bytes_in_file = 1000*1000*mb_in_file
    with open(file_name, 'w') as generated_file:
        file_rounds = file_size/bytes_in_file
        print('file_rounds: ' + str(file_rounds))
        print('bytes_in_file: ' + str(bytes_in_file))
        index = 0
        for number in range(0, int(file_rounds)):
            generated_file.write(str(index) * int(bytes_in_file * 0.6))  # 0.7 crashes App
            generated_file.write('\n')
            index += 1


print('Generating max file')
generate_file(file_1_gb_bytes, 'generated_file_max_gb.txt')
