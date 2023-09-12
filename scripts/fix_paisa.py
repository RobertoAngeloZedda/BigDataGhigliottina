'''
    Removes useless lines
'''

file_input = open('/home/ububu/Ghigliottina/Paisa/paisa.raw.utf8')
file_output = open('/home/ububu/BigData/paisa_fixed.txt', 'w')

for index, line in enumerate(file_input):
    if line[0] != '#' and line[0] != '<' and line != '\n':
        file_output.write(line)
    
    # To track progress
    if index % 1000000 == 0:
        print(index)
    
    # To stop early
    #if index == 100:
        #break

file_input.close()
file_output.close()
