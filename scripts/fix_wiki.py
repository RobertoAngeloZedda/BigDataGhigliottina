'''
    Removes useless lines
    Divides the wiki dump in two corpora: 'Wiki' 'Wiki_titles'
'''

file_input = open('/home/ububu/Ghigliottina/Wiki/wiki_corpus')
file_output1 = open('/home/ububu/BigData/wiki_fixed.txt', 'w')
file_output2 = open('/home/ububu/BigData/wiki_titles.txt', 'w')

isTitle = False
for index, line in enumerate(file_input):
    # Detects when a new page is starting
    if line[:4] == '<doc':
        isTitle = True
    
    # After a title has been found goes back to text
    if isTitle and line == '\n':
        isTitle = False
    
    if line != '\n' and line[:4] != '<doc' and line[:5] != '</doc':
        if isTitle:
            file_output2.write(line)
        else:
            file_output1.write(line)
    
    # To track progress
    if index % 1000000 == 0:
        print(index)
    
    # To stop early
    #if index == 100:
        #break
    
file_input.close()
file_output1.close()
file_output2.close()
